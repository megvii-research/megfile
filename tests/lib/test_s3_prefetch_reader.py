import os
import time
from io import BytesIO

import pytest

from megfile.config import READER_BLOCK_SIZE
from megfile.errors import S3FileChangedError, S3InvalidRangeError
from megfile.lib.s3_prefetch_reader import S3PrefetchReader
from tests.test_s3 import s3_empty_client  # noqa: F401

BUCKET = "bucket"
KEY = "key"
CONTENT = b"block0 block1 block2 block3 block4 "


@pytest.fixture
def client(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    return s3_empty_client


def sleep_until_downloaded(reader, timeout: int = 5):
    for _ in range(timeout * 10):
        if not reader._is_downloading:
            return
        time.sleep(0.1)
    raise TimeoutError


def test_s3_prefetch_reader(client):
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"

        # size = 0
        assert reader.read(0) == b""
        assert reader._read(0) == b""

        # block 内读
        assert reader.read(2) == b"bl"
        assert reader._read(2) == b"oc"

        # 跨 block 读
        assert reader.read(4) == b"k0 b"
        assert reader.read(6) == b"lock1 "

        # 连续读多个 block, 且 size 超过剩余数据大小
        assert reader.read(21 + 1) == b"block2 block3 block4 "
        assert reader._read(1) == b""

        # 从头再读
        reader.seek(0)
        assert reader.read() == CONTENT

    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        assert reader.read() == CONTENT
        buffer = bytearray(1)
        assert reader.readinto(buffer) == 0

    with S3PrefetchReader(BUCKET, KEY, s3_client=client, block_forward=0) as reader:
        assert reader.read() == CONTENT

    with pytest.raises(ValueError):
        with S3PrefetchReader(
            BUCKET,
            KEY,
            s3_client=client,
            max_buffer_size=2,
            block_size=1,
            block_forward=5,
        ) as reader:
            pass


def test_s3_prefetch_reader_random_read(client):
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client,
        block_size=1,
        max_buffer_size=6,
    ) as reader:
        assert reader._block_capacity == 6
        assert reader._block_forward == 5
        for i in range(len(CONTENT) - 1, -1, -2):
            reader.seek(i)
            reader.read(1)
        assert reader._block_forward == 0
        assert reader._is_auto_scaling is False


def test_s3_prefetch_reader_readline(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=b"1\n2\n3\n\n4444\n5")
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=s3_empty_client, max_workers=2, block_size=3
    ) as reader:
        # within block
        assert reader.readline() == b"1\n"
        # cross block
        assert reader.readline() == b"2\n"
        # remaining is enough
        assert reader.readline() == b"3\n"
        # single line break
        assert reader.readline() == b"\n"
        # more than one block
        assert reader.readline() == b"4444\n"
        # tailing bytes
        assert reader.readline() == b"5"

        reader.seek(0)
        assert reader.readline(1) == b"1"

    with pytest.raises(IOError):
        reader.readline()


def test_s3_prefetch_reader_readline_without_line_break_at_all(client):
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=40
    ) as reader:  # block_size > content_length
        reader.read(1)
        assert reader.readline() == b"lock0 block1 block2 block3 block4 "


def test_s3_prefetch_reader_readline_tailing_block(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=b"123456")
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=s3_empty_client, max_workers=2, block_size=3
    ) as reader:
        # next block is empty
        assert reader.readline() == b"123456"


def test_s3_prefetch_reader_read_readline_mix(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=b"1\n2\n3\n4\n")
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=s3_empty_client, max_workers=2, block_size=3
    ) as reader:
        assert reader.readline() == b"1\n"
        assert reader.read(2) == b"2\n"
        assert reader.readline() == b"3\n"
        assert reader.read(1) == b"4"
        assert reader.readline() == b"\n"
        assert reader.readline() == b""


def test_s3_prefetch_reader_seek_out_of_range(s3_empty_client, mocker):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=b"1\n2\n3\n4\n")
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=s3_empty_client, max_workers=2, block_size=3
    ) as reader:
        reader.seek(-2)
        assert reader.tell() == 0
        assert reader.read(2) == b"1\n"
        reader.seek(100)
        assert reader.tell() == 8
        assert reader.read(2) == b""

        with pytest.raises(ValueError):
            reader.seek(0, "error_whence")

    with pytest.raises(IOError):
        reader.seek(0)


def test_s3_prefetch_reader_fetch(client, mocker):
    get_object_func = mocker.spy(client, "get_object")
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client,
        max_workers=2,
        block_size=7,
        max_buffer_size=4 * 7,
        block_forward=2,
    ) as reader:
        # 打开 reader, _executor 没有执行
        # get_object_func.assert_not_called() in Python 3.6+
        assert get_object_func.call_count == 1

        reader._buffer
        # 调用 _buffer 会导致 _executor 开始执行
        # 在下载了两个 block 后阻塞地等待 _downloading 事件
        sleep_until_downloaded(reader)
        get_object_func.assert_any_call(Bucket=BUCKET, Key=KEY, Range="bytes=0-6")
        get_object_func.assert_any_call(Bucket=BUCKET, Key=KEY, Range="bytes=7-13")
        get_object_func.assert_any_call(Bucket=BUCKET, Key=KEY, Range="bytes=14-20")
        assert not reader._is_downloading
        assert get_object_func.call_count == 3
        get_object_func.reset_mock()

        # 以下三次 read 不会引起 _executor 启动
        reader.read(0)
        sleep_until_downloaded(reader)
        reader.read(7)
        sleep_until_downloaded(reader)
        # get_object_func.assert_not_called() in Python 3.6+
        assert get_object_func.call_count == 0

        # 读 block0 的前 2 字节, _executor 预读 block4
        # 完成后阻塞地等待 _downloading 事件
        reader.read(2)
        sleep_until_downloaded(reader)
        get_object_func.assert_called_once_with(
            Bucket=BUCKET, Key=KEY, Range="bytes=21-27"
        )
        assert not reader._is_downloading
        get_object_func.reset_mock()

        # 读到 block1, 引发 _executor 预读 block5
        reader.read(6)
        sleep_until_downloaded(reader)
        get_object_func.assert_called_once_with(
            Bucket=BUCKET, Key=KEY, Range="bytes=28-34"
        )
        assert not reader._is_downloading
        get_object_func.reset_mock()

        # reader._futures 可满足 size, 不会引发 _executor 下载
        # 且 _executor 仍旧阻塞
        reader.read(6)
        assert get_object_func.call_count == 0
        assert not reader._is_downloading

        # reader._futures 可满足 size, 不会引发 _executor 下载
        reader.read(21)
        sleep_until_downloaded(reader)
        assert not reader._is_downloading
        assert reader._is_alive


def test_s3_prefetch_reader_close(client):
    reader = S3PrefetchReader(BUCKET, KEY, s3_client=client)
    reader.close()
    assert reader.closed

    with S3PrefetchReader(BUCKET, KEY, s3_client=client) as reader:
        pass
    assert reader.closed

    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=1, block_size=1
    ) as reader:
        # 主线程休眠, 等待 reader.fetcher 线程阻塞在 _donwloading 事件上
        sleep_until_downloaded(reader)
    assert reader.closed

    with pytest.raises(IOError):
        reader.read()


def test_s3_prefetch_reader_seek(client):
    with S3PrefetchReader(BUCKET, KEY, s3_client=client) as reader:
        reader.seek(0)

        reader.read(7)
        reader.seek(7)
        reader.seek(0, os.SEEK_CUR)
        reader.seek(-28, os.SEEK_END)

        reader.seek(-1, os.SEEK_CUR)
        reader.seek(0, os.SEEK_CUR)
        reader.seek(1, os.SEEK_CUR)

        reader.seek(-1, os.SEEK_END)
        reader.seek(0, os.SEEK_END)
        reader.seek(1, os.SEEK_END)


def test_s3_prefetch_reader_backward_seek_and_the_target_in_remains(client, mocker):
    """目标 offset 在 remains 中 重置 remains 位置"""
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client,
        max_workers=2,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=2,
    ) as reader:
        assert reader._cached_blocks == [0]

        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [2, 1, 0]

        reader.seek(3)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [3, 2, 1]

        reader.seek(1)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [2, 1, 0]


def test_s3_prefetch_reader_max_buffer_size_eq_0(client, mocker):
    """目标 offset 在 remains 中 重置 remains 位置"""
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client,
        max_workers=2,
        block_size=3,
        max_buffer_size=0,
    ) as reader:
        assert reader._cached_blocks == []

        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == []

        reader.seek(3)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == []

        reader.seek(1)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == []


def test_s3_prefetch_reader_block_forward_eq_0(client, mocker):
    """目标 offset 在 remains 中 重置 remains 位置"""
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client,
        max_workers=2,
        block_size=3,
        max_buffer_size=3 * 2,
        block_forward=0,
    ) as reader:
        assert reader._block_forward == 0
        assert reader._cached_blocks == [0]

        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [0]

        reader.seek(3)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [0, 1]

        reader.seek(9)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [1, 3]

        reader.seek(1)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [3, 0]


def test_s3_prefetch_reader_backward_block_forward_eq_1(client, mocker):
    class FakeHistory:
        read_count = 1

    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client,
        max_workers=2,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=1,
    ) as reader:
        assert reader.read(6) == b"block0"
        assert reader._cached_blocks == [0, 2, 1]

        reader._seek_history = [FakeHistory()]
        assert reader.read(7) == b" block1"
        assert reader._cached_blocks == [3, 5, 4]

        assert reader.read(7) == b" block2"
        assert reader._cached_blocks == [5, 7, 6]


def test_s3_prefetch_reader_backward_seek_and_the_target_out_of_remains(client, mocker):
    """
    目标 offset 在 buffer 外, 停止现有 future, 丢弃当前 buffer,
    以目标 offset 作为新的起点启动新的 future
    """
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client,
        max_workers=2,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=2,
    ) as reader:  # buffer 最大为 6B
        assert reader._cached_blocks == [0]

        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [2, 1, 0]

        reader.seek(10)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [5, 4, 3]

        reader.seek(0)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [2, 1, 0]


def test_s3_prefetch_reader_seek_and_the_target_in_buffer(client, mocker):
    """
    目标 offset 在 buffer 中, 丢弃目标 block 之前的全部 block,
    必要时截断目标 block 的前半部分
    """
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client,
        max_workers=3,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=2,
    ) as reader:  # buffer 最长为 9B
        assert reader._cached_blocks == [0]

        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [2, 1, 0]

        reader.seek(1)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [2, 1, 0]

        reader.seek(5)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [3, 2, 1]

        reader.seek(10)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [5, 4, 3]


def test_s3_prefetch_reader_seek_and_the_target_out_of_buffer(client, mocker):
    """
    目标 offset 在 buffer 外, 停止现有 future, 丢弃当前 buffer,
    以目标 offset 作为新的起点启动新的 future
    """
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client,
        max_workers=2,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=2,
    ) as reader:  # buffer 最大为 6B
        assert reader._cached_blocks == [0]

        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [2, 1, 0]

        reader.seek(10)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [5, 4, 3]


def test_s3_prefetch_reader_read_with_forward_seek(client):
    """向后 seek 后, 测试 read 结果的正确性"""
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.seek(2)
        assert reader.read(4) == b"ock0"
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.read(1)
        reader.seek(3)
        assert reader.read(4) == b"ck0 "
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        sleep_until_downloaded(reader)  # 休眠以确保 buffer 被填充满
        reader.seek(7)  # 目标 offset 距当前位置正好为一个 block 大小
        assert reader.read(7) == b"block1 "
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.read(1)
        reader.seek(7)
        assert reader.read(7) == b"block1 "

    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.seek(21)
        assert reader.read(7) == b"block3 "
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.seek(-1, os.SEEK_END)
        assert reader.read(2) == b" "


def test_s3_prefetch_reader_tell(client):
    with S3PrefetchReader(BUCKET, KEY, s3_client=client) as reader:
        assert reader.tell() == 0
        reader.read(0)
        assert reader.tell() == 0
        reader.read(1)
        assert reader.tell() == 1
        reader.read(6)
        assert reader.tell() == 7
        reader.read(28)
        assert reader.tell() == 35


def test_s3_prefetch_reader_tell_after_seek(client):
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.seek(2)
        assert reader.tell() == 2
        reader.seek(3)
        assert reader.tell() == 3
        reader.seek(13)
        assert reader.tell() == 13
        reader.seek(0, os.SEEK_END)
        assert reader.tell() == 35


def test_s3_prefetch_reader_readinto(client):
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client,
        max_workers=2,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=2,
    ) as reader:
        assert reader.readinto(bytearray(b"test")) == 4

    with pytest.raises(IOError):
        reader.readinto(bytearray(b"test"))


def test_s3_prefetch_reader_seek_history(client):
    with S3PrefetchReader(
        BUCKET, KEY, s3_client=client, max_buffer_size=3 * READER_BLOCK_SIZE
    ) as reader:
        reader._seek_buffer(2)
        history = reader._seek_history[0]
        assert history.seek_count == 1
        reader._seek_buffer(2)
        assert history.seek_count == 2
        history.seek_count = reader._block_capacity * 2 + 1
        reader._seek_buffer(2)
        for item in reader._seek_history:
            assert item is not history

        reader._seek_buffer(1)
        for item in reader._seek_history:
            assert item.seek_index != 2


@pytest.fixture
def client_for_get_object(s3_empty_client):
    def fake_get_object(*args, **kwargs):
        import random

        return {
            "ETag": f"test-{random.randint(0, 1000)}",
            "ContentRange": "bytes 0-1/1",
            "Body": BytesIO(b"t"),
        }

    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    s3_empty_client.get_object = fake_get_object
    return s3_empty_client


def test_s3_prefetch_reader_fetch_buffer_error(client_for_get_object):
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client_for_get_object,
        max_buffer_size=3 * READER_BLOCK_SIZE,
    ) as reader:
        with pytest.raises(S3FileChangedError):
            reader._fetch_buffer(1)


def test_empty_file(client):
    with S3PrefetchReader(BUCKET, KEY, s3_client=client) as reader:
        reader._fetch_response(start=0, end=1)

    KEY2 = "key2"
    client.put_object(Bucket=BUCKET, Key=KEY2, Body=b"")

    with pytest.raises(S3InvalidRangeError):
        with S3PrefetchReader(BUCKET, KEY2, s3_client=client) as error_reader:
            error_reader._fetch_response(start=0, end=1)


def test_s3_prefetch_reader_no_buffer(client_for_get_object):
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client_for_get_object,
        max_buffer_size=0,
    ) as reader:
        assert reader._block_capacity == 0
        assert reader._block_forward == 0
        assert list(reader._futures.keys()) == []
        assert reader._content_size > 0
        assert reader._content_etag is not None

        reader.read()

        assert list(reader._futures.keys()) == []


def test_s3_prefetch_reader_size(client_for_get_object, mocker):
    def fake_fetch_response(*args, **kwargs):
        return {
            "ContentLength": 123,
            "Body": None,
            "ETag": "test",
        }

    mocker.patch(
        "megfile.lib.s3_prefetch_reader.S3PrefetchReader._fetch_response",
        fake_fetch_response,
    )
    with S3PrefetchReader(
        BUCKET,
        KEY,
        s3_client=client_for_get_object,
    ) as reader:
        assert reader._content_size == 123

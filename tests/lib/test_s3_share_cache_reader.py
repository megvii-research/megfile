import os
import time

import pytest

from megfile.lib import s3_share_cache_reader
from megfile.lib.s3_share_cache_reader import (
    S3ShareCacheReader,
)
from megfile.utils import thread_local
from tests.test_s3 import s3_empty_client  # noqa: F401

BUCKET = "bucket"
KEY = "key"


def setup_function():
    s3_share_cache_reader.max_buffer_cache_size = 128 * 2**20
    if "S3ShareCacheReader.lru" in thread_local:
        del thread_local["S3ShareCacheReader.lru"]


@pytest.fixture
def client(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    content = b"block0 block1 block2 block3 block4 "
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=content)
    return s3_empty_client


def sleep_until_downloaded(reader, timeout: int = 5):
    for _ in range(timeout * 10):
        if not reader._is_downloading:
            return
        time.sleep(0.1)
    raise TimeoutError


def test_s3_share_cache_reader(client):
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        # size = 0
        assert reader.read(0) == b""

        # block 内读
        assert reader.read(2) == b"bl"

        # 跨 block 读
        assert reader.read(6) == b"ock0 b"

        assert reader.read(6) == b"lock1 "

        # 连续读多个 block, 且 size 超过剩余数据大小
        assert reader.read(21 + 1) == b"block2 block3 block4 "

        # 从头再读
        reader.seek(0)
        assert reader.read() == b"block0 block1 block2 block3 block4 "

    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        assert reader.read() == b"block0 block1 block2 block3 block4 "


def test_s3_share_cache_reader_readline(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=b"1\n2\n3\n\n4444\n5")
    with S3ShareCacheReader(
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


def test_s3_share_cache_reader_readline_without_line_break_at_all(client):
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=40
    ) as reader:  # block_size > content_length
        reader.read(1)
        assert reader.readline() == b"lock0 block1 block2 block3 block4 "


def test_s3_share_cache_reader_readline_tailing_block(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=b"123456")
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=s3_empty_client, max_workers=2, block_size=3
    ) as reader:
        # next block is empty
        assert reader.readline() == b"123456"


def test_s3_share_cache_reader_read_readline_mix(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=b"1\n2\n3\n4\n")
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=s3_empty_client, max_workers=2, block_size=3
    ) as reader:
        assert reader.readline() == b"1\n"
        assert reader.read(2) == b"2\n"
        assert reader.readline() == b"3\n"
        assert reader.read(1) == b"4"
        assert reader.readline() == b"\n"
        assert reader.readline() == b""


def test_s3_share_cache_reader_read_readline_mix_multiple_reader(s3_empty_client):
    KEY2 = "key2"
    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=b"1\n2\n3\n4\n")
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY2, Body=b"5\n6\n7\n8\n")

    reader = S3ShareCacheReader(
        BUCKET, KEY, s3_client=s3_empty_client, max_workers=2, block_size=3
    )
    reader2 = S3ShareCacheReader(
        BUCKET, KEY2, s3_client=s3_empty_client, max_workers=2, block_size=3
    )
    assert reader.readline() == b"1\n"
    assert reader.read(2) == b"2\n"
    assert reader.readline() == b"3\n"
    assert reader.read(1) == b"4"
    assert reader.readline() == b"\n"
    assert reader.readline() == b""

    assert reader2.readline() == b"5\n"
    assert reader2.read(2) == b"6\n"
    assert reader2.readline() == b"7\n"
    assert reader2.read(1) == b"8"
    assert reader2.readline() == b"\n"
    assert reader2.readline() == b""
    reader.close()
    reader2.close()


def test_s3_share_cache_reader_fetch(client, mocker):
    get_object_func = mocker.spy(client, "get_object")
    s3_share_cache_reader.max_buffer_cache_size = 4 * 7
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7, block_forward=2
    ) as reader:
        # 打开 reader, _executor 已经开始执行
        # 在下载了两个 block 后阻塞地等待 _downloading 事件
        reader.read(0)
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

        # 读 block0 的前 2 字节, _executor 预读 block3,
        # 完成后阻塞地等待 _downloading 事件
        reader.read(2)
        sleep_until_downloaded(reader)
        get_object_func.assert_called_once_with(
            Bucket=BUCKET, Key=KEY, Range="bytes=21-27"
        )
        assert not reader._is_downloading
        get_object_func.reset_mock()

        # 读到 block1, 引发 _executor 开始预读 block4
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
        # get_object_func.assert_not_called() in Python 3.6+
        assert get_object_func.call_count == 0
        assert not reader._is_downloading

        # reader._futures 可满足 size, 不会引发 _executor 下载
        reader.read(21)
        sleep_until_downloaded(reader)
        assert not reader._is_downloading
        assert reader._is_alive


def test_s3_share_cache_reader_close(client):
    reader = S3ShareCacheReader(BUCKET, KEY, s3_client=client)
    reader.close()
    assert reader.closed

    with S3ShareCacheReader(BUCKET, KEY, s3_client=client) as reader:
        pass
    assert reader.closed

    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=1, block_size=1
    ) as reader:
        # 主线程休眠, 等待 reader.fetcher 线程阻塞在 _donwloading 事件上
        sleep_until_downloaded(reader)
    assert reader.closed


def test_s3_share_cache_reader_seek(client):
    with S3ShareCacheReader(BUCKET, KEY, s3_client=client) as reader:
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


def test_s3_share_cache_reader_backward_seek_and_the_target_in_remains(client, mocker):
    """目标 offset 在 remains 中 重置 remains 位置"""
    s3_share_cache_reader.DEFAULT_BLOCK_CAPACITY = 3
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=3, block_forward=2
    ) as reader:
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 2),
            ("s3://bucket/key", 1),
            ("s3://bucket/key", 0),
        ]

        reader.seek(3)
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 3),
            ("s3://bucket/key", 2),
            ("s3://bucket/key", 1),
        ]

        reader.seek(1)
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 2),
            ("s3://bucket/key", 1),
            ("s3://bucket/key", 0),
        ]


def test_s3_share_cache_reader_backward_seek_and_the_target_out_of_remains(
    client, mocker
):
    """
    目标 offset 在 buffer 外, 停止现有 future, 丢弃当前 buffer,
    以目标 offset 作为新的起点启动新的 future
    """
    s3_share_cache_reader.DEFAULT_BLOCK_CAPACITY = 3
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=3, block_forward=2
    ) as reader:  # buffer 最大为 6B
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 2),
            ("s3://bucket/key", 1),
            ("s3://bucket/key", 0),
        ]

        reader.seek(10)
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 5),
            ("s3://bucket/key", 4),
            ("s3://bucket/key", 3),
        ]

        reader.seek(0)
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 2),
            ("s3://bucket/key", 1),
            ("s3://bucket/key", 0),
        ]


def test_s3_share_cache_reader_seek_and_the_target_in_buffer(client, mocker):
    """
    目标 offset 在 buffer 中, 丢弃目标 block 之前的全部 block,
    必要时截断目标 block 的前半部分
    """
    s3_share_cache_reader.DEFAULT_BLOCK_CAPACITY = 3
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=3, block_size=3, block_forward=2
    ) as reader:  # buffer 最长为 9B
        sleep_until_downloaded(reader)
        reader.read(0)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 2),
            ("s3://bucket/key", 1),
            ("s3://bucket/key", 0),
        ]

        reader.seek(1)
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 2),
            ("s3://bucket/key", 1),
            ("s3://bucket/key", 0),
        ]

        reader.seek(5)
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 3),
            ("s3://bucket/key", 2),
            ("s3://bucket/key", 1),
        ]

        reader.seek(10)
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 5),
            ("s3://bucket/key", 4),
            ("s3://bucket/key", 3),
        ]


def test_s3_share_cache_reader_seek_and_the_target_out_of_buffer(client, mocker):
    """
    目标 offset 在 buffer 外, 停止现有 future, 丢弃当前 buffer,
    以目标 offset 作为新的起点启动新的 future
    """
    s3_share_cache_reader.DEFAULT_BLOCK_CAPACITY = 3
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=3, block_forward=2
    ) as reader:  # buffer 最大为 6B
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 2),
            ("s3://bucket/key", 1),
            ("s3://bucket/key", 0),
        ]

        reader.seek(10)
        reader.read(0)
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [
            ("s3://bucket/key", 5),
            ("s3://bucket/key", 4),
            ("s3://bucket/key", 3),
        ]


def test_s3_share_cache_reader_read_with_forward_seek(client):
    """向后 seek 后, 测试 read 结果的正确性"""
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.seek(2)
        assert reader.read(4) == b"ock0"
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.read(1)
        reader.seek(3)
        assert reader.read(4) == b"ck0 "
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        sleep_until_downloaded(reader)  # 休眠以确保 buffer 被填充满
        reader.seek(7)  # 目标 offset 距当前位置正好为一个 block 大小
        assert reader.read(7) == b"block1 "
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.read(1)
        reader.seek(7)
        assert reader.read(7) == b"block1 "

    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.seek(21)
        assert reader.read(7) == b"block3 "
    with S3ShareCacheReader(
        BUCKET, KEY, s3_client=client, max_workers=2, block_size=7
    ) as reader:
        reader.seek(-1, os.SEEK_END)
        assert reader.read(2) == b" "


def test_s3_share_cache_reader_tell(client):
    with S3ShareCacheReader(BUCKET, KEY, s3_client=client) as reader:
        assert reader.tell() == 0
        reader.read(0)
        assert reader.tell() == 0
        reader.read(1)
        assert reader.tell() == 1
        reader.read(6)
        assert reader.tell() == 7
        reader.read(28)
        assert reader.tell() == 35


def test_s3_share_cache_reader_tell_after_seek(client):
    with S3ShareCacheReader(
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


def test_s3_share_cache_reader_read_large_amount_of_files(s3_empty_client):
    content = b" " * 100
    s3_empty_client.create_bucket(Bucket=BUCKET)

    readers = []
    for index in range(128):
        key = "%s-%d" % (KEY, index)
        s3_empty_client.put_object(Bucket=BUCKET, Key=key, Body=content)

        reader = S3ShareCacheReader(
            BUCKET, key, s3_client=s3_empty_client, block_size=16
        )
        readers.append(reader)

    for _ in range(10):
        for reader in readers:
            assert reader.read(10) == b" " * 10

    for reader in readers:
        assert reader.read() == b""

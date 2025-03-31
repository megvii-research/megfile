import logging
import os
import time
from io import BytesIO

import pytest
import requests

from megfile.config import READER_BLOCK_SIZE
from megfile.errors import UnsupportedError
from megfile.lib.http_prefetch_reader import HttpPrefetchReader

URL = "http://test"
CONTENT = b"block0 block1 block2 block3 block4 "
CONTENT_SIZE = len(CONTENT)


class FakeResponse:
    status_code = 0

    def __init__(self, content=CONTENT) -> None:
        self._content = content
        pass

    @property
    def raw(self):
        return BytesIO(self._content)

    @property
    def content(self):
        return self._content

    @property
    def headers(self):
        return {
            "Content-Length": len(self._content),
            "Content-Type": "text/html",
            "Last-Modified": "Wed, 24 Nov 2021 07:18:41 GMT",
            "Accept-Ranges": "bytes",
        }

    @property
    def cookies(self):
        return {}

    def raise_for_status(self):
        if self.status_code // 100 == 2:
            return
        error = requests.exceptions.HTTPError()
        error.response = self
        raise error

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()


class FakeResponse200(FakeResponse):
    status_code = 200


class FakeResponse400(FakeResponse):
    status_code = 400


def _fake_get(*args, headers=None, **kwargs):
    if headers and headers.get("Range"):
        start, end = list(map(int, headers["Range"][6:].split("-")))
        return FakeResponse200(CONTENT[start : end + 1])
    return FakeResponse200()


@pytest.fixture
def http_patch(mocker):
    requests_get_func = mocker.patch(
        "megfile.http_path.requests.get", side_effect=_fake_get
    )
    return requests_get_func


def sleep_until_downloaded(reader, timeout: int = 5):
    for _ in range(timeout * 10):
        if not reader._is_downloading:
            return
        time.sleep(0.1)
    raise TimeoutError


def test_http_prefetch_reader(http_patch):
    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=2, block_size=7
    ) as reader:
        assert reader.name == URL
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

    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=2, block_size=7
    ) as reader:
        assert reader.read() == CONTENT


def test_http_prefetch_reader_readline(mocker):
    content = b"1\n2\n3\n\n4444\n5"
    mocker.patch(
        "megfile.http_path.requests.get", return_value=FakeResponse200(content)
    )
    with HttpPrefetchReader(URL, max_workers=2, block_size=3) as reader:
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


def test_http_prefetch_reader_readline_without_line_break_at_all(http_patch):
    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=2, block_size=40
    ) as reader:  # block_size > content_length
        reader.read(1)
        assert reader.readline() == b"lock0 block1 block2 block3 block4 "


def test_http_prefetch_reader_readline_tailing_block(mocker):
    mocker.patch(
        "megfile.http_path.requests.get", return_value=FakeResponse200(b"123456")
    )
    with HttpPrefetchReader(URL, content_size=6, max_workers=2, block_size=3) as reader:
        # next block is empty
        assert reader.readline() == b"123456"


def test_http_prefetch_reader_read_readline_mix(mocker):
    content = b"1\n2\n3\n4\n"
    mocker.patch(
        "megfile.http_path.requests.get", return_value=FakeResponse200(content)
    )
    with HttpPrefetchReader(
        URL, content_size=len(content), max_workers=2, block_size=3
    ) as reader:
        assert reader.readline() == b"1\n"
        assert reader.read(2) == b"2\n"
        assert reader.readline() == b"3\n"
        assert reader.read(1) == b"4"
        assert reader.readline() == b"\n"
        assert reader.readline() == b""


def test_http_prefetch_reader_seek_out_of_range(mocker):
    content = b"1\n2\n3\n4\n"
    mocker.patch(
        "megfile.http_path.requests.get", return_value=FakeResponse200(b"1\n2\n3\n4\n")
    )
    with HttpPrefetchReader(
        URL, content_size=len(content), max_workers=2, block_size=3
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


def test_http_prefetch_reader_close(http_patch):
    reader = HttpPrefetchReader(URL, content_size=CONTENT_SIZE)
    reader.close()
    assert reader.closed

    with HttpPrefetchReader(URL, content_size=CONTENT_SIZE) as reader:
        pass
    assert reader.closed

    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=1, block_size=1
    ) as reader:
        # 主线程休眠, 等待 reader.fetcher 线程阻塞在 _donwloading 事件上
        sleep_until_downloaded(reader)
    assert reader.closed

    with pytest.raises(IOError):
        reader.read()


def test_http_prefetch_reader_seek(http_patch):
    with HttpPrefetchReader(URL, content_size=CONTENT_SIZE) as reader:
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


def test_http_prefetch_reader_backward_seek_and_the_target_in_remains(
    http_patch, mocker
):
    """目标 offset 在 remains 中 重置 remains 位置"""
    with HttpPrefetchReader(
        URL,
        content_size=CONTENT_SIZE,
        max_workers=2,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=2,
    ) as reader:
        assert reader._cached_blocks == []

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


def test_http_prefetch_reader_max_buffer_size_eq_0(http_patch, mocker):
    """目标 offset 在 remains 中 重置 remains 位置"""
    with HttpPrefetchReader(
        URL,
        content_size=CONTENT_SIZE,
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


def test_http_prefetch_reader_block_forward_eq_0(http_patch, mocker):
    """目标 offset 在 remains 中 重置 remains 位置"""
    with HttpPrefetchReader(
        URL,
        content_size=CONTENT_SIZE,
        max_workers=2,
        block_size=3,
        max_buffer_size=3,
        block_forward=0,
    ) as reader:
        assert reader._block_forward == 0
        assert reader._cached_blocks == []

        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [0]

        reader.seek(3)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [1]

        reader.seek(9)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [3]

        reader.seek(1)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [0]


def test_http_prefetch_reader_backward_block_forward_eq_1(http_patch, mocker):
    class FakeHistory:
        read_count = 1

    with HttpPrefetchReader(
        URL,
        content_size=CONTENT_SIZE,
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


def test_http_prefetch_reader_backward_seek_and_the_target_out_of_remains(http_patch):
    """
    目标 offset 在 buffer 外, 停止现有 future, 丢弃当前 buffer,
    以目标 offset 作为新的起点启动新的 future
    """
    with HttpPrefetchReader(
        URL,
        content_size=CONTENT_SIZE,
        max_workers=2,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=2,
    ) as reader:  # buffer 最大为 6B
        assert reader._cached_blocks == []

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


def test_http_prefetch_reader_seek_and_the_target_in_buffer(http_patch, mocker):
    """
    目标 offset 在 buffer 中, 丢弃目标 block 之前的全部 block,
    必要时截断目标 block 的前半部分
    """
    with HttpPrefetchReader(
        URL,
        content_size=CONTENT_SIZE,
        max_workers=3,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=2,
    ) as reader:  # buffer 最长为 9B
        assert reader._cached_blocks == []

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


def test_http_prefetch_reader_seek_and_the_target_out_of_buffer(http_patch):
    """
    目标 offset 在 buffer 外, 停止现有 future, 丢弃当前 buffer,
    以目标 offset 作为新的起点启动新的 future
    """
    with HttpPrefetchReader(
        URL,
        content_size=CONTENT_SIZE,
        max_workers=2,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=2,
    ) as reader:  # buffer 最大为 6B
        assert reader._cached_blocks == []

        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [2, 1, 0]

        reader.seek(10)
        reader._buffer
        sleep_until_downloaded(reader)
        assert reader._cached_blocks == [5, 4, 3]


def test_http_prefetch_reader_read_with_forward_seek(http_patch):
    """向后 seek 后, 测试 read 结果的正确性"""
    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=2, block_size=7
    ) as reader:
        reader.seek(2)
        assert reader.read(4) == b"ock0"
    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=2, block_size=7
    ) as reader:
        reader.read(1)
        reader.seek(3)
        assert reader.read(4) == b"ck0 "
    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=2, block_size=7
    ) as reader:
        sleep_until_downloaded(reader)  # 休眠以确保 buffer 被填充满
        reader.seek(7)  # 目标 offset 距当前位置正好为一个 block 大小
        assert reader.read(7) == b"block1 "
    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=2, block_size=7
    ) as reader:
        reader.read(1)
        reader.seek(7)
        assert reader.read(7) == b"block1 "

    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=2, block_size=7
    ) as reader:
        reader.seek(21)
        assert reader.read(7) == b"block3 "
    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=2, block_size=7
    ) as reader:
        reader.seek(-1, os.SEEK_END)
        assert reader.read(2) == b" "


def test_http_prefetch_reader_tell(http_patch):
    with HttpPrefetchReader(URL, content_size=CONTENT_SIZE) as reader:
        assert reader.tell() == 0
        reader.read(0)
        assert reader.tell() == 0
        reader.read(1)
        assert reader.tell() == 1
        reader.read(6)
        assert reader.tell() == 7
        reader.read(28)
        assert reader.tell() == 35


def test_http_prefetch_reader_tell_after_seek(http_patch):
    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_workers=2, block_size=7
    ) as reader:
        reader.seek(2)
        assert reader.tell() == 2
        reader.seek(3)
        assert reader.tell() == 3
        reader.seek(13)
        assert reader.tell() == 13
        reader.seek(0, os.SEEK_END)
        assert reader.tell() == 35


def test_http_prefetch_reader_readinto(http_patch):
    with HttpPrefetchReader(
        URL,
        content_size=CONTENT_SIZE,
        max_workers=2,
        block_size=3,
        max_buffer_size=3 * 3,
        block_forward=2,
    ) as reader:
        assert reader.readinto(bytearray(b"test")) == 4

    with pytest.raises(IOError):
        reader.readinto(bytearray(b"test"))


def test_http_prefetch_reader_seek_history(http_patch):
    with HttpPrefetchReader(
        URL, content_size=CONTENT_SIZE, max_buffer_size=3 * READER_BLOCK_SIZE
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


def test_http_prefetch_reader_no_buffer(http_patch):
    with HttpPrefetchReader(
        URL,
        content_size=CONTENT_SIZE,
        max_buffer_size=0,
    ) as reader:
        assert reader._block_capacity == 0
        assert reader._block_forward == 0
        assert list(reader._futures.keys()) == []
        assert reader._content_size == CONTENT_SIZE

        reader.read()

        assert list(reader._futures.keys()) == []


def test_http_prefetch_reader_headers(mocker):
    class FakeResponse200WithoutAcceptRange(FakeResponse200):
        @property
        def headers(self):
            headers = super().headers
            headers.pop("Accept-Ranges", None)
            return headers

    mocker.patch(
        "megfile.http_path.requests.get",
        return_value=FakeResponse200WithoutAcceptRange(),
    )

    with pytest.raises(UnsupportedError):
        HttpPrefetchReader(
            URL,
        )


def test_http_prefetch_reader_retry(mocker, caplog):
    with caplog.at_level(logging.INFO, logger="megfile"):

        class FakeResponse200Retry(FakeResponse200):
            def __init__(self, content=CONTENT) -> None:
                super().__init__(content)
                self.times = 0

            @property
            def content(self):
                if self.times < 1:
                    self.times += 1
                    return b""
                return super().content

        fake_response = FakeResponse200Retry()

        mocker.patch(
            "megfile.http_path.requests.get",
            return_value=fake_response,
        )

        with HttpPrefetchReader(
            URL,
            max_retries=2,
        ) as f:
            f.read()
        assert "The downloaded content is incomplete" in caplog.text

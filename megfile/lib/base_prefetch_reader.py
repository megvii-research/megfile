import os
from abc import ABC, abstractmethod
from collections import OrderedDict
from concurrent.futures import Future, ThreadPoolExecutor
from io import BytesIO
from logging import getLogger as get_logger
from math import ceil
from typing import Optional

from megfile.config import (
    DEFAULT_MAX_RETRY_TIMES,
    GLOBAL_MAX_WORKERS,
    NEWLINE,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
)
from megfile.interfaces import Readable, Seekable
from megfile.utils import ProcessLocal, process_local

_logger = get_logger(__name__)


class SeekRecord:
    def __init__(self, seek_index: int):
        self.seek_index = seek_index
        self.seek_count = 0
        self.read_count = 0


class BasePrefetchReader(Readable[bytes], Seekable, ABC):
    def __init__(
        self,
        *,
        block_size: int = READER_BLOCK_SIZE,
        max_buffer_size: int = READER_MAX_BUFFER_SIZE,
        block_forward: Optional[int] = None,
        max_retries: int = DEFAULT_MAX_RETRY_TIMES,
        max_workers: Optional[int] = None,
        **kwargs,
    ):
        self._is_global_executor = False
        if max_workers is None:
            self._executor = process_local(
                f"{self.__class__.__name__}.executor",
                ThreadPoolExecutor,
                max_workers=GLOBAL_MAX_WORKERS,
            )
            self._is_global_executor = True
        else:
            self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._process_local = ProcessLocal()

        if max_buffer_size == 0:
            block_capacity = block_forward = 0
        else:
            block_capacity = max(max_buffer_size // block_size, 1)

        self._is_auto_scaling = False
        if block_forward is None:
            block_forward = max(block_capacity - 1, 0)
            self._is_auto_scaling = block_forward > 0

        if 0 < block_capacity <= block_forward:
            raise ValueError(
                "max_buffer_size should greater than block_forward * block_size, "
                "got: max_buffer_size=%s, block_size=%s, block_forward=%s"
                % (max_buffer_size, block_size, block_forward)
            )

        # user maybe put block_size with 'numpy.uint64' type
        block_size = int(block_size)

        self._max_retries = max_retries
        self._block_size = block_size
        self._block_capacity = block_capacity  # Max number of blocks

        # Number of blocks every prefetch, which should be smaller than block_capacity
        self._block_forward = block_forward

        self._content_size = self._get_content_size()
        self._block_stop = ceil(self._content_size / block_size)

        self._offset = 0
        self._cached_buffer = None
        self._block_index = None  # Current block index
        self._seek_history = []

        self._seek_buffer(0)

        _logger.debug("open file: %r, mode: %s" % (self.name, self.mode))

    @abstractmethod
    def _get_content_size(self):
        pass  # pragma: no cover

    @property
    def _futures(self) -> "LRUCacheFutureManager":
        return self._process_local("futures", self._get_futures)

    def _get_futures(self):
        return LRUCacheFutureManager()

    @property
    @abstractmethod
    def name(self) -> str:
        pass  # pragma: no cover

    @property
    def mode(self) -> str:
        return "rb"

    def tell(self) -> int:
        return self._offset

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Change stream position.

        Seek to byte offset pos relative to position indicated by whence:

            0  Start of stream (the default).  pos should be >= 0;
            1  Current position - pos may be negative;
            2  End of stream - pos usually negative.

        Returns the new absolute position.
        """
        offset = int(offset)  # user maybe put offset with 'numpy.uint64' type
        if self.closed:
            raise IOError("file already closed: %r" % self.name)
        if whence == os.SEEK_CUR:
            target_offset = self._offset + offset
        elif whence == os.SEEK_END:
            target_offset = self._content_size + offset
        elif whence == os.SEEK_SET:
            target_offset = offset
        else:
            raise ValueError("invalid whence: %r" % whence)

        if target_offset == self._offset:
            return target_offset

        self._offset = max(min(target_offset, self._content_size), 0)
        block_index = self._offset // self._block_size
        block_offset = self._offset % self._block_size
        self._seek_buffer(block_index, block_offset)
        return self._offset

    def read(self, size: Optional[int] = None) -> bytes:
        """Read at most size bytes, returned as a bytes object.

        If the size argument is negative, read until EOF is reached.
        Return an empty bytes object at EOF.
        """
        if self.closed:
            raise IOError("file already closed: %r" % self.name)

        if self._offset >= self._content_size:
            return b""

        if size is None or size < 0:
            size = self._content_size - self._offset
        else:
            size = min(size, self._content_size - self._offset)

        buffer = bytearray(size)
        self.readinto(buffer)
        return bytes(buffer)

    def readline(self, size: Optional[int] = None) -> bytes:
        """Next line from the file, as a bytes object.

        Retain newline.  A non-negative size argument limits the maximum
        number of bytes to return (an incomplete line may be returned then).
        If the size argument is negative, read until EOF is reached.
        Return an empty bytes object at EOF.
        """
        if self.closed:
            raise IOError("file already closed: %r" % self.name)

        if len(self._seek_history) > 0:
            self._seek_history[-1].read_count += 1
        if self._offset >= self._content_size:
            return b""

        if size is None or size < 0:
            size = self._content_size - self._offset
        else:
            size = min(size, self._content_size - self._offset)

        data = self._buffer.readline(size)
        if len(data) == size or (len(data) > 0 and data[-1] == NEWLINE):
            self._offset += len(data)
            return data

        buffer = BytesIO()
        buffer.write(data)
        while True:
            remain_size = size - buffer.tell()
            data = self._next_buffer.readline(remain_size)
            buffer.write(data)
            if buffer.tell() == size or data[-1] == NEWLINE:
                break

        self._offset += buffer.tell()
        return buffer.getvalue()

    def _read(self, size: int) -> bytes:
        if size == 0 or self._offset >= self._content_size:
            return b""

        data = self._fetch_response(start=self._offset, end=self._offset + size - 1)[
            "Body"
        ].read()
        self.seek(size, os.SEEK_CUR)
        return data

    def readinto(self, buffer: bytearray) -> int:
        """Read bytes into buffer.

        Returns number of bytes read (0 for EOF), or None if the object
        is set not to block and has no data to read.
        """
        if self.closed:
            raise IOError("file already closed: %r" % self.name)

        if len(self._seek_history) > 0:
            self._seek_history[-1].read_count += 1

        if self._offset >= self._content_size:
            return 0

        size = len(buffer)
        size = min(size, self._content_size - self._offset)

        if self._block_capacity == 0:
            buffer[:size] = self._read(size)
            return size

        if self._block_forward == 0:
            block_index = self._offset // self._block_size
            if block_index not in self._futures:
                buffer[:size] = self._read(size)
                return size

        data = self._buffer.read(size)
        buffer[: len(data)] = data
        if len(data) == size:
            self._offset += len(data)
            return size

        offset = len(data)
        while offset < size:
            remain_size = size - offset
            data = self._next_buffer.read(remain_size)
            buffer[offset : offset + len(data)] = data
            offset += len(data)

        self._offset += offset
        return size

    @property
    def _is_alive(self):
        return not self._executor._shutdown

    @property
    def _is_downloading(self):
        return not self._futures.finished

    @property
    def _cached_blocks(self):
        return list(self._futures.keys())

    @property
    def _buffer(self) -> BytesIO:
        if self._block_capacity == 0:
            buffer = self._fetch_buffer(index=self._block_index)
            buffer.seek(self._cached_offset)
            self._cached_offset = None
            return buffer

        if self._cached_offset is not None:
            if self._block_forward > 0:  # pyre-ignore[58]
                start = self._block_index
                stop = min(start + self._block_forward, self._block_stop)

                # reversed(range(start, stop))
                for index in range(stop, start - 1, -1):
                    self._submit_future(index)
            else:
                self._submit_future(self._block_index)
            self._cleanup_futures()

            self._cached_buffer = self._fetch_future_result(self._block_index)
            self._cached_buffer.seek(self._cached_offset)
            self._cached_offset = None

        return self._cached_buffer

    @property
    def _next_buffer(self) -> BytesIO:
        # Get next buffer by this function when finished reading current buffer
        # (self._buffer)
        #
        # Make sure that _buffer is used before using _next_buffer(), or will make
        # _cached_offset invalid
        self._block_index += 1
        self._cached_offset = 0
        return self._buffer

    def _seek_buffer(self, index: int, offset: int = 0):
        # The corresponding block is probably not downloaded when seek to a new position
        # So record the offset first, set it when it is accessed
        if self._is_auto_scaling:
            history = []
            for item in self._seek_history:
                if item.seek_count > self._block_capacity * 2:
                    # seek interval is bigger than self._block_capacity * 2, drop it
                    # from self._seek_history
                    continue
                if index - 1 < item.seek_index < index + 2:
                    continue
                item.seek_count += 1
                history.append(item)
            history.append(SeekRecord(index))
            self._seek_history = history
            self._block_forward = min(
                max(self._block_capacity // len(self._seek_history), 0),
                self._block_capacity - 1,
            )
            if self._block_forward == 0:
                self._is_auto_scaling = False
                self._seek_history = []

        self._cached_offset = offset
        self._block_index = index

    @abstractmethod
    def _fetch_response(
        self, start: Optional[int] = None, end: Optional[int] = None
    ) -> dict:
        pass  # pragma: no cover

    def _fetch_buffer(self, index: int) -> BytesIO:
        start, end = index * self._block_size, (index + 1) * self._block_size - 1
        response = self._fetch_response(start=start, end=end)
        return response["Body"]

    def _submit_future(self, index: int):
        if index < 0 or index >= self._block_stop:
            return
        self._futures.submit(self._executor, index, self._fetch_buffer, index)

    def _insert_futures(self, index: int, future: Future):
        self._futures[index] = future

    def _fetch_future_result(self, index: int):
        return self._futures.result(index)

    def _cleanup_futures(self):
        self._futures.cleanup(self._block_capacity)

    def _close(self):
        _logger.debug("close file: %r" % self.name)

        if not self._is_global_executor:
            self._executor.shutdown()
        self._futures.clear()  # clean memory


class LRUCacheFutureManager(OrderedDict):
    def __init__(self):
        super().__init__()

    def submit(self, executor, key, *args, **kwargs):
        if key in self:
            self.move_to_end(key, last=True)
            return
        self[key] = executor.submit(*args, **kwargs)

    @property
    def finished(self):
        return all(future.done() for future in self.values())

    def result(self, key):
        self.move_to_end(key, last=True)
        return self[key].result()

    def cleanup(self, block_capacity: int):
        while len(self) > block_capacity:
            _, future = self.popitem(last=False)
            if not future.done():
                future.cancel()

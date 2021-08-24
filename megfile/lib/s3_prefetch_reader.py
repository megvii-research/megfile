import os
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from logging import getLogger as get_logger
from math import ceil
from statistics import mean
from typing import Optional

from megfile.errors import S3FileChangedError, patch_method, raise_s3_error, s3_should_retry
from megfile.interfaces import Readable, Seekable
from megfile.utils import get_human_size, process_local

DEFAULT_BLOCK_SIZE = 8 * 2**20  # 8MB
DEFAULT_BLOCK_CAPACITY = 16
GLOBAL_MAX_WORKERS = 128

BACKOFF_INITIAL = 64 * 2**20  # 64MB
BACKOFF_FACTOR = 4

NEWLINE = ord('\n')

_logger = get_logger(__name__)


class SeekRecord:

    def __init__(self, seek_index: int):
        self.seek_index = seek_index
        self.seek_count = 0
        self.read_count = 0


class S3PrefetchReader(Readable, Seekable):
    '''
    Reader to fast read the s3 content. This will divide the file content into equal parts of block_size size, and will use LRU to cache at most block_capacity blocks in memory.
    open(), seek() and read() will trigger prefetch read. The prefetch will cached block_forward blocks of data from offset position (the position after reading if the called function is read).
    '''

    def __init__(
            self,
            bucket: str,
            key: str,
            *,
            s3_client,
            block_size: int = DEFAULT_BLOCK_SIZE,
            block_capacity: int = DEFAULT_BLOCK_CAPACITY,
            block_forward: Optional[int] = None,
            max_retries: int = 10,
            max_workers: Optional[int] = None):

        block_forward = self._get_block_forward(block_capacity, block_forward)

        assert block_capacity > block_forward, 'block_capacity should greater than block_forward, got: block_capacity=%s, block_forward=%s' % (
            block_capacity, block_forward)

        self._bucket = bucket
        self._key = key
        self._client = s3_client
        self._max_retries = max_retries

        with raise_s3_error(self.name):
            data = self._client.head_object(Bucket=self._bucket, Key=self._key)
            self._content_size = data['ContentLength']
            self._content_etag = data['ETag']
            self._content_info = data

        self._block_size = block_size
        self._block_capacity = block_capacity  # Max number of blocks
        self._block_forward = block_forward  # Number of blocks every prefetch, which should be smaller than block_capacity
        self._block_stop = ceil(self._content_size / block_size)

        self.__offset = 0
        self._backoff_size = BACKOFF_INITIAL
        self._block_index = None  # Current block index
        self._seek_history = []

        self._futures = self._get_futures()
        self._is_global_executor = False
        if max_workers is None:
            self._executor = process_local(
                'S3PrefetchReader.executor',
                ThreadPoolExecutor,
                max_workers=GLOBAL_MAX_WORKERS)
            self._is_global_executor = True
        else:
            self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._seek_buffer(0)

        _logger.debug('open file: %r, mode: %s' % (self.name, self.mode))

    def _get_block_forward(
            self, block_capacity: int, block_forward: Optional[int]):
        self._is_auto_scaling = block_forward is None
        if block_forward is None:
            block_forward = block_capacity - 1
        return block_forward

    def _get_futures(self):
        return LRUCacheFutureManager()

    @property
    def name(self) -> str:
        return 's3://%s/%s' % (self._bucket, self._key)

    @property
    def mode(self) -> str:
        return 'rb'

    def tell(self) -> int:
        return self._offset

    @property
    def _offset(self) -> int:
        return self.__offset

    @_offset.setter
    def _offset(self, value: int):
        if value > self._backoff_size:
            _logger.debug(
                'reading file: %r, current offset / total size: %s / %s' % (
                    self.name, get_human_size(value),
                    get_human_size(self._content_size)))
        while value > self._backoff_size:
            self._backoff_size *= BACKOFF_FACTOR
        self.__offset = value

    def seek(self, cookie: int, whence: int = os.SEEK_SET) -> int:
        '''
        If target offset is longer than file size, set offset to content_size
        '''
        if self.closed:
            raise IOError('file already closed: %r' % self.name)

        if whence == os.SEEK_CUR:
            target_offset = self._offset + cookie
        elif whence == os.SEEK_END:
            target_offset = self._content_size + cookie
        elif whence == os.SEEK_SET:
            target_offset = cookie
        else:
            raise ValueError('invalid whence: %r' % whence)

        if target_offset == self._offset:
            return target_offset

        self._offset = max(min(target_offset, self._content_size), 0)
        block_index = self._offset // self._block_size
        block_offset = self._offset % self._block_size
        self._seek_buffer(block_index, block_offset)
        return self._offset

    def read(self, size: Optional[int] = None) -> bytes:
        '''Read at most size data, size is at least 0

        .. note ::

            This method is blocked

            b'' will be returned when the read to the end of file
        '''
        if self.closed:
            raise IOError('file already closed: %r' % self.name)

        if len(self._seek_history) > 0:
            self._seek_history[-1].read_count += 1

        if self._offset >= self._content_size:
            return b''

        if size is None:
            size = self._content_size - self._offset
        else:
            assert size >= 0, 'size should greater than 0, got: %r' % size
            size = min(size, self._content_size - self._offset)

        if self._block_forward == 1:
            block_index = self._offset // self._block_size
            mean_read_count = mean(
                item.read_count for item in self._seek_history)
            if block_index not in self._futures and mean_read_count < 3:
                # No using LRP will be better if read() are always called less than 3 times after seek()
                return self._read(size)

        data = self._buffer.read(size)
        if len(data) == size:
            self._offset += len(data)
            return data

        buffer = BytesIO()
        buffer.write(data)
        while buffer.tell() < size:
            remain_size = size - buffer.tell()
            data = self._next_buffer.read(remain_size)
            buffer.write(data)

        self._offset += buffer.tell()
        return buffer.getvalue()

    def readline(self, size: Optional[int] = None):
        '''
        According to the definition of python IOBase, readline() of BinaryIO will read the content util the first b'\n'
        '''
        if self.closed:
            raise IOError('file already closed: %r' % self.name)

        if len(self._seek_history) > 0:
            self._seek_history[-1].read_count += 1

        if self._offset >= self._content_size:
            return b''

        if size is None:
            size = self._content_size - self._offset
        else:
            assert size >= 0, 'size should greater than 0, got: %r' % size
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

    def _read(self, size: int):
        if size == 0 or self._offset >= self._content_size:
            return b''

        range_str = 'bytes=%d-%d' % (self._offset, self._offset + size - 1)
        data = self._client.get_object(
            Bucket=self._bucket, Key=self._key, Range=range_str)['Body'].read()
        self.seek(size, os.SEEK_CUR)
        return data

    def readinto(self, buffer: bytearray) -> int:
        if self.closed:
            raise IOError('file already closed: %r' % self.name)

        if self._offset >= self._content_size:
            return 0

        size = len(buffer)
        size = min(size, self._content_size - self._offset)

        data = self._buffer.read(size)
        buffer[:len(data)] = data
        if len(data) == size:
            self._offset += len(data)
            return size

        offset = len(data)
        while offset < size:
            remain_size = size - offset
            data = self._next_buffer.read(remain_size)
            buffer[offset:offset + len(data)] = data
            offset += len(data)

        self._offset += offset
        return size

    @property
    def _is_alive(self):
        return not self._executor._shutdown  # pytype: disable=attribute-error

    @property
    def _is_downloading(self):
        return not self._futures.finished

    @property
    def _cached_blocks(self):
        return list(self._futures.keys())

    @property
    def _buffer(self) -> BytesIO:
        if self._cached_offset is not None:
            start = self._block_index
            stop = min(start + self._block_forward, self._block_stop)

            # reversed(range(start, stop))
            for index in range(stop - 1, start - 1, -1):
                self._submit_future(index)
            self._cleanup_futures()

            self._cached_buffer = self._fetch_future_result(self._block_index)
            self._cached_buffer.seek(self._cached_offset)
            self._cached_offset = None

        return self._cached_buffer

    @property
    def _next_buffer(self) -> BytesIO:
        # Get next buffer by this function when finished reading current buffer (self._buffer)
        # Make sure that _buffer is used before using _next_buffer(), or will make _cached_offset invalid
        self._block_index += 1
        self._cached_offset = 0
        return self._buffer

    def _seek_buffer(self, index: int, offset: int = 0):
        # The corresponding block is probably not downloaded when seeked to a new position
        # So record the offset first, set it when it is accessed
        if self._is_auto_scaling:  # When user doesn't define forward
            history = []
            for item in self._seek_history:
                if item.seek_count > self._block_capacity * 2:
                    # seek interval is bigger than self._block_capacity * 2, drop it from self._seek_history
                    continue
                if index - 1 < item.seek_index < index + 2:
                    continue
                item.seek_count += 1
                history.append(item)
            history.append(SeekRecord(index))
            self._seek_history = history
            self._block_forward = max(
                (self._block_capacity - 1) // len(self._seek_history), 1)

        self._cached_offset = offset
        self._block_index = index

    def _fetch_buffer(self, index: int) -> BytesIO:
        range_str = 'bytes=%d-%d' % (
            index * self._block_size, (index + 1) * self._block_size - 1)

        def fetch_bytes() -> bytes:
            data = self._client.get_object(
                Bucket=self._bucket, Key=self._key, Range=range_str)
            etag = data.get('ETag', None)
            if etag is not None and etag != self._content_etag:
                raise S3FileChangedError(
                    'File changed: %r, etag before: %s, after: %s' %
                    (self.name, self._content_info, data))
            return data['Body'].read()

        fetch_bytes = patch_method(
            fetch_bytes,
            max_retries=self._max_retries,
            should_retry=s3_should_retry)

        with raise_s3_error(self.name):
            return BytesIO(fetch_bytes())

    def _submit_future(self, index: int):
        if index < 0 or index >= self._block_stop:
            return
        self._futures.submit(self._executor, index, self._fetch_buffer, index)

    def _fetch_future_result(self, index: int):
        return self._futures.result(index)

    def _cleanup_futures(self):
        self._futures.cleanup(self._block_capacity)

    def _close(self):
        _logger.debug('close file: %r' % self.name)

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

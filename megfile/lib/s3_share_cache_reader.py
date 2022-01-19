from collections import Counter
from logging import getLogger as get_logger
from typing import Optional

from megfile.lib.s3_prefetch_reader import LRUCacheFutureManager, S3PrefetchReader
from megfile.utils import thread_local

DEFAULT_BLOCK_SIZE = 8 * 2**20  # 8MB
DEFAULT_BLOCK_FORWARD = 12
GLOBAL_MAX_WORKERS = 128

DEFAULT_BLOCK_CAPACITY = 32

NEWLINE = ord('\n')

_logger = get_logger(__name__)


class S3ShareCacheReader(S3PrefetchReader):
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
            cache_key: str = 'lru',
            max_workers: Optional[int] = None):

        self._cache_key = cache_key

        super().__init__(
            bucket,
            key,
            s3_client=s3_client,
            block_size=block_size,
            block_capacity=block_capacity,
            block_forward=block_forward,
            max_retries=max_retries,
            max_workers=max_workers,
        )

    def _get_block_forward(
            self, block_capacity: int, block_forward: Optional[int]):
        if block_forward is None:
            block_forward = DEFAULT_BLOCK_FORWARD
        return block_forward

    def _get_futures(self):
        futures = thread_local(
            'S3ShareCacheReader.' + self._cache_key, ShareCacheFutureManager)
        futures.register(self.name)
        return futures

    def _seek_buffer(self, index: int, offset: int = 0):
        # The corresponding block is probably not downloaded when seeked to a new position
        # So record the offset first, set it when it is accessed
        self._cached_offset = offset
        self._block_index = index

    def _submit_future(self, index: int):
        if index < 0 or index >= self._block_stop:
            return  # pragma: no cover
        self._futures.submit(
            self._executor, (self.name, index), self._fetch_buffer, index)

    def _fetch_future_result(self, index: int):
        return self._futures.result((self.name, index))

    def _cleanup_futures(self):
        self._futures.cleanup(DEFAULT_BLOCK_CAPACITY)

    def _close(self):
        _logger.debug('close file: %r' % self.name)

        if not self._is_global_executor:
            self._executor.shutdown()
        self._futures.unregister(self.name)  # pytype: disable=attribute-error


class ShareCacheFutureManager(LRUCacheFutureManager):

    def __init__(self):
        super().__init__()
        self._references = Counter()

    def register(self, key):
        self._references[key] += 1

    def unregister(self, key):
        self._references[key] -= 1
        if self._references[key] == 0:
            self._references.pop(key)
            for key_tuple in list(self):
                if key_tuple[0] != key:
                    continue
                future = self.pop(key_tuple)
                if not future.done():
                    future.cancel()  # pragma: no cover

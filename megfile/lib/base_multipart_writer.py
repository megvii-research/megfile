import os
from abc import ABC, abstractmethod
from collections import OrderedDict
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from io import BytesIO
from logging import getLogger as get_logger
from typing import Optional

from megfile.config import (
    DEFAULT_WRITER_BLOCK_AUTOSCALE,
    GLOBAL_MAX_WORKERS,
    WRITER_BLOCK_SIZE,
    WRITER_MAX_BUFFER_SIZE,
)
from megfile.interfaces import Writable
from megfile.utils import process_local

_logger = get_logger(__name__)


class BaseMultipartWriter(Writable[bytes], ABC):
    # Multi-upload part size must be between 5 MiB and 5 GiB.
    # There is no minimum size limit on the last part of your multipart upload.
    MIN_BLOCK_SIZE = 8 * 2**20

    def __init__(
        self,
        *,
        block_size: int = WRITER_BLOCK_SIZE,
        block_autoscale: bool = DEFAULT_WRITER_BLOCK_AUTOSCALE,
        max_buffer_size: int = WRITER_MAX_BUFFER_SIZE,
        max_workers: Optional[int] = None,
        atomic: bool = False,
    ):
        self.__atomic__ = atomic

        # user maybe put block_size with 'numpy.uint64' type
        self._base_block_size = int(block_size)
        self._block_autoscale = block_autoscale

        self._max_buffer_size = max_buffer_size
        self._total_buffer_size = 0
        self._offset = 0
        self._content_size = 0
        self._buffer = BytesIO()

        self._uploaded_results = OrderedDict()
        self._uploading_futures = set()
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

        self._part_number = 0

        _logger.debug("open file: %r, mode: %s" % (self.name, self.mode))

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    def mode(self) -> str:
        return "wb"

    def tell(self) -> int:
        return self._offset

    @property
    def _block_size(self) -> int:
        if self._block_autoscale:
            if self._part_number < 10:
                return self._base_block_size
            elif self._part_number < 100:
                return min(self._base_block_size * 2, self._max_buffer_size)
            elif self._part_number < 1000:
                return min(self._base_block_size * 4, self._max_buffer_size)
            elif self._part_number < 10000:
                return min(self._base_block_size * 8, self._max_buffer_size)
            return min(self._base_block_size * 16, self._max_buffer_size)  # unreachable
        return self._base_block_size

    @abstractmethod
    def _upload_buffer(self, part_number, content):
        pass

    def _submit_upload_buffer(self, part_number: int, content: bytes):
        self._uploading_futures.add(
            self._executor.submit(self._upload_buffer, part_number, content)
        )
        self._total_buffer_size += len(content)

        while (
            self._uploading_futures and self._total_buffer_size >= self._max_buffer_size
        ):
            wait_result = wait(self._uploading_futures, return_when=FIRST_COMPLETED)
            for future in wait_result.done:
                result = future.result()
                self._total_buffer_size -= result.content_size
                self._uploaded_results[result.part_number] = result.asdict()
            self._uploading_futures = wait_result.not_done

    def _submit_upload_content(self, content: bytes):
        # s3 part needs at least 5MB,
        # so we need to divide content into equal-size parts,
        # and give last part more size.
        # e.g. 257MB can be divided into 2 parts, 128MB and 129MB
        block_size = self._block_size
        while len(content) - block_size > self.MIN_BLOCK_SIZE:
            self._part_number += 1
            current_content, content = (
                content[:block_size],
                content[block_size:],
            )
            self._submit_upload_buffer(self._part_number, current_content)
            block_size = self._block_size

        if content:
            self._part_number += 1
            self._submit_upload_buffer(self._part_number, content)

    def _submit_futures(self):
        content = self._buffer.getvalue()
        if len(content) == 0:
            return
        self._buffer.seek(0, os.SEEK_SET)
        self._buffer.truncate()
        self._submit_upload_content(content)

    def write(self, data: bytes) -> int:
        if self.closed:
            raise IOError("file already closed: %r" % self.name)

        result = self._buffer.write(data)
        if self._buffer.tell() >= self._block_size:
            self._submit_futures()
        self._offset += result
        self._content_size = self._offset
        return result

    def _shutdown(self):
        if not self._is_global_executor:
            self._executor.shutdown()

    @abstractmethod
    def _abort(self):
        pass

    @abstractmethod
    def _close(self):
        pass

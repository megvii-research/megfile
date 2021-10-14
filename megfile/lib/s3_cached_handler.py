import os
from io import UnsupportedOperation
from typing import Iterable, List, Optional

from megfile.errors import translate_fs_error, translate_s3_error
from megfile.lib.s3_memory_handler import S3MemoryHandler
from megfile.utils import generate_cache_path


class S3CachedHandler(S3MemoryHandler):

    def __init__(
            self,
            bucket: str,
            key: str,
            mode: str,
            *,
            s3_client,
            cache_path: Optional[str] = None,
            remove_cache_when_open: bool = True):

        assert mode in ('rb', 'wb', 'ab', 'rb+', 'wb+', 'ab+')

        self._bucket = bucket
        self._key = key
        self._mode = mode
        self._client = s3_client

        if cache_path is None:
            cache_path = generate_cache_path(self.name)

        self._cache_path = cache_path
        self._fileobj = open(self._cache_path, 'wb+')
        self._download_fileobj()

        if remove_cache_when_open:
            os.unlink(self._cache_path)

    def fileno(self) -> int:
        # allow numpy.array to create a memmaped ndarray
        return self._fileobj.fileno()

    def _translate_error(self, error: Exception):
        error = translate_fs_error(error, self._cache_path)
        error = translate_s3_error(error, self.name)
        return error

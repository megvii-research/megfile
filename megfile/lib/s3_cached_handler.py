import os
import sys
from typing import Any, Optional

from megfile.errors import translate_fs_error, translate_s3_error
from megfile.lib.s3_memory_handler import S3MemoryHandler
from megfile.pathlike import UNKNOWN_STAT
from megfile.utils import generate_cache_path


class S3CachedHandler(S3MemoryHandler):
    def __init__(
        self,
        bucket: str,
        key: str,
        mode: str,
        *,
        s3_client: Any,
        cache_path: Optional[str] = None,
        remove_cache_when_open: bool = True,
        profile_name: Optional[str] = None,
        atomic: bool = False,
    ) -> None:
        """Open a disk-backed S3 stream.

        :param remove_cache_when_open: Unlink the open cache on POSIX or delete
            it when the last handle closes on Windows. False keeps the cache.
        """
        self._bucket = bucket
        self._key = key
        self._mode = mode
        self._client = s3_client
        self._profile_name = profile_name
        self._content_stat = UNKNOWN_STAT

        if mode not in ("rb", "wb", "ab", "rb+", "wb+", "ab+"):
            raise ValueError("unacceptable mode: %r" % mode)

        if cache_path is None:
            cache_path = generate_cache_path(self.name)

        self._cache_path = cache_path
        # Only a fully initialized stream may upload on close or finalization.
        self.__closed__ = True
        try:
            if not remove_cache_when_open:
                self._fileobj = open(cache_path, "wb+")
            elif sys.platform == "win32":
                self._fileobj = open(cache_path, "wb+", opener=_open_delete_on_close)
            else:
                self._fileobj = open(cache_path, "wb+")
                os.unlink(cache_path)
            self._download_fileobj()
        except BaseException:
            self._abort()
            raise

        self.__atomic__ = atomic
        self.__closed__ = False

    def fileno(self) -> int:
        # allow numpy.array to create a memmaped ndarray
        return self._fileobj.fileno()

    def _translate_error(self, error: Exception):
        error = translate_fs_error(error, self._cache_path)  # pyre-ignore[6]
        error = translate_s3_error(error, self.name)
        return error


def _open_delete_on_close(path: str, flags: int) -> int:
    """Open a Windows cache file that is deleted with its last handle."""
    return os.open(path, flags | os.O_TEMPORARY, 0o600)

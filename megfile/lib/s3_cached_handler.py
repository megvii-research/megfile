import os
from io import UnsupportedOperation
from typing import Iterable, List, Optional

from megfile.errors import S3ConfigError, UnknownError, raise_s3_error, translate_fs_error, translate_s3_error
from megfile.interfaces import Readable, Seekable, Writable


class S3CachedHandler(Readable, Seekable, Writable):

    def __init__(
            self,
            bucket: str,
            key: str,
            mode: str,
            *,
            s3_client,
            cache_path: str,
            remove_cache_when_open: bool = True):

        assert mode in ('rb', 'wb', 'ab', 'rb+', 'wb+', 'ab+')

        self._bucket = bucket
        self._key = key
        self._mode = mode
        self._client = s3_client

        self._cache_path = cache_path
        self._fileobj = open(self._cache_path, 'wb+')
        self._download_fileobj()

        if remove_cache_when_open:
            os.unlink(self._cache_path)

    @property
    def name(self) -> str:
        return 's3://%s/%s' % (self._bucket, self._key)

    @property
    def mode(self) -> str:
        return self._mode

    def tell(self) -> int:
        return self._fileobj.tell()

    def seek(self, cookie: int, whence: int = os.SEEK_SET) -> int:
        # TODO: pytype deleted the pytype comment after being fixed
        return self._fileobj.seek(cookie, whence)  # pytype: disable=bad-return-type

    def readable(self) -> bool:
        return self._mode[0] == 'r' or self._mode[-1] == '+'

    def read(self, size: Optional[int] = None) -> bytes:
        if not self.readable():
            raise UnsupportedOperation('not readable')
        return self._fileobj.read(size)

    def readline(self, size: Optional[int] = None) -> bytes:
        if not self.readable():
            raise UnsupportedOperation('not readable')
        return self._fileobj.readline(size)

    def readlines(self) -> List[bytes]:
        if not self.readable():
            raise UnsupportedOperation('not readable')
        return self._fileobj.readlines()

    def writable(self) -> bool:
        return self._mode[0] == 'w' or \
            self._mode[0] == 'a' or \
            self._mode[-1] == '+'

    def flush(self):
        self._fileobj.flush()

    def write(self, data: bytes) -> int:
        if not self.writable():
            raise UnsupportedOperation('not writable')
        if self._mode[0] == 'a':
            self.seek(0, os.SEEK_END)
        return self._fileobj.write(data)

    def writelines(self, lines: Iterable[bytes]):
        if not self.writable():
            raise UnsupportedOperation('not writable')
        if self._mode[0] == 'a':
            self.seek(0, os.SEEK_END)
        self._fileobj.writelines(lines)

    def _file_exists(self) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._key)
        except Exception as error:
            error = translate_s3_error(error, self.name)
            if isinstance(error, (UnknownError, S3ConfigError)):
                raise error
            return False
        return True

    def _download_fileobj(self):
        need_download = self._mode[0] == 'r' or (
            self._mode[0] == 'a' and self._file_exists())
        if not need_download:
            return
        # directly download to the file handle
        try:
            self._client.download_fileobj(
                self._bucket, self._key, self._fileobj)
        except Exception as error:
            error = translate_fs_error(error, self._cache_path)
            error = translate_s3_error(error, self.name)
            raise error
        if self._mode[0] == 'r':
            self.seek(0, os.SEEK_SET)

    def _upload_fileobj(self):
        need_upload = self.writable()
        if not need_upload:
            return
        # directly upload from file handle
        self.seek(0, os.SEEK_SET)
        with raise_s3_error(self.name):
            self._client.upload_fileobj(self._fileobj, self._bucket, self._key)

    def _close(self, need_upload: bool = True):
        if need_upload:
            self._upload_fileobj()
        self._fileobj.close()

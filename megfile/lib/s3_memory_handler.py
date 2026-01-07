import os
from typing import Optional

from megfile.errors import (
    S3ConfigError,
    S3PermissionError,
    S3UnknownError,
    translate_s3_error,
)
from megfile.lib.base_memory_handler import BaseMemoryHandler


class S3MemoryHandler(BaseMemoryHandler):
    def __init__(
        self,
        bucket: str,
        key: str,
        mode: str,
        *,
        s3_client,
        profile_name: Optional[str] = None,
        atomic: bool = False,
    ):
        self._bucket = bucket
        self._key = key
        self._client = s3_client
        self._profile_name = profile_name
        super().__init__(mode=mode, atomic=atomic)

    @property
    def name(self) -> str:
        protocol = f"s3+{self._profile_name}" if self._profile_name else "s3"
        return f"{protocol}://{self._bucket}/{self._key}"

    def _translate_error(self, error: Exception):
        return translate_s3_error(error, self.name)

    def _file_exists(self) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=self._key)
        except Exception as error:
            error = self._translate_error(error)
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error
            return False
        return True

    def _download_fileobj(self):
        need_download = self._mode[0] == "r"
        need_download = need_download or (self._mode[0] == "a" and self._file_exists())
        if not need_download:
            return
        # directly download to the file handle
        try:
            self._client.download_fileobj(self._bucket, self._key, self._fileobj)
        except Exception as error:
            raise self._translate_error(error)
        if self._mode[0] == "r":
            self.seek(0, os.SEEK_SET)

    def _upload_fileobj(self):
        need_upload = self.writable()
        if not need_upload:
            return
        # directly upload from file handle
        self.seek(0, os.SEEK_SET)
        try:
            self._client.upload_fileobj(self._fileobj, self._bucket, self._key)
        except Exception as error:
            raise self._translate_error(error)

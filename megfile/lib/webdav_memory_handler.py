import os

from webdav3.client import Client as WebdavClient
from webdav3.exceptions import RemoteResourceNotFound

from megfile.lib.base_memory_handler import BaseMemoryHandler


class WebdavMemoryHandler(BaseMemoryHandler):
    def __init__(
        self,
        remote_path: str,
        mode: str,
        *,
        webdav_client: WebdavClient,
        name: str,
    ):
        self._remote_path = remote_path
        self._client = webdav_client
        self._name = name
        super().__init__(mode=mode)

    @property
    def name(self) -> str:
        return self._name

    def _file_exists(self) -> bool:
        from megfile.webdav_path import _webdav_stat

        try:
            return not _webdav_stat(self._client, self._remote_path)["is_dir"]
        except RemoteResourceNotFound:
            return False

    def _download_fileobj(self):
        need_download = self._mode[0] == "r"
        need_download = need_download or (self._mode[0] == "a" and self._file_exists())
        if not need_download:
            return
        # directly download to the file handle
        self._client.download_from(self._fileobj, self._remote_path)
        if self._mode[0] == "r":
            self.seek(0, os.SEEK_SET)

    def _upload_fileobj(self):
        need_upload = self.writable()
        if not need_upload:
            return
        # directly upload from file handle
        self.seek(0, os.SEEK_SET)
        self._client.upload_to(self._fileobj, self._remote_path)

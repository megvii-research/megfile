import os

from webdav3.client import Client as WebdavClient
from webdav3.client import Urn, WebDavXmlUtils, wrap_connection_error
from webdav3.exceptions import (
    OptionNotValid,
    RemoteResourceNotFound,
)

from megfile.lib.base_memory_handler import BaseMemoryHandler


def _webdav_stat(client: WebdavClient, remote_path: str):
    urn = Urn(remote_path)
    response = client.execute_request(
        action="info", path=urn.quote(), headers_ext=["Depth: 0"]
    )
    path = client.get_full_path(urn)
    info = WebDavXmlUtils.parse_info_response(
        response.content, path, client.webdav.hostname
    )
    info["isdir"] = WebDavXmlUtils.parse_is_dir_response(
        response.content, path, client.webdav.hostname
    )
    return info


@wrap_connection_error
def _webdav_download_from(client: WebdavClient, buff, remote_path):
    urn = Urn(remote_path)
    if client.is_dir(urn.path()):
        raise OptionNotValid(name="remote_path", value=remote_path)

    if not client.check(urn.path()):
        raise RemoteResourceNotFound(urn.path())

    response = client.execute_request(action="download", path=urn.quote())

    for chunk in response.iter_content(chunk_size=client.chunk_size):
        buff.write(chunk)


class WebdavMemoryHandler(BaseMemoryHandler):
    def __init__(
        self,
        remote_path: str,
        mode: str,
        *,
        webdav_client: WebdavClient,
        name: str,
        atomic: bool = False,
    ):
        self._remote_path = remote_path
        self._client = webdav_client
        self._name = name
        super().__init__(mode=mode, atomic=atomic)

    @property
    def name(self) -> str:
        return self._name

    def _file_exists(self) -> bool:
        try:
            return not _webdav_stat(self._client, self._remote_path)["isdir"]
        except RemoteResourceNotFound:
            return False

    def _download_fileobj(self):
        need_download = self._mode[0] == "r"
        need_download = need_download or (self._mode[0] == "a" and self._file_exists())
        if not need_download:
            return
        # directly download to the file handle
        _webdav_download_from(self._client, self._fileobj, self._remote_path)
        if self._mode[0] == "r":
            self.seek(0, os.SEEK_SET)

    def _upload_fileobj(self):
        need_upload = self.writable()
        if not need_upload:
            return
        # directly upload from file handle
        self.seek(0, os.SEEK_SET)
        self._client.upload_to(self._fileobj, self._remote_path)

import time
from copy import deepcopy
from functools import partial
from io import BufferedReader, BytesIO
from logging import getLogger as get_logger
from threading import Lock
from typing import Iterable, Iterator, Optional, Tuple, Union

import requests
from urllib3 import HTTPResponse

from megfile.config import (
    HTTP_MAX_RETRY_TIMES,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
)
from megfile.errors import http_should_retry, patch_method, translate_http_error
from megfile.interfaces import PathLike, Readable, StatResult, URIPath
from megfile.lib.compat import fspath
from megfile.lib.http_prefetch_reader import DEFAULT_TIMEOUT, HttpPrefetchReader
from megfile.lib.url import get_url_scheme
from megfile.smart_path import SmartPath
from megfile.utils import _is_pickle, binary_open

__all__ = [
    "HttpPath",
    "HttpsPath",
    "get_http_session",
    "is_http",
]

_logger = get_logger(__name__)


def get_http_session(
    timeout: Optional[Union[int, Tuple[int, int]]] = DEFAULT_TIMEOUT,
    status_forcelist: Iterable[int] = (500, 502, 503, 504),
) -> requests.Session:
    session = requests.Session()

    def after_callback(response, *args, **kwargs):
        if response.status_code in status_forcelist:
            response.raise_for_status()
        return response

    def before_callback(method, url, **kwargs):
        _logger.debug(
            "send http request: %s %r, with parameters: %s", method, url, kwargs
        )

    def retry_callback(
        error,
        method,
        url,
        params=None,
        data=None,
        headers=None,
        cookies=None,
        files=None,
        auth=None,
        timeout=None,
        allow_redirects=True,
        proxies=None,
        hooks=None,
        stream=None,
        verify=None,
        cert=None,
        json=None,
        **kwargs,
    ):
        if data and hasattr(data, "seek"):
            data.seek(0)
        elif isinstance(data, Iterator):
            _logger.warning("Can not retry http request with iterator data")
            raise
        if files:

            def seek_or_reopen(file_object):
                if isinstance(file_object, (str, bytes)):
                    return file_object
                elif hasattr(file_object, "seek"):
                    file_object.seek(0)
                    return file_object
                elif hasattr(file_object, "name"):
                    with SmartPath(file_object.name).open("rb") as f:
                        return BytesIO(f.read())
                else:
                    _logger.warning(
                        "Can not retry http request, because the file object "
                        'is not seekable and not support "name"'
                    )
                    raise

            for key, file_info in files.items():
                if hasattr(file_info, "seek"):
                    file_info.seek(0)
                elif isinstance(file_info, (tuple, list)) and len(file_info) >= 2:
                    file_info = list(file_info)
                    file_info[1] = seek_or_reopen(file_info[1])
                    files[key] = file_info

    session.request = patch_method(
        partial(session.request, timeout=timeout),
        max_retries=HTTP_MAX_RETRY_TIMES,
        should_retry=http_should_retry,
        before_callback=before_callback,
        after_callback=after_callback,
        retry_callback=retry_callback,
    )
    return session


def is_http(path: PathLike) -> bool:
    """http scheme definition: http(s)://domain/path

    :param path: Path to be tested
    :returns: True if path is http url, else False
    """

    path = fspath(path)
    if not isinstance(path, str) or not (
        path.startswith("http://") or path.startswith("https://")
    ):
        return False

    scheme = get_url_scheme(path)
    return scheme == "http" or scheme == "https"


@SmartPath.register
class HttpPath(URIPath):
    protocol = "http"

    def __init__(self, path: PathLike, *other_paths: PathLike):
        super().__init__(path, *other_paths)

        if fspath(path).startswith("https://"):
            self.protocol = "https"
        self.request_kwargs = {}

    @binary_open
    def open(
        self,
        mode: str = "rb",
        *,
        max_workers: Optional[int] = None,
        max_buffer_size: int = READER_MAX_BUFFER_SIZE,
        block_forward: Optional[int] = None,
        block_size: int = READER_BLOCK_SIZE,
        **kwargs,
    ) -> Union[BufferedReader, HttpPrefetchReader]:
        """Open a BytesIO to read binary data of given http(s) url

        .. note ::

            Essentially, it reads data of http(s) url to memory by requests,
            and then return BytesIO to user.

        :param mode: Only supports 'r' or 'rb' mode now
        :param encoding: encoding is the name of the encoding used to decode or encode
            the file. This should only be used in text mode.
        :param errors: errors is an optional string that specifies how encoding and
            decoding errors are to be handled—this cannot be used in binary mode.
        :param max_workers: Max download thread number, `None` by default,
            will use global thread pool with 8 threads.
        :param max_buffer_size: Max cached buffer size in memory, 128MB by default.
            Set to `0` will disable cache.
        :param block_forward: How many blocks of data cached from offset position
        :param block_size: Size of single block, 8MB by default. Each block will
            be uploaded or downloaded by single thread.
        :return: A file-like object with http(s) data
        """
        if mode not in ("rb",):
            raise ValueError("unacceptable mode: %r" % mode)

        response = None
        request_kwargs = deepcopy(self.request_kwargs)
        timeout = request_kwargs.pop("timeout", DEFAULT_TIMEOUT)
        stream = request_kwargs.pop("stream", True)
        try:
            response = get_http_session(timeout=timeout, status_forcelist=()).get(
                self.path_with_protocol, stream=stream, **request_kwargs
            )
            response.raise_for_status()
        except Exception as error:
            if response:
                response.close()
            raise translate_http_error(error, self.path_with_protocol)

        content_size = int(response.headers["Content-Length"])
        if (
            response.headers.get("Accept-Ranges") == "bytes"
            and content_size >= block_size * 2
            and not response.headers.get("Content-Encoding")
        ):
            response.close()

            reader = HttpPrefetchReader(
                self,
                content_size=content_size,
                block_size=block_size,
                max_buffer_size=max_buffer_size,
                block_forward=block_forward,
                max_retries=HTTP_MAX_RETRY_TIMES,
                max_workers=max_workers,
            )
            if _is_pickle(reader):
                reader = BufferedReader(reader)  # type: ignore
            return reader

        response.raw.name = self.path_with_protocol
        # TODO: When python version must bigger than 3.10,
        # use urllib3>=2.0.0 instead of 'Response'
        # response.raw.auto_close = False
        # response.raw.decode_content = True
        # return BufferedReader(response.raw)
        return BufferedReader(Response(response.raw))  # type: ignore

    def stat(self, follow_symlinks=True) -> StatResult:
        """
        Get StatResult of http_url response, including size and mtime,
        referring to http_getsize and http_getmtime

        :param follow_symlinks: Ignore this parameter, just for compatibility
        :returns: StatResult
        :raises: HttpPermissionError, HttpFileNotFoundError
        """

        request_kwargs = deepcopy(self.request_kwargs)
        timeout = request_kwargs.pop("timeout", DEFAULT_TIMEOUT)
        stream = request_kwargs.pop("stream", True)

        try:
            with get_http_session(timeout=timeout, status_forcelist=()).get(
                self.path_with_protocol, stream=stream, **request_kwargs
            ) as response:
                response.raise_for_status()
                headers = response.headers
        except Exception as error:
            raise translate_http_error(error, self.path_with_protocol)

        size = headers.get("Content-Length")
        if size:
            size = int(size)
        else:
            size = 0

        last_modified = headers.get("Last-Modified")
        if last_modified:
            last_modified = time.mktime(
                time.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
            )
        else:
            last_modified = 0.0

        return StatResult(
            size=size, mtime=last_modified, isdir=False, islnk=False, extra=headers
        )

    def getsize(self, follow_symlinks: bool = False) -> int:
        """
        Get file size on the given http_url path.

        If http response header don't support Content-Length, will return None

        :param follow_symlinks: Ignore this parameter, just for compatibility
        :returns: File size (in bytes)
        :raises: HttpPermissionError, HttpFileNotFoundError
        """
        return self.stat().size

    def getmtime(self, follow_symlinks: bool = False) -> float:
        """
        Get Last-Modified time of the http request on the given http_url path.

        If http response header don't support Last-Modified, will return None

        :param follow_symlinks: Ignore this parameter, just for compatibility
        :returns: Last-Modified time (in Unix timestamp format)
        :raises: HttpPermissionError, HttpFileNotFoundError
        """
        return self.stat().mtime

    def exists(self, followlinks: bool = False) -> bool:
        """Test if http path exists

        :param followlinks: ignore this parameter, just for compatibility
        :type followlinks: bool, optional
        :return: return True if exists
        :rtype: bool
        """
        request_kwargs = deepcopy(self.request_kwargs)
        timeout = request_kwargs.pop("timeout", DEFAULT_TIMEOUT)
        stream = request_kwargs.pop("stream", True)

        try:
            with get_http_session(timeout=timeout, status_forcelist=()).get(
                self.path_with_protocol, stream=stream, **request_kwargs
            ) as response:
                if response.status_code == 404:
                    return False
                return True
        except requests.exceptions.ConnectionError:
            return False


@SmartPath.register
class HttpsPath(HttpPath):
    protocol = "https"


class Response(Readable[bytes]):
    def __init__(self, raw: HTTPResponse) -> None:
        super().__init__()

        raw.auto_close = False
        self._block_size = 128 * 2**10  # 128KB
        self._raw = raw
        self._offset = 0
        self._buffer = BytesIO()
        self._lock = Lock()

    @property
    def name(self):
        return self._raw.name

    @property
    def mode(self):
        return "rb"

    def tell(self) -> int:
        return self._offset

    def _clear_buffer(self) -> None:
        self._buffer.seek(0)
        self._buffer.truncate()

    def read(self, size: Optional[int] = None) -> bytes:
        if size == 0:
            return b""
        if size is not None and size < 0:
            size = None

        with self._lock:
            while not size or self._buffer.tell() < size:
                data = self._raw.read(self._block_size, decode_content=True)
                if not data:
                    break
                self._buffer.write(data)
            self._buffer.seek(0)
            content = self._buffer.read(size)
            residue = self._buffer.read()
            self._clear_buffer()
            if residue:
                self._buffer.write(residue)
            self._offset += len(content)
        return content

    def readline(self, size: Optional[int] = None) -> bytes:
        if size == 0:
            return b""
        if size is not None and size < 0:
            size = None

        with self._lock:
            self._buffer.seek(0)
            buffer = self._buffer.read()
            self._clear_buffer()
            if b"\n" in buffer:
                content = buffer[: buffer.index(b"\n") + 1]
                if size:
                    content = content[:size]
                self._buffer.write(buffer[len(content) :])
            elif size and len(buffer) >= size:
                content = buffer[:size]
                self._buffer.write(buffer[size:])
            else:
                content = None
                self._buffer.write(buffer)
                while True:
                    if size and self._buffer.tell() >= size:
                        break
                    data = self._raw.read(self._block_size, decode_content=True)
                    if not data:
                        break
                    elif b"\n" in data:
                        last_content, residue = data.split(b"\n", 1)
                        self._buffer.write(last_content)
                        self._buffer.write(b"\n")
                        self._buffer.seek(0)
                        content = self._buffer.read()
                        self._clear_buffer()
                        if size and len(content) > size:
                            self._buffer.write(content[size:])
                            content = content[:size]
                        if residue:
                            self._buffer.write(residue)
                        break
                    else:
                        self._buffer.write(data)

                if content is None:
                    self._buffer.seek(0)
                    content = self._buffer.read(size)
                    residue = self._buffer.read()
                    self._clear_buffer()
                    if residue:
                        self._buffer.write(residue)
            self._offset += len(content)
        return content

    def _close(self) -> None:
        return self._raw.close()

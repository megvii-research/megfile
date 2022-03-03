import time
from io import BufferedReader

import requests

from megfile.errors import translate_http_error
from megfile.interfaces import StatResult, URIPath
from megfile.smart_path import SmartPath
from megfile.utils import binary_open

__all__ = [
    'HttpPath',
    'HttpsPath',
]


@SmartPath.register
class HttpPath(URIPath):

    protocol = "http"

    @binary_open
    def open(self, mode: str = 'rb') -> BufferedReader:
        '''Open a BytesIO to read binary data of given http(s) url

        .. note ::

            Essentially, it reads data of http(s) url to memory by requests, and then return BytesIO to user.

        :param http_url: http(s) url, e.g.: http(s)://domain/path
        :param mode: Only supports 'rb' mode now
        :return: BytesIO initialized with http(s) data
        '''
        if mode not in ('rb',):
            raise ValueError('unacceptable mode: %r' % mode)

        try:
            response = requests.get(
                self.path_with_protocol, stream=True, timeout=10.0)
            response.raise_for_status()
        except Exception as error:
            raise translate_http_error(error, self.path_with_protocol)

        response.raw.auto_close = False
        return BufferedReader(response.raw)

    def stat(self) -> StatResult:
        '''
        Get StatResult of http_url response, including size and mtime, referring to http_getsize and http_getmtime

        :param http_url: Given http url
        :returns: StatResult
        :raises: HttpPermissionError, HttpFileNotFoundError
        '''

        try:
            response = requests.get(
                self.path_with_protocol, stream=True, timeout=10.0)
            response.raise_for_status()
        except Exception as error:
            raise translate_http_error(error, self.path_with_protocol)

        size = response.headers.get('Content-Length')
        if size:
            size = int(size)

        last_modified = response.headers.get('Last-Modified')
        if last_modified:
            last_modified = time.mktime(
                time.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z"))

        return StatResult(  # pyre-ignore[20]
            size=size, mtime=last_modified, isdir=False,
            islnk=False, extra=response.headers)

    def getsize(self) -> int:
        '''
        Get file size on the given http_url path.

        If http response header don't support Content-Length, will return None

        :param http_url: Given http path
        :returns: File size (in bytes)
        :raises: HttpPermissionError, HttpFileNotFoundError
        '''
        return self.stat().size

    def getmtime(self) -> float:
        '''
        Get Last-Modified time of the http request on the given http_url path.
        
        If http response header don't support Last-Modified, will return None

        :param http_url: Given http url
        :returns: Last-Modified time (in Unix timestamp format)
        :raises: HttpPermissionError, HttpFileNotFoundError
        '''
        return self.stat().mtime


@SmartPath.register
class HttpsPath(HttpPath):

    protocol = "https"

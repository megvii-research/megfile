from io import BufferedReader
from typing import BinaryIO, Callable, Iterator, List, Optional, Tuple

from megfile.http_path import HttpPath, HttpsPath, get_http_session, is_http
from megfile.interfaces import Access, FileEntry, PathLike, StatResult

__all__ = [
    'HttpPath',
    'HttpsPath',
    'get_http_session',
    'is_http',
    'http_open',
    'http_stat',
    'http_getsize',
    'http_getmtime',
    'http_md5',
]


def http_open(path: PathLike, mode: str = 'rb') -> BufferedReader:
    '''Open a BytesIO to read binary data of given http(s) url

    .. note ::

        Essentially, it reads data of http(s) url to memory by requests, and then return BytesIO to user.

    :param path: Given path
    :param mode: Only supports 'rb' mode now
    :return: BytesIO initialized with http(s) data
    '''
    return HttpPath(path).open(mode)


def http_stat(path: PathLike) -> StatResult:
    '''
    Get StatResult of http_url response, including size and mtime, referring to http_getsize and http_getmtime

    :param path: Given path
    :returns: StatResult
    :raises: HttpPermissionError, HttpFileNotFoundError
    '''
    return HttpPath(path).stat()


def http_getsize(path: PathLike) -> int:
    '''
    Get file size on the given http_url path.

    If http response header don't support Content-Length, will return None

    :param path: Given path
    :returns: File size (in bytes)
    :raises: HttpPermissionError, HttpFileNotFoundError
    '''
    return HttpPath(path).getsize()


def http_getmtime(path: PathLike) -> float:
    '''
    Get Last-Modified time of the http request on the given http_url path.

    If http response header don't support Last-Modified, will return None

    :param path: Given path
    :returns: Last-Modified time (in Unix timestamp format)
    :raises: HttpPermissionError, HttpFileNotFoundError
    '''
    return HttpPath(path).getmtime()


def http_md5(
        path: PathLike, recalculate: bool = False,
        followlinks: bool = False) -> str:
    '''
    Calculate the md5 value of the file by http download

    :param path: Given path
    :param recalculate: Not useful for http path, just for compatibility.
    :param followlinks: Not useful for http path, just for compatibility.
    :returns: md5 meta info
    '''
    return HttpPath(path).md5(recalculate, followlinks)

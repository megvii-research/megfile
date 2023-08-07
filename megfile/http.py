from io import BufferedReader
from typing import Optional

from megfile.http_path import HttpPath, HttpsPath, get_http_session, is_http
from megfile.interfaces import PathLike, StatResult
from megfile.lib.base_prefetch_reader import DEFAULT_BLOCK_SIZE
from megfile.lib.s3_buffered_writer import DEFAULT_MAX_BUFFER_SIZE

__all__ = [
    'get_http_session',
    'is_http',
    'http_open',
    'http_stat',
    'http_getsize',
    'http_getmtime',
    'http_exists',
]


def http_open(
        path: PathLike,
        mode: str = 'rb',
        *,
        max_concurrency: Optional[int] = None,
        max_buffer_size: int = DEFAULT_MAX_BUFFER_SIZE,
        forward_ratio: Optional[float] = None,
        block_size: int = DEFAULT_BLOCK_SIZE,
        **kwargs) -> BufferedReader:
    '''Open a BytesIO to read binary data of given http(s) url

    .. note ::

        Essentially, it reads data of http(s) url to memory by requests, and then return BytesIO to user.

    :param path: Given path
    :param mode: Only supports 'rb' mode now
    :param max_concurrency: Max download thread number, None by default
    :param max_buffer_size: Max cached buffer size in memory, 128MB by default
    :param block_size: Size of single block, 8MB by default. Each block will be uploaded or downloaded by single thread.
    :return: BytesIO initialized with http(s) data
    '''
    return HttpPath(path).open(
        mode,
        max_concurrency=max_concurrency,
        max_buffer_size=max_buffer_size,
        forward_ratio=forward_ratio,
        block_size=block_size)


def http_stat(path: PathLike, follow_symlinks=True) -> StatResult:
    '''
    Get StatResult of http_url response, including size and mtime, referring to http_getsize and http_getmtime

    :param path: Given path
    :param follow_symlinks: Ignore this parameter, just for compatibility
    :returns: StatResult
    :raises: HttpPermissionError, HttpFileNotFoundError
    '''
    return HttpPath(path).stat(follow_symlinks)


def http_getsize(path: PathLike, follow_symlinks: bool = False) -> int:
    '''
    Get file size on the given http_url path.

    If http response header don't support Content-Length, will return None

    :param path: Given path
    :param follow_symlinks: Ignore this parameter, just for compatibility
    :returns: File size (in bytes)
    :raises: HttpPermissionError, HttpFileNotFoundError
    '''
    return HttpPath(path).getsize(follow_symlinks)


def http_getmtime(path: PathLike, follow_symlinks: bool = False) -> float:
    '''
    Get Last-Modified time of the http request on the given http_url path.

    If http response header don't support Last-Modified, will return None

    :param path: Given path
    :param follow_symlinks: Ignore this parameter, just for compatibility
    :returns: Last-Modified time (in Unix timestamp format)
    :raises: HttpPermissionError, HttpFileNotFoundError
    '''
    return HttpPath(path).getmtime(follow_symlinks)


def http_exists(path: PathLike, followlinks: bool = False) -> bool:
    """Test if http path exists

    :param path: Given path
    :param followlinks: ignore this parameter, just for compatibility
    :type followlinks: bool, optional
    :return: return True if exists
    :rtype: bool
    """
    return HttpPath(path).exists(followlinks)

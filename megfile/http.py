from io import BufferedReader
from typing import Optional, Union

from megfile.config import READER_BLOCK_SIZE, READER_MAX_BUFFER_SIZE
from megfile.http_path import HttpPath, HttpPrefetchReader, get_http_session, is_http
from megfile.interfaces import PathLike, StatResult

__all__ = [
    "get_http_session",
    "is_http",
    "http_open",
    "http_stat",
    "http_getsize",
    "http_getmtime",
    "http_exists",
]


def http_open(
    path: PathLike,
    mode: str = "rb",
    *,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
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

    :param path: Given path
    :param mode: Only supports 'r' or 'rb' mode now
    :param encoding: encoding is the name of the encoding used to decode or encode
        the file. This should only be used in text mode.
    :param errors: errors is an optional string that specifies how encoding and decoding
        errors are to be handled—this cannot be used in binary mode.
    :param max_workers: Max download thread number, `None` by default,
        will use global thread pool with 8 threads.
    :param max_buffer_size: Max cached buffer size in memory, 128MB by default.
        Set to `0` will disable cache.
    :param block_forward: How many blocks of data cached from offset position
    :param block_size: Size of single block, 8MB by default. Each block will be uploaded
        or downloaded by single thread.
    :return: A file-like object with http(s) data
    """
    return HttpPath(path).open(
        mode,
        encoding=encoding,
        errors=errors,
        max_workers=max_workers,
        max_buffer_size=max_buffer_size,
        block_forward=block_forward,
        block_size=block_size,
    )


def http_stat(path: PathLike, follow_symlinks=True) -> StatResult:
    """
    Get StatResult of http_url response, including size and mtime,
    referring to http_getsize and http_getmtime

    :param path: Given path
    :param follow_symlinks: Ignore this parameter, just for compatibility
    :returns: StatResult
    :raises: HttpPermissionError, HttpFileNotFoundError
    """
    return HttpPath(path).stat(follow_symlinks)


def http_getsize(path: PathLike, follow_symlinks: bool = False) -> int:
    """
    Get file size on the given http_url path.

    If http response header don't support Content-Length, will return None

    :param path: Given path
    :param follow_symlinks: Ignore this parameter, just for compatibility
    :returns: File size (in bytes)
    :raises: HttpPermissionError, HttpFileNotFoundError
    """
    return HttpPath(path).getsize(follow_symlinks)


def http_getmtime(path: PathLike, follow_symlinks: bool = False) -> float:
    """
    Get Last-Modified time of the http request on the given http_url path.

    If http response header don't support Last-Modified, will return None

    :param path: Given path
    :param follow_symlinks: Ignore this parameter, just for compatibility
    :returns: Last-Modified time (in Unix timestamp format)
    :raises: HttpPermissionError, HttpFileNotFoundError
    """
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

import io
import os
import typing
from collections import defaultdict
from functools import partial
from itertools import chain
from pathlib import PurePath
from urllib.parse import urlsplit

import megfile
from megfile.errors import ProtocolNotFoundError, UnsupportedError
from megfile.fs import fs_access, fs_copy, fs_exists, fs_getmd5, fs_getmtime, fs_getsize, fs_glob, fs_glob_stat, fs_iglob, fs_isdir, fs_isfile, fs_islink, fs_listdir, fs_load_from, fs_makedirs, fs_open, fs_path_join, fs_readlink, fs_remove, fs_rename, fs_save_as, fs_scan, fs_scan_stat, fs_scandir, fs_stat, fs_symlink, fs_unlink, fs_walk
from megfile.http import http_getmtime, http_getsize, http_open, http_stat
from megfile.interfaces import Access, FileEntry, NullCacher, PathLike
from megfile.lib.combine_reader import CombineReader
from megfile.lib.compat import fspath
from megfile.lib.glob import globlize, ungloblize
from megfile.s3 import S3Cacher, is_s3, s3_access, s3_copy, s3_download, s3_exists, s3_getmd5, s3_getmtime, s3_getsize, s3_glob, s3_glob_stat, s3_iglob, s3_isdir, s3_isfile, s3_islink, s3_listdir, s3_load_content, s3_load_from, s3_makedirs, s3_open, s3_path_join, s3_readlink, s3_remove, s3_rename, s3_save_as, s3_scan, s3_scan_stat, s3_scandir, s3_stat, s3_symlink, s3_unlink, s3_upload, s3_walk
from megfile.stdio import stdio_open
from megfile.utils import combine, get_content_offset


def smart_access(
        path: typing.Union[str, os.PathLike],
        mode: megfile.pathlike.Access = Access.READ) -> bool:
    '''
    Test if path has access permission described by mode
    
    :param path: Path to be tested
    :param mode: Access mode(Access.READ, Access.WRITE, Access.BUCKETREAD, Access.BUCKETWRITE)
    :returns: bool, if the path has read/write access.
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_access(path=path, mode=mode)
    if protocol == 's3':
        return s3_access(path=path, mode=mode)
    raise UnsupportedError(
        operation='smart_access', path=path)  # pragma: no cover


def smart_exists(
        path: typing.Union[str, os.PathLike],
        followlinks: bool = False) -> bool:
    '''
    Test if path or s3_url exists
    
    :param path: Path to be tested
    :returns: True if path eixsts, else False
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_exists(path=path, followlinks=followlinks)
    if protocol == 's3':
        return s3_exists(path=path, followlinks=followlinks)
    raise UnsupportedError(
        operation='smart_exists', path=path)  # pragma: no cover


def smart_getmd5(
        path: typing.Union[str, os.PathLike], recalculate: bool = False) -> str:
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_getmd5(path=path, recalculate=recalculate)
    if protocol == 's3':
        return s3_getmd5(path=path, recalculate=recalculate)
    raise UnsupportedError(
        operation='smart_getmd5', path=path)  # pragma: no cover


def smart_getmtime(path: typing.Union[str, os.PathLike]) -> float:
    '''
    Get last-modified time of the file on the given s3_url or file path (in Unix timestamp format).
    If the path is an existent directory, return the latest modified time of all file in it. The mtime of empty directory is 1970-01-01 00:00:00
    
    :param path: Given path
    :returns: Last-modified time
    :raises: FileNotFoundError
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_getmtime(path=path)
    if protocol == 's3':
        return s3_getmtime(path=path)
    if protocol == 'http':
        return http_getmtime(path=path)
    raise UnsupportedError(
        operation='smart_getmtime', path=path)  # pragma: no cover


def smart_getsize(path: typing.Union[str, os.PathLike]) -> int:
    '''
    Get file size on the given s3_url or file path (in bytes).
    If the path in a directory, return the sum of all file size in it, including file in subdirectories (if exist).
    The result excludes the size of directory itself. In other words, return 0 Byte on an empty directory path.
    
    :param path: Given path
    :returns: File size
    :raises: FileNotFoundError
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_getsize(path=path)
    if protocol == 's3':
        return s3_getsize(path=path)
    if protocol == 'http':
        return http_getsize(path=path)
    raise UnsupportedError(
        operation='smart_getsize', path=path)  # pragma: no cover


def smart_isdir(
        path: typing.Union[str, os.PathLike],
        followlinks: bool = False) -> bool:
    '''
    Test if a file path or an s3 url is directory
    
    :param path: Path to be tested
    :returns: True if path is directory, else False
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_isdir(path=path, followlinks=followlinks)
    if protocol == 's3':
        return s3_isdir(path=path)
    raise UnsupportedError(
        operation='smart_isdir', path=path)  # pragma: no cover


def smart_isfile(
        path: typing.Union[str, os.PathLike],
        followlinks: bool = False) -> bool:
    '''
    Test if a file path or an s3 url is file
    
    :param path: Path to be tested
    :returns: True if path is file, else False
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_isfile(path=path, followlinks=followlinks)
    if protocol == 's3':
        return s3_isfile(path=path, followlinks=followlinks)
    raise UnsupportedError(
        operation='smart_isfile', path=path)  # pragma: no cover


def smart_islink(path: typing.Union[str, os.PathLike]) -> bool:
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_islink(path=path)
    if protocol == 's3':
        return s3_islink(path=path)
    raise UnsupportedError(
        operation='smart_islink', path=path)  # pragma: no cover


def smart_listdir(path: typing.Union[str, os.PathLike]) -> typing.List:
    '''
    Get all contents of given s3_url or file path. The result is in acsending alphabetical order.
    
    :param path: Given path
    :returns: All contents of given s3_url or file path in acsending alphabetical order.
    :raises: FileNotFoundError, NotADirectoryError
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_listdir(path=path)
    if protocol == 's3':
        return s3_listdir(path=path)
    raise UnsupportedError(
        operation='smart_listdir', path=path)  # pragma: no cover


def smart_load_from(path: typing.Union[str, os.PathLike]) -> typing.BinaryIO:
    '''
    Read all content in binary on specified path and write into memory
    
    User should close the BinaryIO manually
    
    :param path: Specified path
    :returns: BinaryIO
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_load_from(path=path)
    if protocol == 's3':
        return s3_load_from(path=path)
    raise UnsupportedError(
        operation='smart_load_from', path=path)  # pragma: no cover


def smart_makedirs(
        path: typing.Union[str, os.PathLike], exist_ok: bool = False):
    '''
    Create a directory if is on fs.
    If on s3, it actually check if target exists, and check if bucket has WRITE access
    
    :param path: Given path
    :param exist_ok: if False and target directory not exists, raise FileNotFoundError
    :raises: PermissionError, FileExistsError
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_makedirs(path=path, exist_ok=exist_ok)
    if protocol == 's3':
        return s3_makedirs(path=path, exist_ok=exist_ok)
    raise UnsupportedError(
        operation='smart_makedirs', path=path)  # pragma: no cover


def smart_open(
        path: typing.Union[str, os.PathLike],
        mode: str = 'r',
        s3_open_func: typing.Callable = megfile.s3.s3_buffered_open
) -> typing.Union[typing.IO[typing.AnyStr], io.BufferedReader, megfile.lib.
                  stdio_handler.STDReader, megfile.lib.stdio_handler.
                  STDWriter, io.TextIOWrapper]:
    '''
    Open a file on the path
    
    .. note ::
    
    On fs, the difference between this function and ``io.open`` is that this function create directories automatically, instead of raising FileNotFoundError
    
    Currently, supported protocols are:
    
    1. s3:      "s3://<bucket>/<key>"
    
    2. http(s): http(s) url
    
    3. stdio:   "stdio://-"
    
    4. FS file: Besides above mentioned protocols, other path are considered fs path
    
    Here are a few examples: ::
    
    >>> import cv2
    >>> import numpy as np
    >>> raw = smart_open('https://ss2.bdstatic.com/70cFvnSh_Q1YnxGkpoWK1HF6hhy/it/u=2275743969,3715493841&fm=26&gp=0.jpg').read()
    >>> img = cv2.imdecode(np.frombuffer(raw, np.uint8), cv2.IMREAD_ANYDEPTH | cv2.IMREAD_COLOR)
    
    :param path: Given path
    :param mode: Mode to open file, supports r'[rwa][tb]?\+?'
    :param s3_open_func: Function used to open s3_url. Require the function includes 2 necessary parameters, file path and mode
    :returns: File-Like object
    :raises: FileNotFoundError, IsADirectoryError, ValueError
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_open(path=path, mode=mode)
    if protocol == 's3':
        return s3_open(path=path, mode=mode, s3_open_func=s3_open_func)
    if protocol == 'http':
        return http_open(path=path, mode=mode)
    if protocol == 'stdio':
        return stdio_open(path=path, mode=mode)
    raise UnsupportedError(
        operation='smart_open', path=path)  # pragma: no cover


def smart_readlink(path: typing.Union[str, os.PathLike]
                  ) -> typing.Union[str, os.PathLike]:
    '''
    Return a string representing the path to which the symbolic link points.
    :param path: Path to be read
    :returns: Return a string representing the path to which the symbolic link points.
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_readlink(path=path)
    if protocol == 's3':
        return s3_readlink(path=path)
    raise UnsupportedError(
        operation='smart_readlink', path=path)  # pragma: no cover


def smart_remove(
        path: typing.Union[str, os.PathLike],
        missing_ok: bool = False,
        followlinks: bool = False) -> None:
    '''
    Remove the file or directory on s3 or fs, `s3://` and `s3://bucket` are not permitted to remove
    
    :param path: Given path
    :param missing_ok: if False and target file/directory not exists, raise FileNotFoundError
    :raises: PermissionError, FileNotFoundError
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_remove(
            path=path, missing_ok=missing_ok, followlinks=followlinks)
    if protocol == 's3':
        return s3_remove(path=path, missing_ok=missing_ok)
    raise UnsupportedError(
        operation='smart_remove', path=path)  # pragma: no cover


def smart_save_as(
        file_object: typing.BinaryIO,
        path: typing.Union[str, os.PathLike]) -> None:
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_save_as(file_object=file_object, path=path)
    if protocol == 's3':
        return s3_save_as(file_object=file_object, path=path)
    raise UnsupportedError(
        operation='smart_save_as', path=path)  # pragma: no cover


def smart_scan(
        path: typing.Union[str, os.PathLike],
        missing_ok: bool = True,
        followlinks: bool = False) -> typing.Iterator:
    '''
    Iteratively traverse only files in given directory, in alphabetical order.
    Every iteration on generator yields a path string.
    
    If path is a file path, yields the file only
    If path is a non-existent path, return an empty generator
    If path is a bucket path, return all file paths in the bucket
    
    :param path: Given path
    :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
    :raises: UnsupportedError
    :returns: A file path generator
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_scan(
            path=path, missing_ok=missing_ok, followlinks=followlinks)
    if protocol == 's3':
        return s3_scan(path=path, missing_ok=missing_ok)
    raise UnsupportedError(
        operation='smart_scan', path=path)  # pragma: no cover


def smart_scan_stat(
        path: typing.Union[str, os.PathLike],
        missing_ok: bool = True,
        followlinks: bool = False) -> typing.Iterator:
    '''
    Iteratively traverse only files in given directory, in alphabetical order.
    Every iteration on generator yields a tuple of path string and file stat
    
    :param path: Given path
    :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
    :raises: UnsupportedError
    :returns: A file path generator
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_scan_stat(
            path=path, missing_ok=missing_ok, followlinks=followlinks)
    if protocol == 's3':
        return s3_scan_stat(path=path, missing_ok=missing_ok)
    raise UnsupportedError(
        operation='smart_scan_stat', path=path)  # pragma: no cover


def smart_scandir(path: typing.Union[str, os.PathLike]) -> typing.Iterator:
    '''
    Get all content of given s3_url or file path.
    
    :param path: Given path
    :returns: An iterator contains all contents have prefix path
    :raises: FileNotFoundError, NotADirectoryError
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_scandir(path=path)
    if protocol == 's3':
        return s3_scandir(path=path)
    raise UnsupportedError(
        operation='smart_scandir', path=path)  # pragma: no cover


def smart_stat(
        path: typing.Union[str, os.PathLike]) -> megfile.pathlike.StatResult:
    '''
    Get StatResult of s3_url or file path
    
    :param path: Given path
    :returns: StatResult
    :raises: FileNotFoundError
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_stat(path=path)
    if protocol == 's3':
        return s3_stat(path=path)
    if protocol == 'http':
        return http_stat(path=path)
    raise UnsupportedError(
        operation='smart_stat', path=path)  # pragma: no cover


def smart_symlink(
        dst_path: typing.Union[str, os.PathLike],
        src_path: typing.Union[str, os.PathLike]) -> None:
    '''
    Create a symbolic link pointing to src_path named path.
    
    :param dst_path: Desination path
    :param src_path: Source path
    '''
    protocol = _extract_protocol(src_path)
    if protocol == 'fs':
        return fs_symlink(dst_path=dst_path, src_path=src_path)
    if protocol == 's3':
        return s3_symlink(dst_path=dst_path, src_path=src_path)
    raise UnsupportedError(
        operation='smart_symlink', path=src_path)  # pragma: no cover


def smart_unlink(
        path: typing.Union[str, os.PathLike], missing_ok: bool = False) -> None:
    '''
    Remove the file on s3 or fs
    
    :param path: Given path
    :param missing_ok: if False and target file not exists, raise FileNotFoundError
    :raises: PermissionError, FileNotFoundError, IsADirectoryError
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_unlink(path=path, missing_ok=missing_ok)
    if protocol == 's3':
        return s3_unlink(path=path, missing_ok=missing_ok)
    raise UnsupportedError(
        operation='smart_unlink', path=path)  # pragma: no cover


def smart_walk(
        path: typing.Union[str, os.PathLike],
        followlinks: bool = False) -> typing.Iterator:
    '''
    Generate the file names in a directory tree by walking the tree top-down.
    For each directory in the tree rooted at directory path (including path itself),
    it yields a 3-tuple (root, dirs, files).
    
    root: a string of current path
    dirs: name list of subdirectories (excluding '.' and '..' if they exist) in 'root'. The list is sorted by ascending alphabetical order
    files: name list of non-directory files (link is regarded as file) in 'root'. The list is sorted by ascending alphabetical order
    
    If path not exists, return an empty generator
    If path is a file, return an empty generator
    If try to apply walk() on unsupported path, raise UnsupportedError
    
    :param path: Given path
    :raises: UnsupportedError
    :returns: A 3-tuple generator
    '''
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fs_walk(path=path, followlinks=followlinks)
    if protocol == 's3':
        return s3_walk(path=path)
    raise UnsupportedError(
        operation='smart_walk', path=path)  # pragma: no cover


_copy_funcs = {
    's3': {
        's3': s3_copy,
        'fs': s3_download
    },
    'fs': {
        's3': s3_upload,
        'fs': fs_copy,
    }
}


def register_copy_func(
        src_protocol: str,
        dst_protocol: str,
        copy_func: typing.Optional[typing.Callable[
            [str, str, typing.Optional[typing.
                                       Callable[[int], None]]], None]] = None,
) -> None:
    '''
    Used to register copy func between protocols, and do not allow duplicate registration

    :param src_protocol: protocol name of source file, e.g. 's3'
    :param dst_protocol: protocol name of destination file, e.g. 's3'
    :param copy_func: copy func, its type is: typing.Callable[[str, str, typing.Optional[typing.Callable[[int], None]]], None]
    '''
    try:
        _copy_funcs[src_protocol][dst_protocol]
    except KeyError:
        dst_dict = _copy_funcs.get(src_protocol, {})
        dst_dict[dst_protocol] = copy_func
        _copy_funcs[src_protocol] = dst_dict
    except Exception as error:  # pragma: no cover
        raise error
    else:
        raise ValueError(
            'Copy Function has already existed: {}->{}'.format(
                src_protocol, dst_protocol))


def _default_copy_func(
        src_path: PathLike,
        dst_path: PathLike,
        callback: typing.Optional[typing.Callable[[int], None]] = None) -> None:
    with smart_open(src_path, 'rb') as fsrc:
        with smart_open(dst_path, 'wb') as fdst:
            # This magic number is copied from  copyfileobj
            length = 16 * 1024
            while True:
                buf = fsrc.read(length)  # type: ignore
                if not buf:
                    break
                fdst.write(buf)  # type: ignore
                if callback is None:
                    continue
                callback(len(buf))


def _extract_protocol(path: typing.Union[PathLike, int]) -> str:
    if isinstance(path, int):
        protocol = "fs"
    elif isinstance(path, str):
        protocol = urlsplit(path).scheme
        if protocol == "":
            protocol = "fs"
    elif isinstance(path, PurePath):
        protocol = "fs"
    else:
        raise ProtocolNotFoundError('protocol not found: %r' % path)
    if protocol == 'https':
        protocol = 'http'
    return protocol


def get_traditional_path(path: PathLike):
    return fspath(path)


def smart_rename(
        src_path: PathLike, dst_path: PathLike,
        followlinks: bool = False) -> None:
    '''
    Move file on s3 or fs. `s3://` or `s3://bucket` is not allowed to move

    :param src_path: Given source path
    :param dst_path: Given destination path
    '''
    if smart_isdir(src_path, followlinks=followlinks):
        raise IsADirectoryError('%r is a directory' % PathLike)
    src_protocol = _extract_protocol(src_path)
    dst_protocol = _extract_protocol(dst_path)
    if src_protocol == dst_protocol:
        if src_protocol == 'fs':
            fs_rename(src_path, dst_path)
        elif src_protocol == 's3':
            s3_rename(src_path, dst_path)
        else:
            raise UnsupportedError(
                operation='smart_rename', path=src_path)  # pragma: no cover
        return
    smart_copy(src_path, dst_path)
    smart_unlink(src_path)


def smart_copy(
        src_path: PathLike,
        dst_path: PathLike,
        callback: typing.Optional[typing.Callable[[int], None]] = None,
        followlinks: bool = False) -> None:
    '''
    Copy file from source path to destination path

    Here are a few examples: ::

        >>> from tqdm import tqdm
        >>> from megfile import smart_copy, smart_stat
        >>> class Bar:
        ...     def __init__(self, total=10):
        ...         self._bar = tqdm(total=10)
        ...
        ...     def __call__(self, bytes_num):
        ...         self._bar.update(bytes_num)
        ...
        >>> src_path = 'test.png'
        >>> dst_path = 'test1.png'
        >>> smart_copy(src_path, dst_path, callback=Bar(total=smart_stat(src_path).size), followlinks=False)
        856960it [00:00, 260592384.24it/s]

    :param src_path: Given source path
    :param dst_path: Given destination path
    :param callback: Called periodically during copy, and the input parameter is the data size (in bytes) of copy since the last call
    :param followlinks: False if regard symlink as file, else True
    '''
    # this function contains plenty of manual polymorphism
    if smart_islink(src_path) and is_s3(dst_path) and not followlinks:
        return

    src_protocol = _extract_protocol(src_path)
    dst_protocol = _extract_protocol(dst_path)

    try:
        copy_func = _copy_funcs[src_protocol][dst_protocol]
    except KeyError:
        copy_func = _default_copy_func
    if copy_func == fs_copy:
        fs_copy(src_path, dst_path, callback=callback, followlinks=followlinks)
    else:
        copy_func(src_path, dst_path, callback=callback)  # pytype: disable=wrong-keyword-args


def smart_sync(
        src_path: PathLike,
        dst_path: PathLike,
        callback: typing.Optional[typing.Callable[[str, int], None]] = None,
        followlinks: bool = False) -> None:
    '''
    Sync file or directory on s3 and fs

    .. note ::

        When the parameter is file, this function bahaves like ``smart_copy``.

        If file and directory of same name and same level, sync consider it's file first.

    Here are a few examples: ::

        >>> from tqdm import tqdm
        >>> from threading import Lock
        >>> from megfile import smart_sync, smart_stat, smart_glob
        >>> class Bar:
        ...     def __init__(self, total_file):
        ...         self._total_file = total_file
        ...         self._bar = None
        ...         self._now = None
        ...         self._file_index = 0
        ...         self._lock = Lock()
        ...     def __call__(self, path, num_bytes):
        ...         with self._lock:
        ...             if path != self._now:
        ...                 self._file_index += 1
        ...                 print("copy file {}/{}:".format(self._file_index, self._total_file))
        ...                 if self._bar:
        ...                     self._bar.close()
        ...                 self._bar = tqdm(total=smart_stat(path).size)
        ...                 self._now = path
        ...            self._bar.update(num_bytes)
        >>> total_file = len(list(smart_glob('src_path')))
        >>> smart_sync('src_path', 'dst_path', callback=Bar(total_file=total_file))

    :param src_path: Given source path
    :param dst_path: Given destination path
    :param callback: Called periodically during copy, and the input parameter is the data size (in bytes) of copy since the last call
    '''
    src_path, dst_path = get_traditional_path(src_path), get_traditional_path(
        dst_path)
    for src_file_path in smart_scan(src_path, followlinks=followlinks):
        content_path = src_file_path[len(src_path):]
        if len(content_path):
            content_path = content_path.lstrip('/')
            dst_abs_file_path = smart_path_join(dst_path, content_path)
        else:
            # if content_path is empty, which means smart_isfile(src_path) is True, this function is equal to smart_copy
            dst_abs_file_path = dst_path
        copy_callback = partial(callback, src_file_path) if callback else None
        smart_copy(
            src_file_path,
            dst_abs_file_path,
            callback=copy_callback,
            followlinks=followlinks)


def smart_move(
        src_path: PathLike, dst_path: PathLike,
        followlinks: bool = False) -> None:
    '''
    Move file/directory on s3 or fs. `s3://` or `s3://bucket` is not allowed to move

    :param src_path: Given source path
    :param dst_path: Given destination path
    '''
    src_protocol = _extract_protocol(src_path)
    dst_protocol = _extract_protocol(dst_path)
    if src_protocol == dst_protocol:
        if src_protocol == 'fs':
            fs_rename(src_path, dst_path)
        elif src_protocol == 's3':
            s3_rename(src_path, dst_path)
        else:
            raise UnsupportedError(
                operation='smart_move', path=src_path)  # pragma: no cover
        return
    smart_sync(src_path, dst_path, followlinks=followlinks)
    smart_remove(src_path, followlinks=followlinks)


def _group_glob(globstr: str) -> typing.List[str]:
    '''
    Split path, and group them by protocol, return the glob list of same group.

    :param globstr: A glob string
    :returns: A glob list after being grouped by protocol
    '''
    glob_dict = defaultdict(list)
    expanded_glob = ungloblize(globstr)

    for single_glob in expanded_glob:
        protocol = _extract_protocol(single_glob)
        glob_dict[protocol].append(single_glob)

    group_glob_list = []

    for protocol, glob_list in glob_dict.items():
        group_glob_list.append(globlize(glob_list))
    return group_glob_list


def smart_glob(path: PathLike, recursive: bool = True,
               missing_ok: bool = True) -> typing.List[str]:
    '''
    Given path may contain shell wildcard characters, return path list in ascending alphabetical order, in which path matches glob pattern

    :param path: A path pattern may contain shell wildcard characters
    :param recursive: If False, this function will not glob recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    '''
    result = []
    group_glob_list = _group_glob(str(path))
    for glob_path in group_glob_list:
        protocol = _extract_protocol(glob_path)
        if protocol == 'fs':
            glob_path_list = fs_glob(glob_path, recursive, missing_ok)
        elif protocol == 's3':
            glob_path_list = s3_glob(glob_path, recursive, missing_ok)
        else:
            raise UnsupportedError(
                operation='smart_glob', path=path)  # pragma: no cover
        result.extend(glob_path_list)
    return result


def smart_iglob(
        path: PathLike, recursive: bool = True,
        missing_ok: bool = True) -> typing.Iterator[str]:
    '''
    Given path may contain shell wildcard characters, return path iterator in ascending alphabetical order, in which path matches glob pattern

    :param path: A path pattern may contain shell wildcard characters
    :param recursive: If False, this function will not glob recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    '''
    result = []
    group_glob_list = _group_glob(str(path))
    for glob_path in group_glob_list:
        protocol = _extract_protocol(glob_path)
        if protocol == 'fs':
            iglob_path = fs_iglob(glob_path, recursive, missing_ok)
        elif protocol == 's3':
            iglob_path = s3_iglob(glob_path, recursive, missing_ok)
        else:
            raise UnsupportedError(
                operation='smart_iglob', path=path)  # pragma: no cover
        result.append(iglob_path)
    iterableres = chain(*result)
    return iterableres


def smart_glob_stat(
        path: PathLike, recursive: bool = True,
        missing_ok: bool = True) -> typing.Iterator[FileEntry]:
    '''
    Given path may contain shell wildcard characters, return a list contains tuples of path and file stat in ascending alphabetical order, in which path matches glob pattern

    :param path: A path pattern may contain shell wildcard characters
    :param recursive: If False, this function will not glob recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    '''
    result = []
    group_glob_list = _group_glob(str(path))
    for glob_path in group_glob_list:
        protocol = _extract_protocol(glob_path)
        if protocol == 'fs':
            stat = fs_glob_stat(glob_path, recursive, missing_ok)
        elif protocol == 's3':
            stat = s3_glob_stat(glob_path, recursive, missing_ok)
        else:
            raise UnsupportedError(
                operation='smart_glob_stat', path=path)  # pragma: no cover
        result.append(stat)
    iterableres = chain(*result)
    return iterableres


def smart_combine_open(
        path_glob: str, mode: str = 'rb',
        open_func=smart_open) -> CombineReader:
    '''Open a unified reader that supports multi file reading。

    :param path_glob: A path may contain shell wildcard characters
    :param mode: Mode to open file, supports 'rb'
    :returns: A ```CombineReader```
    '''
    file_objects = list(
        open_func(path, mode) for path in sorted(smart_glob(path_glob)))
    return combine(file_objects, path_glob)


def smart_save_content(path: PathLike, content: bytes) -> None:
    '''Save bytes content to specified path

    param path: Path to save content
    '''
    with smart_open(path, 'wb') as fd:
        fd.write(content)  # type: ignore


def smart_load_text(path: PathLike) -> str:
    '''
    Read content from path

    param path: Path to be read
    '''
    with smart_open(path) as fd:
        return fd.read()  # type: ignore


def smart_save_text(path: PathLike, text: str) -> None:
    '''Save text to specified path

    param path: Path to save text
    '''
    with smart_open(path, 'w') as fd:
        fd.write(text)  # type: ignore


def smart_cache(path, s3_cacher=S3Cacher, **options):
    '''Return a path to Posixpath Interface

    param path: Path to cache
    param s3_cacher: Cacher for s3 path
    param options: Optional arguments for s3_cacher
    '''
    if is_s3(path):
        return s3_cacher(path, **options)
    return NullCacher(path)


def smart_touch(path: PathLike):
    '''Create a new file on path

    param path: Path to create file
    '''
    with smart_open(path, 'w'):
        pass


def smart_load_content(
        path: PathLike,
        start: typing.Optional[int] = None,
        stop: typing.Optional[int] = None) -> bytes:
    '''
    Get specified file from [start, stop) in bytes

    :param path: Specified path
    :param start: start index
    :param stop: stop index
    :returns: bytes content in range [start, stop)
    '''
    if is_s3(path):
        return s3_load_content(path, start, stop)

    start, stop = get_content_offset(start, stop, fs_getsize(path))

    with open(path, 'rb') as fd:
        fd.seek(start)
        return fd.read(stop - start)


def smart_path_join(
        path: typing.Union[str, os.PathLike],
        *other_paths: typing.Union[str, os.PathLike]) -> str:
    protocol = _extract_protocol(path)
    if protocol == 'fs':
        return fspath(os.path.normpath(fs_path_join(path, *other_paths)))
    if protocol == 's3':
        return fspath(s3_path_join(path, *other_paths))
    raise UnsupportedError(
        operation='smart_path_join', path=path)  # pragma: no cover

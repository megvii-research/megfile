import io
import os
import sys
from collections import defaultdict
from functools import partial
from inspect import cleandoc
from itertools import chain
from typing import IO, AnyStr, BinaryIO, Callable, Iterator, List, Optional, Tuple

from megfile.fs import fs_copy, fs_getsize, fs_scandir
from megfile.interfaces import Access, FileEntry, MegfilePathLike, NullCacher, StatResult
from megfile.lib.combine_reader import CombineReader
from megfile.lib.compat import fspath
from megfile.lib.fakefs import FakefsCacher
from megfile.lib.get_image_size import Image, get_image_metadata_from_bytesio
from megfile.lib.glob import globlize, ungloblize
from megfile.s3 import is_s3, s3_copy, s3_download, s3_load_content, s3_open, s3_upload
from megfile.smart_path import SmartPath, get_traditional_path
from megfile.utils import combine, get_content_offset

__all__ = [
    'smart_access',
    'smart_cache',
    'smart_combine_open',
    'smart_copy',
    'smart_exists',
    'smart_getmtime',
    'smart_getsize',
    'smart_glob_stat',
    'smart_glob',
    'smart_iglob',
    'smart_isdir',
    'smart_isfile',
    'smart_islink',
    'smart_listdir',
    'smart_load_content',
    'smart_save_content',
    'smart_load_from',
    'smart_load_image_metadata',
    'smart_load_text',
    'smart_save_text',
    'smart_makedirs',
    'smart_open',
    'smart_path_join',
    'smart_remove',
    'smart_move',
    'smart_rename',
    'smart_save_as',
    'smart_scan_stat',
    'smart_scan',
    'smart_scandir',
    'smart_stat',
    'smart_sync',
    'smart_touch',
    'smart_unlink',
    'smart_walk',
    'smart_getmd5',
    'smart_realpath',
    'smart_ismount',
    'smart_relpath',
    'smart_abspath',
    'smart_isabs',
    'register_copy_func',
    'IMAGE_EXTNAMES',
]

IMAGE_EXTNAMES = [".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".webp"]


def smart_isdir(path: MegfilePathLike) -> bool:
    '''
    Test if a file path or an s3 url is directory

    :param path: Path to be tested
    :returns: True if path is directory, else False
    '''
    return SmartPath(path).is_dir()


def smart_isfile(path: MegfilePathLike) -> bool:
    '''
    Test if a file path or an s3 url is file

    :param path: Path to be tested
    :returns: True if path is file, else False
    '''
    return SmartPath(path).is_file()


def smart_islink(path: MegfilePathLike) -> bool:
    return SmartPath(path).is_symlink()


def smart_access(path: MegfilePathLike, mode: Access) -> bool:
    '''
    Test if path has access permission described by mode

    :param path: Path to be tested
    :param mode: Access mode(Access.READ, Access.WRITE, Access.BUCKETREAD, Access.BUCKETWRITE)
    :returns: bool, if the path has read/write access.
    '''
    return SmartPath(path).access(mode)


def smart_exists(path: MegfilePathLike) -> bool:
    '''
    Test if path or s3_url exists

    :param path: Path to be tested
    :returns: True if path eixsts, else False
    '''
    return SmartPath(path).exists()


def smart_listdir(path: Optional[MegfilePathLike] = None) -> List[str]:
    '''
    Get all contents of given s3_url or file path. The result is in acsending alphabetical order.

    :param path: Given path
    :returns: All contents of given s3_url or file path in acsending alphabetical order.
    :raises: FileNotFoundError, NotADirectoryError
    '''
    if path is None:
        return sorted(os.listdir(path))
    return SmartPath(path).listdir()


def smart_scandir(path: Optional[MegfilePathLike] = None
                 ) -> Iterator[FileEntry]:
    '''
    Get all content of given s3_url or file path.

    :param path: Given path
    :returns: An iterator contains all contents have prefix path
    :raises: FileNotFoundError, NotADirectoryError
    '''
    if path is None:
        return fs_scandir(path)
    return SmartPath(path).scandir()


def smart_getsize(path: MegfilePathLike) -> int:
    '''
    Get file size on the given s3_url or file path (in bytes). 
    If the path in a directory, return the sum of all file size in it, including file in subdirectories (if exist).
    The result exludes the size of directory itself. In other words, return 0 Byte on an empty directory path.

    :param path: Given path
    :returns: File size
    :raises: FileNotFoundError
    '''
    return SmartPath(path).getsize()


def smart_getmtime(path: MegfilePathLike) -> float:
    '''
    Get last-modified time of the file on the given s3_url or file path (in Unix timestamp format).
    If the path is an existent directory, return the latest modified time of all file in it. The mtime of empty directory is 1970-01-01 00:00:00
    
    :param path: Given path
    :returns: Last-modified time
    :raises: FileNotFoundError
    '''
    return SmartPath(path).getmtime()


def smart_stat(path: MegfilePathLike) -> StatResult:
    '''
    Get StatResult of s3_url or file path

    :param path: Given path
    :returns: StatResult
    :raises: FileNotFoundError
    '''
    return SmartPath(path).stat()


_copy_funcs = {
    's3': {
        's3': s3_copy,
        'file': s3_download
    },
    'file': {
        's3': s3_upload,
        'file': fs_copy,
    }
}


def register_copy_func(
        src_protocol: str,
        dst_protocol: str,
        copy_func: Optional[
            Callable[[str, str, Optional[Callable[[int], None]]], None]] = None,
) -> None:
    '''
    Used to register copy func between protocols, and do not allow duplicate registration

    :param src_protocol: protocol name of source file, e.g. 's3'
    :param dst_protocol: protocol name of destination file, e.g. 's3'
    :param copy_func: copy func, its type is: Callable[[str, str, Optional[Callable[[int], None]]], None]
    '''
    try:
        _copy_funcs[src_protocol][dst_protocol]
    except KeyError:
        dst_dict = _copy_funcs.get(src_protocol, {})
        dst_dict[dst_protocol] = copy_func
        _copy_funcs[src_protocol] = dst_dict
    except Exception as error:
        raise error
    else:
        raise ValueError(
            'Copy Function has already existed: {}->{}'.format(
                src_protocol, dst_protocol))


def _default_copy_func(
        src_path: MegfilePathLike,
        dst_path: MegfilePathLike,
        callback: Optional[Callable[[int], None]] = None) -> None:
    with smart_open(src_path, 'rb') as fsrc:
        with smart_open(dst_path, 'wb') as fdst:
            # This magic number is copied from  copyfileobj
            length = 16 * 1024
            while True:
                buf = fsrc.read(length)
                if not buf:
                    break
                fdst.write(buf)
                if callback is None:
                    continue
                callback(len(buf))


def smart_copy(
        src_path: MegfilePathLike,
        dst_path: MegfilePathLike,
        callback: Optional[Callable[[int], None]] = None) -> None:
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
        >>> smart_copy(src_path, dst_path, callback=Bar(total=smart_stat(src_path).size))
        856960it [00:00, 260592384.24it/s]

    :param src_path: Given source path
    :param dst_path: Given destination path
    :param callback: Called periodically during copy, and the input parameter is the data size (in bytes) of copy since the last call
    '''
    # this function contains plenty of mannual polymorphism
    if smart_islink(src_path) and is_s3(dst_path):
        return

    src_protocol, _ = SmartPath._extract_protocol(src_path)
    dst_protocol, _ = SmartPath._extract_protocol(dst_path)

    try:
        copy_func = _copy_funcs[src_protocol][dst_protocol]
    except KeyError:
        copy_func = _default_copy_func
    copy_func(src_path, dst_path, callback=callback)  # pytype: disable=wrong-keyword-args


def smart_sync(
        src_path: MegfilePathLike,
        dst_path: MegfilePathLike,
        callback: Optional[Callable[[str, int], None]] = None) -> None:
    '''
    Sync file or directory on s3 and fs

    .. note ::

        When the paramter is file, this function bahaves like ``smart_copy``.

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
    for src_file_path in smart_scan(src_path):
        content_path = src_file_path[len(src_path):]
        if len(content_path):
            content_path = content_path.lstrip('/')
            dst_abs_file_path = smart_path_join(dst_path, content_path)
        else:
            # if content_path is empty, which means smart_isfile(src_path) is True, this function is equal to smart_copy
            dst_abs_file_path = dst_path
        copy_callback = partial(callback, src_file_path) if callback else None
        smart_copy(src_file_path, dst_abs_file_path, callback=copy_callback)


def smart_remove(path: MegfilePathLike, missing_ok: bool = False) -> None:
    '''
    Remove the file or directory on s3 or fs, `s3://` and `s3://bucket` are not permitted to remove

    :param path: Given path
    :param missing_ok: if False and target file/directory not exists, raise FileNotFoundError
    :raises: PermissionError, FileNotFoundError
    '''
    SmartPath(path).remove(missing_ok=missing_ok)


def smart_rename(src_path: MegfilePathLike, dst_path: MegfilePathLike) -> None:
    '''
    Move file on s3 or fs. `s3://` or `s3://bucket` is not allowed to move

    :param src_path: Given source path
    :param dst_path: Given destination path
    '''
    if smart_isdir(src_path):
        raise IsADirectoryError('%r is a directory' % MegfilePathLike)
    src_protocol, _ = SmartPath._extract_protocol(src_path)
    dst_protocol, _ = SmartPath._extract_protocol(dst_path)
    if src_protocol == dst_protocol:
        SmartPath(src_path).rename(dst_path)
        return
    smart_copy(src_path, dst_path)
    smart_unlink(src_path)


def smart_move(src_path: MegfilePathLike, dst_path: MegfilePathLike) -> None:
    '''
    Move file/directory on s3 or fs. `s3://` or `s3://bucket` is not allowed to move

    :param src_path: Given source path
    :param dst_path: Given destination path
    '''
    src_protocol, _ = SmartPath._extract_protocol(src_path)
    dst_protocol, _ = SmartPath._extract_protocol(dst_path)
    if src_protocol == dst_protocol:
        SmartPath(src_path).rename(dst_path)
        return
    smart_sync(src_path, dst_path)
    smart_remove(src_path)


def smart_unlink(path: MegfilePathLike, missing_ok: bool = False) -> None:
    '''
    Remove the file on s3 or fs

    :param path: Given path
    :param missing_ok: if False and target file not exists, raise FileNotFoundError
    :raises: PermissionError, FileNotFoundError, IsADirectoryError
    '''
    SmartPath(path).unlink(missing_ok=missing_ok)


def smart_makedirs(path: MegfilePathLike, exist_ok: bool = False) -> None:
    '''
    Create a directory if is on fs.
    If on s3, it actually check if target exists, and check if bucket has WRITE access

    :param path: Given path
    :param missing_ok: if False and target directory not exists, raise FileNotFoundError
    :raises: PermissionError, FileExistsError
    '''
    SmartPath(path).makedirs(exist_ok)


def smart_open(
        path: MegfilePathLike,
        mode: str = 'r',
        s3_open_func: Callable[[str, str], BinaryIO] = s3_open,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        **options) -> IO[AnyStr]:
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
    :param s3_open_func: Function used to open s3_url. Require the function includes 2 neccessary parameters, file path and mode
    :returns: File-Like object
    :raises: FileNotFoundError, IsADirectoryError, ValueError
    '''
    options = {
        's3_open_func': s3_open_func,
        'encoding': encoding,
        'errors': errors,
        **options,
    }
    return SmartPath(path).open(mode, **options)


def smart_path_join(
        path: MegfilePathLike, *other_paths: MegfilePathLike) -> str:
    '''
    Concat 2 or more path to a complete path

    :param path: Given path
    :param other_paths: Paths to be concatenated
    :returns: Concatenated complete path

    .. note ::

        For URI, the difference between this function and ``os.path.join`` is that this funcion ignores left side slash (which indicates absolute path) in ``other_paths`` and will directly concat.
        e.g. os.path.join('s3://path', 'to', '/file') => '/file', and smart_path_join('s3://path', 'to', '/file') => '/path/to/file'
        But for fs path, this function behaves exactly like ``os.path.join``
        e.g. smart_path_join('/path', 'to', '/file') => '/file'
    '''
    return fspath(SmartPath(path).joinpath(*other_paths))


def smart_walk(path: MegfilePathLike
              ) -> Iterator[Tuple[str, List[str], List[str]]]:
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
    return SmartPath(path).walk()


def smart_scan(path: MegfilePathLike, missing_ok: bool = True) -> Iterator[str]:
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
    return SmartPath(path).scan(missing_ok)


def smart_scan_stat(path: MegfilePathLike,
                    missing_ok: bool = True) -> Iterator[FileEntry]:
    '''
    Iteratively traverse only files in given directory, in alphabetical order. 
    Every iteration on generator yields a tuple of path string and file stat

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
    :raises: UnsupportedError
    :returns: A file path generator
    '''
    return SmartPath(path).scan_stat(missing_ok)


def _group_glob(globstr: str) -> List[str]:
    '''
    Split pathname, and group them by protocol, return the glob list of same group.

    :param globstr: A glob string
    :returns: A glob list after being grouped by protocol
    '''
    glob_dict = defaultdict(list)
    expanded_glob = ungloblize(globstr)

    for single_glob in expanded_glob:
        protocol, _ = SmartPath._extract_protocol(single_glob)
        glob_dict[protocol].append(single_glob)

    group_glob_list = []

    for protocol, glob_list in glob_dict.items():
        group_glob_list.append(globlize(glob_list))
    return group_glob_list


def smart_glob(
        pathname: MegfilePathLike, recursive: bool = True,
        missing_ok: bool = True) -> List[str]:
    '''
    Given pathname may contain shell wildcard characters, return path list in ascending alphabetical order, in which path matches glob pattern

    :param pathname: A path pattern may contain shell wildcard characters
    :param recursive: If False, this function will not glob recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    '''
    # Split pathname, group by protocol, call glob respectively
    # SmartPath(pathname).glob(recursive, missing_ok)
    result = []
    group_glob_list = _group_glob(pathname)
    for glob_path in group_glob_list:
        result.extend(SmartPath(glob_path).glob(recursive, missing_ok))
    return result


def smart_iglob(
        pathname: MegfilePathLike, recursive: bool = True,
        missing_ok: bool = True) -> Iterator[str]:
    '''
    Given pathname may contain shell wildcard characters, return path iterator in ascending alphabetical order, in which path matches glob pattern

    :param pathname: A path pattern may contain shell wildcard characters
    :param recursive: If False, this function will not glob recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    '''
    # Split pathname, group by protocol, call glob respectively
    # SmartPath(pathname).glob(recursive, missing_ok)
    result = []
    group_glob_list = _group_glob(pathname)
    for glob_path in group_glob_list:
        result.append(SmartPath(glob_path).iglob(recursive, missing_ok))
    iterableres = chain(*result)
    return iterableres


def smart_glob_stat(
        pathname: MegfilePathLike, recursive: bool = True,
        missing_ok: bool = True) -> Iterator[FileEntry]:
    '''
    Given pathname may contain shell wildcard characters, return a list contains tuples of path and file stat in ascending alphabetical order, in which path matches glob pattern

    :param pathname: A path pattern may contain shell wildcard characters
    :param recursive: If False, this function will not glob recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    '''
    # Split pathname, group by protocol, call glob respectively
    # SmartPath(pathname).glob(recursive, missing_ok)
    result = []
    group_glob_list = _group_glob(pathname)
    for glob_path in group_glob_list:
        result.append(SmartPath(glob_path).glob_stat(recursive, missing_ok))
    iterableres = chain(*result)
    return iterableres


def smart_save_as(file_object: BinaryIO, path: MegfilePathLike) -> None:
    '''Write the opened binary stream to specified path, but the stream won't be closed

    :param file_object: Stream to be read
    :param path: Specified target path
    '''
    SmartPath(path).save(file_object)


def smart_load_from(path: MegfilePathLike) -> BinaryIO:
    '''Read all content in binary on specified path and write into memory

    User should close the BinaryIO manually

    :param path: Specified path
    :returns: BinaryIO
    '''
    return SmartPath(path).load()


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


def smart_abspath(path: MegfilePathLike):
    '''Return the absolute path of given path

    :param path: Given path
    :returns: Absolute path of given path
    '''
    return SmartPath(path).abspath()


def smart_realpath(path: MegfilePathLike):
    '''Return the real path of given path

    :param path: Given path
    :returns: Real path of given path
    '''
    return SmartPath(path).realpath()


def smart_relpath(path: MegfilePathLike, start=None):
    '''Return the relative path of given path

    :param path: Given path
    :param start
    :returns: Relative path from start
    '''
    return SmartPath(path).relpath(start)


def smart_isabs(path: MegfilePathLike) -> bool:
    '''Test whether a path is absolute

    :param path: Given path
    :returns: True if a path is absolute, else False
    '''
    return SmartPath(path).is_absolute()


def smart_ismount(path: MegfilePathLike) -> bool:
    '''Test whether a path is a mount point

    :param path: Given path
    :returns: True if a path is a mount point, else False
    '''
    return SmartPath(path).is_mount()


def smart_load_image_metadata(path: MegfilePathLike) -> Image:
    '''
    Get metadata of image, including:

        - path: str
        - type: str, one of ['BMP', 'GIF', 'ICO', 'JPEG', 'PNG', 'TIFF']
        - file_size: int, in bytes
        - width: int
        - height

    Supported protocol: ::

        - None
        - s3
        - http(s)

    :param path: Image path
    :return: Image metadata
    '''

    def _s3_read(path, mode='rb', **kwargs):
        if mode not in ('rb'):
            raise ValueError('unacceptable mode: %r' % mode)
        # metadata only needs the first 26 bits
        # Doesn't use s3_prefetch_open to avoid wasting download.
        # Doesn't use open-source smart_open, because we want to replace it in the future optimization
        return io.BytesIO(s3_load_content(path, 0, 26))

    with smart_open(path, 'rb', s3_open_func=_s3_read) as fd:
        size = smart_getsize(path)
        return get_image_metadata_from_bytesio(fd, size, path)


def smart_load_content(
        path: MegfilePathLike,
        start: Optional[int] = None,
        stop: Optional[int] = None) -> bytes:
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


def smart_save_content(path: MegfilePathLike, content: bytes) -> None:
    '''Save bytes content to specified path

    param path: Path to save content
    '''
    with smart_open(path, 'wb') as fd:
        fd.write(content)


def smart_load_text(path: MegfilePathLike) -> str:
    '''
    Read content from path

    param path: Path to be read
    '''
    with smart_open(path) as fd:
        return fd.read()


def smart_save_text(path: MegfilePathLike, text: str) -> None:
    '''Save text to specified path

    param path: Path to save text
    '''
    with smart_open(path, 'w') as fd:
        fd.write(text)


def smart_cache(path, s3_cacher=FakefsCacher, **options):
    '''Return a path to Posixpath Interface

    param path: Path to cache
    param s3_cacher: Cacher for s3 path
    param options: Optional arguments for s3_cacher
    '''
    if is_s3(path):
        return s3_cacher(path, **options)
    return NullCacher(path)


def smart_touch(path: MegfilePathLike):
    '''Create a new file on path

    param path: Path to create file
    '''
    with smart_open(path, 'w'):
        pass


def smart_getmd5(path: MegfilePathLike):
    '''Get md5 value of file

    param path: File path
    '''
    return SmartPath(path).md5()

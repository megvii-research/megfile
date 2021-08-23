import hashlib
import os
import shutil
from io import BytesIO
from stat import S_ISDIR as stat_isdir
from stat import S_ISLNK as stat_islnk
from typing import BinaryIO, Callable, Iterator, List, Optional, Tuple
from unittest.mock import patch
from urllib.parse import urlsplit

from megfile.errors import _create_missing_ok_generator
from megfile.interfaces import Access, FileEntry, MegfilePathLike, StatResult
from megfile.lib.compat import fspath
from megfile.lib.glob import iglob
from megfile.lib.joinpath import path_join

__all__ = [
    'fs_abspath',
    'fs_access',
    'fs_copy',
    'fs_exists',
    'fs_getmtime',
    'fs_getsize',
    'fs_glob_stat',
    'fs_glob',
    'fs_iglob',
    'fs_isabs',
    'fs_isdir',
    'fs_isfile',
    'fs_islink',
    'fs_ismount',
    'fs_listdir',
    'fs_load_from',
    'fs_makedirs',
    'fs_realpath',
    'fs_relpath',
    'fs_remove',
    'fs_rename',
    'fs_move',
    'fs_sync',
    'fs_save_as',
    'fs_scan_stat',
    'fs_scan',
    'fs_scandir',
    'fs_stat',
    'fs_unlink',
    'fs_walk',
    'is_fs',
    'fs_cwd',
    'fs_home',
    'fs_expanduser',
    'fs_resolve',
    'fs_getmd5',
    'StatResult',
    'fs_path_join',
]


def _make_stat(stat: os.stat_result) -> StatResult:
    return StatResult(
        size=stat.st_size,
        ctime=stat.st_ctime,
        mtime=stat.st_mtime,
        isdir=stat_isdir(stat.st_mode),
        islnk=stat_islnk(stat.st_mode),
        extra=stat,
    )


def is_fs(path: MegfilePathLike) -> bool:
    '''Test if a path is fs path

    :param path: Path to be tested
    :returns: True of a path is fs path, else False
    '''
    path = fspath(path)
    parts = urlsplit(path)
    return parts.scheme == '' or parts.scheme == 'file'


def fs_stat(path: MegfilePathLike) -> StatResult:
    '''
    Get StatResult of file on fs, including file size and mtime, referring to fs_getsize and fs_getmtime

    :param path: Given file path
    :returns: StatResult
    '''
    result = _make_stat(os.lstat(path))
    if result.islnk or not result.isdir:
        return result

    size = 0
    ctime = result.ctime
    mtime = result.mtime
    for root, _, files in os.walk(path):
        for filename in files:
            canonical_path = os.path.join(root, filename)
            stat = os.lstat(canonical_path)
            size += stat.st_size
            if ctime > stat.st_ctime:
                ctime = stat.st_ctime
            if mtime < stat.st_mtime:
                mtime = stat.st_mtime
    return result._replace(size=size, ctime=ctime, mtime=mtime)


def fs_getsize(path: MegfilePathLike) -> int:
    '''
    Get file size on the given file path (in bytes). 
    If the path in a directory, return the sum of all file size in it, including file in subdirectories (if exist).
    The result exludes the size of directory itself. In other words, return 0 Byte on an empty directory path.

    :param path: Given file path
    :returns: File size

    '''
    return fs_stat(path).size


def fs_getmtime(path: MegfilePathLike) -> float:
    '''
    Get last-modified time of the file on the given path (in Unix timestamp format).
    If the path is an existent directory, return the latest modified time of all file in it.

    :param path: Given file path
    :returns: last-modified time
    '''
    return fs_stat(path).mtime


def fs_isdir(path: MegfilePathLike) -> bool:
    '''
    Test if a path is directory

    .. note::

        The difference between this function and ``os.path.isdir`` is that this function regard symlink as file

    :param path: Given file path
    :returns: True if the path is a directory, else False

    '''
    if os.path.islink(path):
        return False
    return os.path.isdir(path)


def fs_isfile(path: MegfilePathLike) -> bool:
    '''
    Test if a path is file

    .. note::

        The difference between this function and ``os.path.isfile`` is that this function regard symlink as file

    :param path: Given file path
    :returns: True if the path is a file, else False

    '''
    if os.path.islink(path):
        return True
    return os.path.isfile(path)


def fs_exists(path: MegfilePathLike) -> bool:
    '''
    Test if the path exists

    .. note::

        The difference between this function and ``os.path.exists`` is that this function regard symlink as file.
        In other words, this function is equal to ``os.path.lexists``

    :param path: Given file path
    :returns: True if the path exists, else False

    '''
    return os.path.lexists(path)


def fs_remove(path: MegfilePathLike, missing_ok: bool = False) -> None:
    '''
    Remove the file or directory on fs

    :param path: Given path
    :param missing_ok: if False and target file/directory not exists, raise FileNotFoundError
    '''
    if missing_ok and not fs_exists(path):
        return
    if fs_isdir(path):
        shutil.rmtree(path)
    else:
        os.remove(path)


def fs_unlink(path: MegfilePathLike, missing_ok: bool = False) -> None:
    '''
    Remove the file on fs

    :param path: Given path
    :param missing_ok: if False and target file not exists, raise FileNotFoundError
    '''
    if missing_ok and not fs_exists(path):
        return
    os.unlink(path)


def fs_makedirs(path: MegfilePathLike, exist_ok: bool = False):
    '''
    make a directory on fs, including parent directory

    If there exists a file on the path, raise FileExistsError

    :param path: Given path
    :param exist_ok: If False and target directory exists, raise FileExistsError
    :raises: FileExistsError
    '''
    if exist_ok and path == '':
        return
    return os.makedirs(path, exist_ok=exist_ok)


def fs_path_join(path: MegfilePathLike, *other_paths: MegfilePathLike) -> str:
    return path_join(fspath(path), *map(fspath, other_paths))


def fs_walk(path: MegfilePathLike
           ) -> Iterator[Tuple[str, List[str], List[str]]]:
    '''
    Generate the file names in a directory tree by walking the tree top-down.
    For each directory in the tree rooted at directory path (including path itself), 
    it yields a 3-tuple (root, dirs, files).

    root: a string of current path
    dirs: name list of subdirectories (excluding '.' and '..' if they exist) in 'root'. The list is sorted by ascending alphabetical order
    files: name list of non-directory files (link is regarded as file) in 'root'. The list is sorted by ascending alphabetical order

    If path not exists, or path is a file (link is regarded as file), return an empty generator

    :param path: A fs directory path
    :returns: A 3-tuple generator
    '''
    if not fs_exists(path):
        return

    if fs_isfile(path):
        return

    path = fspath(path)
    path = os.path.normpath(path)

    stack = [path]
    while stack:
        root = stack.pop()
        dirs, files = [], []
        for entry in os.scandir(root):
            name = fspath(entry.name)
            if entry.is_file() or entry.is_symlink():
                files.append(name)
            else:
                dirs.append(name)

        dirs = sorted(dirs)
        files = sorted(files)

        yield root, dirs, files

        stack.extend(
            (os.path.join(root, directory) for directory in reversed(dirs)))


def fs_scandir(path: MegfilePathLike) -> Iterator[FileEntry]:
    '''
    Get all content of given file path.

    :param path: Given path
    :returns: An iterator contains all contents have prefix path
    '''
    for entry in os.scandir(path):
        yield FileEntry(entry.path, _make_stat(entry.stat()))


def _fs_scan(pathname: MegfilePathLike,
             missing_ok: bool = True) -> Iterator[str]:
    if fs_isfile(pathname):
        path = fspath(pathname)
        yield path

    for root, _, files in fs_walk(pathname):
        for filename in files:
            yield os.path.join(root, filename)


def fs_scan(pathname: MegfilePathLike,
            missing_ok: bool = True) -> Iterator[str]:
    '''
    Iteratively traverse only files in given directory, in alphabetical order. 
    Every iteration on generator yields a path string.

    If path is a file path, yields the file only
    If path is a non-existent path, return an empty generator
    If path is a bucket path, return all file paths in the bucket

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
    :returns: A file path generator
    '''
    return _create_missing_ok_generator(
        _fs_scan(pathname), missing_ok,
        FileNotFoundError('No match file: %r' % pathname))


def fs_scan_stat(pathname: MegfilePathLike,
                 missing_ok: bool = True) -> Iterator[FileEntry]:
    '''
    Iteratively traverse only files in given directory, in alphabetical order. 
    Every iteration on generator yields a tuple of path string and file stat

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
    :returns: A file path generator
    '''
    for path in _fs_scan(pathname):
        yield FileEntry(path, _make_stat(os.lstat(path)))
    else:
        if missing_ok:
            return
        raise FileNotFoundError('No match file: %r' % pathname)


def fs_glob(
        pathname: MegfilePathLike, recursive: bool = True,
        missing_ok: bool = True) -> List[str]:
    '''Return path list in ascending alphabetical order, in which path matches glob pattern

    1. If doesn't match any path, return empty list
        Notice： ``glob.glob`` in standard library returns ['a/'] instead of empty list when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist. fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
    2. No guarantee that each path in result is different, which means:
        Assume there exists a path `/a/b/c/b/d.txt`
        use path pattern like `/**/b/**/*.txt` to glob, the path above will be returned twice
    3. `**` will match any matched file, directory, symlink and '' by default, when recursive is `True`
    4. fs_glob returns same as glob.glob(pathname, recursive=True) in acsending alphabetical order.
    5. Hidden files (filename stars with '.') will not be found in the result

    :param pathname: A path pattern may contain shell wildcard characters
    :param recursive: If False，`**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    :returns: A list contains paths match `pathname`
    '''
    return list(fs_iglob(pathname, recursive=recursive, missing_ok=missing_ok))


def fs_iglob(
        pathname: MegfilePathLike, recursive: bool = True,
        missing_ok: bool = True) -> Iterator[str]:
    '''Return path iterator in ascending alphabetical order, in which path matches glob pattern

    1. If doesn't match any path, return empty list
        Notice： ``glob.glob`` in standard library returns ['a/'] instead of empty list when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist. fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
    2. No guarantee that each path in result is different, which means:
        Assume there exists a path `/a/b/c/b/d.txt`
        use path pattern like `/**/b/**/*.txt` to glob, the path above will be returned twice
    3. `**` will match any matched file, directory, symlink and '' by default, when recursive is `True`
    4. fs_glob returns same as glob.glob(pathname, recursive=True) in acsending alphabetical order.
    5. Hidden files (filename stars with '.') will not be found in the result

    :param pathname: A path pattern may contain shell wildcard characters
    :param recursive: If False，`**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    :returns: An iterator contains paths match `pathname`
    '''
    return _create_missing_ok_generator(
        iglob(fspath(pathname), recursive=recursive), missing_ok,
        FileNotFoundError('No match file: %r' % pathname))


def fs_glob_stat(
        pathname: MegfilePathLike, recursive: bool = True,
        missing_ok: bool = True) -> Iterator[FileEntry]:
    '''Return a list contains tuples of path and file stat, in ascending alphabetical order, in which path matches glob pattern

    1. If doesn't match any path, return empty list
        Notice： ``glob.glob`` in standard library returns ['a/'] instead of empty list when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist. fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
    2. No guarantee that each path in result is different, which means:
        Assume there exists a path `/a/b/c/b/d.txt`
        use path pattern like `/**/b/**/*.txt` to glob, the path above will be returned twice
    3. `**` will match any matched file, directory, symlink and '' by default, when recursive is `True`
    4. fs_glob returns same as glob.glob(pathname, recursive=True) in acsending alphabetical order.
    5. Hidden files (filename stars with '.') will not be found in the result

    :param pathname: A path pattern may contain shell wildcard characters
    :param recursive: If False，`**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    :returns: A list contains tuples of path and file stat, in which paths match `pathname`
    '''
    for path in fs_iglob(pathname, recursive=recursive, missing_ok=missing_ok):
        yield FileEntry(path, _make_stat(os.lstat(path)))


def fs_save_as(file_object: BinaryIO, path: MegfilePathLike) -> None:
    '''Write the opened binary stream to path
    If parent directory of path doesn't exist, it will be created.

    :param file_object: stream to be read
    :param path: Specified target path
    '''
    fs_makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as output:
        output.write(file_object.read())


def fs_load_from(path: MegfilePathLike) -> BinaryIO:
    '''Read all content on specified path and write into memory

    User should close the BinaryIO manually

    :param path: Specified path
    :returns: Binary stream
    '''
    with open(path, 'rb') as f:
        data = f.read()
    return BytesIO(data)


def _copyfile(
        src_path: MegfilePathLike,
        dst_path: MegfilePathLike,
        callback: Optional[Callable[[int], None]] = None):

    def _patch_copyfileobj(callback=None):

        def _copyfileobj(fsrc, fdst, length=16 * 1024):
            """copy data from file-like object fsrc to file-like object fdst"""
            while 1:
                buf = fsrc.read(length)
                if not buf:
                    break
                fdst.write(buf)
                if callback is None:
                    continue
                callback(len(buf))

        return _copyfileobj

    src_stat = fs_stat(src_path)
    if src_stat.is_symlink():
        shutil.copyfile(src_path, dst_path, follow_symlinks=False)
        if callback:
            callback(src_stat.size)
        return

    with patch('shutil.copyfileobj', _patch_copyfileobj(callback)):
        shutil.copyfile(src_path, dst_path)


def fs_copy(
        src_path: MegfilePathLike,
        dst_path: MegfilePathLike,
        callback: Optional[Callable[[int], None]] = None):
    ''' File copy on file system
    Copy content (excluding meta date) of file on `src_path` to `dst_path`. `dst_path` must be a complete file name

    .. note ::

        The differences between this function and shutil.copyfile are:

            1. If parent directory of dst_path doesn't exist, create it

            2. Allow callback function, None by default. callback: Optional[Callable[[int], None]],  

        the int data is menas the size (in bytes) of the written data that is passed periodically

            3. This function is thread-unsafe

    TODO: get shutil implementation, to make fs_copy thread-safe
    :param src_path: Source file path
    :param dst_path: Target file path
    :param callback: Called periodically during copy, and the input parameter is the data size (in bytes) of copy since the last call
    '''
    try:
        _copyfile(src_path, dst_path, callback=callback)
    except FileNotFoundError as error:
        # Prevent the dst_path directory from being created when src_path does not exist
        if dst_path == error.filename:
            fs_makedirs(os.path.dirname(dst_path), exist_ok=True)
            _copyfile(src_path, dst_path, callback=callback)
        else:
            raise


def fs_sync(src_path: MegfilePathLike, dst_path: MegfilePathLike):
    '''Force write of everything to disk.

    :param src_path: Source file path
    :param dst_path: Target file path
    '''
    if fs_isdir(src_path):
        shutil.copytree(src_path, dst_path)
    else:
        fs_copy(src_path, dst_path)


def fs_listdir(path: MegfilePathLike) -> List[str]:
    '''
    Get all contents of given fs path. The result is in acsending alphabetical order.
 
    :param s3_url: Given path
    :returns: All contents have in the path in acsending alphabetical order
    '''
    return sorted(os.listdir(path))


def fs_islink(path: MegfilePathLike) -> bool:
    return os.path.islink(path)


def fs_isabs(path: MegfilePathLike) -> bool:
    '''Test whether a path is absolute

    :param path: Given path
    :returns: True if a path is absolute, else False
    '''
    return os.path.isabs(path)


def fs_ismount(path: MegfilePathLike) -> bool:
    '''Test whether a path is a mount point

    :param path: Given path
    :returns: True if a path is a mount point, else False
    '''
    return os.path.ismount(path)


def fs_abspath(path: MegfilePathLike) -> str:
    '''Return the absolute path of given path

    :param path: Given path
    :returns: Absolute path of given path
    '''
    return fspath(os.path.abspath(path))


def fs_realpath(path: MegfilePathLike) -> str:
    '''Return the real path of given path

    :param path: Given path
    :returns: Real path of given path
    '''
    return fspath(os.path.realpath(path))


def fs_relpath(path: MegfilePathLike, start: Optional[str] = None) -> str:
    '''Return the relative path of given path

    :param path: Given path
    :param start
    :returns: Relative path from start
    '''
    return fspath(os.path.relpath(path, start=start))


def fs_rename(src_path: MegfilePathLike, dst_path: MegfilePathLike) -> None:
    '''
    move file on fs

    :param src_path: Given source path
    :param dst_path: Given destination path
    '''
    os.rename(src_path, dst_path)


def fs_move(src_path: MegfilePathLike, dst_path: MegfilePathLike) -> None:
    '''
    move file on fs

    :param src_path: Given source path
    :param dst_path: Given destination path
    '''
    if fs_isdir(src_path):
        shutil.move(src_path, dst_path)
    else:
        os.rename(src_path, dst_path)


def fs_access(path: MegfilePathLike, mode: Access = Access.READ) -> bool:
    '''
    Test if path has access permission described by mode
    Using ``os.access``

    :param path: path to be tested
    :param mode: access mode
    :returns: Acceess: Enum, the read/write access that path has.
    '''
    if not isinstance(mode, Access):
        raise TypeError(
            'Unsupported mode: {} -- Mode should use one of the enums belonging to:  {}'
            .format(mode, ', '.join([str(a) for a in Access])))
    if mode not in (Access.READ, Access.WRITE):
        raise TypeError('Unsupported mode: {}'.format(mode))
    if mode == Access.READ:
        return os.access(path, os.R_OK)
    if mode == Access.WRITE:
        return os.access(path, os.W_OK)


def fs_cwd() -> str:
    '''Return current working directory

    returns: Current working directory
    '''
    return os.getcwd()


def fs_home():
    '''Return the home directory

    returns: Home directory path
    '''
    return os.path.expanduser('~')


def fs_expanduser(path: MegfilePathLike):
    '''Expand ~ and ~user constructions.  If user or $HOME is unknown,
do nothing.
    '''
    return os.path.expanduser(path)


def fs_resolve(path: MegfilePathLike):
    '''
    Equal to fs_realpath
    '''
    return os.path.realpath(path)


def fs_getmd5(path: MegfilePathLike):
    '''
    Calculate the md5 value of the file

    returns: md5 of file
    '''
    if not os.path.isfile(path):
        raise FileNotFoundError('%s is not a file' % path)
    with open(path, 'rb') as src:
        hash_md5 = hashlib.md5()
        for chunk in iter(lambda: src.read(4096), b''):
            hash_md5.update(chunk)
        md5 = hash_md5.hexdigest()
        src.seek(0)
    return md5

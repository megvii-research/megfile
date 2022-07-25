import hashlib
import io
import os
import shutil
from stat import S_ISDIR as stat_isdir
from stat import S_ISLNK as stat_islnk
from typing import IO, AnyStr, BinaryIO, Callable, Iterator, List, Optional, Tuple, Union
from unittest.mock import patch
from urllib.parse import urlsplit

from megfile.errors import _create_missing_ok_generator
from megfile.interfaces import Access, FileEntry, PathLike, StatResult
from megfile.lib.glob import iglob
from megfile.utils import calculate_md5

from .interfaces import PathLike, URIPath
from .lib.compat import fspath
from .lib.joinpath import path_join
from .smart_path import SmartPath

__all__ = [
    'FSPath',
    'is_fs',
    'StatResult',
    'fs_path_join',
    '_make_stat',
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


def is_fs(path: PathLike) -> bool:
    '''Test if a path is fs path

    :param path: Path to be tested
    :returns: True of a path is fs path, else False
    '''
    path = fspath(path)
    parts = urlsplit(path)
    return parts.scheme == '' or parts.scheme == 'file'


def fs_path_join(path: PathLike, *other_paths: PathLike) -> str:
    return path_join(fspath(path), *map(fspath, other_paths))


@SmartPath.register
class FSPath(URIPath):
    """file protocol
    e.g. file:///data/test/ or /data/test
    """

    protocol = "file"

    def __init__(self, path: Union["PathLike", int], *other_paths: "PathLike"):
        if not isinstance(path, int):
            if len(other_paths) > 0:
                path = self.from_path(path).joinpath(*other_paths)
            path = str(path)
        self.path = path

    def __fspath__(self) -> str:
        return os.path.normpath(self.path_without_protocol)

    @classmethod
    def from_uri(cls, path: str) -> "FSPath":
        return cls.from_path(path)

    @property
    def path_with_protocol(self) -> Union[str, int]:
        if isinstance(self.path, int):
            return self.path
        if self.path.startswith(self.anchor):
            return self.path
        return self.anchor + self.path

    def is_absolute(self) -> bool:
        '''Test whether a path is absolute

        :returns: True if a path is absolute, else False
        '''
        return os.path.isabs(self.path_without_protocol)

    def abspath(self) -> str:
        '''Return the absolute path of given path

        :returns: Absolute path of given path
        '''
        return fspath(os.path.abspath(self.path_without_protocol))

    def access(self, mode: Access = Access.READ) -> bool:
        '''
        Test if path has access permission described by mode
        Using ``os.access``

        :param mode: access mode
        :returns: Access: Enum, the read/write access that path has.
        '''
        if not isinstance(mode, Access):
            raise TypeError(
                'Unsupported mode: {} -- Mode should use one of the enums belonging to:  {}'
                .format(mode, ', '.join([str(a) for a in Access])))
        if mode == Access.READ:
            return os.access(self.path_without_protocol, os.R_OK)
        if mode == Access.WRITE:
            return os.access(self.path_without_protocol, os.W_OK)
        else:
            raise TypeError(  # pragma: no cover
                'Unsupported mode: {}'.format(mode))

    def exists(self, followlinks: bool = False) -> bool:
        '''
        Test if the path exists

        .. note::

            The difference between this function and ``os.path.exists`` is that this function regard symlink as file.
            In other words, this function is equal to ``os.path.lexists``

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path exists, else False

        '''
        if followlinks:
            return os.path.exists(self.path_without_protocol)
        return os.path.lexists(self.path_without_protocol)

    def getmtime(self) -> float:
        '''
        Get last-modified time of the file on the given path (in Unix timestamp format).
        If the path is an existent directory, return the latest modified time of all file in it.

        :returns: last-modified time
        '''
        return self.stat().mtime

    def getsize(self) -> int:
        '''
        Get file size on the given file path (in bytes).
        If the path in a directory, return the sum of all file size in it, including file in subdirectories (if exist).
        The result excludes the size of directory itself. In other words, return 0 Byte on an empty directory path.

        :returns: File size

        '''
        return self.stat().size

    def glob(self, recursive: bool = True,
             missing_ok: bool = True) -> List[str]:
        '''Return path list in ascending alphabetical order, in which path matches glob pattern

        1. If doesn't match any path, return empty list
            Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist. fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
        2. No guarantee that each path in result is different, which means:
            Assume there exists a path `/a/b/c/b/d.txt`
            use path pattern like `/**/b/**/*.txt` to glob, the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default, when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True) in acsending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
        :returns: A list contains paths match `pathname`
        '''
        return list(self.iglob(recursive=recursive, missing_ok=missing_ok))

    def glob_stat(self, recursive: bool = True,
                  missing_ok: bool = True) -> Iterator[FileEntry]:
        '''Return a list contains tuples of path and file stat, in ascending alphabetical order, in which path matches glob pattern

        1. If doesn't match any path, return empty list
            Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist. fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
        2. No guarantee that each path in result is different, which means:
            Assume there exists a path `/a/b/c/b/d.txt`
            use path pattern like `/**/b/**/*.txt` to glob, the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default, when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True) in acsending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
        :returns: A list contains tuples of path and file stat, in which paths match `pathname`
        '''
        for path in self.iglob(recursive=recursive, missing_ok=missing_ok):
            yield FileEntry(path, _make_stat(os.lstat(path)))

    def expanduser(self):
        '''Expand ~ and ~user constructions.  If user or $HOME is unknown,
           do nothing.
        '''
        return os.path.expanduser(self.path_without_protocol)

    def iglob(self, recursive: bool = True,
              missing_ok: bool = True) -> Iterator[str]:
        '''Return path iterator in ascending alphabetical order, in which path matches glob pattern

        1. If doesn't match any path, return empty list
            Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist. fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
        2. No guarantee that each path in result is different, which means:
            Assume there exists a path `/a/b/c/b/d.txt`
            use path pattern like `/**/b/**/*.txt` to glob, the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default, when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True) in acsending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
        :returns: An iterator contains paths match `pathname`
        '''
        return _create_missing_ok_generator(
            iglob(fspath(self.path_without_protocol), recursive=recursive),
            missing_ok,
            FileNotFoundError('No match file: %r' % self.path_without_protocol))

    def is_dir(self, followlinks: bool = False) -> bool:
        '''
        Test if a path is directory

        .. note::

            The difference between this function and ``os.path.isdir`` is that this function regard symlink as file

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path is a directory, else False

        '''
        if os.path.islink(self.path_without_protocol) and not followlinks:
            return False
        return os.path.isdir(self.path_without_protocol)

    def is_file(self, followlinks: bool = False) -> bool:
        '''
        Test if a path is file

        .. note::
        
            The difference between this function and ``os.path.isfile`` is that this function regard symlink as file

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path is a file, else False

        '''
        if os.path.islink(self.path_without_protocol) and not followlinks:
            return True
        return os.path.isfile(self.path_without_protocol)

    def listdir(self) -> List[str]:
        '''
        Get all contents of given fs path. The result is in acsending alphabetical order.

        :returns: All contents have in the path in acsending alphabetical order
        '''
        return sorted(os.listdir(self.path_without_protocol))

    def load(self) -> BinaryIO:
        '''Read all content on specified path and write into memory

        User should close the BinaryIO manually

        :returns: Binary stream
        '''
        with open(self.path_without_protocol, 'rb') as f:
            data = f.read()
        return io.BytesIO(data)

    def mkdir(self, exist_ok: bool = False):
        '''
        make a directory on fs, including parent directory

        If there exists a file on the path, raise FileExistsError

        :param exist_ok: If False and target directory exists, raise FileExistsError
        :raises: FileExistsError
        '''
        if exist_ok and self.path_without_protocol == '':
            return
        return os.makedirs(self.path_without_protocol, exist_ok=exist_ok)

    def realpath(self) -> str:
        '''Return the real path of given path

        :returns: Real path of given path
        '''
        return fspath(os.path.realpath(self.path_without_protocol))

    def relpath(self, start: Optional[str] = None) -> str:
        '''Return the relative path of given path

        :param start: Given start directory
        :returns: Relative path from start
        '''
        return fspath(os.path.relpath(self.path_without_protocol, start=start))

    def rename(self, dst_path: PathLike, followlinks: bool = False) -> None:
        '''
        rename file on fs

        :param dst_path: Given destination path
        '''
        if self.is_dir(followlinks=followlinks):
            shutil.move(self.path_without_protocol, dst_path)
        else:
            os.rename(self.path_without_protocol, dst_path)

    def replace(self, dst_path: PathLike, followlinks: bool = False) -> None:
        '''
        move file on fs

        :param dst_path: Given destination path
        '''
        return self.rename(dst_path=dst_path, followlinks=followlinks)

    def remove(
            self, missing_ok: bool = False, followlinks: bool = False) -> None:
        '''
        Remove the file or directory on fs

        :param missing_ok: if False and target file/directory not exists, raise FileNotFoundError
        '''
        if missing_ok and not self.exists():
            return
        if self.is_dir(followlinks=followlinks):
            shutil.rmtree(self.path_without_protocol)
        else:
            os.remove(self.path_without_protocol)

    def _scan(self, missing_ok: bool = True,
              followlinks: bool = False) -> Iterator[str]:
        if self.is_file(followlinks=followlinks):
            path = fspath(self.path_without_protocol)
            yield path

        for root, _, files in self.walk(followlinks=followlinks):
            for filename in files:
                yield os.path.join(root, filename)

    def scan(self, missing_ok: bool = True,
             followlinks: bool = False) -> Iterator[str]:
        '''
        Iteratively traverse only files in given directory, in alphabetical order.
        Every iteration on generator yields a path string.

        If path is a file path, yields the file only
        If path is a non-existent path, return an empty generator
        If path is a bucket path, return all file paths in the bucket

        :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
        :returns: A file path generator
        '''
        return _create_missing_ok_generator(
            self._scan(followlinks=followlinks), missing_ok,
            FileNotFoundError('No match file: %r' % self.path_without_protocol))

    def scan_stat(self, missing_ok: bool = True,
                  followlinks: bool = False) -> Iterator[FileEntry]:
        '''
        Iteratively traverse only files in given directory, in alphabetical order.
        Every iteration on generator yields a tuple of path string and file stat

        :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
        :returns: A file path generator
        '''
        for path in self._scan(followlinks=followlinks):
            yield FileEntry(path, _make_stat(os.lstat(path)))
        else:
            if missing_ok:
                return
            raise FileNotFoundError(
                'No match file: %r' % self.path_without_protocol)

    def scandir(self) -> Iterator[FileEntry]:
        '''
        Get all content of given file path.

        :returns: An iterator contains all contents have prefix path
        '''
        for entry in os.scandir(self.path_without_protocol):
            yield FileEntry(entry.path, _make_stat(entry.stat()))

    def stat(self) -> StatResult:
        '''
        Get StatResult of file on fs, including file size and mtime, referring to fs_getsize and fs_getmtime

        :returns: StatResult
        '''
        result = _make_stat(os.lstat(self.path_without_protocol))
        if result.islnk or not result.isdir:
            return result

        size = 0
        ctime = result.ctime
        mtime = result.mtime
        for root, _, files in os.walk(self.path_without_protocol):
            for filename in files:
                canonical_path = os.path.join(root, filename)
                stat = os.lstat(canonical_path)
                size += stat.st_size
                if ctime > stat.st_ctime:
                    ctime = stat.st_ctime
                if mtime < stat.st_mtime:
                    mtime = stat.st_mtime
        return result._replace(size=size, ctime=ctime, mtime=mtime)

    def unlink(self, missing_ok: bool = False) -> None:
        '''
        Remove the file on fs

        :param missing_ok: if False and target file not exists, raise FileNotFoundError
        '''
        if missing_ok and not self.exists():
            return
        os.unlink(self.path_without_protocol)

    def walk(self, followlinks: bool = False
            ) -> Iterator[Tuple[str, List[str], List[str]]]:
        '''
        Generate the file names in a directory tree by walking the tree top-down.
        For each directory in the tree rooted at directory path (including path itself),
        it yields a 3-tuple (root, dirs, files).

        root: a string of current path
        dirs: name list of subdirectories (excluding '.' and '..' if they exist) in 'root'. The list is sorted by ascending alphabetical order
        files: name list of non-directory files (link is regarded as file) in 'root'. The list is sorted by ascending alphabetical order

        If path not exists, or path is a file (link is regarded as file), return an empty generator

        .. note::

            Be aware that setting ``followlinks`` to True can lead to infinite recursion if a link points to a parent directory of itself. fs_walk() does not keep track of the directories it visited already.

        :param followlinks: False if regard symlink as file, else True
        :returns: A 3-tuple generator
        '''
        if not self.exists(followlinks=followlinks):
            return

        if self.is_file(followlinks=followlinks):
            return

        path = fspath(self.path_without_protocol)
        path = os.path.normpath(self.path_without_protocol)

        stack = [path]
        while stack:
            root = stack.pop()
            dirs, files = [], []
            for entry in os.scandir(root):
                name = fspath(entry.name)
                path = entry.path
                if FSPath(path).is_file(followlinks=followlinks):
                    files.append(name)
                elif FSPath(path).is_dir(followlinks=followlinks):
                    dirs.append(name)

            dirs = sorted(dirs)
            files = sorted(files)

            yield root, dirs, files

            stack.extend(
                (os.path.join(root, directory) for directory in reversed(dirs)))

    def resolve(self) -> str:
        '''Equal to fs_realpath

        :return: Return the canonical path of the specified filename, eliminating any symbolic links encountered in the path.
        :rtype: str
        '''
        return os.path.realpath(self.path_without_protocol)

    def md5(self, recalculate: bool = False):
        '''
        Calculate the md5 value of the file

        returns: md5 of file
        '''
        if os.path.isdir(self.path_without_protocol):
            hash_md5 = hashlib.md5()  # nosec
            for file_name in self.listdir():
                chunk = FSPath(self.path_without_protocol,
                               file_name).md5(recalculate=recalculate).encode()
                hash_md5.update(chunk)
            return hash_md5.hexdigest()
        with open(self.path_without_protocol, 'rb') as src:  # type: ignore
            md5 = calculate_md5(src)
        return md5

    def _copyfile(
            self,
            dst_path: PathLike,
            callback: Optional[Callable[[int], None]] = None,
            followlinks: bool = False):

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
                    callback(len(buf))  # pragma: no cover

            return _copyfileobj

        src_stat = self.stat()
        with patch('shutil.copyfileobj', _patch_copyfileobj(callback)):
            shutil.copyfile(
                self.path_without_protocol,
                dst_path,
                follow_symlinks=followlinks)
            if src_stat.is_symlink() and not followlinks:
                if callback:
                    callback(src_stat.size)
                return

    def copy(
            self,
            dst_path: PathLike,
            callback: Optional[Callable[[int], None]] = None,
            followlinks: bool = False):
        ''' File copy on file system
        Copy content (excluding meta date) of file on `src_path` to `dst_path`. `dst_path` must be a complete file name

        .. note ::

            The differences between this function and shutil.copyfile are:

                1. If parent directory of dst_path doesn't exist, create it

                2. Allow callback function, None by default. callback: Optional[Callable[[int], None]],

            the int data is means the size (in bytes) of the written data that is passed periodically

                3. This function is thread-unsafe

        TODO: get shutil implementation, to make fs_copy thread-safe

        :param dst_path: Target file path
        :param callback: Called periodically during copy, and the input parameter is the data size (in bytes) of copy since the last call
        :param followlinks: False if regard symlink as file, else True
        '''
        try:
            self._copyfile(dst_path, callback=callback, followlinks=followlinks)
        except FileNotFoundError as error:
            # Prevent the dst_path directory from being created when src_path does not exist
            if dst_path == error.filename:
                FSPath(os.path.dirname(dst_path)).mkdir(exist_ok=True)
                self._copyfile(
                    dst_path, callback=callback, followlinks=followlinks)
            else:
                raise  # pragma: no cover

    def sync(self, dst_path: PathLike, followlinks: bool = False):
        '''Force write of everything to disk.

        :param dst_path: Target file path
        '''
        if self.is_dir(followlinks=followlinks):
            shutil.copytree(self.path_without_protocol, dst_path)
        else:
            self.copy(dst_path, followlinks=followlinks)

    def symlink(self, dst_path: PathLike) -> None:
        '''
        Create a symbolic link pointing to src_path named dst_path.

        :param dst_path: Desination path
        '''
        return os.symlink(self.path_without_protocol, dst_path)

    def readlink(self) -> PathLike:
        '''
        Return a string representing the path to which the symbolic link points.
        :returns: Return a string representing the path to which the symbolic link points.
        '''
        return os.readlink(self.path_without_protocol)

    def is_symlink(self) -> bool:
        '''Test whether a path is a symbolic link

        :return: If path is a symbolic link return True, else False
        :rtype: bool
        '''
        return os.path.islink(self.path_without_protocol)

    def is_mount(self) -> bool:
        '''Test whether a path is a mount point

        :returns: True if a path is a mount point, else False
        '''
        return os.path.ismount(self.path_without_protocol)

    @staticmethod
    def cwd() -> str:
        '''Return current working directory

        returns: Current working directory
        '''
        return os.getcwd()

    @staticmethod
    def home():
        '''Return the home directory

        returns: Home directory path
        '''
        return os.path.expanduser('~')

    def joinpath(self, *other_paths: PathLike) -> "FSPath":
        path = fspath(self)
        if path == '.':
            path = ''
        return self.from_path(path_join(path, *map(fspath, other_paths)))

    def save(self, file_object: BinaryIO):
        '''Write the opened binary stream to path
        If parent directory of path doesn't exist, it will be created.

        :param file_object: stream to be read
        '''
        FSPath(os.path.dirname(self.path_without_protocol)).mkdir(exist_ok=True)
        with open(self.path_without_protocol, 'wb') as output:
            output.write(file_object.read())

    def open(self, mode: str, **kwargs) -> IO[AnyStr]:
        if not isinstance(self.path_without_protocol, int) and ('w' in mode or
                                                                'x' in mode or
                                                                'a' in mode):
            FSPath(os.path.dirname(
                self.path_without_protocol)).mkdir(exist_ok=True)
        return io.open(self.path_without_protocol, mode)

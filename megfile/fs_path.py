import hashlib
import io
import os
import pathlib
import shutil
from functools import cached_property
from stat import S_ISBLK as stat_isblk
from stat import S_ISCHR as stat_ischr
from stat import S_ISDIR as stat_isdir
from stat import S_ISFIFO as stat_isfifo
from stat import S_ISLNK as stat_islnk
from stat import S_ISSOCK as stat_issock
from typing import IO, BinaryIO, Callable, Iterator, List, Optional, Tuple, Union

from megfile.errors import _create_missing_ok_generator
from megfile.interfaces import (
    Access,
    ContextIterator,
    FileEntry,
    PathLike,
    StatResult,
    URIPath,
)
from megfile.lib.compare import is_same_file
from megfile.lib.compat import fspath
from megfile.lib.glob import iglob
from megfile.lib.joinpath import path_join
from megfile.lib.url import get_url_scheme
from megfile.smart_path import SmartPath
from megfile.utils import calculate_md5

__all__ = [
    "FSPath",
    "is_fs",
    "fs_path_join",
]


def _make_stat(stat: os.stat_result) -> StatResult:
    return StatResult(
        size=stat.st_size,
        ctime=stat.st_ctime,  # pyre-ignore[16]
        mtime=stat.st_mtime,
        isdir=stat_isdir(stat.st_mode),
        islnk=stat_islnk(stat.st_mode),
        extra=stat,
    )


def is_fs(path: Union["PathLike", int]) -> bool:
    """Test if a path is fs path

    :param path: Path to be tested
    :returns: True of a path is fs path, else False
    """
    if isinstance(path, int):
        return True
    path = fspath(path)
    scheme = get_url_scheme(path)
    return scheme == "" or scheme == "file"


def fs_path_join(path: PathLike, *other_paths: PathLike) -> str:
    return path_join(fspath(path), *map(fspath, other_paths))


def _fs_rename_file(
    src_path: PathLike, dst_path: PathLike, overwrite: bool = True
) -> None:
    """
    rename file on fs

    :param src_path: Given path
    :param dst_path: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    src_path, dst_path = fspath(src_path), fspath(dst_path)

    if not overwrite and os.path.exists(dst_path):
        return

    dst_dir = os.path.dirname(dst_path)
    if dst_dir and dst_dir != ".":
        os.makedirs(dst_dir, exist_ok=True)
    shutil.move(src_path, dst_path)


@SmartPath.register
class FSPath(URIPath):
    """file protocol
    e.g. file:///data/test/ or /data/test
    """

    protocol = "file"

    def __init__(self, path: Union[PathLike, int], *other_paths: PathLike):
        if not isinstance(path, int):
            if len(other_paths) > 0:
                path = self.from_path(path).joinpath(*other_paths)
            path = str(path)
        self.path = path

    def _check_int_path(self) -> None:
        if isinstance(self.path_without_protocol, int):
            raise TypeError("not support the path of int type")

    def __fspath__(self) -> str:
        self._check_int_path()
        return os.path.normpath(self.path_without_protocol)  # pyre-ignore[6]

    @cached_property
    def root(self) -> str:
        if isinstance(self.path_without_protocol, int):
            return "/"
        return pathlib.Path(self.path_without_protocol).root  # pyre-ignore[6]

    @cached_property
    def anchor(self) -> str:
        if isinstance(self.path_without_protocol, int):
            return "/"
        return pathlib.Path(self.path_without_protocol).anchor  # pyre-ignore[6]

    @cached_property
    def drive(self) -> str:
        if isinstance(self.path_without_protocol, int):
            return ""
        return pathlib.Path(self.path_without_protocol).drive  # pyre-ignore[6]

    @classmethod
    def from_uri(cls, path: PathLike) -> "FSPath":
        return cls.from_path(path)

    @cached_property
    def path_with_protocol(self) -> Union[str, int]:
        """Return path with protocol, like file:///root"""
        if isinstance(self.path, int):
            return self.path
        protocol_prefix = self.protocol + "://"
        if self.path.startswith(protocol_prefix):  # pyre-ignore[16]
            return self.path  # pyre-ignore[7]
        return protocol_prefix + self.path  # pyre-ignore[58]

    @cached_property
    def path_without_protocol(self) -> Union[str, int]:
        """
        Return path without protocol, example: if path is file:///root,
        return /root
        """
        if isinstance(self.path, int):
            return self.path
        return super().path_without_protocol

    def is_absolute(self) -> bool:
        """Test whether a path is absolute

        :returns: True if a path is absolute, else False
        """
        if isinstance(self.path_without_protocol, int):
            return False
        return os.path.isabs(self.path_without_protocol)  # pyre-ignore[6]

    def abspath(self) -> str:
        """Return the absolute path of given path

        :returns: Absolute path of given path
        """
        self._check_int_path()
        return fspath(os.path.abspath(self.path_without_protocol))  # pyre-ignore[6]

    def access(self, mode: Access = Access.READ) -> bool:
        """
        Test if path has access permission described by mode
        Using ``os.access``

        :param mode: access mode
        :returns: Access: Enum, the read/write access that path has.
        """
        if mode == Access.READ:
            return os.access(self.path_without_protocol, os.R_OK)
        elif mode == Access.WRITE:
            return os.access(self.path_without_protocol, os.W_OK)
        else:
            raise TypeError(
                "Unsupported mode: {} -- Mode should use one of "
                "the enums belonging to:  {}".format(
                    mode, ", ".join([str(a) for a in Access])
                )
            )

    def exists(self, followlinks: bool = False) -> bool:
        """
        Test if the path exists

        .. note::

            The difference between this function and ``os.path.exists`` is that
            this function regard symlink as file.
            In other words, this function is equal to ``os.path.lexists``

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path exists, else False

        """
        if followlinks:
            return os.path.exists(self.path_without_protocol)
        return os.path.lexists(self.path_without_protocol)

    def getmtime(self, follow_symlinks: bool = False) -> float:
        """
        Get last-modified time of the file on the given path (in Unix timestamp format).
        If the path is an existent directory,
        return the latest modified time of all file in it.

        :returns: last-modified time
        """
        return self.stat(follow_symlinks=follow_symlinks).mtime

    def getsize(self, follow_symlinks: bool = False) -> int:
        """
        Get file size on the given file path (in bytes).
        If the path in a directory, return the sum of all file size in it,
        including file in subdirectories (if exist).
        The result excludes the size of directory itself.
        In other words, return 0 Byte on an empty directory path.

        :returns: File size

        """
        return self.stat(follow_symlinks=follow_symlinks).size

    def glob(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> List["FSPath"]:
        """Return path list in ascending alphabetical order,
        in which path matches glob pattern

        1. If doesn't match any path, return empty list
            Notice:  ``glob.glob`` in standard library returns ['a/'] instead of
            empty list when pathname is like `a/**`,
            recursive is True and directory 'a' doesn't exist.
            fs_glob behaves like ``glob.glob`` in standard library
            under such circumstance.
        2. No guarantee that each path in result is different, which means:
            Assume there exists a path `/a/b/c/b/d.txt`
            use path pattern like `/**/b/**/*.txt` to glob,
            the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default,
            when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True)
            in ascending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param pattern: Glob the given relative pattern in the directory represented
            by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :returns: A list contains paths match `pathname`
        """
        return list(
            self.iglob(pattern=pattern, recursive=recursive, missing_ok=missing_ok)
        )

    def glob_stat(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> Iterator[FileEntry]:
        """Return a list contains tuples of path and file stat,
        in ascending alphabetical order, in which path matches glob pattern

        1. If doesn't match any path, return empty list
            Notice:  ``glob.glob`` in standard library returns ['a/'] instead of
            empty list when pathname is like `a/**`,
            recursive is True and directory 'a' doesn't exist.
            fs_glob behaves like ``glob.glob`` in standard library
            under such circumstance.
        2. No guarantee that each path in result is different, which means:
            Assume there exists a path `/a/b/c/b/d.txt`
            use path pattern like `/**/b/**/*.txt` to glob,
            the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default,
            when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True)
            in ascending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param pattern: Glob the given relative pattern in the directory represented
            by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :returns: A list contains tuples of path and file stat,
            in which paths match `pathname`
        """
        for path_obj in self.iglob(
            pattern=pattern, recursive=recursive, missing_ok=missing_ok
        ):
            yield FileEntry(
                path_obj.name,
                path_obj.path,  # pyre-ignore[6]
                _make_stat(os.lstat(path_obj.path)),  # pyre-ignore[6]
            )

    def expanduser(self):
        """Expand ~ and ~user constructions.  If user or $HOME is unknown,
        do nothing.
        """
        return os.path.expanduser(self.path_without_protocol)

    def iglob(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> Iterator["FSPath"]:
        """Return path iterator in ascending alphabetical order,
        in which path matches glob pattern

        1. If doesn't match any path, return empty list
            Notice:  ``glob.glob`` in standard library returns ['a/'] instead of
            empty list when pathname is like `a/**`,
            recursive is True and directory 'a' doesn't exist.
            fs_glob behaves like ``glob.glob`` in standard library
            under such circumstance.
        2. No guarantee that each path in result is different, which means:
            Assume there exists a path `/a/b/c/b/d.txt`
            use path pattern like `/**/b/**/*.txt` to glob,
            the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default,
            when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True)
            in ascending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param pattern: Glob the given relative pattern in the directory represented
            by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :returns: An iterator contains paths match `pathname`
        """
        glob_path = self.path_without_protocol
        if pattern:
            glob_path = self.joinpath(pattern).path_without_protocol

        for path in _create_missing_ok_generator(
            iglob(fspath(glob_path), recursive=recursive),
            missing_ok,
            FileNotFoundError("No match any file: %r" % glob_path),
        ):
            yield self.from_path(path)

    def is_dir(self, followlinks: bool = False) -> bool:
        """
        Test if a path is directory

        .. note::

            The difference between this function and ``os.path.isdir`` is that
            this function regard symlink as file

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path is a directory, else False

        """
        if os.path.islink(self.path_without_protocol) and not followlinks:
            return False
        return os.path.isdir(self.path_without_protocol)

    def is_file(self, followlinks: bool = False) -> bool:
        """
        Test if a path is file

        .. note::

            The difference between this function and ``os.path.isfile`` is that
            this function regard symlink as file

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path is a file, else False

        """
        if os.path.islink(self.path_without_protocol) and not followlinks:
            return True
        return os.path.isfile(self.path_without_protocol)

    def listdir(self) -> List[str]:
        """
        Get all contents of given fs path.
        The result is in ascending alphabetical order.

        :returns: All contents have in the path in ascending alphabetical order
        """
        self._check_int_path()
        return sorted(os.listdir(self.path_without_protocol))  # pyre-ignore[6]

    def iterdir(self) -> Iterator["FSPath"]:
        """
        Get all contents of given fs path. The order of result is in arbitrary order.

        :returns: All contents have in the path.
        """
        self._check_int_path()
        for path in pathlib.Path(
            self.path_without_protocol  # pyre-ignore[6]
        ).iterdir():
            yield self.from_path(fspath(path))

    def load(self) -> BinaryIO:
        """Read all content on specified path and write into memory

        User should close the BinaryIO manually

        :returns: Binary stream
        """
        with open(self.path_without_protocol, "rb") as f:
            data = f.read()
        return io.BytesIO(data)

    def mkdir(self, mode=0o777, parents: bool = False, exist_ok: bool = False):
        """
        make a directory on fs, including parent directory.
        If there exists a file on the path, raise FileExistsError

        :param mode: If mode is given, it is combined with the process’ umask value to
            determine the file mode and access flags.
        :param parents: If parents is true, any missing parents of this path
            are created as needed; If parents is false (the default),
            a missing parent raises FileNotFoundError.
        :param exist_ok: If False and target directory exists, raise FileExistsError

        :raises: FileExistsError
        """
        if exist_ok and (
            self.path_without_protocol == ""
            or isinstance(self.path_without_protocol, int)
        ):
            return
        self._check_int_path()
        return pathlib.Path(self.path_without_protocol).mkdir(  # pyre-ignore[6]
            mode=mode, parents=parents, exist_ok=exist_ok
        )

    def realpath(self) -> str:
        """Return the real path of given path

        :returns: Real path of given path
        """
        self._check_int_path()
        return fspath(os.path.realpath(self.path_without_protocol))  # pyre-ignore[6]

    def relpath(self, start: Optional[str] = None) -> str:
        """Return the relative path of given path

        :param start: Given start directory
        :returns: Relative path from start
        """
        self._check_int_path()
        return fspath(
            os.path.relpath(
                self.path_without_protocol,  # pyre-ignore[6]
                start=start,
            )
        )

    def rename(self, dst_path: PathLike, overwrite: bool = True) -> "FSPath":
        """
        rename file on fs

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        self._check_int_path()

        src_path, dst_path = fspath(self.path_without_protocol), fspath(dst_path)
        if os.path.isfile(src_path):
            _fs_rename_file(src_path, dst_path, overwrite)
            if os.path.exists(src_path):
                os.remove(src_path)
            return self.from_path(dst_path)
        else:
            os.makedirs(dst_path, exist_ok=True)

        with os.scandir(src_path) as entries:
            for file_entry in entries:
                src_file_path = file_entry.path
                dst_file_path = dst_path
                relative_path = os.path.relpath(src_file_path, start=src_path)
                if relative_path and relative_path != ".":
                    dst_file_path = os.path.join(dst_file_path, relative_path)
                if os.path.exists(dst_file_path) and file_entry.is_dir():
                    self.from_path(src_file_path).rename(dst_file_path, overwrite)
                else:
                    _fs_rename_file(src_file_path, dst_file_path, overwrite)

            shutil.rmtree(src_path, ignore_errors=True)

        return self.from_path(dst_path)

    def replace(self, dst_path: PathLike, overwrite: bool = True) -> "FSPath":
        """
        move file on fs

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        return self.rename(dst_path=dst_path, overwrite=overwrite)

    def remove(self, missing_ok: bool = False) -> None:
        """
        Remove the file or directory on fs

        :param missing_ok: if False and target file/directory not exists,
            raise FileNotFoundError
        """
        if missing_ok and not self.exists():
            return
        if self.is_dir():
            shutil.rmtree(self.path_without_protocol)  # pyre-ignore[6]
        else:
            os.remove(self.path_without_protocol)  # pyre-ignore[6]

    def _scan(
        self, missing_ok: bool = True, followlinks: bool = False
    ) -> Iterator[str]:
        self._check_int_path()

        if self.is_file(followlinks=followlinks):
            path = fspath(self.path_without_protocol)
            yield path

        for root, _, files in self.walk(followlinks=followlinks):
            for filename in files:
                yield os.path.join(root, filename)

    def scan(self, missing_ok: bool = True, followlinks: bool = False) -> Iterator[str]:
        """
        Iteratively traverse only files in given directory, in alphabetical order.
        Every iteration on generator yields a path string.

        If path is a file path, yields the file only
        If path is a non-existent path, return an empty generator
        If path is a bucket path, return all file paths in the bucket

        :param missing_ok: If False and there's no file in the directory,
            raise FileNotFoundError
        :returns: A file path generator
        """
        return _create_missing_ok_generator(
            self._scan(followlinks=followlinks),
            missing_ok,
            FileNotFoundError("No match any file in: %r" % self.path),
        )

    def scan_stat(
        self, missing_ok: bool = True, followlinks: bool = False
    ) -> Iterator[FileEntry]:
        """
        Iteratively traverse only files in given directory, in alphabetical order.
        Every iteration on generator yields a tuple of path string and file stat

        :param missing_ok: If False and there's no file in the directory,
            raise FileNotFoundError
        :returns: A file path generator
        """
        file_not_found = True
        for path in self._scan(followlinks=followlinks):
            yield FileEntry(os.path.basename(path), path, _make_stat(os.lstat(path)))
            file_not_found = False
        if file_not_found:
            if missing_ok:
                return
            raise FileNotFoundError(
                "No match any file in: %r" % self.path_without_protocol
            )

    def scandir(self) -> ContextIterator:
        """
        Get all content of given file path.

        :returns: An iterator contains all contents have prefix path
        """
        self._check_int_path()

        def create_generator():
            with os.scandir(self.path_without_protocol) as entries:
                for entry in entries:
                    yield FileEntry(
                        entry.name,
                        entry.path,
                        _make_stat(entry.stat(follow_symlinks=False)),
                    )

        return ContextIterator(create_generator())

    def stat(self, follow_symlinks=True) -> StatResult:
        """
        Get StatResult of file on fs, including file size and mtime,
        referring to fs_getsize and fs_getmtime

        :returns: StatResult
        """
        if follow_symlinks or isinstance(self.path_without_protocol, int):
            result = _make_stat(os.stat(self.path_without_protocol))
        else:
            result = _make_stat(os.lstat(self.path_without_protocol))  # pyre-ignore[6]

        if result.islnk or not result.isdir:
            return result

        size = 0
        ctime = result.ctime
        mtime = result.mtime
        if not isinstance(self.path_without_protocol, int):
            for root, _, files in os.walk(self.path_without_protocol):  # pyre-ignore[6]
                for filename in files:
                    canonical_path = os.path.join(root, filename)
                    stat = os.lstat(canonical_path)
                    size += stat.st_size
                    if ctime > stat.st_ctime:  # pyre-ignore[16]
                        ctime = stat.st_ctime
                    if mtime < stat.st_mtime:
                        mtime = stat.st_mtime
        return result._replace(size=size, ctime=ctime, mtime=mtime)

    def unlink(self, missing_ok: bool = False) -> None:
        """
        Remove the file on fs

        :param missing_ok: if False and target file not exists, raise FileNotFoundError
        """
        self._check_int_path()

        if missing_ok and not self.exists():
            return
        os.unlink(self.path_without_protocol)  # pyre-ignore[6]

    def walk(
        self, followlinks: bool = False
    ) -> Iterator[Tuple[str, List[str], List[str]]]:
        """
        Generate the file names in a directory tree by walking the tree top-down.
        For each directory in the tree rooted at directory path (including path itself),
        it yields a 3-tuple (root, dirs, files).

        - root: a string of current path
        - dirs: name list of subdirectories (excluding '.' and '..' if they exist)
          in 'root'. The list is sorted by ascending alphabetical order
        - files: name list of non-directory files (link is regarded as file) in 'root'.
          The list is sorted by ascending alphabetical order

        If path not exists, or path is a file (link is regarded as file),
        return an empty generator

        .. note::

            Be aware that setting ``followlinks`` to True can lead to infinite recursion
            if a link points to a parent directory of itself. fs_walk() does not keep
            track of the directories it visited already.

        :param followlinks: False if regard symlink as file, else True

        :returns: A 3-tuple generator
        """
        self._check_int_path()

        if not self.exists(followlinks=followlinks):
            return

        if self.is_file(followlinks=followlinks):
            return

        path = fspath(self.path_without_protocol)
        path = os.path.normpath(self.path_without_protocol)  # pyre-ignore[6]

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
                (os.path.join(root, directory) for directory in reversed(dirs))
            )

    def resolve(self, strict=False) -> "FSPath":
        """Equal to fs_realpath

        :return: Return the canonical path of the specified filename,
            eliminating any symbolic links encountered in the path.
        :rtype: FSPath
        """
        self._check_int_path()
        return self.from_path(
            fspath(
                pathlib.Path(
                    self.path_without_protocol  # pyre-ignore[6]
                ).resolve(strict=strict)
            )
        )

    def md5(self, recalculate: bool = False, followlinks: bool = False):
        """
        Calculate the md5 value of the file

        :param recalculate: Ignore this parameter, just for compatibility
        :param followlinks: Ignore this parameter, just for compatibility

        returns: md5 of file
        """
        if self.is_dir():
            hash_md5 = hashlib.md5()  # nosec
            for file_name in self.listdir():
                chunk = (
                    self.joinpath(file_name)
                    .md5(recalculate=recalculate, followlinks=followlinks)
                    .encode()
                )
                hash_md5.update(chunk)
            return hash_md5.hexdigest()
        with open(self.path_without_protocol, "rb") as src:
            md5 = calculate_md5(src)
        return md5

    def _copyfile(
        self,
        dst_path: PathLike,
        callback: Optional[Callable[[int], None]] = None,
        followlinks: bool = False,
    ):
        if isinstance(self.path_without_protocol, int):
            with open(fspath(dst_path), "wb") as fdst:
                # This magic number is copied from  copyfileobj
                length = 16 * 1024
                while True:
                    buf = os.read(self.path_without_protocol, length)  # pyre-ignore[6]
                    if not buf:
                        break
                    fdst.write(buf)
                    if callback:
                        callback(len(buf))
        else:
            shutil.copy2(
                self.path_without_protocol,  # pyre-ignore[6]
                fspath(dst_path),
                follow_symlinks=followlinks,
            )

            # After python3.8, patch `shutil.copyfile` is not a good way,
            # because `shutil.copy2` will not call it in some cases.
            if callback:
                callback(self.stat(follow_symlinks=followlinks).size)

    def copy(
        self,
        dst_path: PathLike,
        callback: Optional[Callable[[int], None]] = None,
        followlinks: bool = False,
        overwrite: bool = True,
    ):
        """File copy on file system
        Copy content (excluding meta date) of file on `src_path` to `dst_path`.
        `dst_path` must be a complete file name

        .. note ::

            The differences between this function and shutil.copyfile are:

                1. If parent directory of dst_path doesn't exist, create it

                2. Allow callback function, None by default.
                    callback: Optional[Callable[[int], None]], the int data is means
                    the size (in bytes) of the written data that is passed periodically

                3. This function is thread-unsafe

        :param dst_path: Target file path
        :param callback: Called periodically during copy, and the input parameter is
            the data size (in bytes) of copy since the last call
        :param followlinks: False if regard symlink as file, else True
        :param overwrite: whether or not overwrite file when exists, default is True
        """
        dst_path = fspath(dst_path)
        if not overwrite and os.path.exists((dst_path)):
            return

        try:
            self._copyfile(dst_path, callback=callback, followlinks=followlinks)
        except FileNotFoundError as error:
            # Prevent the dst_path directory from being created when src_path does not
            # exist
            dst_parent_dir = os.path.dirname(dst_path)
            if (
                dst_parent_dir
                and dst_parent_dir != "."
                and error.filename in (dst_path, dst_parent_dir)
            ):
                self.from_path(dst_parent_dir).mkdir(parents=True, exist_ok=True)
                self._copyfile(dst_path, callback=callback, followlinks=followlinks)
            else:
                raise

    def sync(
        self,
        dst_path: PathLike,
        followlinks: bool = False,
        force: bool = False,
        overwrite: bool = True,
    ) -> None:
        """Force write of everything to disk.

        :param dst_path: Target file path
        :param followlinks: False if regard symlink as file, else True
        :param force: Sync file forcible, do not ignore same files,
            priority is higher than 'overwrite', default is False
        :param overwrite: whether or not overwrite file when exists, default is True
        """
        self._check_int_path()

        if self.is_dir(followlinks=followlinks):

            def ignore_same_file(src: str, names: List[str]) -> List[str]:
                ignore_files = []
                for name in names:
                    dst_obj = self.from_path(dst_path).joinpath(name)
                    if not overwrite and dst_obj.exists():
                        ignore_files.append(name)
                    elif dst_obj.exists() and is_same_file(
                        self.joinpath(name).stat(), dst_obj.stat(), "copy"
                    ):
                        ignore_files.append(name)
                return ignore_files

            shutil.copytree(
                self.path_without_protocol,  # pyre-ignore[6]
                dst_path,
                ignore=None if force else ignore_same_file,
                dirs_exist_ok=True,
            )
        else:
            self.copy(dst_path, followlinks=followlinks, overwrite=force or overwrite)

    def symlink(self, dst_path: PathLike) -> None:
        """
        Create a symbolic link pointing to src_path named dst_path.

        :param dst_path: Destination path
        """
        self._check_int_path()
        return os.symlink(self.path_without_protocol, dst_path)  # pyre-ignore[6]

    def readlink(self) -> "FSPath":
        """
        Return a FSPath instance representing the path to which
        the symbolic link points.

        :returns: Return a FSPath instance representing the path to which
            the symbolic link points.
        """
        self._check_int_path()
        return self.from_path(
            os.readlink(
                self.path_without_protocol  # pyre-ignore[6]
            )
        )

    def is_symlink(self) -> bool:
        """Test whether a path is a symbolic link

        :return: If path is a symbolic link return True, else False
        :rtype: bool
        """
        return os.path.islink(self.path_without_protocol)

    def is_mount(self) -> bool:
        """Test whether a path is a mount point

        :returns: True if a path is a mount point, else False
        """
        return os.path.ismount(self.path_without_protocol)

    def cwd(self) -> "FSPath":
        """Return current working directory

        returns: Current working directory
        """
        return self.from_path(os.getcwd())

    def home(self):
        """Return the home directory

        returns: Home directory path
        """
        return self.from_path(os.path.expanduser("~"))

    def joinpath(self, *other_paths: PathLike) -> "FSPath":
        self._check_int_path()

        path = fspath(self)
        if path == ".":
            path = ""
        return self.from_path(path_join(path, *map(fspath, other_paths)))

    def save(self, file_object: BinaryIO):
        """Write the opened binary stream to path
        If parent directory of path doesn't exist, it will be created.

        :param file_object: stream to be read
        """
        FSPath(
            os.path.dirname(
                self.path_without_protocol  # pyre-ignore[6]
            )
        ).mkdir(parents=True, exist_ok=True)
        with open(self.path_without_protocol, "wb") as output:
            output.write(file_object.read())

    def open(
        self,
        mode: str = "r",
        buffering=-1,
        encoding=None,
        errors=None,
        newline=None,
        closefd=True,
        **kwargs,
    ) -> IO:
        if not isinstance(self.path_without_protocol, int) and (
            "w" in mode or "x" in mode or "a" in mode
        ):
            FSPath(
                os.path.dirname(
                    self.path_without_protocol  # pyre-ignore[6]
                )
            ).mkdir(parents=True, exist_ok=True)
        return io.open(
            self.path_without_protocol,
            mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
        )

    @cached_property
    def parts(self) -> Tuple[str, ...]:
        """
        A tuple giving access to the path’s various components
        """
        self._check_int_path()
        return pathlib.Path(self.path_without_protocol).parts  # pyre-ignore[6]

    def chmod(self, mode: int, *, follow_symlinks: bool = True):
        """
        Change the file mode and permissions, like os.chmod().

        This method normally follows symlinks.
        Some Unix flavours support changing permissions on the symlink itself;
        on these platforms you may add the argument follow_symlinks=False,
        or use lchmod().
        """
        return os.chmod(
            path=self.path_without_protocol, mode=mode, follow_symlinks=follow_symlinks
        )

    def group(self) -> str:
        """
        Return the name of the group owning the file. KeyError is raised if
        the file’s gid isn’t found in the system database.
        """
        self._check_int_path()
        return pathlib.Path(self.path_without_protocol).group()  # pyre-ignore[6]

    def is_socket(self) -> bool:
        """
        Return True if the path points to a Unix socket (or a symbolic link pointing to
        a Unix socket), False if it points to another kind of file.

        False is also returned if the path doesn’t exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        if isinstance(self.path_without_protocol, int):
            return bool(stat_issock(os.stat(self.path_without_protocol).st_mode))
        return pathlib.Path(self.path_without_protocol).is_socket()  # pyre-ignore[6]

    def is_fifo(self) -> bool:
        """
        Return True if the path points to a FIFO (or a symbolic link pointing to a FIFO)
        Return False if it points to another kind of file.

        False is also returned if the path doesn’t exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        if isinstance(self.path_without_protocol, int):
            return bool(stat_isfifo(os.stat(self.path_without_protocol).st_mode))
        return pathlib.Path(self.path_without_protocol).is_fifo()  # pyre-ignore[6]

    def is_block_device(self) -> bool:
        """
        Return True if the path points to a block device (or a symbolic link pointing to
        a block device), False if it points to another kind of file.

        False is also returned if the path doesn’t exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        if isinstance(self.path_without_protocol, int):
            return bool(stat_isblk(os.stat(self.path_without_protocol).st_mode))
        return pathlib.Path(
            self.path_without_protocol  # pyre-ignore[6]
        ).is_block_device()

    def is_char_device(self) -> bool:
        """
        Return True if the path points to a character device (or a symbolic link
        pointing to a character device), False if it points to another kind of file.

        False is also returned if the path doesn’t exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        if isinstance(self.path_without_protocol, int):
            return bool(stat_ischr(os.stat(self.path_without_protocol).st_mode))
        return pathlib.Path(
            self.path_without_protocol  # pyre-ignore[6]
        ).is_char_device()

    def owner(self) -> str:
        """
        Return the name of the user owning the file. KeyError is raised if the file’s
        uid isn’t found in the system database.
        """
        self._check_int_path()
        return pathlib.Path(self.path_without_protocol).owner()  # pyre-ignore[6]

    def absolute(self) -> "FSPath":
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object
        """
        self._check_int_path()
        return self.from_path(
            os.path.abspath(
                self.path_without_protocol  # pyre-ignore[6]
            )
        )

    def rmdir(self):
        """
        Remove this directory. The directory must be empty.
        """
        return os.rmdir(self.path_without_protocol)

    def hardlink_to(self, target):
        """
        Make this path a hard link to the same file as target.
        """
        return os.link(target, self.path)

    def utime(self, atime: Union[float, int], mtime: Union[float, int]):
        """
        Set the access and modified times of the file specified by path.

        :param atime: a float or int representing the access time to be set.
                      If it is set to None, the access time is set to the current time.
        :param mtime: a float or int representing the modified time to be set.
                      If it is set to None, the modified time is set
                      to the current time.
        :return: None
        """
        return os.utime(self.path_without_protocol, times=(atime, mtime))

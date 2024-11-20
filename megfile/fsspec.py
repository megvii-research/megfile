import io
import os
from datetime import datetime
from typing import IO, BinaryIO, Callable, Iterator, List, Optional, Tuple

try:
    import fsspec
except ImportError:  # pragma: no cover
    fsspec = None

from megfile.errors import _create_missing_ok_generator
from megfile.interfaces import ContextIterator, FileEntry, PathLike, StatResult, URIPath


def _parse_is_link(info):
    if "islink" in info:  # LocalFileSystem
        return info["islink"]
    return info["type"] == "link"  # SFTPFileSystem


def _to_timestamp(data):
    if isinstance(data, datetime):
        return data.timestamp()
    return data


def _parse_ctime(info):
    if "created" in info:  # LocalFileSystem
        return _to_timestamp(info["created"])
    if "time" in info:  # SFTPFileSystem
        return _to_timestamp(info["time"])
    return None


def _parse_mtime(info):
    if "mtime" in info:  # LocalFileSystem & SFTPFileSystem
        return _to_timestamp(info["mtime"])
    if "last_commit" in info and "date" in info["last_commit"]:  # HfFileSystem
        return _to_timestamp(info["last_commit"]["date"])
    if "LastModified" in info:  # S3FileSystem
        return _to_timestamp(info["LastModified"])
    return None


def _make_stat(info):
    return StatResult(
        islnk=_parse_is_link(info),
        isdir=info["type"] == "directory",
        size=info["size"],
        ctime=_parse_ctime(info),
        mtime=_parse_mtime(info),
        extra=info,
    )


def _make_entry(filesystem, info):
    return FileEntry(
        name=os.path.basename(info["name"]),
        path=filesystem.unstrip_protocol(info["name"]),
        stat=_make_stat(info),
    )


class BaseFSSpecPath(URIPath):
    protocol: str
    filesystem: "fsspec.AbstractFileSystem"

    def __init__(self, path: PathLike, *other_paths: PathLike):
        super().__init__(path, *other_paths)

    def exists(self, followlinks: bool = False) -> bool:
        """
        Test if the path exists

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path exists, else False

        """
        return self.filesystem.exists(self.path_without_protocol)

    def getmtime(self, follow_symlinks: bool = False) -> float:
        """
        Get last-modified time of the file on the given path (in Unix timestamp format).

        If the path is an existent directory,
        return the latest modified time of all file in it.

        :returns: last-modified time
        """
        return self.filesystem.modified(self.path_without_protocol)

    def getsize(self, follow_symlinks: bool = False) -> int:
        """
        Get file size on the given file path (in bytes).

        If the path in a directory, return the sum of all file size in it,
        including file in subdirectories (if exist).

        The result excludes the size of directory itself. In other words,
        return 0 Byte on an empty directory path.

        :returns: File size

        """
        return self.filesystem.size(self.path_without_protocol)

    def glob(
        self,
        pattern,
        recursive: bool = True,
        missing_ok: bool = True,
        followlinks: bool = False,
    ) -> List["BaseFSSpecPath"]:
        """Return path list in ascending alphabetical order,
        in which path matches glob pattern

        1. If doesn't match any path, return empty list
        Notice:  ``glob.glob`` in standard library returns ['a/'] instead of
        empty list when pathname is like `a/**`, recursive is True and directory 'a'
        doesn't exist. fs_glob behaves like ``glob.glob`` in standard library under
        such circumstance.
        2. No guarantee that each path in result is different, which means:
        Assume there exists a path `/a/b/c/b/d.txt`
        use path pattern like `/**/b/**/*.txt` to glob,
        the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default,
        when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True) in
        ascending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param pattern: Glob the given relative pattern in the directory represented
            by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :returns: A list contains paths match `pathname`
        """
        return list(
            self.iglob(
                pattern=pattern,
                recursive=recursive,
                missing_ok=missing_ok,
                followlinks=followlinks,
            )
        )

    def glob_stat(
        self,
        pattern,
        recursive: bool = True,
        missing_ok: bool = True,
        followlinks: bool = False,
    ) -> Iterator[FileEntry]:
        """Return a list contains tuples of path and file stat,
        in ascending alphabetical order, in which path matches glob pattern

        1. If doesn't match any path, return empty list
        Notice:  ``glob.glob`` in standard library returns ['a/'] instead of
        empty list when pathname is like `a/**`, recursive is True and
        directory 'a' doesn't exist. fsspec_glob behaves like ``glob.glob`` in
        standard library under such circumstance.
        2. No guarantee that each path in result is different, which means:
        Assume there exists a path `/a/b/c/b/d.txt`
        use path pattern like `/**/b/**/*.txt` to glob,
        the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default,
        when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True) in
        ascending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param pattern: Glob the given relative pattern in the directory represented
            by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :returns: A list contains tuples of path and file stat,
            in which paths match `pathname`
        """

        def create_generator():
            for info in self.filesystem.find(
                self.path_without_protocol, withdirs=True, detail=True
            ).values():
                yield _make_entry(self.filesystem, info)

        return _create_missing_ok_generator(
            create_generator(),
            missing_ok,
            FileNotFoundError("No match any file: %r" % self.path_with_protocol),
        )

    def iglob(
        self,
        pattern,
        recursive: bool = True,
        missing_ok: bool = True,
        followlinks: bool = False,
    ) -> Iterator["BaseFSSpecPath"]:
        """Return path iterator in ascending alphabetical order,
        in which path matches glob pattern

        1. If doesn't match any path, return empty list
        Notice:  ``glob.glob`` in standard library returns ['a/'] instead of
        empty list when pathname is like `a/**`, recursive is True and
        directory 'a' doesn't exist. fs_glob behaves like ``glob.glob`` in
        standard library under such circumstance.
        2. No guarantee that each path in result is different, which means:
        Assume there exists a path `/a/b/c/b/d.txt`
        use path pattern like `/**/b/**/*.txt` to glob,
        the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default,
        when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True) in
        ascending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param pattern: Glob the given relative pattern in the directory represented
            by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :returns: An iterator contains paths match `pathname`
        """
        for file_entry in self.glob_stat(
            pattern=pattern,
            recursive=recursive,
            missing_ok=missing_ok,
            followlinks=followlinks,
        ):
            yield self.from_path(file_entry.path)

    def is_dir(self, followlinks: bool = False) -> bool:
        """
        Test if a path is directory

        .. note::

            The difference between this function and ``os.path.isdir`` is that
            this function regard symlink as file

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path is a directory, else False

        """
        return self.filesystem.isdir(self.path_without_protocol)

    def is_file(self, followlinks: bool = False) -> bool:
        """
        Test if a path is file

        .. note::

            The difference between this function and ``os.path.isfile`` is that
            this function regard symlink as file

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path is a file, else False

        """
        return self.filesystem.isfile(self.path_without_protocol)

    def listdir(self, followlinks: bool = False) -> List[str]:
        """
        Get all contents of given fsspec path.
        The result is in ascending alphabetical order.

        :returns: All contents have in the path in ascending alphabetical order
        """
        entries = list(self.scandir(followlinks=followlinks))
        return sorted([entry.name for entry in entries])

    def iterdir(self, followlinks: bool = False) -> Iterator["BaseFSSpecPath"]:
        """
        Get all contents of given fsspec path.
        The result is in ascending alphabetical order.

        :returns: All contents have in the path in ascending alphabetical order
        """
        for path in self.listdir(followlinks=followlinks):
            yield self.joinpath(path)

    def load(self) -> BinaryIO:
        """Read all content on specified path and write into memory

        User should close the BinaryIO manually

        :returns: Binary stream
        """
        with self.open(mode="rb") as f:
            data = f.read()
        return io.BytesIO(data)

    def save(self, file_object: BinaryIO):
        """Write the opened binary stream to path
        If parent directory of path doesn't exist, it will be created.

        :param file_object: stream to be read
        """
        with self.open(mode="wb") as output:
            output.write(file_object.read())

    def mkdir(self, mode=0o777, parents: bool = False, exist_ok: bool = False):
        """
        make a directory with fsspec, including parent directory.
        If there exists a file on the path, raise FileExistsError

        :param mode: If mode is given, it is combined with the process’ umask value to
            determine the file mode and access flags.
        :param parents: If parents is true, any missing parents of this path
            are created as needed; If parents is false (the default),
            a missing parent raises FileNotFoundError.
        :param exist_ok: If False and target directory exists, raise FileExistsError

        :raises: FileExistsError
        """
        if self.exists():
            if not exist_ok:
                raise FileExistsError(f"File exists: '{self.path_with_protocol}'")
            return
        return self.filesystem.mkdir(self.path_without_protocol, create_parents=parents)

    def rmdir(self):
        """
        Remove this directory. The directory must be empty.
        """
        return self.filesystem.rmdir(self._real_path)

    def realpath(self) -> str:
        """Return the real path of given path

        :returns: Real path of given path
        """
        return self.resolve().path_with_protocol

    def copy(
        self,
        dst_path: PathLike,
        callback: Optional[Callable[[int], None]] = None,
        followlinks: bool = False,
        overwrite: bool = True,
    ):
        """
        Copy the file to the given destination path.

        :param dst_path: The destination path to copy the file to.
        :param callback: An optional callback function that takes an integer parameter
            and is called periodically during the copy operation to report the number
            of bytes copied.
        :param followlinks: Whether to follow symbolic links when copying directories.
        :raises IsADirectoryError: If the source is a directory.
        :raises OSError: If there is an error copying the file.
        """
        return self.filesystem.copy(
            self.path_without_protocol, dst_path, recursive=False
        )

    def sync(
        self,
        dst_path: PathLike,
        followlinks: bool = False,
        force: bool = False,
        overwrite: bool = True,
    ):
        """Copy file/directory on src_url to dst_url

        :param dst_url: Given destination path
        :param followlinks: False if regard symlink as file, else True
        :param force: Sync file forcible, do not ignore same files,
            priority is higher than 'overwrite', default is False
        :param overwrite: whether or not overwrite file when exists, default is True
        """
        return self.filesystem.copy(
            self.path_without_protocol, dst_path, recursive=True
        )

    def rename(self, dst_path: PathLike, overwrite: bool = True):
        """
        rename file with fsspec

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        self.filesystem.mv(self.path_without_protocol, dst_path, recursive=False)

    def move(self, dst_path: PathLike, overwrite: bool = True):
        """
        move file/directory with fsspec

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        self.filesystem.mv(self.path_without_protocol, dst_path, recursive=True)

    def unlink(self, missing_ok: bool = False):
        """
        Remove the file with fsspec

        :param missing_ok: if False and target file not exists, raise FileNotFoundError
        """
        if missing_ok and not self.exists():
            return
        self.filesystem.rm(self.path_without_protocol, recursive=False)

    def remove(self, missing_ok: bool = False):
        """
        Remove the file or directory with fsspec

        :param missing_ok: if False and target file/directory not exists,
            raise FileNotFoundError
        """
        if missing_ok and not self.exists():
            return
        self.filesystem.rm(self.path_without_protocol, recursive=True)

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
        scan_stat_iter = self.scan_stat(missing_ok=missing_ok, followlinks=followlinks)

        for file_entry in scan_stat_iter:
            yield file_entry.path

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

        def create_generator():
            for info in self.filesystem.find(
                self.path_without_protocol, withdirs=False, detail=True
            ).values():
                yield _make_entry(self.filesystem, info)

        return _create_missing_ok_generator(
            create_generator(),
            missing_ok,
            FileNotFoundError("No match any file in: %r" % self.path_with_protocol),
        )

    def scandir(self, followlinks: bool = False) -> Iterator[FileEntry]:
        """
        Get all content of given file path.

        :returns: An iterator contains all contents have prefix path
        """
        if not self.exists():
            raise FileNotFoundError("No such directory: %r" % self.path_with_protocol)

        if not self.is_dir():
            raise NotADirectoryError("Not a directory: %r" % self.path_with_protocol)

        def create_generator():
            for info in self.filesystem.ls(self.path_without_protocol, detail=True):
                yield _make_entry(self.filesystem, info)

        return ContextIterator(create_generator())

    def stat(self, follow_symlinks=True) -> StatResult:
        """
        Get StatResult of file with fsspec, including file size and mtime,
        referring to fs_getsize and fs_getmtime

        :returns: StatResult
        """
        return _make_stat(self.filesystem.info(self.path_without_protocol))

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
        if not self.exists(followlinks=followlinks):
            return

        if self.is_file(followlinks=followlinks):
            return

        for root, dirs, files in self.filesystem.walk(self.path_without_protocol):
            yield self.from_path(root).path_with_protocol, dirs, files

    def open(
        self,
        mode: str = "r",
        buffering=-1,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        **kwargs,
    ) -> IO:
        """Open a file on the path.

        :param mode: Mode to open file
        :param buffering: buffering is an optional integer used to
            set the buffering policy.
        :param encoding: encoding is the name of the encoding used to decode or encode
            the file. This should only be used in text mode.
        :param errors: errors is an optional string that specifies how encoding and
            decoding errors are to be handled—this cannot be used in binary mode.
        :returns: File-Like object
        """
        return self.filesystem.open(self.path_without_protocol, mode)

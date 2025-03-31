import os
from stat import S_ISDIR as stat_isdir
from stat import S_ISLNK as stat_islnk
from typing import BinaryIO, Callable, Iterator, List, Optional, Tuple

from megfile.fs_path import (
    FSPath,
    _make_stat,
    fs_path_join,
    is_fs,
)
from megfile.interfaces import Access, ContextIterator, FileEntry, PathLike, StatResult

__all__ = [
    "is_fs",
    "fs_path_join",
    "fs_readlink",
    "fs_cwd",
    "fs_home",
    "fs_iglob",
    "fs_glob",
    "fs_glob_stat",
    "fs_rename",
    "fs_resolve",
    "fs_move",
    "fs_makedirs",
    "fs_lstat",
    "fs_isabs",
    "fs_abspath",
    "fs_access",
    "fs_exists",
    "fs_getmtime",
    "fs_getsize",
    "fs_expanduser",
    "fs_isdir",
    "fs_isfile",
    "fs_listdir",
    "fs_load_from",
    "fs_realpath",
    "fs_relpath",
    "fs_remove",
    "fs_scan",
    "fs_scan_stat",
    "fs_scandir",
    "fs_stat",
    "fs_unlink",
    "fs_walk",
    "fs_getmd5",
    "fs_copy",
    "fs_sync",
    "fs_symlink",
    "fs_islink",
    "fs_ismount",
    "fs_save_as",
]


def fs_isabs(path: PathLike) -> bool:
    """Test whether a path is absolute

    :param path: Given path
    :returns: True if a path is absolute, else False
    """
    return FSPath(path).is_absolute()


def fs_abspath(path: PathLike) -> str:
    """Return the absolute path of given path

    :param path: Given path
    :returns: Absolute path of given path
    """
    return FSPath(path).abspath()


def fs_access(path: PathLike, mode: Access = Access.READ) -> bool:
    """
    Test if path has access permission described by mode
    Using ``os.access``

    :param path: Given path
    :param mode: access mode
    :returns: Access: Enum, the read/write access that path has.
    """
    return FSPath(path).access(mode)


def fs_exists(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if the path exists

    .. note::

        The difference between this function and ``os.path.exists`` is that
        this function regard symlink as file.
        In other words, this function is equal to ``os.path.lexists``

    :param path: Given path
    :param followlinks: False if regard symlink as file, else True
    :returns: True if the path exists, else False

    """
    return FSPath(path).exists(followlinks)


def fs_getmtime(path: PathLike, follow_symlinks: bool = False) -> float:
    """
    Get last-modified time of the file on the given path (in Unix timestamp format).
    If the path is an existent directory,
    return the latest modified time of all file in it.

    :param path: Given path
    :returns: last-modified time
    """
    return FSPath(path).getmtime(follow_symlinks)


def fs_getsize(path: PathLike, follow_symlinks: bool = False) -> int:
    """
    Get file size on the given file path (in bytes).
    If the path in a directory, return the sum of all file size in it,
    including file in subdirectories (if exist).
    The result excludes the size of directory itself.
    In other words, return 0 Byte on an empty directory path.

    :param path: Given path
    :returns: File size

    """
    return FSPath(path).getsize(follow_symlinks)


def fs_expanduser(path: PathLike):
    """Expand ~ and ~user constructions.  If user or $HOME is unknown,
    do nothing.
    """
    return FSPath(path).expanduser()


def fs_isdir(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if a path is directory

    .. note::

        The difference between this function and ``os.path.isdir`` is that
        this function regard symlink as file

    :param path: Given path
    :param followlinks: False if regard symlink as file, else True
    :returns: True if the path is a directory, else False

    """
    return FSPath(path).is_dir(followlinks)


def fs_isfile(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if a path is file

    .. note::

        The difference between this function and ``os.path.isfile`` is that
        this function regard symlink as file

    :param path: Given path
    :param followlinks: False if regard symlink as file, else True
    :returns: True if the path is a file, else False

    """
    return FSPath(path).is_file(followlinks)


def fs_listdir(path: Optional[PathLike] = None) -> List[str]:
    """
    Get all contents of given fs path.
    The result is in ascending alphabetical order.

    :param path: Given path
    :returns: All contents have in the path in ascending alphabetical order
    """
    if path is None:
        return sorted(os.listdir(path))
    return FSPath(path).listdir()


def fs_load_from(path: PathLike) -> BinaryIO:
    """Read all content on specified path and write into memory

    User should close the BinaryIO manually

    :param path: Given path
    :returns: Binary stream
    """
    return FSPath(path).load()


def fs_realpath(path: PathLike) -> str:
    """Return the real path of given path

    :param path: Given path
    :returns: Real path of given path
    """
    return FSPath(path).realpath()


def fs_relpath(path: PathLike, start: Optional[str] = None) -> str:
    """Return the relative path of given path

    :param path: Given path
    :param start: Given start directory
    :returns: Relative path from start
    """
    return FSPath(path).relpath(start)


def fs_remove(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file or directory on fs

    :param path: Given path
    :param missing_ok: if False and target file/directory not exists,
        raise FileNotFoundError
    """
    return FSPath(path).remove(missing_ok)


def fs_scan(
    path: PathLike, missing_ok: bool = True, followlinks: bool = False
) -> Iterator[str]:
    """
    Iteratively traverse only files in given directory, in alphabetical order.
    Every iteration on generator yields a path string.

    If path is a file path, yields the file only
    If path is a non-existent path, return an empty generator
    If path is a bucket path, return all file paths in the bucket

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory,
        raise FileNotFoundError
    :returns: A file path generator
    """
    return FSPath(path).scan(missing_ok, followlinks)


def fs_scan_stat(
    path: PathLike, missing_ok: bool = True, followlinks: bool = False
) -> Iterator[FileEntry]:
    """
    Iteratively traverse only files in given directory, in alphabetical order.
    Every iteration on generator yields a tuple of path string and file stat

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory,
        raise FileNotFoundError
    :returns: A file path generator
    """
    return FSPath(path).scan_stat(missing_ok, followlinks)


def fs_scandir(path: Optional[PathLike] = None) -> Iterator[FileEntry]:
    """
    Get all content of given file path.

    :param path: Given path
    :returns: An iterator contains all contents have prefix path
    """
    if path is None:

        def create_generator():
            with os.scandir(None) as entries:
                for entry in entries:
                    stat = entry.stat()
                    yield FileEntry(
                        entry.name,
                        entry.path,
                        StatResult(
                            size=stat.st_size,
                            ctime=stat.st_ctime,
                            mtime=stat.st_mtime,
                            isdir=stat_isdir(stat.st_mode),
                            islnk=stat_islnk(stat.st_mode),
                            extra=stat,
                        ),
                    )

        return ContextIterator(create_generator())

    return FSPath(path).scandir()


def fs_stat(path: PathLike, follow_symlinks=True) -> StatResult:
    """
    Get StatResult of file on fs, including file size and mtime,
    referring to fs_getsize and fs_getmtime

    :param path: Given path
    :returns: StatResult
    """
    return FSPath(path).stat(follow_symlinks)


def fs_unlink(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file on fs

    :param path: Given path
    :param missing_ok: if False and target file not exists, raise FileNotFoundError
    """
    return FSPath(path).unlink(missing_ok)


def fs_walk(
    path: PathLike, followlinks: bool = False
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

    :param path: Given path
    :param followlinks: False if regard symlink as file, else True

    :returns: A 3-tuple generator
    """
    return FSPath(path).walk(followlinks)


def fs_getmd5(path: PathLike, recalculate: bool = False, followlinks: bool = False):
    """
    Calculate the md5 value of the file

    :param path: Given path
    :param recalculate: Ignore this parameter, just for compatibility
    :param followlinks: Ignore this parameter, just for compatibility

    returns: md5 of file
    """
    return FSPath(path).md5(recalculate, followlinks)


def fs_copy(
    src_path: PathLike,
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

    :param src_path: Given path
    :param dst_path: Target file path
    :param callback: Called periodically during copy, and the input parameter is
        the data size (in bytes) of copy since the last call
    :param followlinks: False if regard symlink as file, else True
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    return FSPath(src_path).copy(dst_path, callback, followlinks, overwrite)


def fs_sync(
    src_path: PathLike,
    dst_path: PathLike,
    followlinks: bool = False,
    force: bool = False,
    overwrite: bool = True,
) -> None:
    """Force write of everything to disk.

    :param src_path: Given path
    :param dst_path: Target file path
    :param followlinks: False if regard symlink as file, else True
    :param force: Sync file forcible, do not ignore same files,
        priority is higher than 'overwrite', default is False
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    return FSPath(src_path).sync(dst_path, followlinks, force, overwrite)


def fs_symlink(src_path: PathLike, dst_path: PathLike) -> None:
    """
    Create a symbolic link pointing to src_path named dst_path.

    :param src_path: Given path
    :param dst_path: Destination path
    """
    return FSPath(src_path).symlink(dst_path)


def fs_islink(path: PathLike) -> bool:
    """Test whether a path is a symbolic link

    :param path: Given path
    :return: If path is a symbolic link return True, else False
    :rtype: bool
    """
    return FSPath(path).is_symlink()


def fs_ismount(path: PathLike) -> bool:
    """Test whether a path is a mount point

    :param path: Given path
    :returns: True if a path is a mount point, else False
    """
    return FSPath(path).is_mount()


def fs_save_as(file_object: BinaryIO, path: PathLike):
    """Write the opened binary stream to path
    If parent directory of path doesn't exist, it will be created.

    :param path: Given path
    :param file_object: stream to be read
    """
    return FSPath(path).save(file_object)


def fs_readlink(path) -> str:
    """
    Return a string representing the path to which the symbolic link points.
    :returns: Return a string representing the path to which the symbolic link points.
    """
    return FSPath(path).readlink().path_without_protocol  # pyre-ignore[7]


def fs_cwd() -> str:
    """Return current working directory

    returns: Current working directory
    """
    return os.getcwd()


def fs_home():
    """Return the home directory

    returns: Home directory path
    """
    return os.path.expanduser("~")


def fs_iglob(
    path: PathLike, recursive: bool = True, missing_ok: bool = True
) -> Iterator[str]:
    """Return path iterator in ascending alphabetical order,
    in which path matches glob pattern

    1. If doesn't match any path, return empty list
        Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list
        when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist.
        fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
    2. No guarantee that each path in result is different, which means:
        Assume there exists a path `/a/b/c/b/d.txt`
        use path pattern like `/**/b/**/*.txt` to glob,
        the path above will be returned twice
    3. `**` will match any matched file, directory, symlink and '' by default,
        when recursive is `True`
    4. fs_glob returns same as glob.glob(pathname, recursive=True)
        in ascending alphabetical order.
    5. Hidden files (filename stars with '.') will not be found in the result

    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :returns: An iterator contains paths match `pathname`
    """
    for path_obj in FSPath(path).iglob(
        pattern="", recursive=recursive, missing_ok=missing_ok
    ):
        yield path_obj.path_without_protocol  # pyre-ignore[7]


def fs_glob(
    path: PathLike, recursive: bool = True, missing_ok: bool = True
) -> List[str]:
    """Return path list in ascending alphabetical order,
    in which path matches glob pattern

    1. If doesn't match any path, return empty list
        Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list
        when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist.
        fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
    2. No guarantee that each path in result is different, which means:
        Assume there exists a path `/a/b/c/b/d.txt`
        use path pattern like `/**/b/**/*.txt` to glob,
        the path above will be returned twice
    3. `**` will match any matched file, directory, symlink and '' by default,
        when recursive is `True`
    4. fs_glob returns same as glob.glob(pathname, recursive=True)
        in ascending alphabetical order.
    5. Hidden files (filename stars with '.') will not be found in the result

    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :returns: A list contains paths match `pathname`
    """
    return list(fs_iglob(path=path, recursive=recursive, missing_ok=missing_ok))


def fs_glob_stat(
    path: PathLike, recursive: bool = True, missing_ok: bool = True
) -> Iterator[FileEntry]:
    """Return a list contains tuples of path and file stat,
    in ascending alphabetical order, in which path matches glob pattern

    1. If doesn't match any path, return empty list
        Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list
        when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist.
        fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
    2. No guarantee that each path in result is different, which means:
        Assume there exists a path `/a/b/c/b/d.txt`
        use path pattern like `/**/b/**/*.txt` to glob,
        the path above will be returned twice.
    3. `**` will match any matched file, directory, symlink and '' by default,
        when recursive is `True`
    4. fs_glob returns same as glob.glob(pathname, recursive=True)
        in ascending alphabetical order.
    5. Hidden files (filename stars with '.') will not be found in the result

    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :returns: A list contains tuples of path and file stat,
        in which paths match `pathname`
    """
    for path in fs_iglob(path=path, recursive=recursive, missing_ok=missing_ok):
        yield FileEntry(os.path.basename(path), path, _make_stat(os.lstat(path)))


def fs_resolve(path: PathLike) -> str:
    """Equal to fs_realpath, return the real path of given path

    :param path: Given path
    :returns: Real path of given path
    """
    return FSPath(path).realpath()


def fs_makedirs(path: PathLike, exist_ok: bool = False):
    """
    make a directory on fs, including parent directory

    If there exists a file on the path, raise FileExistsError

    :param path: Given path
    :param exist_ok: If False and target directory exists, raise FileExistsError
    :raises: FileExistsError
    """
    return FSPath(path).mkdir(parents=True, exist_ok=exist_ok)


def fs_lstat(path: PathLike) -> StatResult:
    """
    Like Path.stat() but, if the path points to a symbolic link,
    return the symbolic link’s information rather than its target’s.

    :param path: Given path
    :returns: StatResult
    """
    return FSPath(path).lstat()


def fs_rename(src_path: PathLike, dst_path: PathLike, overwrite: bool = True) -> None:
    """
    rename file on fs

    :param src_path: Given path
    :param dst_path: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    FSPath(src_path).rename(dst_path, overwrite)


def fs_move(src_path: PathLike, dst_path: PathLike, overwrite: bool = True) -> None:
    """
    rename file on fs

    :param src_path: Given path
    :param dst_path: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    return fs_rename(src_path, dst_path, overwrite)

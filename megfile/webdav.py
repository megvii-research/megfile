from logging import getLogger as get_logger
from typing import IO, BinaryIO, Callable, Iterator, List, Optional, Tuple

from megfile.interfaces import FileEntry, PathLike, StatResult
from megfile.lib.compat import fspath
from megfile.lib.joinpath import uri_join
from megfile.webdav_path import (
    WebdavPath,
    is_webdav,
)

_logger = get_logger(__name__)

__all__ = [
    "is_webdav",
    "webdav_glob",
    "webdav_iglob",
    "webdav_glob_stat",
    "webdav_resolve",
    "webdav_download",
    "webdav_upload",
    "webdav_path_join",
    "webdav_exists",
    "webdav_getmtime",
    "webdav_getsize",
    "webdav_isdir",
    "webdav_isfile",
    "webdav_listdir",
    "webdav_load_from",
    "webdav_makedirs",
    "webdav_realpath",
    "webdav_rename",
    "webdav_move",
    "webdav_remove",
    "webdav_scan",
    "webdav_scan_stat",
    "webdav_scandir",
    "webdav_stat",
    "webdav_unlink",
    "webdav_walk",
    "webdav_getmd5",
    "webdav_save_as",
    "webdav_open",
    "webdav_absolute",
    "webdav_rmdir",
    "webdav_copy",
    "webdav_sync",
]


def webdav_glob(
    path: PathLike, recursive: bool = True, missing_ok: bool = True
) -> List[str]:
    """Return path list in ascending alphabetical order,
    in which path matches glob pattern

    :param path: Given path
    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :returns: A list contains paths match `pathname`
    """
    return list(
        sorted(webdav_iglob(path=path, recursive=recursive, missing_ok=missing_ok))
    )


def webdav_glob_stat(
    path: PathLike, recursive: bool = True, missing_ok: bool = True
) -> Iterator[FileEntry]:
    """Return a list contains tuples of path and file stat, in ascending alphabetical
    order, in which path matches glob pattern

    :param path: Given path
    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :returns: A list contains tuples of path and file stat,
        in which paths match `pathname`
    """
    for entry in WebdavPath(path).glob_stat(
        pattern="", recursive=recursive, missing_ok=missing_ok
    ):
        yield entry


def webdav_iglob(
    path: PathLike, recursive: bool = True, missing_ok: bool = True
) -> Iterator[str]:
    """Return path iterator in ascending alphabetical order,
    in which path matches glob pattern

    :param path: Given path
    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :returns: An iterator contains paths match `pathname`
    """
    for path in WebdavPath(path).iglob(
        pattern="", recursive=recursive, missing_ok=missing_ok
    ):
        yield path.path_with_protocol


def webdav_resolve(path: PathLike, strict=False) -> "str":
    """Return the absolute path

    :param path: Given path
    :param strict: Ignored for WebDAV
    :return: Absolute path
    """
    return WebdavPath(path).resolve(strict).path_with_protocol


def webdav_download(
    src_url: PathLike,
    dst_url: PathLike,
    callback: Optional[Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
):
    """
    Downloads a file from WebDAV to local filesystem.

    :param src_url: source WebDAV path
    :param dst_url: target fs path
    :param callback: Called periodically during copy
    :param followlinks: Ignored for WebDAV
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    from megfile.fs import is_fs
    from megfile.fs_path import FSPath

    if not is_fs(dst_url):
        raise OSError(f"dst_url is not fs path: {dst_url}")
    if not is_webdav(src_url) and not isinstance(src_url, WebdavPath):
        raise OSError(f"src_url is not webdav path: {src_url}")

    dst_path = FSPath(dst_url)
    if not overwrite and dst_path.exists():
        return

    if isinstance(src_url, WebdavPath):
        src_path: WebdavPath = src_url
    else:
        src_path: WebdavPath = WebdavPath(src_url)

    if src_path.is_dir():
        raise IsADirectoryError("Is a directory: %r" % src_url)
    if str(dst_url).endswith("/"):
        raise IsADirectoryError("Is a directory: %r" % dst_url)

    dst_path.parent.makedirs(exist_ok=True)

    # Download file
    with src_path.open("rb") as fsrc:
        with dst_path.open("wb") as fdst:
            if callback:
                bytes_written = 0
                while True:
                    chunk = fsrc.read(8192)
                    if not chunk:
                        break
                    fdst.write(chunk)
                    callback(len(chunk))
                    bytes_written += len(chunk)
            else:
                fdst.write(fsrc.read())

    # Preserve modification time
    src_stat = src_path.stat()
    dst_path.utime(src_stat.st_atime, src_stat.st_mtime)


def webdav_upload(
    src_url: PathLike,
    dst_url: PathLike,
    callback: Optional[Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
):
    """
    Uploads a file from local filesystem to WebDAV server.

    :param src_url: source fs path
    :param dst_url: target WebDAV path
    :param callback: Called periodically during copy
    :param followlinks: Follow symlinks for local files
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    import os

    from megfile.fs import is_fs
    from megfile.fs_path import FSPath

    if not is_fs(src_url):
        raise OSError(f"src_url is not fs path: {src_url}")
    if not is_webdav(dst_url) and not isinstance(dst_url, WebdavPath):
        raise OSError(f"dst_url is not webdav path: {dst_url}")

    if followlinks and os.path.islink(src_url):
        src_url = os.readlink(src_url)
    if os.path.isdir(src_url):
        raise IsADirectoryError("Is a directory: %r" % src_url)
    if str(dst_url).endswith("/"):
        raise IsADirectoryError("Is a directory: %r" % dst_url)

    src_path = FSPath(src_url)
    if isinstance(dst_url, WebdavPath):
        dst_path: WebdavPath = dst_url
    else:
        dst_path: WebdavPath = WebdavPath(dst_url)

    if not overwrite and dst_path.exists():
        return

    dst_path.parent.makedirs(exist_ok=True)

    # Upload file
    with src_path.open("rb") as fsrc:
        with dst_path.open("wb") as fdst:
            if callback:
                while True:
                    chunk = fsrc.read(8192)
                    if not chunk:
                        break
                    fdst.write(chunk)
                    callback(len(chunk))
            else:
                fdst.write(fsrc.read())


def webdav_path_join(path: PathLike, *other_paths: PathLike) -> str:
    """
    Concat 2 or more path to a complete path

    :param path: Given path
    :param other_paths: Paths to be concatenated
    :returns: Concatenated complete path
    """
    return uri_join(fspath(path), *map(fspath, other_paths))


def webdav_exists(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if the path exists

    :param path: Given path
    :param followlinks: Ignored for WebDAV
    :returns: True if the path exists, else False
    """
    return WebdavPath(path).exists(followlinks)


def webdav_getmtime(path: PathLike, follow_symlinks: bool = False) -> float:
    """
    Get last-modified time of the file on the given path (in Unix timestamp format).

    :param path: Given path
    :param follow_symlinks: Ignored for WebDAV
    :returns: last-modified time
    """
    return WebdavPath(path).getmtime(follow_symlinks)


def webdav_getsize(path: PathLike, follow_symlinks: bool = False) -> int:
    """
    Get file size on the given file path (in bytes).

    :param path: Given path
    :param follow_symlinks: Ignored for WebDAV
    :returns: File size
    """
    return WebdavPath(path).getsize(follow_symlinks)


def webdav_isdir(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if a path is directory

    :param path: Given path
    :param followlinks: Ignored for WebDAV
    :returns: True if the path is a directory, else False
    """
    return WebdavPath(path).is_dir(followlinks)


def webdav_isfile(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if a path is file

    :param path: Given path
    :param followlinks: Ignored for WebDAV
    :returns: True if the path is a file, else False
    """
    return WebdavPath(path).is_file(followlinks)


def webdav_listdir(path: PathLike) -> List[str]:
    """
    Get all contents of given WebDAV path.
    The result is in ascending alphabetical order.

    :param path: Given path
    :returns: All contents in ascending alphabetical order
    """
    return WebdavPath(path).listdir()


def webdav_load_from(path: PathLike) -> BinaryIO:
    """Read all content on specified path and write into memory

    User should close the BinaryIO manually

    :param path: Given path
    :returns: Binary stream
    """
    return WebdavPath(path).load()


def webdav_makedirs(
    path: PathLike, mode=0o777, parents: bool = False, exist_ok: bool = False
):
    """
    Make a directory on WebDAV, including parent directory.

    :param path: Given path
    :param mode: Ignored for WebDAV
    :param parents: If parents is true, any missing parents are created
    :param exist_ok: If False and target directory exists, raise FileExistsError
    """
    return WebdavPath(path).mkdir(mode, parents, exist_ok)


def webdav_realpath(path: PathLike) -> str:
    """Return the real path of given path

    :param path: Given path
    :returns: Real path of given path
    """
    return WebdavPath(path).realpath()


def webdav_rename(
    src_path: PathLike, dst_path: PathLike, overwrite: bool = True
) -> "WebdavPath":
    """
    Rename file on WebDAV

    :param src_path: Given path
    :param dst_path: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    return WebdavPath(src_path).rename(dst_path, overwrite)


def webdav_move(
    src_path: PathLike, dst_path: PathLike, overwrite: bool = True
) -> "WebdavPath":
    """
    Move file on WebDAV

    :param src_path: Given path
    :param dst_path: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    return WebdavPath(src_path).replace(dst_path, overwrite)


def webdav_remove(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file or directory on WebDAV

    :param path: Given path
    :param missing_ok: if False and target file/directory not exists,
        raise FileNotFoundError
    """
    return WebdavPath(path).remove(missing_ok)


def webdav_scan(
    path: PathLike, missing_ok: bool = True, followlinks: bool = False
) -> Iterator[str]:
    """
    Iteratively traverse only files in given directory, in alphabetical order.

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory,
        raise FileNotFoundError
    :param followlinks: Ignored for WebDAV
    :returns: A file path generator
    """
    return WebdavPath(path).scan(missing_ok, followlinks)


def webdav_scan_stat(
    path: PathLike, missing_ok: bool = True, followlinks: bool = False
) -> Iterator[FileEntry]:
    """
    Iteratively traverse only files in given directory, in alphabetical order.

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory,
        raise FileNotFoundError
    :param followlinks: Ignored for WebDAV
    :returns: A file path generator yielding FileEntry objects
    """
    return WebdavPath(path).scan_stat(missing_ok, followlinks)


def webdav_scandir(path: PathLike) -> Iterator[FileEntry]:
    """
    Get all content of given file path.

    :param path: Given path
    :returns: An iterator contains all contents
    """
    return WebdavPath(path).scandir()


def webdav_stat(path: PathLike, follow_symlinks=True) -> StatResult:
    """
    Get StatResult of file on WebDAV

    :param path: Given path
    :param follow_symlinks: Ignored for WebDAV
    :returns: StatResult
    """
    return WebdavPath(path).stat(follow_symlinks)


def webdav_unlink(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file on WebDAV

    :param path: Given path
    :param missing_ok: if False and target file not exists, raise FileNotFoundError
    """
    return WebdavPath(path).unlink(missing_ok)


def webdav_walk(
    path: PathLike, followlinks: bool = False
) -> Iterator[Tuple[str, List[str], List[str]]]:
    """
    Generate the file names in a directory tree by walking the tree top-down.

    :param path: Given path
    :param followlinks: Ignored for WebDAV
    :returns: A 3-tuple generator (root, dirs, files)
    """
    return WebdavPath(path).walk(followlinks)


def webdav_getmd5(path: PathLike, recalculate: bool = False, followlinks: bool = False):
    """
    Calculate the md5 value of the file

    :param path: Given path
    :param recalculate: Ignored for WebDAV
    :param followlinks: Ignored for WebDAV
    :returns: md5 of file
    """
    return WebdavPath(path).md5(recalculate, followlinks)


def webdav_save_as(file_object: BinaryIO, path: PathLike):
    """Write the opened binary stream to path

    :param file_object: stream to be read
    :param path: Given path
    """
    return WebdavPath(path).save(file_object)


def webdav_open(
    path: PathLike,
    mode: str = "r",
    *,
    buffering=-1,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    **kwargs,
) -> IO:
    """Open a file on the path.

    :param path: Given path
    :param mode: Mode to open file
    :param buffering: buffering policy
    :param encoding: encoding for text mode
    :param errors: error handling for text mode
    :returns: File-Like object
    """
    return WebdavPath(path).open(
        mode, buffering=buffering, encoding=encoding, errors=errors
    )


def webdav_absolute(path: PathLike) -> "WebdavPath":
    """
    Make the path absolute

    :param path: Given path
    :returns: Absolute path
    """
    return WebdavPath(path).absolute()


def webdav_rmdir(path: PathLike):
    """
    Remove this directory. The directory must be empty.

    :param path: Given path
    """
    return WebdavPath(path).rmdir()


def webdav_copy(
    src_path: PathLike,
    dst_path: PathLike,
    callback: Optional[Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
):
    """
    Copy the file to the given destination path.

    :param src_path: Given path
    :param dst_path: The destination path to copy the file to.
    :param callback: An optional callback function
    :param followlinks: Ignored for WebDAV
    :param overwrite: whether to overwrite existing file
    """
    return WebdavPath(src_path).copy(dst_path, callback, followlinks, overwrite)


def webdav_sync(
    src_path: PathLike,
    dst_path: PathLike,
    followlinks: bool = False,
    force: bool = False,
    overwrite: bool = True,
):
    """Copy file/directory on src_path to dst_path

    :param src_path: Given path
    :param dst_path: Given destination path
    :param followlinks: Ignored for WebDAV
    :param force: Sync file forcibly
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    return WebdavPath(src_path).sync(dst_path, followlinks, force, overwrite)

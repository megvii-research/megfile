import base64
import hashlib
import os
from logging import getLogger as get_logger
from typing import IO, BinaryIO, Callable, Iterator, List, Optional, Tuple

import ssh2.session  # type: ignore
import ssh2.utils  # type: ignore

from megfile.interfaces import FileEntry, PathLike, StatResult
from megfile.lib.compat import fspath
from megfile.lib.joinpath import uri_join
from megfile.sftp2_path import (
    Sftp2Path,
    is_sftp2,
)
from megfile.utils import copyfileobj

_logger = get_logger(__name__)

__all__ = [
    "is_sftp2",
    "sftp2_readlink",
    "sftp2_glob",
    "sftp2_iglob",
    "sftp2_glob_stat",
    "sftp2_resolve",
    "sftp2_download",
    "sftp2_upload",
    "sftp2_path_join",
    "sftp2_concat",
    "sftp2_lstat",
    "sftp2_exists",
    "sftp2_getmtime",
    "sftp2_getsize",
    "sftp2_isdir",
    "sftp2_isfile",
    "sftp2_listdir",
    "sftp2_load_from",
    "sftp2_makedirs",
    "sftp2_realpath",
    "sftp2_rename",
    "sftp2_move",
    "sftp2_remove",
    "sftp2_scan",
    "sftp2_scan_stat",
    "sftp2_scandir",
    "sftp2_stat",
    "sftp2_unlink",
    "sftp2_walk",
    "sftp2_getmd5",
    "sftp2_symlink",
    "sftp2_islink",
    "sftp2_save_as",
    "sftp2_open",
    "sftp2_chmod",
    "sftp2_absolute",
    "sftp2_rmdir",
    "sftp2_copy",
    "sftp2_sync",
    "sftp2_add_host_key",
]


def sftp2_readlink(path: PathLike) -> "str":
    """
    Return a Sftp2Path instance representing the path to which the symbolic link points.

    :param path: Given path
    :returns: Return a Sftp2Path instance representing the path to
        which the symbolic link points.
    """
    return Sftp2Path(path).readlink().path_with_protocol


def sftp2_glob(
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

    :param path: Given path
    :param pattern: Glob the given relative pattern in the directory represented
        by this path
    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :returns: A list contains paths match `pathname`
    """
    return list(sftp2_iglob(path=path, recursive=recursive, missing_ok=missing_ok))


def sftp2_glob_stat(
    path: PathLike, recursive: bool = True, missing_ok: bool = True
) -> Iterator[FileEntry]:
    """Return a list contains tuples of path and file stat, in ascending alphabetical
    order, in which path matches glob pattern

    1. If doesn't match any path, return empty list
       Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list
       when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist.
       sftp2_glob behaves like ``glob.glob`` in standard library under such
       circumstance.
    2. No guarantee that each path in result is different, which means:
       Assume there exists a path `/a/b/c/b/d.txt`
       use path pattern like `/**/b/**/*.txt` to glob,
       the path above will be returned twice
    3. `**` will match any matched file, directory, symlink and '' by default,
       when recursive is `True`
    4. fs_glob returns same as glob.glob(pathname, recursive=True) in
       ascending alphabetical order.
    5. Hidden files (filename stars with '.') will not be found in the result

    :param path: Given path
    :param pattern: Glob the given relative pattern in the directory represented
        by this path
    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :returns: A list contains tuples of path and file stat,
        in which paths match `pathname`
    """
    for path in sftp2_iglob(path=path, recursive=recursive, missing_ok=missing_ok):
        path_object = Sftp2Path(path)
        yield FileEntry(
            path_object.name, path_object.path_with_protocol, path_object.lstat()
        )


def sftp2_iglob(
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
    4. fs_glob returns same as glob.glob(pathname, recursive=True) in
       ascending alphabetical order.
    5. Hidden files (filename stars with '.') will not be found in the result

    :param path: Given path
    :param pattern: Glob the given relative pattern in the directory represented
        by this path
    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :returns: An iterator contains paths match `pathname`
    """

    for path in Sftp2Path(path).iglob(
        pattern="", recursive=recursive, missing_ok=missing_ok
    ):
        yield path.path_with_protocol


def sftp2_resolve(path: PathLike, strict=False) -> "str":
    """Equal to fs_realpath

    :param path: Given path
    :param strict: Ignore this parameter, just for compatibility
    :return: Return the canonical path of the specified filename,
        eliminating any symbolic links encountered in the path.
    :rtype: Sftp2Path
    """
    return Sftp2Path(path).resolve(strict).path_with_protocol


def sftp2_download(
    src_url: PathLike,
    dst_url: PathLike,
    callback: Optional[Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
):
    """
    Downloads a file from sftp2 to local filesystem.

    :param src_url: source sftp2 path
    :param dst_url: target fs path
    :param callback: Called periodically during copy, and the input parameter is
        the data size (in bytes) of copy since the last call
    :param followlinks: False if regard symlink as file, else True
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    from megfile.fs import is_fs
    from megfile.fs_path import FSPath

    if not is_fs(dst_url):
        raise OSError(f"dst_url is not fs path: {dst_url}")
    if not is_sftp2(src_url) and not isinstance(src_url, Sftp2Path):
        raise OSError(f"src_url is not sftp2 path: {src_url}")

    dst_path = FSPath(dst_url)
    if not overwrite and dst_path.exists():
        return

    if isinstance(src_url, Sftp2Path):
        src_path: Sftp2Path = src_url
    else:
        src_path: Sftp2Path = Sftp2Path(src_url)

    if followlinks and src_path.is_symlink():
        src_path = src_path.readlink()
    if src_path.is_dir():
        raise IsADirectoryError(f"Is a directory: {src_url!r}")
    if str(dst_url).endswith("/"):
        raise IsADirectoryError(f"Is a directory: {dst_url!r}")

    dst_path.parent.makedirs(exist_ok=True)

    with src_path.open("rb") as src_file, dst_path.open("wb") as dst_file:
        copyfileobj(src_file, dst_file, callback)

    src_stat = src_path.stat()
    dst_path.utime(src_stat.st_atime, src_stat.st_mtime)
    dst_path.chmod(src_stat.st_mode)


def sftp2_upload(
    src_url: PathLike,
    dst_url: PathLike,
    callback: Optional[Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
):
    """
    Uploads a file from local filesystem to sftp2 server.

    :param src_url: source fs path
    :param dst_url: target sftp2 path
    :param callback: Called periodically during copy, and the input parameter is
        the data size (in bytes) of copy since the last call
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    from megfile.fs import is_fs
    from megfile.fs_path import FSPath

    if not is_fs(src_url):
        raise OSError(f"src_url is not fs path: {src_url}")
    if not is_sftp2(dst_url) and not isinstance(dst_url, Sftp2Path):
        raise OSError(f"dst_url is not sftp2 path: {dst_url}")

    if followlinks and os.path.islink(src_url):
        src_url = os.readlink(src_url)
    if os.path.isdir(src_url):
        raise IsADirectoryError(f"Is a directory: {src_url!r}")
    if str(dst_url).endswith("/"):
        raise IsADirectoryError(f"Is a directory: {dst_url!r}")

    src_path = FSPath(src_url)
    if isinstance(dst_url, Sftp2Path):
        dst_path: Sftp2Path = dst_url
    else:
        dst_path: Sftp2Path = Sftp2Path(dst_url)
    if not overwrite and dst_path.exists():
        return

    dst_path.parent.makedirs(exist_ok=True)

    with src_path.open("rb") as src_file, dst_path.open("wb") as dst_file:
        copyfileobj(src_file, dst_file, callback)

    src_stat = src_path.stat()
    dst_path.utime(src_stat.st_atime, src_stat.st_mtime)
    dst_path.chmod(src_stat.st_mode)


def sftp2_path_join(path: PathLike, *other_paths: PathLike) -> str:
    """
    Concat 2 or more path to a complete path

    :param path: Given path
    :param other_paths: Paths to be concatenated
    :returns: Concatenated complete path

    .. note ::

        The difference between this function and ``os.path.join`` is that this function
        ignores left side slash (which indicates absolute path) in ``other_paths``
        and will directly concat.

        e.g. os.path.join('/path', 'to', '/file') => '/file',
        but sftp2_path_join('/path', 'to', '/file') => '/path/to/file'
    """
    return uri_join(fspath(path), *map(fspath, other_paths))


def sftp2_concat(src_paths: List[PathLike], dst_path: PathLike) -> None:
    """Concatenate sftp2 files to one file.

    :param src_paths: Given source paths
    :param dst_path: Given destination path
    """
    dst_path_obj = Sftp2Path(dst_path)

    if len(src_paths) == 0:
        return

    # Check if all sources are on the same server as destination
    all_same_backend = all(
        dst_path_obj._is_same_backend(Sftp2Path(src_path)) for src_path in src_paths
    )

    if all_same_backend and len(src_paths) > 1:
        # Use server-side cat command for efficiency
        def get_real_path(path: PathLike) -> str:
            return Sftp2Path(path)._real_path

        exec_result = dst_path_obj._exec_command(
            [
                "cat",
                *map(get_real_path, src_paths),
                ">",
                get_real_path(dst_path),
            ]
        )

        if exec_result.returncode != 0:
            # Log the failure but fall back to SFTP method
            _logger.error(exec_result.stderr)
            raise OSError(
                f"Failed to concat files, returncode: {exec_result.returncode}, "
                f"{exec_result.stderr}"
            )

    # Fallback to traditional SFTP concat (download then upload)
    with dst_path_obj.open("wb") as dst_file:
        for src_path in src_paths:
            src_path_obj = Sftp2Path(src_path)
            with src_path_obj.open("rb") as src_file:
                # Use the copyfileobj utility function
                copyfileobj(src_file, dst_file)


def sftp2_lstat(path: PathLike) -> StatResult:
    """
    Get StatResult of file on sftp2, including file size and mtime,
    referring to fs_getsize and fs_getmtime

    :param path: Given path
    :returns: StatResult
    """
    return Sftp2Path(path).lstat()


def sftp2_exists(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if the path exists

    :param path: Given path
    :param followlinks: False if regard symlink as file, else True
    :returns: True if the path exists, else False

    """
    return Sftp2Path(path).exists(followlinks)


def sftp2_getmtime(path: PathLike, follow_symlinks: bool = False) -> float:
    """
    Get last-modified time of the file on the given path (in Unix timestamp format).

    If the path is an existent directory,
    return the latest modified time of all file in it.

    :param path: Given path
    :returns: last-modified time
    """
    return Sftp2Path(path).getmtime(follow_symlinks)


def sftp2_getsize(path: PathLike, follow_symlinks: bool = False) -> int:
    """
    Get file size on the given file path (in bytes).

    If the path in a directory, return the sum of all file size in it,
    including file in subdirectories (if exist).

    The result excludes the size of directory itself. In other words,
    return 0 Byte on an empty directory path.

    :param path: Given path
    :returns: File size

    """
    return Sftp2Path(path).getsize(follow_symlinks)


def sftp2_isdir(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if a path is directory

    .. note::

        The difference between this function and ``os.path.isdir`` is that
        this function regard symlink as file

    :param path: Given path
    :param followlinks: False if regard symlink as file, else True
    :returns: True if the path is a directory, else False

    """
    return Sftp2Path(path).is_dir(followlinks)


def sftp2_isfile(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if a path is file

    .. note::

        The difference between this function and ``os.path.isfile`` is that
        this function regard symlink as file

    :param path: Given path
    :param followlinks: False if regard symlink as file, else True
    :returns: True if the path is a file, else False

    """
    return Sftp2Path(path).is_file(followlinks)


def sftp2_listdir(path: PathLike) -> List[str]:
    """
    Get all contents of given sftp2 path.
    The result is in ascending alphabetical order.

    :param path: Given path
    :returns: All contents have in the path in ascending alphabetical order
    """
    return Sftp2Path(path).listdir()


def sftp2_load_from(path: PathLike) -> BinaryIO:
    """Read all content on specified path and write into memory

    User should close the BinaryIO manually

    :param path: Given path
    :returns: Binary stream
    """
    return Sftp2Path(path).load()


def sftp2_makedirs(
    path: PathLike, mode=0o777, parents: bool = False, exist_ok: bool = False
):
    """
    make a directory on sftp2, including parent directory.
    If there exists a file on the path, raise FileExistsError

    :param path: Given path
    :param mode: If mode is given, it is combined with the process' umask value to
        determine the file mode and access flags.
    :param parents: If parents is true, any missing parents of this path
        are created as needed; If parents is false (the default),
        a missing parent raises FileNotFoundError.
    :param exist_ok: If False and target directory exists, raise FileExistsError

    :raises: FileExistsError
    """
    return Sftp2Path(path).mkdir(mode, parents, exist_ok)


def sftp2_realpath(path: PathLike) -> str:
    """Return the real path of given path

    :param path: Given path
    :returns: Real path of given path
    """
    return Sftp2Path(path).realpath()


def sftp2_rename(
    src_path: PathLike, dst_path: PathLike, overwrite: bool = True
) -> "Sftp2Path":
    """
    rename file on sftp2

    :param src_path: Given path
    :param dst_path: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    return Sftp2Path(src_path).rename(dst_path, overwrite)


def sftp2_move(
    src_path: PathLike, dst_path: PathLike, overwrite: bool = True
) -> "Sftp2Path":
    """
    move file on sftp2

    :param src_path: Given path
    :param dst_path: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    return Sftp2Path(src_path).replace(dst_path, overwrite)


def sftp2_remove(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file or directory on sftp2

    :param path: Given path
    :param missing_ok: if False and target file/directory not exists,
        raise FileNotFoundError
    """
    return Sftp2Path(path).remove(missing_ok)


def sftp2_scan(
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
    return Sftp2Path(path).scan(missing_ok, followlinks)


def sftp2_scan_stat(
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
    return Sftp2Path(path).scan_stat(missing_ok, followlinks)


def sftp2_scandir(path: PathLike) -> Iterator[FileEntry]:
    """
    Get all content of given file path.

    :param path: Given path
    :returns: An iterator contains all contents have prefix path
    """
    return Sftp2Path(path).scandir()


def sftp2_stat(path: PathLike, follow_symlinks=True) -> StatResult:
    """
    Get StatResult of file on sftp2, including file size and mtime,
    referring to fs_getsize and fs_getmtime

    :param path: Given path
    :returns: StatResult
    """
    return Sftp2Path(path).stat(follow_symlinks)


def sftp2_unlink(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file on sftp2

    :param path: Given path
    :param missing_ok: if False and target file not exists, raise FileNotFoundError
    """
    return Sftp2Path(path).unlink(missing_ok)


def sftp2_walk(
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
    return Sftp2Path(path).walk(followlinks)


def sftp2_getmd5(path: PathLike, recalculate: bool = False, followlinks: bool = False):
    """
    Calculate the md5 value of the file

    :param path: Given path
    :param recalculate: Ignore this parameter, just for compatibility
    :param followlinks: Ignore this parameter, just for compatibility

    returns: md5 of file
    """
    return Sftp2Path(path).md5(recalculate, followlinks)


def sftp2_symlink(src_path: PathLike, dst_path: PathLike) -> None:
    """
    Create a symbolic link pointing to src_path named dst_path.

    :param src_path: Given path
    :param dst_path: Destination path
    """
    return Sftp2Path(src_path).symlink(dst_path)


def sftp2_islink(path: PathLike) -> bool:
    """Test whether a path is a symbolic link

    :param path: Given path
    :return: If path is a symbolic link return True, else False
    :rtype: bool
    """
    return Sftp2Path(path).is_symlink()


def sftp2_save_as(file_object: BinaryIO, path: PathLike):
    """Write the opened binary stream to path
    If parent directory of path doesn't exist, it will be created.

    :param path: Given path
    :param file_object: stream to be read
    """
    return Sftp2Path(path).save(file_object)


def sftp2_open(
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
    :param buffering: buffering is an optional integer used to
        set the buffering policy.
    :param encoding: encoding is the name of the encoding used to decode or encode
        the file. This should only be used in text mode.
    :param errors: errors is an optional string that specifies how encoding and
        decoding errors are to be handled—this cannot be used in binary mode.
    :returns: File-Like object
    """
    return Sftp2Path(path).open(
        mode, buffering=buffering, encoding=encoding, errors=errors
    )


def sftp2_chmod(path: PathLike, mode: int, *, follow_symlinks: bool = True):
    """
    Change the file mode and permissions, like os.chmod().

    :param path: Given path
    :param mode: the file mode you want to change
    :param followlinks: Ignore this parameter, just for compatibility
    """
    return Sftp2Path(path).chmod(mode, follow_symlinks=follow_symlinks)


def sftp2_absolute(path: PathLike) -> "Sftp2Path":
    """
    Make the path absolute, without normalization or resolving symlinks.
    Returns a new path object
    """
    return Sftp2Path(path).absolute()


def sftp2_rmdir(path: PathLike):
    """
    Remove this directory. The directory must be empty.
    """
    return Sftp2Path(path).rmdir()


def sftp2_copy(
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
    :param callback: An optional callback function that takes an integer parameter
        and is called periodically during the copy operation to report the number
        of bytes copied.
    :param followlinks: Whether to follow symbolic links when copying directories.
    :raises IsADirectoryError: If the source is a directory.
    :raises OSError: If there is an error copying the file.
    """
    return Sftp2Path(src_path).copy(dst_path, callback, followlinks, overwrite)


def sftp2_sync(
    src_path: PathLike,
    dst_path: PathLike,
    followlinks: bool = False,
    force: bool = False,
    overwrite: bool = True,
):
    """Copy file/directory on src_url to dst_url

    :param src_path: Given path
    :param dst_url: Given destination path
    :param followlinks: False if regard symlink as file, else True
    :param force: Sync file forcible, do not ignore same files,
        priority is higher than 'overwrite', default is False
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    return Sftp2Path(src_path).sync(dst_path, followlinks, force, overwrite)


def _check_input(input_str: str, fingerprint: str, times: int = 0) -> bool:
    answers = input_str.strip()
    if answers.lower() in ("yes", "y") or answers == fingerprint:
        return True
    elif answers.lower() in ("no", "n"):
        return False
    elif times >= 10:
        _logger.warning("Retried more than 10 times, give up")
        return False
    else:
        input_str = input("Please type 'yes', 'no' or the fingerprint: ")
        return _check_input(input_str, fingerprint, times=times + 1)


def _prompt_add_to_known_hosts(hostname, key) -> bool:
    fingerprint = hashlib.sha256(key).digest()
    fingerprint = f"SHA256:{base64.b64encode(fingerprint).decode('utf-8')}"
    answers = input(
        f"""The authenticity of host '{hostname}' can't be established.
SSH key fingerprint is {fingerprint}.
This key is not known by any other names.
Are you sure you want to continue connecting (yes/no/[fingerprint])? """
    )
    return _check_input(answers, fingerprint)


def sftp2_add_host_key(
    hostname: str,
    port: int = 22,
    prompt: bool = False,
    host_key_path: Optional["str"] = None,
):
    """Add a host key to known_hosts.

    :param hostname: hostname
    :param port: port, default is 22
    :param prompt: If True, requires user input of 'yes' or 'no' to decide whether to
        add this host key
    :param host_key_path: path of known_hosts, default is ~/.ssh/known_hosts
    """
    if not host_key_path:
        host_key_path = os.path.expanduser("~/.ssh/known_hosts")

    if not os.path.exists(host_key_path):
        dirname = os.path.dirname(host_key_path)
        if dirname and dirname != ".":
            os.makedirs(dirname, exist_ok=True, mode=0o700)
        with open(host_key_path, "w"):
            pass
        os.chmod(host_key_path, 0o600)

    sock = ssh2.utils.Socket.open(hostname, port)
    session = ssh2.session.Session()
    session.handshake(sock)

    hostkey = session.hostkey()

    if prompt:
        result = _prompt_add_to_known_hosts(hostname, hostkey)
        if not result:
            return

    with open(host_key_path, "a") as f:
        f.write(f"{hostname} {base64.b64encode(hostkey).decode('ascii')}\n")

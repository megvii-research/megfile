from typing import IO, BinaryIO, Iterator, List, Optional, Tuple

from megfile.config import READER_BLOCK_SIZE, READER_MAX_BUFFER_SIZE
from megfile.hdfs_path import (
    HdfsPath,
    is_hdfs,
)
from megfile.interfaces import FileEntry, PathLike, StatResult

__all__ = [
    "is_hdfs",
    "hdfs_glob",
    "hdfs_glob_stat",
    "hdfs_iglob",
    "hdfs_makedirs",
    "hdfs_exists",
    "hdfs_stat",
    "hdfs_getmtime",
    "hdfs_getsize",
    "hdfs_isdir",
    "hdfs_isfile",
    "hdfs_listdir",
    "hdfs_load_from",
    "hdfs_move",
    "hdfs_remove",
    "hdfs_scan",
    "hdfs_scan_stat",
    "hdfs_scandir",
    "hdfs_unlink",
    "hdfs_walk",
    "hdfs_getmd5",
    "hdfs_save_as",
    "hdfs_open",
]


def hdfs_exists(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if path exists

    If the bucket of path are not permitted to read, return False

    :param path: Given path
    :returns: True if path exists, else False
    """
    return HdfsPath(path).exists(followlinks)


def hdfs_stat(path: PathLike, follow_symlinks=True) -> StatResult:
    """
    Get StatResult of path file, including file size and mtime,
    referring to hdfs_getsize and hdfs_getmtime

    If path is not an existent path, which means hdfs_exist(path) returns False,
    then raise FileNotFoundError

    If attempt to get StatResult of complete hdfs, such as hdfs_dir_url == 'hdfs://',
    raise BucketNotFoundError

    :param path: Given path
    :returns: StatResult
    :raises: FileNotFoundError
    """
    return HdfsPath(path).stat(follow_symlinks)


def hdfs_getmtime(path: PathLike, follow_symlinks: bool = False) -> float:
    """
    Get last-modified time of the file on the given path path (in Unix timestamp
    format).
    If the path is an existent directory, return the latest modified time of all
    file in it. The mtime of empty directory is 1970-01-01 00:00:00

    If path is not an existent path, which means hdfs_exist(path) returns False,
    then raise FileNotFoundError

    :param path: Given path
    :returns: Last-modified time
    :raises: FileNotFoundError
    """
    return HdfsPath(path).getmtime(follow_symlinks)


def hdfs_getsize(path: PathLike, follow_symlinks: bool = False) -> int:
    """
    Get file size on the given path path (in bytes).
    If the path in a directory, return the sum of all file size in it,
    including file in subdirectories (if exist).

    The result excludes the size of directory itself. In other words,
    return 0 Byte on an empty directory path.

    If path is not an existent path, which means hdfs_exist(path) returns False,
    then raise FileNotFoundError

    :param path: Given path
    :returns: File size
    :raises: FileNotFoundError
    """
    return HdfsPath(path).getsize(follow_symlinks)


def hdfs_isdir(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if an hdfs url is directory
    Specific procedures are as follows:
    If there exists a suffix, of which ``os.path.join(path, suffix)`` is a file
    If the url is empty bucket or hdfs://

    :param path: Given path
    :param followlinks: whether followlinks is True or False, result is the same.
        Because hdfs symlink not support dir.
    :returns: True if path is hdfs directory, else False
    """
    return HdfsPath(path).is_dir(followlinks)


def hdfs_isfile(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if an path is file

    :param path: Given path
    :returns: True if path is hdfs file, else False
    """
    return HdfsPath(path).is_file(followlinks)


def hdfs_listdir(path: PathLike) -> List[str]:
    """
    Get all contents of given path.

    :param path: Given path
    :returns: All contents have prefix of path.
    :raises: FileNotFoundError, NotADirectoryError
    """
    return HdfsPath(path).listdir()


def hdfs_load_from(path: PathLike) -> BinaryIO:
    """Read all content in binary on specified path and write into memory

    User should close the BinaryIO manually

    :param path: Given path
    :returns: BinaryIO
    """
    return HdfsPath(path).load()


def hdfs_move(src_path: PathLike, dst_path: PathLike, overwrite: bool = True) -> None:
    """
    Move file/directory path from src_path to dst_path

    :param src_path: Given path
    :param dst_path: Given destination path
    """
    return HdfsPath(src_path).move(dst_path, overwrite)


def hdfs_remove(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file or directory on hdfs, `hdfs://` and `hdfs://bucket` are not
    permitted to remove

    :param path: Given path
    :param missing_ok: if False and target file/directory not exists,
        raise FileNotFoundError
    :raises: FileNotFoundError, UnsupportedError
    """
    return HdfsPath(path).remove(missing_ok)


def hdfs_scan(
    path: PathLike, missing_ok: bool = True, followlinks: bool = False
) -> Iterator[str]:
    """
    Iteratively traverse only files in given hdfs directory.
    Every iteration on generator yields a path string.

    If path is a file path, yields the file only
    If path is a non-existent path, return an empty generator
    If path is a bucket path, return all file paths in the bucket
    If path is an empty bucket, return an empty generator
    If path doesn't contain any bucket, which is path == 'hdfs://',
    raise UnsupportedError. walk() on complete hdfs is not supported in megfile

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory,
        raise FileNotFoundError
    :raises: UnsupportedError
    :returns: A file path generator
    """
    return HdfsPath(path).scan(missing_ok, followlinks)


def hdfs_scan_stat(
    path: PathLike, missing_ok: bool = True, followlinks: bool = False
) -> Iterator[FileEntry]:
    """
    Iteratively traverse only files in given directory.
    Every iteration on generator yields a tuple of path string and file stat

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory,
        raise FileNotFoundError
    :raises: UnsupportedError
    :returns: A file path generator
    """
    return HdfsPath(path).scan_stat(missing_ok, followlinks)


def hdfs_scandir(path: PathLike) -> Iterator[FileEntry]:
    """
    Get all contents of given path, the order of result is in arbitrary order.

    :param path: Given path
    :returns: All contents have prefix of path
    :raises: FileNotFoundError, NotADirectoryError
    """
    return HdfsPath(path).scandir()


def hdfs_unlink(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file on hdfs

    :param path: Given path
    :param missing_ok: if False and target file not exists, raise FileNotFoundError
    :raises: FileNotFoundError, IsADirectoryError
    """
    return HdfsPath(path).unlink(missing_ok)


def hdfs_walk(
    path: PathLike, followlinks: bool = False
) -> Iterator[Tuple[str, List[str], List[str]]]:
    """
    Iteratively traverse the given hdfs directory, in top-bottom order.
    In other words, firstly traverse parent directory, if subdirectories exist,
    traverse the subdirectories.

    Every iteration on generator yields a 3-tuple: (root, dirs, files)

    - root: Current hdfs path;
    - dirs: Name list of subdirectories in current directory.
    - files: Name list of files in current directory.

    If path is a file path, return an empty generator

    If path is a non-existent path, return an empty generator

    If path is a bucket path, bucket will be the top directory,
    and will be returned at first iteration of generator

    If path is an empty bucket, only yield one 3-tuple
    (notes: hdfs doesn't have empty directory)

    If path doesn't contain any bucket, which is path == 'hdfs://',
    raise UnsupportedError. walk() on complete hdfs is not supported in megfile

    :param path: Given path
    :param followlinks: whether followlinks is True or False, result is the same.
        Because hdfs not support symlink.
    :returns: A 3-tuple generator
    """
    return HdfsPath(path).walk(followlinks)


def hdfs_getmd5(
    path: PathLike, recalculate: bool = False, followlinks: bool = False
) -> str:
    """
    Get checksum of the file or dir.

    :param path: Given path
    :param recalculate: Ignore this parameter, just for compatibility
    :param followlinks: Ignore this parameter, just for compatibility
    :returns: checksum
    """
    return HdfsPath(path).md5(recalculate, followlinks)


def hdfs_save_as(file_object: BinaryIO, path: PathLike):
    """Write the opened binary stream to specified path,
    but the stream won't be closed

    :param path: Given path
    :param file_object: Stream to be read
    """
    return HdfsPath(path).save(file_object)


def hdfs_open(
    path: PathLike,
    mode: str = "r",
    *,
    buffering: Optional[int] = None,
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    max_workers: Optional[int] = None,
    max_buffer_size: int = READER_MAX_BUFFER_SIZE,
    block_forward: Optional[int] = None,
    block_size: int = READER_BLOCK_SIZE,
    **kwargs,
) -> IO:
    """
    Open a file on the specified path.

    :param path: Given path
    :param mode: Mode to open the file. Supports 'r', 'rb', 'w', 'wb', 'a', 'ab'.
    :param buffering: Optional integer used to set the buffering policy.
    :param encoding: Name of the encoding used to decode or encode the file.
                    Should only be used in text mode.
    :param errors: Optional string specifying how encoding and decoding errors are
                to be handled. Cannot be used in binary mode.
    :param max_workers: Max download thread number, `None` by default,
        will use global thread pool with 8 threads.
    :param max_buffer_size: Max cached buffer size in memory, 128MB by default.
        Set to `0` will disable cache.
    :param block_forward: Number of blocks of data for reader cached from the
        offset position.
    :param block_size: Size of a single block for reader, default is 8MB.
    :returns: A file-like object.
    :raises ValueError: If an unacceptable mode is provided.
    """
    return HdfsPath(path).open(
        mode,
        buffering=buffering,
        encoding=encoding,
        errors=errors,
        max_workers=max_workers,
        max_buffer_size=max_buffer_size,
        block_forward=block_forward,
        block_size=block_size,
    )


def hdfs_glob(
    path: PathLike, recursive: bool = True, missing_ok: bool = True
) -> List[str]:
    """Return hdfs path list in ascending alphabetical order,
    in which path matches glob pattern

    Notes: Only glob in bucket. If trying to match bucket with wildcard characters,
    raise UnsupportedError

    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :raises: UnsupportedError, when bucket part contains wildcard characters
    :returns: A list contains paths match `path`
    """
    return list(hdfs_iglob(path, recursive=recursive, missing_ok=missing_ok))


def hdfs_glob_stat(
    path: PathLike, recursive: bool = True, missing_ok: bool = True
) -> Iterator[FileEntry]:
    """Return a generator contains tuples of path and file stat,
    in ascending alphabetical order, in which path matches glob pattern

    Notes: Only glob in bucket. If trying to match bucket with wildcard characters,
    raise UnsupportedError

    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :raises: UnsupportedError, when bucket part contains wildcard characters
    :returns: A generator contains tuples of path and file stat,
        in which paths match `path`
    """
    return HdfsPath(path).glob_stat(
        pattern="", recursive=recursive, missing_ok=missing_ok
    )


def hdfs_iglob(
    path: PathLike, recursive: bool = True, missing_ok: bool = True
) -> Iterator[str]:
    """Return hdfs path iterator in ascending alphabetical order,
    in which path matches glob pattern

    Notes: Only glob in bucket. If trying to match bucket with wildcard characters,
    raise UnsupportedError

    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :raises: UnsupportedError, when bucket part contains wildcard characters
    :returns: An iterator contains paths match `path`
    """
    for path_obj in HdfsPath(path).iglob(
        pattern="", recursive=recursive, missing_ok=missing_ok
    ):
        yield path_obj.path_with_protocol


def hdfs_makedirs(path: PathLike, exist_ok: bool = False):
    """
    Create an hdfs directory.
    Purely creating directory is invalid because it's unavailable on OSS.
    This function is to test the target bucket have WRITE access.

    :param path: Given path
    :param exist_ok: If False and target directory exists, raise S3FileExistsError
    :raises: FileExistsError
    """
    return HdfsPath(path).mkdir(parents=True, exist_ok=exist_ok)

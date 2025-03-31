from typing import BinaryIO, Callable, Iterator, List, Optional, Tuple

from megfile.interfaces import Access, FileEntry, PathLike, StatResult
from megfile.s3_path import (
    S3Path,
    get_endpoint_url,
    get_s3_client,
    get_s3_session,
    is_s3,
    parse_s3_url,
    s3_buffered_open,
    s3_cached_open,
    s3_concat,
    s3_download,
    s3_load_content,
    s3_memory_open,
    s3_open,
    s3_path_join,
    s3_pipe_open,
    s3_prefetch_open,
    s3_share_cache_open,
    s3_upload,
)

__all__ = [
    "parse_s3_url",
    "get_endpoint_url",
    "get_s3_session",
    "get_s3_client",
    "s3_path_join",
    "is_s3",
    "s3_buffered_open",
    "s3_cached_open",
    "s3_memory_open",
    "s3_pipe_open",
    "s3_prefetch_open",
    "s3_share_cache_open",
    "s3_open",
    "s3_upload",
    "s3_download",
    "s3_load_content",
    "s3_readlink",
    "s3_glob",
    "s3_glob_stat",
    "s3_iglob",
    "s3_rename",
    "s3_makedirs",
    "s3_concat",
    "s3_lstat",
    "s3_access",
    "s3_exists",
    "s3_getmtime",
    "s3_getsize",
    "s3_isdir",
    "s3_isfile",
    "s3_listdir",
    "s3_load_from",
    "s3_hasbucket",
    "s3_move",
    "s3_remove",
    "s3_scan",
    "s3_scan_stat",
    "s3_scandir",
    "s3_stat",
    "s3_unlink",
    "s3_walk",
    "s3_getmd5",
    "s3_copy",
    "s3_sync",
    "s3_symlink",
    "s3_islink",
    "s3_save_as",
]


def s3_access(path: PathLike, mode: Access = Access.READ) -> bool:
    """
    Test if path has access permission described by mode

    :param path: Given path
    :param mode: access mode
    :returns: bool, if the bucket of s3_url has read/write access.
    """
    return S3Path(path).access(mode)


def s3_exists(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if s3_url exists

    If the bucket of s3_url are not permitted to read, return False

    :param path: Given path
    :returns: True if s3_url exists, else False
    """
    return S3Path(path).exists(followlinks)


def s3_getmtime(path: PathLike, follow_symlinks: bool = False) -> float:
    """
    Get last-modified time of the file on the given s3_url path
    (in Unix timestamp format).

    If the path is an existent directory, return the latest modified time of
    all file in it. The mtime of empty directory is 1970-01-01 00:00:00

    If s3_url is not an existent path, which means s3_exist(s3_url) returns False,
    then raise S3FileNotFoundError

    :param path: Given path
    :returns: Last-modified time
    :raises: S3FileNotFoundError, UnsupportedError
    """
    return S3Path(path).getmtime(follow_symlinks)


def s3_getsize(path: PathLike, follow_symlinks: bool = False) -> int:
    """
    Get file size on the given s3_url path (in bytes).

    If the path in a directory, return the sum of all file size in it,
    including file in subdirectories (if exist).

    The result excludes the size of directory itself.
    In other words, return 0 Byte on an empty directory path.

    If s3_url is not an existent path, which means s3_exist(s3_url) returns False,
    then raise S3FileNotFoundError

    :param path: Given path
    :returns: File size
    :raises: S3FileNotFoundError, UnsupportedError
    """
    return S3Path(path).getsize(follow_symlinks)


def s3_isdir(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if an s3 url is directory
    Specific procedures are as follows:
    If there exists a suffix, of which ``os.path.join(s3_url, suffix)`` is a file
    If the url is empty bucket or s3://

    :param path: Given path
    :param followlinks: whether followlinks is True or False, result is the same.
        Because s3 symlink not support dir.
    :returns: True if path is s3 directory, else False
    """
    return S3Path(path).is_dir(followlinks)


def s3_isfile(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if an s3_url is file

    :param path: Given path
    :returns: True if path is s3 file, else False
    """
    return S3Path(path).is_file(followlinks)


def s3_listdir(path: PathLike) -> List[str]:
    """
    Get all contents of given s3_url. The result is in ascending alphabetical order.

    :param path: Given path
    :returns: All contents have prefix of s3_url in ascending alphabetical order
    :raises: S3FileNotFoundError, S3NotADirectoryError
    """
    return S3Path(path).listdir()


def s3_load_from(path: PathLike) -> BinaryIO:
    """Read all content in binary on specified path and write into memory

    User should close the BinaryIO manually

    :param path: Given path
    :returns: BinaryIO
    """
    return S3Path(path).load()


def s3_hasbucket(path: PathLike) -> bool:
    """
    Test if the bucket of s3_url exists

    :param path: Given path
    :returns: True if bucket of s3_url exists, else False
    """
    return S3Path(path).hasbucket()


def s3_move(src_url: PathLike, dst_url: PathLike, overwrite: bool = True) -> None:
    """
    Move file/directory path from src_url to dst_url

    :param src_url: Given path
    :param dst_url: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    return S3Path(src_url).move(dst_url, overwrite)


def s3_remove(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file or directory on s3, `s3://` and `s3://bucket`
    are not permitted to remove

    :param path: Given path
    :param missing_ok: if False and target file/directory not exists,
        raise S3FileNotFoundError
    :raises: S3PermissionError, S3FileNotFoundError, UnsupportedError
    """
    return S3Path(path).remove(missing_ok)


def s3_scan(
    path: PathLike, missing_ok: bool = True, followlinks: bool = False
) -> Iterator[str]:
    """
    Iteratively traverse only files in given s3 directory, in alphabetical order.
    Every iteration on generator yields a path string.

    If s3_url is a file path, yields the file only

    If s3_url is a non-existent path, return an empty generator

    If s3_url is a bucket path, return all file paths in the bucket

    If s3_url is an empty bucket, return an empty generator

    If s3_url doesn't contain any bucket, which is s3_url == 's3://',
    raise UnsupportedError. walk() on complete s3 is not supported in megfile

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory,
        raise FileNotFoundError
    :raises: UnsupportedError
    :returns: A file path generator
    """
    return S3Path(path).scan(missing_ok, followlinks)


def s3_scan_stat(
    path: PathLike, missing_ok: bool = True, followlinks: bool = False
) -> Iterator[FileEntry]:
    """
    Iteratively traverse only files in given directory, in alphabetical order.
    Every iteration on generator yields a tuple of path string and file stat

    :param path: Given path
    :param missing_ok: If False and there's no file in the directory,
        raise FileNotFoundError
    :raises: UnsupportedError
    :returns: A file path generator
    """
    return S3Path(path).scan_stat(missing_ok, followlinks)


def s3_scandir(path: PathLike) -> Iterator[FileEntry]:
    """
    Get all contents of given s3_url, the order of result is in arbitrary order.

    :param path: Given path
    :returns: All contents have prefix of s3_url
    :raises: S3FileNotFoundError, S3NotADirectoryError
    """
    return S3Path(path).scandir()


def s3_stat(path: PathLike, follow_symlinks=True) -> StatResult:
    """
    Get StatResult of s3_url file, including file size and mtime,
    referring to s3_getsize and s3_getmtime

    If s3_url is not an existent path, which means s3_exist(s3_url) returns False,
    then raise S3FileNotFoundError

    If attempt to get StatResult of complete s3, such as s3_dir_url == 's3://',
    raise S3BucketNotFoundError

    :param path: Given path
    :returns: StatResult
    :raises: S3FileNotFoundError, S3BucketNotFoundError
    """
    return S3Path(path).stat(follow_symlinks)


def s3_unlink(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file on s3

    :param path: Given path
    :param missing_ok: if False and target file not exists,
        raise S3FileNotFoundError
    :raises: S3PermissionError, S3FileNotFoundError, S3IsADirectoryError
    """
    return S3Path(path).unlink(missing_ok)


def s3_walk(
    path: PathLike, followlinks: bool = False
) -> Iterator[Tuple[str, List[str], List[str]]]:
    """
    Iteratively traverse the given s3 directory, in top-bottom order.
    In other words, firstly traverse parent directory, if subdirectories exist,
    traverse the subdirectories in alphabetical order.

    Every iteration on generator yields a 3-tuple: (root, dirs, files)

    - root: Current s3 path;
    - dirs: Name list of subdirectories in current directory.
      The list is sorted by name in ascending alphabetical order;
    - files: Name list of files in current directory.
      The list is sorted by name in ascending alphabetical order;

    If s3_url is a file path, return an empty generator

    If s3_url is a non-existent path, return an empty generator

    If s3_url is a bucket path, bucket will be the top directory,
    and will be returned at first iteration of generator

    If s3_url is an empty bucket, only yield one 3-tuple
    (notes: s3 doesn't have empty directory)

    If s3_url doesn't contain any bucket, which is s3_url == 's3://',
    raise UnsupportedError. walk() on complete s3 is not supported in megfile

    :param path: Given path
    :param followlinks: whether followlinks is True or False, result is the same.
        Because s3 symlink not support dir.
    :raises: UnsupportedError
    :returns: A 3-tuple generator
    """
    return S3Path(path).walk(followlinks)


def s3_getmd5(
    path: PathLike, recalculate: bool = False, followlinks: bool = False
) -> str:
    """
    Get md5 meta info in files that uploaded/copied via megfile

    If meta info is lost or non-existent, return None

    :param path: Given path
    :param recalculate: calculate md5 in real-time or return s3 etag
    :param followlinks: If is True, calculate md5 for real file
    :returns: md5 meta info
    """
    return S3Path(path).md5(recalculate, followlinks)


def s3_copy(
    src_url: PathLike,
    dst_url: PathLike,
    callback: Optional[Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """File copy on S3
    Copy content of file on `src_path` to `dst_path`.
    It's caller's responsibility to ensure the s3_isfile(src_url) is True

    :param src_url: Given path
    :param dst_path: Target file path
    :param callback: Called periodically during copy, and the input parameter is
        the data size (in bytes) of copy since the last call
    :param followlinks: False if regard symlink as file, else True
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    return S3Path(src_url).copy(dst_url, callback, followlinks, overwrite)


def s3_sync(
    src_url: PathLike,
    dst_url: PathLike,
    followlinks: bool = False,
    force: bool = False,
    overwrite: bool = True,
) -> None:
    """
    Copy file/directory on src_url to dst_url

    :param src_url: Given path
    :param dst_url: Given destination path
    :param followlinks: False if regard symlink as file, else True
    :param force: Sync file forcible, do not ignore same files,
        priority is higher than 'overwrite', default is False
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    return S3Path(src_url).sync(dst_url, followlinks, force, overwrite)


def s3_symlink(src_path: PathLike, dst_path: PathLike) -> None:
    """
    Create a symbolic link pointing to src_path named dst_path.

    :param src_path: Given path
    :param dst_path: Destination path
    :raises: S3NameTooLongError, S3BucketNotFoundError, S3IsADirectoryError
    """
    return S3Path(src_path).symlink(dst_path)


def s3_islink(path: PathLike) -> bool:
    """
    Test whether a path is link

    :param path: Given path
    :returns: True if a path is link, else False
    :raises: S3NotALinkError
    """
    return S3Path(path).is_symlink()


def s3_save_as(file_object: BinaryIO, path: PathLike):
    """Write the opened binary stream to specified path,
    but the stream won't be closed

    :param path: Given path
    :param file_object: Stream to be read
    """
    return S3Path(path).save(file_object)


def s3_readlink(path) -> str:
    """
    Return a string representing the path to which the symbolic link points.

    :returns: Return a string representing the path to which the symbolic link points.
    :raises: S3NameTooLongError, S3BucketNotFoundError, S3IsADirectoryError,
        S3NotALinkError
    """
    return S3Path(path).readlink().path_with_protocol


def s3_rename(src_url: PathLike, dst_url: PathLike, overwrite: bool = True) -> None:
    """
    Move s3 file path from src_url to dst_url

    :param dst_url: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    S3Path(src_url).rename(dst_url, overwrite)


def s3_glob(
    path: PathLike,
    recursive: bool = True,
    missing_ok: bool = True,
) -> List[str]:
    """Return s3 path list in ascending alphabetical order,
    in which path matches glob pattern

    Notes: Only glob in bucket. If trying to match bucket with wildcard characters,
    raise UnsupportedError

    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :raises: UnsupportedError, when bucket part contains wildcard characters
    :returns: A list contains paths match `s3_pathname`
    """
    return list(
        s3_iglob(
            path=path,
            recursive=recursive,
            missing_ok=missing_ok,
        )
    )


def s3_glob_stat(
    path: PathLike,
    recursive: bool = True,
    missing_ok: bool = True,
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
        in which paths match `s3_pathname`
    """
    return S3Path(path).glob_stat(
        pattern="", recursive=recursive, missing_ok=missing_ok
    )


def s3_iglob(
    path: PathLike,
    recursive: bool = True,
    missing_ok: bool = True,
) -> Iterator[str]:
    """Return s3 path iterator in ascending alphabetical order,
    in which path matches glob pattern

    Notes: Only glob in bucket. If trying to match bucket with wildcard characters,
    raise UnsupportedError

    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    :raises: UnsupportedError, when bucket part contains wildcard characters
    :returns: An iterator contains paths match `s3_pathname`
    """
    for path_obj in S3Path(path).iglob(
        pattern="", recursive=recursive, missing_ok=missing_ok
    ):
        yield path_obj.path_with_protocol


def s3_makedirs(path: PathLike, exist_ok: bool = False):
    """
    Create an s3 directory.
    Purely creating directory is invalid because it's unavailable on OSS.
    This function is to test the target bucket have WRITE access.

    :param path: Given path
    :param exist_ok: If False and target directory exists, raise S3FileExistsError
    :raises: S3BucketNotFoundError, S3FileExistsError
    """
    return S3Path(path).mkdir(parents=True, exist_ok=exist_ok)


def s3_lstat(path: PathLike) -> StatResult:
    """
    Like Path.stat() but, if the path points to a symbolic link,
    return the symbolic link’s information rather than its target’s.
    """
    return S3Path(path).lstat()

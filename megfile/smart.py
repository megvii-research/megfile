import os
from collections import defaultdict
from functools import partial
from typing import (
    IO,
    Any,
    BinaryIO,
    Callable,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
)

from tqdm import tqdm

from megfile.errors import S3UnknownError
from megfile.fs import (
    fs_copy,
    fs_listdir,
    fs_scandir,
    is_fs,
)
from megfile.interfaces import (
    Access,
    FileCacher,
    FileEntry,
    NullCacher,
    PathLike,
    StatResult,
)
from megfile.lib.combine_reader import CombineReader
from megfile.lib.compare import get_sync_type, is_same_file
from megfile.lib.compat import fspath
from megfile.lib.glob import globlize, ungloblize
from megfile.s3 import (
    is_s3,
    s3_concat,
    s3_copy,
    s3_download,
    s3_load_content,
    s3_open,
    s3_upload,
)
from megfile.sftp import sftp_concat, sftp_copy, sftp_download, sftp_upload
from megfile.smart_path import SmartPath, get_traditional_path
from megfile.utils import combine, generate_cache_path

__all__ = [
    "smart_access",
    "smart_cache",
    "smart_combine_open",
    "smart_copy",
    "smart_exists",
    "smart_getmtime",
    "smart_getsize",
    "smart_glob_stat",
    "smart_glob",
    "smart_iglob",
    "smart_isdir",
    "smart_isfile",
    "smart_islink",
    "smart_listdir",
    "smart_load_content",
    "smart_save_content",
    "smart_load_from",
    "smart_load_text",
    "smart_save_text",
    "smart_makedirs",
    "smart_open",
    "smart_path_join",
    "smart_remove",
    "smart_move",
    "smart_rename",
    "smart_save_as",
    "smart_scan_stat",
    "smart_scan",
    "smart_scandir",
    "smart_stat",
    "smart_sync",
    "smart_sync_with_progress",
    "smart_touch",
    "smart_unlink",
    "smart_walk",
    "smart_getmd5",
    "smart_realpath",
    "smart_ismount",
    "smart_relpath",
    "smart_abspath",
    "smart_isabs",
    "smart_symlink",
    "smart_readlink",
    "register_copy_func",
    "smart_concat",
    "SmartCacher",
]


def smart_symlink(src_path: PathLike, dst_path: PathLike) -> None:
    """
    Create a symbolic link pointing to src_path named path.

    :param src_path: Source path
    :param dst_path: Destination path
    """
    return SmartPath(src_path).symlink(dst_path)


def smart_readlink(path: PathLike) -> PathLike:
    """
    Return a string representing the path to which the symbolic link points.
    :param path: Path to be read
    :returns: Return a string representing the path to which the symbolic link points.
    """
    return SmartPath(path).readlink()


def smart_isdir(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if a file path or an s3 url is directory

    :param path: Path to be tested
    :returns: True if path is directory, else False
    """
    return SmartPath(path).is_dir(followlinks=followlinks)


def smart_isfile(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if a file path or an s3 url is file

    :param path: Path to be tested
    :returns: True if path is file, else False
    """
    return SmartPath(path).is_file(followlinks=followlinks)


def smart_islink(path: PathLike) -> bool:
    return SmartPath(path).is_symlink()


def smart_access(path: PathLike, mode: Access) -> bool:
    """
    Test if path has access permission described by mode

    :param path: Path to be tested
    :param mode: Access mode(Access.READ, Access.WRITE, Access.BUCKETREAD,
        Access.BUCKETWRITE)
    :returns: bool, if the path has read/write access.
    """
    return SmartPath(path).access(mode)


def smart_exists(path: PathLike, followlinks: bool = False) -> bool:
    """
    Test if path or s3_url exists

    :param path: Path to be tested
    :returns: True if path exists, else False
    """
    return SmartPath(path).exists(followlinks=followlinks)


def smart_listdir(path: Optional[PathLike] = None) -> List[str]:
    """
    Get all contents of given s3_url or file path. The result is in
    ascending alphabetical order.

    :param path: Given path
    :returns: All contents of given s3_url or file path in ascending alphabetical order.
    :raises: FileNotFoundError, NotADirectoryError
    """
    if path is None:
        return fs_listdir()
    return SmartPath(path).listdir()


def smart_scandir(path: Optional[PathLike] = None) -> Iterator[FileEntry]:
    """
    Get all content of given s3_url or file path.

    :param path: Given path
    :returns: An iterator contains all contents have prefix path
    :raises: FileNotFoundError, NotADirectoryError
    """
    if path is None:
        return fs_scandir()
    return SmartPath(path).scandir()


def smart_getsize(path: PathLike) -> int:
    """
    Get file size on the given s3_url or file path (in bytes).

    If the path in a directory, return the sum of all file size in it, including file
    in subdirectories (if exist).

    The result excludes the size of directory itself. In other words, return 0 Byte on
    an empty directory path.

    :param path: Given path
    :returns: File size
    :raises: FileNotFoundError
    """
    return SmartPath(path).getsize()


def smart_getmtime(path: PathLike) -> float:
    """
    Get last-modified time of the file on the given s3_url or file path (in Unix
    timestamp format).

    If the path is an existent directory, return the latest modified time of
    all file in it. The mtime of empty directory is 1970-01-01 00:00:00

    :param path: Given path
    :returns: Last-modified time
    :raises: FileNotFoundError
    """
    return SmartPath(path).getmtime()


def smart_stat(path: PathLike, follow_symlinks=True) -> StatResult:
    """
    Get StatResult of s3_url or file path

    :param path: Given path
    :returns: StatResult
    :raises: FileNotFoundError
    """
    return SmartPath(path).stat(follow_symlinks=follow_symlinks)


def smart_lstat(path: PathLike) -> StatResult:
    """
    Get StatResult of path but do not follow symbolic links

    :param path: Given path
    :returns: StatResult
    :raises: FileNotFoundError
    """
    return SmartPath(path).lstat()


_copy_funcs = {
    "s3": {"s3": s3_copy, "file": s3_download},
    "file": {"s3": s3_upload, "file": fs_copy, "sftp": sftp_upload},
    "sftp": {"file": sftp_download, "sftp": sftp_copy},
}


def register_copy_func(
    src_protocol: str, dst_protocol: str, copy_func: Optional[Callable] = None
) -> None:
    """
    Used to register copy func between protocols,
    and do not allow duplicate registration

    :param src_protocol: protocol name of source file, e.g. 's3'
    :param dst_protocol: protocol name of destination file, e.g. 's3'
    :param copy_func: copy func, its type is:
        Callable[[str, str, Optional[Callable[[int], None]], Optional[bool],
        Optional[bool]], None]
    """
    try:
        _copy_funcs[src_protocol][dst_protocol]
    except KeyError:
        dst_dict = _copy_funcs.get(src_protocol, {})
        dst_dict[dst_protocol] = copy_func
        _copy_funcs[src_protocol] = dst_dict
    else:
        raise ValueError(
            "Copy Function has already existed: {}->{}".format(
                src_protocol, dst_protocol
            )
        )


def _default_copy_func(
    src_path: PathLike,
    dst_path: PathLike,
    callback: Optional[Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    if not overwrite and smart_exists(dst_path):
        return

    with smart_open(src_path, "rb", followlinks=followlinks) as fsrc:
        with smart_open(dst_path, "wb") as fdst:
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
    try:
        src_stat = smart_stat(src_path)
        dst_path = SmartPath(dst_path)
        dst_path.utime(src_stat.st_atime, src_stat.st_mtime)
    except (NotImplementedError, TypeError):
        pass


def _get_copy_func(src_protocol, dst_protocol):
    if src_protocol.startswith("s3+") and src_protocol == dst_protocol:
        src_protocol = dst_protocol = "s3"

    try:
        return _copy_funcs[src_protocol][dst_protocol]
    except KeyError:
        return _default_copy_func


def smart_copy(
    src_path: PathLike,
    dst_path: PathLike,
    callback: Optional[Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """
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
        >>> smart_copy(
        ...     src_path,
        ...     dst_path,
        ...     callback=Bar(total=smart_stat(src_path).size), followlinks=False
        ... )
        856960it [00:00, 260592384.24it/s]

    :param src_path: Given source path
    :param dst_path: Given destination path
    :param callback: Called periodically during copy, and the input parameter is the
        data size (in bytes) of copy since the last call
    :param followlinks: False if regard symlink as file, else True
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    # this function contains plenty of manual polymorphism
    if smart_islink(src_path) and is_s3(dst_path) and not followlinks:
        return

    src_protocol, _ = SmartPath._extract_protocol(src_path)
    dst_protocol, _ = SmartPath._extract_protocol(dst_path)

    copy_func = _get_copy_func(src_protocol, dst_protocol)

    try:
        copy_func(
            src_path,
            dst_path,
            callback=callback,
            followlinks=followlinks,
            overwrite=overwrite,
        )
    except S3UnknownError as e:
        if "cannot schedule new futures after interpreter shutdown" in str(e):
            _default_copy_func(
                src_path,
                dst_path,
                callback=callback,
                followlinks=followlinks,
                overwrite=overwrite,
            )
        else:
            raise


def _smart_sync_single_file(items: dict):
    src_root_path = items["src_root_path"]
    dst_root_path = items["dst_root_path"]
    src_file_entry = items["src_file_entry"]
    src_file_path = src_file_entry.path
    src_file_stat = src_file_entry.stat
    callback = items["callback"]
    followlinks = items["followlinks"]
    callback_after_copy_file = items["callback_after_copy_file"]
    force = items["force"]
    overwrite = items["overwrite"]

    content_path = smart_relpath(src_file_path, start=src_root_path)
    if len(content_path) and content_path != ".":
        content_path = content_path.lstrip("/")
        dst_abs_file_path = smart_path_join(dst_root_path, content_path)
    else:
        # if content_path is empty, which means smart_isfile(src_path) is True,
        # this function is equal to smart_copy
        dst_abs_file_path = dst_root_path

    src_protocol, _ = SmartPath._extract_protocol(src_file_path)
    dst_protocol, _ = SmartPath._extract_protocol(dst_abs_file_path)
    should_sync = True
    try:
        if not force:
            dst_file_stat = smart_stat(dst_abs_file_path, follow_symlinks=followlinks)
            if not overwrite:
                should_sync = False
            elif is_same_file(
                src_file_stat,
                dst_file_stat,
                get_sync_type(src_protocol, dst_protocol),
            ):
                should_sync = False
    except (NotImplementedError, FileNotFoundError):
        pass

    if should_sync:
        copy_callback = partial(callback, src_file_path) if callback else None
        smart_copy(
            src_file_path,
            dst_abs_file_path,
            callback=copy_callback,
            followlinks=followlinks,
        )
    elif callback:
        callback(src_file_path, src_file_stat.size)
    if callback_after_copy_file:
        callback_after_copy_file(src_file_path, dst_abs_file_path)
    return should_sync


def smart_sync(
    src_path: PathLike,
    dst_path: PathLike,
    callback: Optional[Callable[[str, int], None]] = None,
    followlinks: bool = False,
    callback_after_copy_file: Optional[Callable[[str, str], None]] = None,
    src_file_stats: Optional[Iterable[FileEntry]] = None,
    map_func: Callable[[Callable, Iterable], Any] = map,
    force: bool = False,
    overwrite: bool = True,
) -> None:
    """
    Sync file or directory

    .. note ::

        When the parameter is file, this function bahaves like ``smart_copy``.

        If file and directory of same name and same level, sync consider it's file first

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
        ...                 print("copy file {}/{}:".format(self._file_index,
        ...                                                 self._total_file))
        ...                 if self._bar:
        ...                     self._bar.close()
        ...                 self._bar = tqdm(total=smart_stat(path).size)
        ...                 self._now = path
        ...            self._bar.update(num_bytes)
        >>> total_file = len(list(smart_glob('src_path')))
        >>> smart_sync('src_path', 'dst_path', callback=Bar(total_file=total_file))

    :param src_path: Given source path
    :param dst_path: Given destination path
    :param callback: Called periodically during copy, and the input parameter is
        the data size (in bytes) of copy since the last call
    :param followlinks: False if regard symlink as file, else True
    :param callback_after_copy_file: Called after copy success, and the input parameter
        is src file path and dst file path
    :param src_file_stats: If this parameter is not None, only this parameter's files
        will be synced,and src_path is the root_path of these files used to calculate
        the path of the target file. This parameter is in order to reduce file traversal
        times.
    :param map_func: A Callable func like `map`. You can use ThreadPoolExecutor.map,
        Pool.map and so on if you need concurrent capability. default is standard
        library `map`.
    :param force: Sync file forcible, do not ignore same files, priority is higher than
        'overwrite', default is False
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    if not smart_exists(src_path):
        raise FileNotFoundError(f"No match file: {src_path}")

    src_path, dst_path = get_traditional_path(src_path), get_traditional_path(dst_path)
    if not src_file_stats:
        src_file_stats = smart_scan_stat(src_path, followlinks=followlinks)
    if not smart_exists(dst_path):
        force = True

    def create_generator():
        for src_file_entry in src_file_stats:
            if src_file_entry.name:
                yield dict(
                    src_root_path=src_path,
                    dst_root_path=dst_path,
                    src_file_entry=src_file_entry,
                    callback=callback,
                    followlinks=followlinks,
                    callback_after_copy_file=callback_after_copy_file,
                    force=force,
                    overwrite=overwrite,
                )

    for _ in map_func(_smart_sync_single_file, create_generator()):
        pass


def smart_sync_with_progress(
    src_path,
    dst_path,
    callback: Optional[Callable[[str, int], None]] = None,
    followlinks: bool = False,
    map_func: Callable[[Callable, Iterable], Iterator] = map,
    force: bool = False,
    overwrite: bool = True,
):
    """
    Sync file or directory with progress bar

    :param src_path: Given source path
    :param dst_path: Given destination path
    :param callback: Called periodically during copy, and the input parameter is
        the data size (in bytes) of copy since the last call
    :param followlinks: False if regard symlink as file, else True
    :param callback_after_copy_file: Called after copy success, and the input parameter
        is src file path and dst file path
    :param src_file_stats: If this parameter is not None, only this parameter's files
        will be synced, and src_path is the root_path of these files used to calculate
        the path of the target file. This parameter is in order to reduce file traversal
        times.
    :param map_func: A Callable func like `map`. You can use ThreadPoolExecutor.map,
        Pool.map and so on if you need concurrent capability. default is standard
        library `map`.
    :param force: Sync file forcible, do not ignore same files, priority is higher than
        'overwrite', default is False
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    if not smart_exists(src_path):
        raise FileNotFoundError(f"No match file: {src_path}")

    src_path, dst_path = get_traditional_path(src_path), get_traditional_path(dst_path)
    file_stats = list(smart_scan_stat(src_path, followlinks=followlinks))
    tbar = tqdm(total=len(file_stats), ascii=True)
    sbar = tqdm(unit="B", ascii=True, unit_scale=True, unit_divisor=1024)

    def tqdm_callback(current_src_path, length: int):
        sbar.update(length)
        if callback:
            callback(current_src_path, length)

    def callback_after_copy_file(src_file_path, dst_file_path):
        tbar.update(1)

    try:
        smart_sync(
            src_path,
            dst_path,
            callback=tqdm_callback,
            followlinks=followlinks,
            callback_after_copy_file=callback_after_copy_file,
            src_file_stats=file_stats,
            map_func=map_func,
            force=force,
            overwrite=overwrite,
        )
    finally:
        tbar.close()
        sbar.close()


def smart_remove(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file or directory on s3 or fs, `s3://` and `s3://bucket` are
    not permitted to remove

    :param path: Given path
    :param missing_ok: if False and target file/directory not exists,
        raise FileNotFoundError
    :raises: PermissionError, FileNotFoundError
    """
    SmartPath(path).remove(missing_ok=missing_ok)


def smart_rename(
    src_path: PathLike, dst_path: PathLike, overwrite: bool = True
) -> None:
    """
    Move file on s3 or fs. `s3://` or `s3://bucket` is not allowed to move

    :param src_path: Given source path
    :param dst_path: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    if smart_isdir(src_path):
        raise IsADirectoryError("%r is a directory" % src_path)
    src_protocol, _ = SmartPath._extract_protocol(src_path)
    dst_protocol, _ = SmartPath._extract_protocol(dst_path)
    if src_protocol == dst_protocol:
        SmartPath(src_path).rename(dst_path, overwrite=overwrite)
        return
    smart_copy(src_path, dst_path, overwrite=overwrite)
    smart_unlink(src_path)


def smart_move(src_path: PathLike, dst_path: PathLike, overwrite: bool = True) -> None:
    """
    Move file/directory on s3 or fs. `s3://` or `s3://bucket` is not allowed to move

    :param src_path: Given source path
    :param dst_path: Given destination path
    :param overwrite: whether or not overwrite file when exists
    """
    src_protocol, _ = SmartPath._extract_protocol(src_path)
    dst_protocol, _ = SmartPath._extract_protocol(dst_path)
    if src_protocol == dst_protocol:
        SmartPath(src_path).rename(dst_path, overwrite=overwrite)
        return
    smart_sync(src_path, dst_path, followlinks=True, overwrite=overwrite)
    smart_remove(src_path)


def smart_unlink(path: PathLike, missing_ok: bool = False) -> None:
    """
    Remove the file on s3 or fs

    :param path: Given path
    :param missing_ok: if False and target file not exists, raise FileNotFoundError
    :raises: PermissionError, FileNotFoundError, IsADirectoryError
    """
    SmartPath(path).unlink(missing_ok=missing_ok)


def smart_makedirs(path: PathLike, exist_ok: bool = False) -> None:
    """
    Create a directory if is on fs.
    If on s3, it actually check if target exists, and check if bucket has WRITE access

    :param path: Given path
    :param missing_ok: if False and target directory not exists, raise FileNotFoundError
    :raises: PermissionError, FileExistsError
    """
    SmartPath(path).makedirs(exist_ok)


def smart_open(
    path: PathLike,
    mode: str = "r",
    encoding: Optional[str] = None,
    errors: Optional[str] = None,
    *,
    s3_open_func: Callable[[str, str], BinaryIO] = s3_open,
    **options,
) -> IO:
    r"""
    Open a file on the path

    .. note ::

        On fs, the difference between this function and ``io.open`` is that
        this function create directories automatically, instead of
        raising FileNotFoundError

    Here are a few examples: ::

        >>> import cv2
        >>> import numpy as np
        >>> raw = smart_open(
        ...     'https://ss2.bdstatic.com/70cFvnSh_Q1YnxGkpoWK1HF6hhy'
        ...     '/it/u=2275743969,3715493841&fm=26&gp=0.jpg'
        ... ).read()
        >>> img = cv2.imdecode(np.frombuffer(raw, np.uint8),
        ...                    cv2.IMREAD_ANYDEPTH | cv2.IMREAD_COLOR)

    :param path: Given path
    :param mode: Mode to open file, supports r'[rwa][tb]?\+?'
    :param encoding: encoding is the name of the encoding used to decode or encode
        the file. This should only be used in text mode.
    :param errors: errors is an optional string that specifies how encoding and decoding
        errors are to be handled—this cannot be used in binary mode.
    :param buffering: buffering is an optional integer used to
        set the buffering policy. Only be used when support.
    :param followlinks: follow symbolic link, default `False`. Only be used when support
    :param s3_open_func: Function used to open s3_url. Require the function includes
        2 necessary parameters, file path and mode. only be used in s3 path.
    :param max_workers: Max download / upload thread number, `None` by default,
        will use global thread pool with 8 threads. Only be used in s3, http, hdfs.
    :param max_buffer_size: Max cached buffer size in memory, 128MB by default.
        Set to `0` will disable cache. Only be used in s3, http, hdfs.
    :param block_forward: How many blocks of data cached from offset position, only for
        read mode. Only be used in s3, http, hdfs.
    :param block_size: Size of single block. Each block will be uploaded by single
        thread. Only be used in s3, http, hdfs.

    :returns: File-Like object
    :raises: FileNotFoundError, IsADirectoryError, ValueError
    """
    options = {
        "s3_open_func": s3_open_func,
        "encoding": encoding,
        "errors": errors,
        **options,
    }
    return SmartPath(path).open(mode, **options)


def smart_path_join(path: PathLike, *other_paths: PathLike) -> str:
    """
    Concat 2 or more path to a complete path

    :param path: Given path
    :param other_paths: Paths to be concatenated
    :returns: Concatenated complete path

    .. note ::

        For URI, the difference between this function and ``os.path.join`` is that this
        function ignores left side slash (which indicates absolute path) in
        ``other_paths`` and will directly concat.

        e.g. os.path.join('s3://path', 'to', '/file') => '/file', and
        smart_path_join('s3://path', 'to', '/file') => '/path/to/file'

        But for fs path, this function behaves exactly like ``os.path.join``

        e.g. smart_path_join('/path', 'to', '/file') => '/file'
    """
    return fspath(SmartPath(path).joinpath(*other_paths))


def smart_walk(
    path: PathLike, followlinks: bool = False
) -> Iterator[Tuple[str, List[str], List[str]]]:
    """
    Generate the file names in a directory tree by walking the tree top-down.
    For each directory in the tree rooted at directory path (including path itself),
    it yields a 3-tuple (root, dirs, files).

    - root: a string of current path
    - dirs: name list of subdirectories (excluding '.' and '..' if they exist) in 'root'
      The list is sorted by ascending alphabetical order
    - files: name list of non-directory files (link is regarded as file) in 'root'.
      The list is sorted by ascending alphabetical order

    If path not exists, return an empty generator
    If path is a file, return an empty generator
    If try to apply walk() on unsupported path, raise UnsupportedError

    :param path: Given path
    :raises: UnsupportedError
    :returns: A 3-tuple generator
    """
    return SmartPath(path).walk(followlinks=followlinks)


def smart_scan(
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
    :raises: UnsupportedError
    :returns: A file path generator
    """
    return SmartPath(path).scan(missing_ok=missing_ok, followlinks=followlinks)


def smart_scan_stat(
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
    return SmartPath(path).scan_stat(missing_ok=missing_ok, followlinks=followlinks)


def _group_glob(globstr: PathLike) -> List[str]:
    """
    Split pathname, and group them by protocol, return the glob list of same group.

    :param globstr: A glob string
    :returns: A glob list after being grouped by protocol
    """
    globstr = fspath(globstr)
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
    pathname: PathLike, recursive: bool = True, missing_ok: bool = True
) -> List[str]:
    """
    Given pathname may contain shell wildcard characters, return path list in ascending
    alphabetical order, in which path matches glob pattern

    :param pathname: A path pattern may contain shell wildcard characters
    :param recursive: If False, this function will not glob recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    """
    # Split pathname, group by protocol, call glob respectively
    # SmartPath(pathname).glob(recursive, missing_ok)
    result = []
    group_glob_list = _group_glob(pathname)
    for glob_path in group_glob_list:
        for path_obj in SmartPath(glob_path).glob(
            pattern="", recursive=recursive, missing_ok=missing_ok
        ):
            result.append(path_obj.path)
    return result


def smart_iglob(
    pathname: PathLike, recursive: bool = True, missing_ok: bool = True
) -> Iterator[str]:
    """
    Given pathname may contain shell wildcard characters, return path iterator in
    ascending alphabetical order, in which path matches glob pattern

    :param pathname: A path pattern may contain shell wildcard characters
    :param recursive: If False, this function will not glob recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    """
    # Split pathname, group by protocol, call glob respectively
    # SmartPath(pathname).glob(recursive, missing_ok)
    group_glob_list = _group_glob(pathname)
    for glob_path in group_glob_list:
        for path_obj in SmartPath(glob_path).iglob(
            pattern="", recursive=recursive, missing_ok=missing_ok
        ):
            yield path_obj.path


def smart_glob_stat(
    pathname: PathLike, recursive: bool = True, missing_ok: bool = True
) -> Iterator[FileEntry]:
    """
    Given pathname may contain shell wildcard characters, return a list contains tuples
    of path and file stat in ascending alphabetical order,
    in which path matches glob pattern

    :param pathname: A path pattern may contain shell wildcard characters
    :param recursive: If False, this function will not glob recursively
    :param missing_ok: If False and target path doesn't match any file,
        raise FileNotFoundError
    """
    # Split pathname, group by protocol, call glob respectively
    # SmartPath(pathname).glob(recursive, missing_ok)
    group_glob_list = _group_glob(pathname)
    for glob_path in group_glob_list:
        yield from SmartPath(glob_path).glob_stat(
            pattern="", recursive=recursive, missing_ok=missing_ok
        )


def smart_save_as(file_object: BinaryIO, path: PathLike) -> None:
    """Write the opened binary stream to specified path, but the stream won't be closed

    :param file_object: Stream to be read
    :param path: Specified target path
    """
    SmartPath(path).save(file_object)


def smart_load_from(path: PathLike) -> BinaryIO:
    """Read all content in binary on specified path and write into memory

    User should close the BinaryIO manually

    :param path: Specified path
    :returns: BinaryIO
    """
    return SmartPath(path).load()


def smart_combine_open(
    path_glob: str, mode: str = "rb", open_func=smart_open
) -> CombineReader:
    """Open a unified reader that supports multi file reading.

    :param path_glob: A path may contain shell wildcard characters
    :param mode: Mode to open file, supports 'rb'
    :returns: A ```CombineReader```
    """
    file_objects = list(open_func(path, mode) for path in sorted(smart_glob(path_glob)))
    return combine(file_objects, path_glob)


def smart_abspath(path: PathLike):
    """Return the absolute path of given path

    :param path: Given path
    :returns: Absolute path of given path
    """
    return SmartPath(path).abspath()


def smart_realpath(path: PathLike):
    """Return the real path of given path

    :param path: Given path
    :returns: Real path of given path
    """
    return SmartPath(path).realpath()


def smart_relpath(path: PathLike, start=None):
    """Return the relative path of given path

    :param path: Given path
    :param start: Given start directory
    :returns: Relative path from start
    """
    return SmartPath(path).relpath(start)


def smart_isabs(path: PathLike) -> bool:
    """Test whether a path is absolute

    :param path: Given path
    :returns: True if a path is absolute, else False
    """
    return SmartPath(path).is_absolute()


def smart_ismount(path: PathLike) -> bool:
    """Test whether a path is a mount point

    :param path: Given path
    :returns: True if a path is a mount point, else False
    """
    return SmartPath(path).is_mount()


def smart_load_content(
    path: PathLike, start: Optional[int] = None, stop: Optional[int] = None
) -> bytes:
    """
    Get specified file from [start, stop) in bytes

    :param path: Specified path
    :param start: start index
    :param stop: stop index
    :returns: bytes content in range [start, stop)
    """
    if is_s3(path):
        return s3_load_content(path, start, stop)

    with smart_open(path, "rb") as fd:
        if start is not None:
            fd.seek(start)
        offset = -1
        if stop is not None:
            offset = stop - (start or 0)  # start may be None
            if offset < 0:
                raise ValueError("stop should be greater than start")
        return fd.read(offset)  # pytype: disable=bad-return-type


def smart_save_content(path: PathLike, content: bytes) -> None:
    """Save bytes content to specified path

    param path: Path to save content
    """
    with smart_open(path, "wb") as fd:
        fd.write(content)


def smart_load_text(path: PathLike) -> str:
    """
    Read content from path

    param path: Path to be read
    """
    with smart_open(path) as fd:
        return fd.read()  # pytype: disable=bad-return-type


def smart_save_text(path: PathLike, text: str) -> None:
    """Save text to specified path

    param path: Path to save text
    """
    with smart_open(path, "w") as fd:
        fd.write(text)


class SmartCacher(FileCacher):
    cache_path = None

    def __init__(self, path: str, cache_path: Optional[str] = None, mode: str = "r"):
        if mode not in ("r", "w", "a"):
            raise ValueError("unacceptable mode: %r" % mode)
        if cache_path is None:
            cache_path = generate_cache_path(path)
        if mode in ("r", "a"):
            smart_copy(path, cache_path)
        self.name = path
        self.mode = mode
        self.cache_path = cache_path

    def _close(self):
        if getattr(self, "cache_path", None) is not None and os.path.exists(
            self.cache_path
        ):
            if self.mode in ("w", "a"):
                smart_copy(self.cache_path, self.name)
            os.unlink(self.cache_path)


def smart_cache(path, cacher=SmartCacher, **options):
    """Return a path to Posixpath Interface

    param path: Path to cache
    param s3_cacher: Cacher for s3 path
    param options: Optional arguments for s3_cacher
    """
    if not is_fs(path):
        return cacher(path, **options)
    return NullCacher(path)


def smart_touch(path: PathLike):
    """Create a new file on path

    param path: Path to create file
    """
    with smart_open(path, "w"):
        pass


def smart_getmd5(path: PathLike, recalculate: bool = False, followlinks: bool = False):
    """Get md5 value of file

    param path: File path
    param recalculate: calculate md5 in real-time or not return s3 etag when path is s3
    param followlinks: If is True, calculate md5 for real file
    """
    return SmartPath(path).md5(recalculate=recalculate, followlinks=followlinks)


_concat_funcs = {"s3": s3_concat, "sftp": sftp_concat}


def _default_concat_func(src_paths: List[PathLike], dst_path: PathLike) -> None:
    length = 16 * 1024
    with smart_open(dst_path, "wb") as dst_fd:
        for src_path in src_paths:
            with smart_open(src_path, "rb") as src_fd:
                while True:
                    buf = src_fd.read(length)
                    if not buf:
                        break
                    dst_fd.write(buf)


def smart_concat(src_paths: List[PathLike], dst_path: PathLike) -> None:
    """
    Concatenate src_paths to dst_path

    :param src_paths: List of source paths
    :param dst_path: Destination path
    """
    if not src_paths:
        return

    dst_protocol, _ = SmartPath._extract_protocol(dst_path)
    for src_path in src_paths:
        src_protocol, _ = SmartPath._extract_protocol(src_path)
        if src_protocol != dst_protocol:
            concat_func = _default_concat_func
            break
    else:
        concat_func = _concat_funcs.get(dst_protocol, _default_concat_func)
    concat_func(src_paths, dst_path)  # pyre-ignore[61]

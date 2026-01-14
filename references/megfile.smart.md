# megfile.smart module

### *class* megfile.smart.SmartCacher(path: str, cache_path: str | None = None, mode: str = 'r')

Bases: `FileCacher`

Smart cache files in local filesystem

#### cache_path *= None*

### megfile.smart.register_copy_func(src_protocol: str, dst_protocol: str, copy_func: Callable | None = None) → None

Used to register copy func between protocols,
and do not allow duplicate registration

* **Parameters:**
  * **src_protocol** – protocol name of source file, e.g. ‘s3’
  * **dst_protocol** – protocol name of destination file, e.g. ‘s3’
  * **copy_func** – copy func, its type is:
    Callable[[str, str, Optional[Callable[[int], None]], Optional[bool],
    Optional[bool]], None]

### megfile.smart.smart_abspath(path: str | BasePath | PathLike)

Return the absolute path of given path

* **Parameters:**
  **path** – Given path
* **Returns:**
  Absolute path of given path

### megfile.smart.smart_access(path: str | BasePath | PathLike, mode: Access) → bool

Test if path has access permission described by mode

* **Parameters:**
  * **path** – Path to be tested
  * **mode** – Access mode(Access.READ, Access.WRITE, Access.BUCKETREAD,
    Access.BUCKETWRITE)
* **Returns:**
  bool, if the path has read/write access.

### megfile.smart.smart_cache(path, cacher=<class 'megfile.smart.SmartCacher'>, \*\*options) → FileCacher

Return a path to Posixpath Interface

Examples:

```default
>>> import subprocess
>>> from megfile import smart_cache
>>> with smart_cache(
...     's3://mybucket/myfile.mp4',
...     mode='r',
... ) as cache_path:
...     subprocess.run(['ffprobe', cache_path])
```

* **Parameters:**
  * **path** – Path to cache
  * **s3_cacher** – Cacher for s3 path
  * **options** – Optional arguments for s3_cacher

### megfile.smart.smart_combine_open(path_glob: str, mode: str = 'rb', open_func=<function smart_open>) → CombineReader

Open a unified reader that supports multi file reading.

* **Parameters:**
  * **path_glob** – A path may contain shell wildcard characters
  * **mode** – Mode to open file, supports ‘rb’
* **Returns:**
  A ``CombineReader``

### megfile.smart.smart_concat(src_paths: List[str | BasePath | PathLike], dst_path: str | BasePath | PathLike) → None

Concatenate src_paths to dst_path

* **Parameters:**
  * **src_paths** – List of source paths
  * **dst_path** – Destination path

### megfile.smart.smart_copy(src_path: str | BasePath | PathLike, dst_path: str | BasePath | PathLike, callback: Callable[[int], None] | None = None, followlinks: bool = False, overwrite: bool = True) → None

Copy file from source path to destination path

Here are a few examples:

```default
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
```

* **Parameters:**
  * **src_path** – Given source path
  * **dst_path** – Given destination path
  * **callback** – Called periodically during copy, and the input parameter is the
    data size (in bytes) of copy since the last call
  * **followlinks** – False if regard symlink as file, else True
  * **overwrite** – whether or not overwrite file when exists, default is True

### megfile.smart.smart_exists(path: str | BasePath | PathLike, followlinks: bool = False) → bool

Test if path or s3_url exists

* **Parameters:**
  **path** – Path to be tested
* **Returns:**
  True if path exists, else False

### megfile.smart.smart_getmd5(path: str | BasePath | PathLike, recalculate: bool = False, followlinks: bool = False)

Get md5 value of file

* **Parameters:**
  * **path** – File path
  * **recalculate** – calculate md5 in real-time or not return s3 etag when path is s3
  * **followlinks** – If is True, calculate md5 for real file

### megfile.smart.smart_getmtime(path: str | BasePath | PathLike) → float

Get last-modified time of the file on the given s3_url or file path (in Unix
timestamp format).

If the path is an existent directory, return the latest modified time of
all file in it. The mtime of empty directory is 1970-01-01 00:00:00

* **Parameters:**
  **path** – Given path
* **Returns:**
  Last-modified time
* **Raises:**
  FileNotFoundError

### megfile.smart.smart_getsize(path: str | BasePath | PathLike) → int

Get file size on the given s3_url or file path (in bytes).

If the path in a directory, return the sum of all file size in it, including file
in subdirectories (if exist).

The result excludes the size of directory itself. In other words, return 0 Byte on
an empty directory path.

* **Parameters:**
  **path** – Given path
* **Returns:**
  File size
* **Raises:**
  FileNotFoundError

### megfile.smart.smart_glob(pathname: str | BasePath | PathLike, recursive: bool = True, missing_ok: bool = True) → List[str]

Given pathname may contain shell wildcard characters, return path list in ascending
alphabetical order, in which path matches glob pattern

* **Parameters:**
  * **pathname** – A path pattern may contain shell wildcard characters
  * **recursive** – If False, this function will not glob recursively
  * **missing_ok** – If False and target path doesn’t match any file,
    raise FileNotFoundError

### megfile.smart.smart_glob_stat(pathname: str | BasePath | PathLike, recursive: bool = True, missing_ok: bool = True) → Iterator[FileEntry]

Given pathname may contain shell wildcard characters, return a list contains tuples
of path and file stat in ascending alphabetical order,
in which path matches glob pattern

* **Parameters:**
  * **pathname** – A path pattern may contain shell wildcard characters
  * **recursive** – If False, this function will not glob recursively
  * **missing_ok** – If False and target path doesn’t match any file,
    raise FileNotFoundError

### megfile.smart.smart_iglob(pathname: str | BasePath | PathLike, recursive: bool = True, missing_ok: bool = True) → Iterator[str]

Given pathname may contain shell wildcard characters, return path iterator in
ascending alphabetical order, in which path matches glob pattern

* **Parameters:**
  * **pathname** – A path pattern may contain shell wildcard characters
  * **recursive** – If False, this function will not glob recursively
  * **missing_ok** – If False and target path doesn’t match any file,
    raise FileNotFoundError

### megfile.smart.smart_isabs(path: str | BasePath | PathLike) → bool

Test whether a path is absolute

* **Parameters:**
  **path** – Given path
* **Returns:**
  True if a path is absolute, else False

### megfile.smart.smart_isdir(path: str | BasePath | PathLike, followlinks: bool = False) → bool

Test if a file path or an s3 url is directory

* **Parameters:**
  **path** – Path to be tested
* **Returns:**
  True if path is directory, else False

### megfile.smart.smart_isfile(path: str | BasePath | PathLike, followlinks: bool = False) → bool

Test if a file path or an s3 url is file

* **Parameters:**
  **path** – Path to be tested
* **Returns:**
  True if path is file, else False

### megfile.smart.smart_islink(path: str | BasePath | PathLike) → bool

### megfile.smart.smart_ismount(path: str | BasePath | PathLike) → bool

Test whether a path is a mount point

* **Parameters:**
  **path** – Given path
* **Returns:**
  True if a path is a mount point, else False

### megfile.smart.smart_listdir(path: str | BasePath | PathLike | None = None) → List[str]

Get all contents of given s3_url or file path. The result is in
ascending alphabetical order.

* **Parameters:**
  **path** – Given path
* **Returns:**
  All contents of given s3_url or file path in ascending alphabetical order.
* **Raises:**
  FileNotFoundError, NotADirectoryError

### megfile.smart.smart_load_content(path: str | BasePath | PathLike, start: int | None = None, stop: int | None = None) → bytes

Get specified file from [start, stop) in bytes

* **Parameters:**
  * **path** – Specified path
  * **start** – start index
  * **stop** – stop index
* **Returns:**
  bytes content in range [start, stop)

### megfile.smart.smart_load_from(path: str | BasePath | PathLike) → BinaryIO

Read all content in binary on specified path and write into memory

User should close the BinaryIO manually

* **Parameters:**
  **path** – Specified path
* **Returns:**
  BinaryIO

### megfile.smart.smart_load_text(path: str | BasePath | PathLike) → str

Read content from path

* **Parameters:**
  **path** – Path to be read

### megfile.smart.smart_makedirs(path: str | BasePath | PathLike, exist_ok: bool = False) → None

Create a directory if is on fs.
If on s3, it actually check if target exists, and check if bucket has WRITE access

* **Parameters:**
  * **path** – Given path
  * **missing_ok** – if False and target directory not exists, raise FileNotFoundError
* **Raises:**
  PermissionError, FileExistsError

### megfile.smart.smart_move(src_path: str | BasePath | PathLike, dst_path: str | BasePath | PathLike, overwrite: bool = True) → None

Move file/directory on s3 or fs. s3:// or s3://bucket is not allowed to move

* **Parameters:**
  * **src_path** – Given source path
  * **dst_path** – Given destination path
  * **overwrite** – whether or not overwrite file when exists

### megfile.smart.smart_open(path: str | ~megfile.pathlike.BasePath | ~os.PathLike, mode: str = 'r', encoding: str | None = None, errors: str | None = None, \*, s3_open_func: ~typing.Callable[[str, str], ~typing.BinaryIO] = <function s3_buffered_open>, \*\*options) → IO

Open a file on the path

#### NOTE
On fs, the difference between this function and `io.open` is that
this function create directories automatically, instead of
raising FileNotFoundError

Here are a few examples:

```default
>>> import cv2
>>> import numpy as np
>>> raw = smart_open(
...     'https://ss2.bdstatic.com/70cFvnSh_Q1YnxGkpoWK1HF6hhy'
...     '/it/u=2275743969,3715493841&fm=26&gp=0.jpg'
... ).read()
>>> img = cv2.imdecode(np.frombuffer(raw, np.uint8),
...                    cv2.IMREAD_ANYDEPTH | cv2.IMREAD_COLOR)
```

* **Parameters:**
  * **path** – Given path
  * **mode** – Mode to open file, supports r’[rwa][tb]?+?’
  * **encoding** – encoding is the name of the encoding used to decode or encode
    the file. This should only be used in text mode.
  * **errors** – errors is an optional string that specifies how encoding and decoding
    errors are to be handled—this cannot be used in binary mode.
  * **buffering** – buffering is an optional integer used to
    set the buffering policy. Only be used when support.
  * **followlinks** – follow symbolic link, default False. Only be used when support
  * **s3_open_func** – Function used to open s3_url. Require the function includes
    2 necessary parameters, file path and mode. only be used in s3 path.
  * **max_workers** – Max download / upload thread number, None by default,
    will use global thread pool with 8 threads. Only be used in s3, http, hdfs.
  * **max_buffer_size** – Max cached buffer size in memory, 128MB by default.
    Set to 0 will disable cache. Only be used in s3, http, hdfs.
  * **block_forward** – How many blocks of data cached from offset position, only for
    read mode. Only be used in s3, http, hdfs.
  * **block_size** – Size of single block. Each block will be uploaded by single
    thread. Only be used in s3, http, hdfs.
  * **buffered** – If you are operating pickle file without .pkl or .pickle extension,
    please set this to True to avoid the performance issue.
* **Returns:**
  File-Like object
* **Raises:**
  FileNotFoundError, IsADirectoryError, ValueError

### megfile.smart.smart_path_join(path: str | BasePath | PathLike, \*other_paths: str | BasePath | PathLike) → str

Concat 2 or more path to a complete path

* **Parameters:**
  * **path** – Given path
  * **other_paths** – Paths to be concatenated
* **Returns:**
  Concatenated complete path

#### NOTE
For URI, the difference between this function and `os.path.join` is that this
function ignores left side slash (which indicates absolute path) in
`other_paths` and will directly concat.

e.g. os.path.join(‘s3://path’, ‘to’, ‘/file’) => ‘/file’, and
smart_path_join(‘s3://path’, ‘to’, ‘/file’) => ‘/path/to/file’

But for fs path, this function behaves exactly like `os.path.join`

e.g. smart_path_join(‘/path’, ‘to’, ‘/file’) => ‘/file’

### megfile.smart.smart_readlink(path: str | BasePath | PathLike) → str | BasePath | PathLike

Return a string representing the path to which the symbolic link points.
:param path: Path to be read
:returns: Return a string representing the path to which the symbolic link points.

### megfile.smart.smart_realpath(path: str | BasePath | PathLike)

Return the real path of given path

* **Parameters:**
  **path** – Given path
* **Returns:**
  Real path of given path

### megfile.smart.smart_relpath(path: str | BasePath | PathLike, start=None)

Return the relative path of given path

* **Parameters:**
  * **path** – Given path
  * **start** – Given start directory
* **Returns:**
  Relative path from start

### megfile.smart.smart_remove(path: str | BasePath | PathLike, missing_ok: bool = False) → None

Remove the file or directory on s3 or fs, s3:// and s3://bucket are
not permitted to remove

* **Parameters:**
  * **path** – Given path
  * **missing_ok** – if False and target file/directory not exists,
    raise FileNotFoundError
* **Raises:**
  PermissionError, FileNotFoundError

### megfile.smart.smart_rename(src_path: str | BasePath | PathLike, dst_path: str | BasePath | PathLike, overwrite: bool = True) → None

Move file on s3 or fs. s3:// or s3://bucket is not allowed to move

* **Parameters:**
  * **src_path** – Given source path
  * **dst_path** – Given destination path
  * **overwrite** – whether or not overwrite file when exists

### megfile.smart.smart_save_as(file_object: BinaryIO, path: str | BasePath | PathLike) → None

Write the opened binary stream to specified path, but the stream won’t be closed

* **Parameters:**
  * **file_object** – Stream to be read
  * **path** – Specified target path

### megfile.smart.smart_save_content(path: str | BasePath | PathLike, content: bytes) → None

Save bytes content to specified path

* **Parameters:**
  **path** – Path to save content

### megfile.smart.smart_save_text(path: str | BasePath | PathLike, text: str) → None

Save text to specified path

* **Parameters:**
  **path** – Path to save text

### megfile.smart.smart_scan(path: str | BasePath | PathLike, missing_ok: bool = True, followlinks: bool = False) → Iterator[str]

Iteratively traverse only files in given directory, in alphabetical order.
Every iteration on generator yields a path string.

If path is a file path, yields the file only
If path is a non-existent path, return an empty generator
If path is a bucket path, return all file paths in the bucket

* **Parameters:**
  * **path** – Given path
  * **missing_ok** – If False and there’s no file in the directory,
    raise FileNotFoundError
* **Raises:**
  UnsupportedError
* **Returns:**
  A file path generator

### megfile.smart.smart_scan_stat(path: str | BasePath | PathLike, missing_ok: bool = True, followlinks: bool = False) → Iterator[FileEntry]

Iteratively traverse only files in given directory, in alphabetical order.
Every iteration on generator yields a tuple of path string and file stat

* **Parameters:**
  * **path** – Given path
  * **missing_ok** – If False and there’s no file in the directory,
    raise FileNotFoundError
* **Raises:**
  UnsupportedError
* **Returns:**
  A file path generator

### megfile.smart.smart_scandir(path: str | BasePath | PathLike | None = None) → Iterator[FileEntry]

Get all content of given s3_url or file path.

* **Parameters:**
  **path** – Given path
* **Returns:**
  An iterator contains all contents have prefix path
* **Raises:**
  FileNotFoundError, NotADirectoryError

### megfile.smart.smart_stat(path: str | BasePath | PathLike, follow_symlinks=True) → StatResult

Get StatResult of s3_url or file path

* **Parameters:**
  **path** – Given path
* **Returns:**
  StatResult
* **Raises:**
  FileNotFoundError

### megfile.smart.smart_symlink(src_path: str | BasePath | PathLike, dst_path: str | BasePath | PathLike) → None

Create a symbolic link pointing to src_path named path.

* **Parameters:**
  * **src_path** – Source path
  * **dst_path** – Destination path

### megfile.smart.smart_sync(src_path: str | ~megfile.pathlike.BasePath | ~os.PathLike, dst_path: str | ~megfile.pathlike.BasePath | ~os.PathLike, callback: ~typing.Callable[[str, int], None] | None = None, followlinks: bool = False, callback_after_copy_file: ~typing.Callable[[str, str], None] | None = None, src_file_stats: ~typing.Iterable[~megfile.pathlike.FileEntry] | None = None, map_func: ~typing.Callable[[~typing.Callable, ~typing.Iterable], ~typing.Any] = <class 'map'>, force: bool = False, overwrite: bool = True) → None

Sync file or directory

#### NOTE
When the parameter is file, this function bahaves like `smart_copy`.

If file and directory of same name and same level, sync consider it’s file first

Here are a few examples:

```default
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
```

* **Parameters:**
  * **src_path** – Given source path
  * **dst_path** – Given destination path
  * **callback** – Called periodically during copy, and the input parameter is
    the data size (in bytes) of copy since the last call
  * **followlinks** – False if regard symlink as file, else True
  * **callback_after_copy_file** – Called after copy success, and the input parameter
    is src file path and dst file path
  * **src_file_stats** – If this parameter is not None, only this parameter’s files
    will be synced,and src_path is the root_path of these files used to calculate
    the path of the target file. This parameter is in order to reduce file traversal
    times.
  * **map_func** – A Callable func like map. You can use ThreadPoolExecutor.map,
    Pool.map and so on if you need concurrent capability. default is standard
    library map.
  * **force** – Sync file forcible, do not ignore same files, priority is higher than
    ‘overwrite’, default is False
  * **overwrite** – whether or not overwrite file when exists, default is True

### megfile.smart.smart_sync_with_progress(src_path, dst_path, callback: ~typing.Callable[[str, int], None] | None = None, followlinks: bool = False, map_func: ~typing.Callable[[~typing.Callable, ~typing.Iterable], ~typing.Iterator] = <class 'map'>, force: bool = False, overwrite: bool = True)

Sync file or directory with progress bar

* **Parameters:**
  * **src_path** – Given source path
  * **dst_path** – Given destination path
  * **callback** – Called periodically during copy, and the input parameter is
    the data size (in bytes) of copy since the last call
  * **followlinks** – False if regard symlink as file, else True
  * **callback_after_copy_file** – Called after copy success, and the input parameter
    is src file path and dst file path
  * **src_file_stats** – If this parameter is not None, only this parameter’s files
    will be synced, and src_path is the root_path of these files used to calculate
    the path of the target file. This parameter is in order to reduce file traversal
    times.
  * **map_func** – A Callable func like map. You can use ThreadPoolExecutor.map,
    Pool.map and so on if you need concurrent capability. default is standard
    library map.
  * **force** – Sync file forcible, do not ignore same files, priority is higher than
    ‘overwrite’, default is False
  * **overwrite** – whether or not overwrite file when exists, default is True

### megfile.smart.smart_touch(path: str | BasePath | PathLike)

Create a new file on path

* **Parameters:**
  **path** – Path to create file

### megfile.smart.smart_unlink(path: str | BasePath | PathLike, missing_ok: bool = False) → None

Remove the file on s3 or fs

* **Parameters:**
  * **path** – Given path
  * **missing_ok** – if False and target file not exists, raise FileNotFoundError
* **Raises:**
  PermissionError, FileNotFoundError, IsADirectoryError

### megfile.smart.smart_walk(path: str | BasePath | PathLike, followlinks: bool = False) → Iterator[Tuple[str, List[str], List[str]]]

Generate the file names in a directory tree by walking the tree top-down.
For each directory in the tree rooted at directory path (including path itself),
it yields a 3-tuple (root, dirs, files).

- root: a string of current path
- dirs: name list of subdirectories (excluding ‘.’ and ‘..’ if they exist) in ‘root’
  The list is sorted by ascending alphabetical order
- files: name list of non-directory files (link is regarded as file) in ‘root’.
  The list is sorted by ascending alphabetical order

If path not exists, return an empty generator
If path is a file, return an empty generator
If try to apply walk() on unsupported path, raise UnsupportedError

* **Parameters:**
  **path** – Given path
* **Raises:**
  UnsupportedError
* **Returns:**
  A 3-tuple generator

import atexit
import hashlib
import io
import os
from functools import lru_cache
from logging import getLogger as get_logger
from stat import S_ISDIR, S_ISLNK, S_ISREG
from typing import IO, AnyStr, BinaryIO, Callable, Iterator, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

import paramiko

from megfile.errors import _create_missing_ok_generator
from megfile.interfaces import ContextIterator, FileEntry, PathLike, StatResult
from megfile.lib.glob import FSFunc, iglob
from megfile.lib.joinpath import uri_join
from megfile.utils import calculate_md5

from .interfaces import PathLike, URIPath
from .lib.compat import fspath
from .smart_path import SmartPath

_logger = get_logger(__name__)

__all__ = [
    'SftpPath',
    'is_sftp',
    'sftp_readlink',
    'sftp_glob',
    'sftp_iglob',
    'sftp_glob_stat',
    'sftp_resolve',
    'sftp_download',
    'sftp_upload',
    'sftp_path_join',
]

SFTP_USERNAME = "SFTP_USERNAME"
SFTP_PASSWORD = "SFTP_PASSWORD"
SFTP_PRIVATE_KEY_PATH = "SFTP_PRIVATE_KEY_PATH"
SFTP_PRIVATE_KEY_TYPE = "SFTP_PRIVATE_KEY_TYPE"
SFTP_PRIVATE_KEY_PASSWORD = "SFTP_PRIVATE_KEY_PASSWORD"


def _make_stat(stat: paramiko.SFTPAttributes) -> StatResult:
    return StatResult(
        size=stat.st_size,
        mtime=stat.st_mtime,
        isdir=S_ISDIR(stat.st_mode),
        islnk=S_ISLNK(stat.st_mode),
        extra=stat,
    )


def get_private_key():
    key_with_types = {
        'DSA': paramiko.DSSKey,
        'RSA': paramiko.RSAKey,
        'ECDSA': paramiko.ECDSAKey,
        'ED25519': paramiko.Ed25519Key,
    }
    key_type = os.getenv(SFTP_PRIVATE_KEY_TYPE, 'RSA').upper()
    if os.getenv(SFTP_PRIVATE_KEY_PATH):
        private_key_path = os.getenv(SFTP_PRIVATE_KEY_PATH)
        if not os.path.exists(private_key_path):
            raise FileNotFoundError(
                f"Private key file not exist: '{SFTP_PRIVATE_KEY_PATH}'")
        return key_with_types[key_type].from_private_key_file(
            private_key_path, password=os.getenv(SFTP_PRIVATE_KEY_PASSWORD))
    return None


def provide_connect_info(
        hostname: str,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
):
    if not port:
        port = 22
    if not username:
        username = os.getenv(SFTP_USERNAME)
    if not password:
        password = os.getenv(SFTP_PASSWORD)
    private_key = get_private_key()
    return hostname, port, username, password, private_key


@lru_cache()
def get_sftp_client(
        hostname: str,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
):  # pragma: no cover
    '''Get sftp client

    :returns: sftp client
    '''
    hostname, port, username, password, private_key = provide_connect_info(
        hostname=hostname,
        port=port,
        username=username,
        password=password,
    )
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password, pkey=private_key)
    client = paramiko.SFTPClient.from_transport(transport)
    if not client:
        raise ConnectionError('SFTP connect error')
    atexit.register(client.close)
    return client


@lru_cache()
def get_ssh_client(
        hostname: str,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
):  # pragma: no cover
    hostname, port, username, password, private_key = provide_connect_info(
        hostname=hostname,
        port=port,
        username=username,
        password=password,
    )

    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        hostname=hostname,
        username=username,
        password=password,
        pkey=private_key,
    )
    atexit.register(ssh_client.close)
    return ssh_client


def is_sftp(path: PathLike) -> bool:
    '''Test if a path is sftp path

    :param path: Path to be tested
    :returns: True of a path is sftp path, else False
    '''
    path = fspath(path)
    parts = urlsplit(path)
    return parts.scheme == 'sftp'


def sftp_readlink(path: PathLike) -> 'str':
    '''
    Return a SftpPath instance representing the path to which the symbolic link points.
    :param path: Given path
    :returns: Return a SftpPath instance representing the path to which the symbolic link points.
    '''
    return SftpPath(path).readlink().path_with_protocol


def sftp_glob(path: PathLike, recursive: bool = True,
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

    :param path: Given path
    :param pattern: Glob the given relative pattern in the directory represented by this path
    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    :returns: A list contains paths match `pathname`
    '''
    return list(
        sftp_iglob(path=path, recursive=recursive, missing_ok=missing_ok))


def sftp_glob_stat(
        path: PathLike, recursive: bool = True,
        missing_ok: bool = True) -> Iterator[FileEntry]:
    '''Return a list contains tuples of path and file stat, in ascending alphabetical order, in which path matches glob pattern

    1. If doesn't match any path, return empty list
        Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist. sftp_glob behaves like ``glob.glob`` in standard library under such circumstance.
    2. No guarantee that each path in result is different, which means:
        Assume there exists a path `/a/b/c/b/d.txt`
        use path pattern like `/**/b/**/*.txt` to glob, the path above will be returned twice
    3. `**` will match any matched file, directory, symlink and '' by default, when recursive is `True`
    4. fs_glob returns same as glob.glob(pathname, recursive=True) in acsending alphabetical order.
    5. Hidden files (filename stars with '.') will not be found in the result

    :param path: Given path
    :param pattern: Glob the given relative pattern in the directory represented by this path
    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    :returns: A list contains tuples of path and file stat, in which paths match `pathname`
    '''
    for path in sftp_iglob(path=path, recursive=recursive,
                           missing_ok=missing_ok):
        path_object = SftpPath(path)
        yield FileEntry(
            path_object.name, path_object.path_with_protocol,
            path_object.lstat())


def sftp_iglob(path: PathLike, recursive: bool = True,
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

    :param path: Given path
    :param pattern: Glob the given relative pattern in the directory represented by this path
    :param recursive: If False, `**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    :returns: An iterator contains paths match `pathname`
    '''

    for path in SftpPath(path).iglob(pattern="", recursive=recursive,
                                     missing_ok=missing_ok):
        yield path.path_with_protocol


def sftp_resolve(path: PathLike, strict=False) -> 'str':
    '''Equal to fs_realpath

    :param path: Given path
    :param strict: Ignore this parameter, just for compatibility
    :return: Return the canonical path of the specified filename, eliminating any symbolic links encountered in the path.
    :rtype: SftpPath
    '''
    return SftpPath(path).resolve(strict).path_with_protocol


def _sftp_scan_pairs(src_url: PathLike,
                     dst_url: PathLike) -> Iterator[Tuple[PathLike, PathLike]]:
    for src_file_path in SftpPath(src_url).scan():
        content_path = src_file_path[len(src_url):]
        if len(content_path) > 0:
            dst_file_path = SftpPath(dst_url).joinpath(
                content_path).path_with_protocol
        else:
            dst_file_path = dst_url
        yield src_file_path, dst_file_path


def sftp_download(
        src_url: PathLike,
        dst_url: PathLike,
        callback: Optional[Callable[[int], None]] = None,
        followlinks: bool = False):
    '''
    File download
    '''
    from megfile.fs import is_fs
    if not is_fs(dst_url):
        raise OSError(f'dst_url is not fs path: {dst_url}')
    if not is_sftp(src_url):
        raise OSError(f'src_url is not sftp path: {src_url}')

    src_url = SftpPath(src_url)
    if followlinks and SftpPath(src_url).is_symlink():
        src_url = SftpPath(src_url).readlink()
    if SftpPath(src_url).is_dir():
        raise IsADirectoryError('Is a directory: %r' % src_url)

    dir_path = os.path.dirname(dst_url)
    os.makedirs(dir_path, exist_ok=True)
    src_url._client.get(src_url._real_path, dst_url, callback=callback)


def sftp_upload(
        src_url: PathLike,
        dst_url: PathLike,
        callback: Optional[Callable[[int], None]] = None,
        followlinks: bool = False):
    '''
    File download
    '''
    from megfile.fs import is_fs
    if not is_fs(src_url):
        raise OSError(f'src_url is not fs path: {src_url}')
    if not is_sftp(dst_url):
        raise OSError(f'dst_url is not sftp path: {dst_url}')

    if followlinks and os.path.islink(src_url):
        src_url = os.readlink(src_url)
    if os.path.isdir(src_url):
        raise IsADirectoryError('Is a directory: %r' % src_url)

    dst_url = SftpPath(dst_url)
    dir_path = dst_url.parent
    dir_path.makedirs(exist_ok=True)
    dst_url._client.put(src_url, dst_url._real_path, callback=callback)


def sftp_path_join(path: PathLike, *other_paths: PathLike) -> str:
    '''
    Concat 2 or more path to a complete path

    :param path: Given path
    :param other_paths: Paths to be concatenated
    :returns: Concatenated complete path

    .. note ::

        The difference between this function and ``os.path.join`` is that this function ignores left side slash (which indicates absolute path) in ``other_paths`` and will directly concat.
        e.g. os.path.join('/path', 'to', '/file') => '/file', but sftp_path_join('/path', 'to', '/file') => '/path/to/file'
    '''
    return uri_join(fspath(path), *map(fspath, other_paths))


@SmartPath.register
class SftpPath(URIPath):
    """sftp protocol

    uri format: sftp://[username[:password]@]hostname[:port]/file_path
    e.g. sftp://username:password@127.0.0.1:22/data/test/
    """

    protocol = "sftp"

    def __init__(self, path: "PathLike", *other_paths: "PathLike"):
        super().__init__(path, *other_paths)
        parts = urlsplit(self.path)
        self._real_path = parts.path
        if not self._real_path.startswith('/'):
            self._real_path = f"/{self._real_path}"
        self._urlsplit_parts = parts

    @property
    def _client(self):
        return get_sftp_client(
            hostname=self._urlsplit_parts.hostname,
            port=self._urlsplit_parts.port,
            username=self._urlsplit_parts.username,
            password=self._urlsplit_parts.password)

    def _generate_path_object(self, sftp_local_path: str):
        new_parts = self._urlsplit_parts._replace(
            path=sftp_local_path.lstrip('/'))
        return self.from_path(urlunsplit(new_parts))

    def exists(self, followlinks: bool = False) -> bool:
        '''
        Test if the path exists

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path exists, else False

        '''
        try:
            if followlinks:
                self._client.stat(self._real_path)
            else:
                self._client.lstat(self._real_path)
            return True
        except FileNotFoundError:
            return False

    def getmtime(self, follow_symlinks: bool = False) -> float:
        '''
        Get last-modified time of the file on the given path (in Unix timestamp format).
        If the path is an existent directory, return the latest modified time of all file in it.

        :returns: last-modified time
        '''
        return self.stat(follow_symlinks=follow_symlinks).mtime

    def getsize(self, follow_symlinks: bool = False) -> int:
        '''
        Get file size on the given file path (in bytes).
        If the path in a directory, return the sum of all file size in it, including file in subdirectories (if exist).
        The result excludes the size of directory itself. In other words, return 0 Byte on an empty directory path.

        :returns: File size

        '''
        return self.stat(follow_symlinks=follow_symlinks).size

    def glob(self, pattern, recursive: bool = True,
             missing_ok: bool = True) -> List['SftpPath']:
        '''Return path list in ascending alphabetical order, in which path matches glob pattern

        1. If doesn't match any path, return empty list
            Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist. fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
        2. No guarantee that each path in result is different, which means:
            Assume there exists a path `/a/b/c/b/d.txt`
            use path pattern like `/**/b/**/*.txt` to glob, the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default, when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True) in acsending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param pattern: Glob the given relative pattern in the directory represented by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
        :returns: A list contains paths match `pathname`
        '''
        return list(
            self.iglob(
                pattern=pattern, recursive=recursive, missing_ok=missing_ok))

    def glob_stat(
            self, pattern, recursive: bool = True,
            missing_ok: bool = True) -> Iterator[FileEntry]:
        '''Return a list contains tuples of path and file stat, in ascending alphabetical order, in which path matches glob pattern

        1. If doesn't match any path, return empty list
            Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist. sftp_glob behaves like ``glob.glob`` in standard library under such circumstance.
        2. No guarantee that each path in result is different, which means:
            Assume there exists a path `/a/b/c/b/d.txt`
            use path pattern like `/**/b/**/*.txt` to glob, the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default, when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True) in acsending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param pattern: Glob the given relative pattern in the directory represented by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
        :returns: A list contains tuples of path and file stat, in which paths match `pathname`
        '''
        for path_obj in self.iglob(pattern=pattern, recursive=recursive,
                                   missing_ok=missing_ok):
            yield FileEntry(path_obj.name, path_obj.path, path_obj.lstat())

    def iglob(self, pattern, recursive: bool = True,
              missing_ok: bool = True) -> Iterator['SftpPath']:
        '''Return path iterator in ascending alphabetical order, in which path matches glob pattern

        1. If doesn't match any path, return empty list
            Notice:  ``glob.glob`` in standard library returns ['a/'] instead of empty list when pathname is like `a/**`, recursive is True and directory 'a' doesn't exist. fs_glob behaves like ``glob.glob`` in standard library under such circumstance.
        2. No guarantee that each path in result is different, which means:
            Assume there exists a path `/a/b/c/b/d.txt`
            use path pattern like `/**/b/**/*.txt` to glob, the path above will be returned twice
        3. `**` will match any matched file, directory, symlink and '' by default, when recursive is `True`
        4. fs_glob returns same as glob.glob(pathname, recursive=True) in acsending alphabetical order.
        5. Hidden files (filename stars with '.') will not be found in the result

        :param pattern: Glob the given relative pattern in the directory represented by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
        :returns: An iterator contains paths match `pathname`
        '''
        glob_path = self.path_with_protocol
        if pattern:
            glob_path = self.joinpath(pattern).path_with_protocol

        def _scandir(dirname: str) -> Iterator[Tuple[str, bool]]:
            for entry in self.from_path(dirname).scandir():
                yield entry.name, entry.is_dir()

        def _exist(path: PathLike, followlinks: bool = False):
            return self.from_path(path).exists(followlinks=followlinks)

        def _is_dir(path: PathLike, followlinks: bool = False):
            return self.from_path(path).is_dir(followlinks=followlinks)

        fs = FSFunc(_exist, _is_dir, _scandir)
        for real_path in _create_missing_ok_generator(
                iglob(fspath(glob_path), recursive=recursive, fs=fs),
                missing_ok, FileNotFoundError('No match file: %r' % glob_path)):
            yield self.from_path(real_path)

    def is_dir(self, followlinks: bool = False) -> bool:
        '''
        Test if a path is directory

        .. note::

            The difference between this function and ``os.path.isdir`` is that this function regard symlink as file

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path is a directory, else False

        '''
        stat = self.stat(follow_symlinks=followlinks)
        if S_ISDIR(stat.st_mode):
            return True
        return False

    def is_file(self, followlinks: bool = False) -> bool:
        '''
        Test if a path is file

        .. note::
        
            The difference between this function and ``os.path.isfile`` is that this function regard symlink as file

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path is a file, else False

        '''
        stat = self.stat(follow_symlinks=followlinks)
        if S_ISREG(stat.st_mode):
            return True
        return False

    def listdir(self) -> List[str]:
        '''
        Get all contents of given sftp path. The result is in acsending alphabetical order.

        :returns: All contents have in the path in acsending alphabetical order
        '''
        if not self.is_dir():
            raise NotADirectoryError(
                f"Not a directory: '{self.path_with_protocol}'")
        return sorted(self._client.listdir(self._real_path))

    def iterdir(self) -> Iterator['SftpPath']:
        '''
        Get all contents of given sftp path. The result is in acsending alphabetical order.

        :returns: All contents have in the path in acsending alphabetical order
        '''
        if not self.is_dir():
            raise NotADirectoryError(
                f"Not a directory: '{self.path_with_protocol}'")
        for path in self.listdir():
            yield self.joinpath(path)  # type: ignore

    def load(self) -> BinaryIO:
        '''Read all content on specified path and write into memory

        User should close the BinaryIO manually

        :returns: Binary stream
        '''
        with self.open(mode='rb') as f:
            data = f.read()
        return io.BytesIO(data)

    def mkdir(self, mode=0o777, parents: bool = False, exist_ok: bool = False):
        '''
        make a directory on sftp, including parent directory

        If there exists a file on the path, raise FileExistsError

        :param mode: If mode is given, it is combined with the process’ umask value to determine the file mode and access flags.
        :param parents: If parents is true, any missing parents of this path are created as needed;
        If parents is false (the default), a missing parent raises FileNotFoundError.
        :param exist_ok: If False and target directory exists, raise FileExistsError
        :raises: FileExistsError
        '''
        if self.exists():
            if not exist_ok:
                raise FileExistsError(
                    f"File exists: '{self.path_with_protocol}'")
            return

        if parents:
            parent_path_objects = []
            for parent_path_object in self.parents:
                if parent_path_object.exists():
                    break
                else:
                    parent_path_objects.append(parent_path_object)
            for parent_path_object in parent_path_objects[::-1]:
                parent_path_object.mkdir(
                    mode=mode, parents=False, exist_ok=True)
        self._client.mkdir(path=self._real_path, mode=mode)

    def realpath(self) -> str:
        '''Return the real path of given path

        :returns: Real path of given path
        '''
        return self.resolve().path_with_protocol

    def rename(self, dst_path: PathLike) -> 'SftpPath':
        '''
        rename file on sftp

        :param dst_path: Given destination path
        '''
        dst_path = self.from_path(dst_path)
        self._client.rename(self._real_path, dst_path._real_path)
        return dst_path

    def replace(self, dst_path: PathLike) -> 'SftpPath':
        '''
        move file on sftp

        :param dst_path: Given destination path
        '''
        return self.rename(dst_path=dst_path)

    def remove(self, missing_ok: bool = False) -> None:
        '''
        Remove the file or directory on sftp

        :param missing_ok: if False and target file/directory not exists, raise FileNotFoundError
        '''
        if missing_ok and not self.exists():
            return
        if self.is_dir():
            for file_entry in self.scandir():
                self.from_path(file_entry.path).remove(missing_ok=missing_ok)
            self._client.rmdir(self._real_path)
        else:
            self._client.unlink(self._real_path)

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
        scan_stat_iter = self.scan_stat(
            missing_ok=missing_ok, followlinks=followlinks)

        for file_entry in scan_stat_iter:
            yield file_entry.path

    def scan_stat(self, missing_ok: bool = True,
                  followlinks: bool = False) -> Iterator[FileEntry]:
        '''
        Iteratively traverse only files in given directory, in alphabetical order.
        Every iteration on generator yields a tuple of path string and file stat

        :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
        :returns: A file path generator
        '''

        def create_generator() -> Iterator[FileEntry]:
            path = self
            if path.is_file():
                # On s3, file and directory may be of same name and level, so need to test the path is file or directory
                yield FileEntry(
                    path.name, path.path_with_protocol,
                    path.stat(follow_symlinks=followlinks))
                return

            for name in path.listdir():
                current_path = self.joinpath(name)
                if current_path.is_dir():
                    yield from current_path.scan_stat()
                else:
                    yield FileEntry(
                        current_path.name,  # type: ignore
                        current_path.path_with_protocol,
                        current_path.stat(follow_symlinks=followlinks))

        return _create_missing_ok_generator(
            create_generator(), missing_ok,
            FileNotFoundError('No match file: %r' % self.path_with_protocol))

    def scandir(self) -> Iterator[FileEntry]:
        '''
        Get all content of given file path.

        :returns: An iterator contains all contents have prefix path
        '''
        if not self.exists():
            raise FileNotFoundError(
                'No such directory: %r' % self.path_with_protocol)

        if not self.is_dir():
            raise NotADirectoryError(
                'Not a directory: %r' % self.path_with_protocol)

        def create_generator():
            for name in self.listdir():
                current_path = self.joinpath(name)
                yield FileEntry(
                    current_path.name,  # type: ignore
                    current_path.path_with_protocol,
                    current_path.lstat())  # type: ignore

        return ContextIterator(create_generator())

    def stat(self, follow_symlinks=True) -> StatResult:
        '''
        Get StatResult of file on sftp, including file size and mtime, referring to fs_getsize and fs_getmtime

        :returns: StatResult
        '''
        if follow_symlinks:
            result = _make_stat(self._client.stat(self._real_path))
        else:
            result = _make_stat(self._client.lstat(self._real_path))
        return result

    def lstat(self) -> StatResult:
        '''
        Get StatResult of file on sftp, including file size and mtime, referring to fs_getsize and fs_getmtime

        :returns: StatResult
        '''
        return self.stat(follow_symlinks=False)

    def unlink(self, missing_ok: bool = False) -> None:
        '''
        Remove the file on sftp

        :param missing_ok: if False and target file not exists, raise FileNotFoundError
        '''
        if missing_ok and not self.exists():
            return
        self._client.unlink(self._real_path)

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

        stack = [self._real_path]
        while stack:
            root = stack.pop()
            dirs, files = [], []
            filenames = self._client.listdir(root)
            for name in filenames:
                current_path = self._generate_path_object(root).joinpath(name)
                if current_path.is_file(followlinks=followlinks):
                    files.append(name)
                elif current_path.is_dir(followlinks=followlinks):
                    dirs.append(name)

            dirs = sorted(dirs)
            files = sorted(files)

            yield self._generate_path_object(
                root).path_with_protocol, dirs, files

            stack.extend(
                (os.path.join(root, directory) for directory in reversed(dirs)))

    def resolve(self, strict=False) -> 'SftpPath':
        '''Equal to sftp_realpath

        :param strict: Ignore this parameter, just for compatibility
        :return: Return the canonical path of the specified filename, eliminating any symbolic links encountered in the path.
        :rtype: SftpPath
        '''
        path = self._client.normalize(self._real_path)
        return self._generate_path_object(path)

    def md5(self, recalculate: bool = False, followlinks: bool = True):
        '''
        Calculate the md5 value of the file

        :param recalculate: Ignore this parameter, just for compatibility
        :param followlinks: Ignore this parameter, just for compatibility
        returns: md5 of file
        '''
        if self.is_dir():
            hash_md5 = hashlib.md5()  # nosec
            for file_name in self.listdir():
                chunk = self.joinpath(file_name).md5(  # type: ignore
                    recalculate=recalculate, followlinks=followlinks).encode()
                hash_md5.update(chunk)
            return hash_md5.hexdigest()
        with self.open('rb') as src:  # type: ignore
            md5 = calculate_md5(src)
        return md5

    def symlink(self, dst_path: PathLike) -> None:
        '''
        Create a symbolic link pointing to src_path named dst_path.

        :param dst_path: Desination path
        '''
        dst_path = self.from_path(dst_path)
        if dst_path.exists(followlinks=False):
            raise FileExistsError(
                f"File exists: '{dst_path.path_with_protocol}'")
        return self._client.symlink(self._real_path, dst_path._real_path)

    def readlink(self) -> 'SftpPath':
        '''
        Return a SftpPath instance representing the path to which the symbolic link points.
        :returns: Return a SftpPath instance representing the path to which the symbolic link points.
        '''
        if not self.is_symlink():
            raise OSError('Not a symlink: %s' % self.path_with_protocol)
        return self._generate_path_object(
            self._client.readlink(self._real_path))

    def is_symlink(self) -> bool:
        '''Test whether a path is a symbolic link

        :return: If path is a symbolic link return True, else False
        :rtype: bool
        '''
        return self.lstat().is_symlink()

    def cwd(self) -> 'SftpPath':
        '''Return current working directory

        returns: Current working directory
        '''
        return self._generate_path_object(self._client.getcwd())

    def save(self, file_object: BinaryIO):
        '''Write the opened binary stream to path
        If parent directory of path doesn't exist, it will be created.

        :param file_object: stream to be read
        '''
        with self.open(mode='wb') as output:
            output.write(file_object.read())

    def open(self, mode: str = 'r', buffering=-1, **kwargs) -> IO[AnyStr]:
        if 'w' in mode or 'x' in mode or 'a' in mode:
            try:
                if self.is_dir():
                    raise IsADirectoryError(
                        'Is a directory: %r' % self.path_with_protocol)
            except FileNotFoundError:
                pass
            self.parent.mkdir(parents=True, exist_ok=True)
        elif not self.is_file():
            raise IsADirectoryError(
                'Is a directory: %r' % self.path_with_protocol)
        fileobj = self._client.open(self._real_path, mode, bufsize=buffering)
        if 'r' in mode and 'b' not in mode:
            return io.TextIOWrapper(fileobj)
        return fileobj

    def chmod(self, mode: int, follow_symlinks: bool = True):
        '''
        Change the file mode and permissions, like os.chmod().

        :param mode: the file mode you want to change
        :param followlinks: Ignore this parameter, just for compatibility
        '''
        return self._client.chmod(path=self._real_path, mode=mode)

    def absolute(self) -> str:
        '''
        Make the path absolute, without normalization or resolving symlinks. Returns a new path object
        '''
        return self.realpath()

    def rmdir(self):
        '''
        Remove this directory. The directory must be empty.
        '''
        if len(self.listdir()) > 0:
            raise OSError(f"Directory not empty: '{self.path_with_protocol}'")
        return self._client.rmdir(self._real_path)

    def copy(
            self,
            dst_path: PathLike,
            callback: Optional[Callable[[int], None]] = None,
            followlinks: bool = False):
        '''
        File copy
        '''
        if not is_sftp(dst_path):
            raise OSError(f'dst_path is not sftp path: {dst_path}')

        if followlinks and self.is_symlink():
            return self.readlink().copy(dst_path=dst_path, callback=callback)

        if self.is_dir():
            raise IsADirectoryError(
                'Is a directory: %r' % self.path_with_protocol)

        dst_path = self.from_path(dst_path)
        if self._urlsplit_parts.hostname == dst_path._urlsplit_parts.hostname and self._urlsplit_parts.username == dst_path._urlsplit_parts.username and self._urlsplit_parts.password == dst_path._urlsplit_parts.password and self._urlsplit_parts.port == dst_path._urlsplit_parts.port:
            ssh_client = get_ssh_client(
                hostname=self._urlsplit_parts.hostname,
                port=self._urlsplit_parts.port,
                username=self._urlsplit_parts.username,
                password=self._urlsplit_parts.password,
            )
            _stdin, _stdout, stderr = ssh_client.exec_command(
                f"cp {self._real_path} {dst_path._real_path}")
            if not dst_path.exists():  # pragma: no cover
                _logger.error(stderr)
                raise OSError('Copy file error')
            elif dst_path.lstat().size != self.lstat().size:  # pragma: no cover
                raise OSError('Copy file error, size mismatch')
        else:
            with self.open('rb') as fsrc:
                with dst_path.open('wb') as fdst:
                    length = 16 * 1024
                    while True:
                        buf = fsrc.read(length)
                        if not buf:
                            break
                        fdst.write(buf)
                        if callback:
                            callback(len(buf))

    def sync(self, dst_path: PathLike, followlinks: bool = False):
        '''Copy file/directory on src_url to dst_url

        :param dst_url: Given destination path
        '''
        if not is_sftp(dst_path):
            raise OSError(f'dst_path is not sftp path: {dst_path}')
        for src_file_path, dst_file_path in _sftp_scan_pairs(
                self.path_with_protocol, dst_path):
            self.from_path(os.path.dirname(dst_file_path)).mkdir(
                parents=True, exist_ok=True)
            self.from_path(src_file_path).copy(
                dst_file_path, followlinks=followlinks)

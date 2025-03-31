import atexit
import fcntl
import hashlib
import io
import os
import random
import shlex
import socket
import subprocess  # nosec B404
from functools import cached_property
from logging import getLogger as get_logger
from stat import S_ISDIR, S_ISLNK, S_ISREG
from typing import IO, BinaryIO, Callable, Iterator, List, Optional, Tuple, Type, Union
from urllib.parse import urlsplit, urlunsplit

import paramiko

from megfile.config import SFTP_HOST_KEY_POLICY, SFTP_MAX_RETRY_TIMES
from megfile.errors import SameFileError, _create_missing_ok_generator, patch_method
from megfile.interfaces import ContextIterator, FileEntry, PathLike, StatResult
from megfile.lib.compare import is_same_file
from megfile.lib.compat import fspath
from megfile.lib.glob import FSFunc, iglob
from megfile.pathlike import URIPath
from megfile.smart_path import SmartPath
from megfile.utils import calculate_md5, thread_local

_logger = get_logger(__name__)

__all__ = [
    "SftpPath",
    "is_sftp",
]

SFTP_USERNAME = "SFTP_USERNAME"
SFTP_PASSWORD = "SFTP_PASSWORD"  # nosec B105
SFTP_PRIVATE_KEY_PATH = "SFTP_PRIVATE_KEY_PATH"
SFTP_PRIVATE_KEY_TYPE = "SFTP_PRIVATE_KEY_TYPE"
SFTP_PRIVATE_KEY_PASSWORD = "SFTP_PRIVATE_KEY_PASSWORD"  # nosec B105
SFTP_MAX_UNAUTH_CONN = "SFTP_MAX_UNAUTH_CONN"
MAX_RETRIES = SFTP_MAX_RETRY_TIMES
DEFAULT_SSH_CONNECT_TIMEOUT = 5
DEFAULT_SSH_KEEPALIVE_INTERVAL = 15


def _make_stat(stat: paramiko.SFTPAttributes) -> StatResult:
    return StatResult(
        size=stat.st_size or 0,
        mtime=stat.st_mtime or 0.0,
        isdir=S_ISDIR(stat.st_mode) if stat.st_mode is not None else False,
        islnk=S_ISLNK(stat.st_mode) if stat.st_mode is not None else False,
        extra=stat,
    )


def get_private_key():
    key_with_types = {
        "DSA": paramiko.DSSKey,
        "RSA": paramiko.RSAKey,
        "ECDSA": paramiko.ECDSAKey,
        "ED25519": paramiko.Ed25519Key,
    }
    key_type = os.getenv(SFTP_PRIVATE_KEY_TYPE, "RSA").upper()
    if os.getenv(SFTP_PRIVATE_KEY_PATH):
        private_key_path = os.getenv(SFTP_PRIVATE_KEY_PATH)
        if not os.path.exists(private_key_path):
            raise FileNotFoundError(
                f"Private key file not exist: '{SFTP_PRIVATE_KEY_PATH}'"
            )
        return key_with_types[key_type].from_private_key_file(
            private_key_path, password=os.getenv(SFTP_PRIVATE_KEY_PASSWORD)
        )
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


def sftp_should_retry(error: Exception) -> bool:
    if type(error) is EOFError:
        return False
    elif isinstance(
        error, (paramiko.ssh_exception.SSHException, ConnectionError, socket.timeout)
    ):
        return True
    elif isinstance(error, OSError):
        for err_msg in ["Socket is closed", "Cannot assign requested address"]:
            if err_msg in str(error):
                return True
    return False


def _patch_sftp_client_request(
    client: paramiko.SFTPClient,
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    default_policy: Type[paramiko.MissingHostKeyPolicy] = paramiko.RejectPolicy,
):
    def retry_callback(error, *args, **kwargs):
        client.close()
        ssh_client = get_ssh_client(hostname, port, username, password, default_policy)
        ssh_client.close()
        atexit.unregister(ssh_client.close)
        ssh_key = f"ssh_client:{hostname},{port},{username},{password},{default_policy}"
        if thread_local.get(ssh_key):
            del thread_local[ssh_key]
        sftp_key = (
            f"sftp_client:{hostname},{port},{username},{password},{default_policy}"
        )
        if thread_local.get(sftp_key):
            del thread_local[sftp_key]

        new_sftp_client = get_sftp_client(
            hostname=hostname,
            port=port,
            username=username,
            password=password,
            default_policy=default_policy,
        )
        client.sock = new_sftp_client.sock

    client._request = patch_method(  # pyre-ignore[16]
        client._request,  # pytype: disable=attribute-error
        max_retries=MAX_RETRIES,
        should_retry=sftp_should_retry,
        retry_callback=retry_callback,
    )
    return client


def _get_sftp_client(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    default_policy: Type[paramiko.MissingHostKeyPolicy] = paramiko.RejectPolicy,
) -> paramiko.SFTPClient:
    """Get sftp client

    :returns: sftp client
    """
    session = get_ssh_session(
        hostname=hostname,
        port=port,
        username=username,
        password=password,
        default_policy=default_policy,
    )
    session.invoke_subsystem("sftp")
    sftp_client = paramiko.SFTPClient(session)
    _patch_sftp_client_request(
        sftp_client, hostname, port, username, password, default_policy
    )
    return sftp_client


def get_sftp_client(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    default_policy: Type[paramiko.MissingHostKeyPolicy] = paramiko.RejectPolicy,
) -> paramiko.SFTPClient:
    """Get sftp client

    :returns: sftp client
    """
    return thread_local(
        f"sftp_client:{hostname},{port},{username},{password},{default_policy}",
        _get_sftp_client,
        hostname,
        port,
        username,
        password,
        default_policy,
    )


def _get_ssh_client(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    default_policy: Type[paramiko.MissingHostKeyPolicy] = paramiko.RejectPolicy,
) -> paramiko.SSHClient:
    hostname, port, username, password, private_key = provide_connect_info(
        hostname=hostname, port=port, username=username, password=password
    )

    policies = {
        "auto": paramiko.AutoAddPolicy,
        "reject": paramiko.RejectPolicy,
        "warning": paramiko.WarningPolicy,
    }
    policy = policies.get(SFTP_HOST_KEY_POLICY, default_policy)()  # pyre-ignore[29]

    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.set_missing_host_key_policy(policy)
    max_unauth_connections = int(os.getenv(SFTP_MAX_UNAUTH_CONN, 10))
    try:
        fd = os.open(
            os.path.join(
                "/tmp",  # nosec B108
                f"megfile-sftp-{hostname}-{random.randint(1, max_unauth_connections)}",  # nosec B311
            ),
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        )
    except Exception:  # pragma: no cover
        _logger.warning(
            "Can't create file lock in '/tmp', "
            "please control the SFTP concurrency count by yourself."
        )
        fd = None
    if fd:
        fcntl.flock(fd, fcntl.LOCK_EX)
    ssh_client.connect(
        hostname=hostname,
        port=port,
        username=username,
        password=password,
        pkey=private_key,
        timeout=DEFAULT_SSH_CONNECT_TIMEOUT,
        auth_timeout=DEFAULT_SSH_CONNECT_TIMEOUT,
        banner_timeout=DEFAULT_SSH_CONNECT_TIMEOUT,
    )
    if fd:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
    atexit.register(ssh_client.close)
    return ssh_client


def get_ssh_client(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    default_policy: Type[paramiko.MissingHostKeyPolicy] = paramiko.RejectPolicy,
) -> paramiko.SSHClient:
    return thread_local(
        f"ssh_client:{hostname},{port},{username},{password},{default_policy}",
        _get_ssh_client,
        hostname,
        port,
        username,
        password,
        default_policy,
    )


def get_ssh_session(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    default_policy: Type[paramiko.MissingHostKeyPolicy] = paramiko.RejectPolicy,
) -> paramiko.Channel:
    def retry_callback(error, *args, **kwargs):
        ssh_client = get_ssh_client(hostname, port, username, password, default_policy)
        ssh_client.close()
        atexit.unregister(ssh_client.close)
        ssh_key = f"ssh_client:{hostname},{port},{username},{password},{default_policy}"
        if thread_local.get(ssh_key):
            del thread_local[ssh_key]
        sftp_key = (
            f"sftp_client:{hostname},{port},{username},{password},{default_policy}"
        )
        if thread_local.get(sftp_key):
            del thread_local[sftp_key]

    return patch_method(
        _open_session,
        max_retries=MAX_RETRIES,
        should_retry=sftp_should_retry,
        retry_callback=retry_callback,
    )(hostname, port, username, password, default_policy)


def _open_session(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    default_policy: Type[paramiko.MissingHostKeyPolicy] = paramiko.RejectPolicy,
) -> paramiko.Channel:
    ssh_client = get_ssh_client(hostname, port, username, password, default_policy)
    transport = ssh_client.get_transport()
    if not transport:
        raise paramiko.SSHException("Get transport error")
    transport.set_keepalive(DEFAULT_SSH_KEEPALIVE_INTERVAL)
    session = transport.open_session(timeout=DEFAULT_SSH_CONNECT_TIMEOUT)
    if not session:
        raise paramiko.SSHException("Create session error")
    session.settimeout(DEFAULT_SSH_CONNECT_TIMEOUT)
    return session


def is_sftp(path: PathLike) -> bool:
    """Test if a path is sftp path

    :param path: Path to be tested
    :returns: True of a path is sftp path, else False
    """
    path = fspath(path)
    parts = urlsplit(path)
    return parts.scheme == "sftp"


def _sftp_scan_pairs(
    src_url: PathLike, dst_url: PathLike
) -> Iterator[Tuple[PathLike, PathLike]]:
    for src_file_path in SftpPath(src_url).scan():
        content_path = src_file_path[len(fspath(src_url)) :]
        if len(content_path) > 0:
            dst_file_path = SftpPath(dst_url).joinpath(content_path).path_with_protocol
        else:
            dst_file_path = dst_url
        yield src_file_path, dst_file_path


@SmartPath.register
class SftpPath(URIPath):
    """sftp protocol

    uri format:
        - absolute path
            - sftp://[username[:password]@]hostname[:port]//file_path
        - relative path
            - sftp://[username[:password]@]hostname[:port]/file_path
    """

    protocol = "sftp"
    default_policy = paramiko.RejectPolicy

    def __init__(self, path: "PathLike", *other_paths: "PathLike"):
        super().__init__(path, *other_paths)
        parts = urlsplit(self.path)
        self._urlsplit_parts = parts
        self._real_path = parts.path
        if parts.path.startswith("//"):
            self._root_dir = "/"
        else:
            self._root_dir = self._client.normalize(".")
        self._real_path = os.path.join(self._root_dir, parts.path.lstrip("/"))

    @cached_property
    def parts(self) -> Tuple[str, ...]:
        """A tuple giving access to the path’s various components"""
        if self._urlsplit_parts.path.startswith("//"):
            new_parts = self._urlsplit_parts._replace(path="//")
        else:
            new_parts = self._urlsplit_parts._replace(path="/")
        parts = [urlunsplit(new_parts)]
        path = self._urlsplit_parts.path.lstrip("/")
        if path != "":
            parts.extend(path.split("/"))
        return tuple(parts)  # pyre-ignore[7]

    @property
    def _client(self):
        return get_sftp_client(
            hostname=self._urlsplit_parts.hostname,
            port=self._urlsplit_parts.port,
            username=self._urlsplit_parts.username,
            password=self._urlsplit_parts.password,
            default_policy=self.default_policy,
        )

    def _generate_path_object(self, sftp_local_path: str, resolve: bool = False):
        if resolve or self._root_dir == "/":
            sftp_local_path = f"//{sftp_local_path.lstrip('/')}"
        else:
            sftp_local_path = os.path.relpath(sftp_local_path, start=self._root_dir)
            if sftp_local_path == ".":
                sftp_local_path = "/"
        new_parts = self._urlsplit_parts._replace(path=sftp_local_path)
        return self.from_path(urlunsplit(new_parts))  # pyre-ignore[6]

    def exists(self, followlinks: bool = False) -> bool:
        """
        Test if the path exists

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path exists, else False

        """
        try:
            if followlinks:
                self._client.stat(self._real_path)
            else:
                self._client.lstat(self._real_path)
            return True
        except FileNotFoundError:
            return False

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

        The result excludes the size of directory itself. In other words,
        return 0 Byte on an empty directory path.

        :returns: File size

        """
        return self.stat(follow_symlinks=follow_symlinks).size

    def glob(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> List["SftpPath"]:
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
            self.iglob(pattern=pattern, recursive=recursive, missing_ok=missing_ok)
        )

    def glob_stat(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> Iterator[FileEntry]:
        """Return a list contains tuples of path and file stat,
        in ascending alphabetical order, in which path matches glob pattern

        1. If doesn't match any path, return empty list
           Notice:  ``glob.glob`` in standard library returns ['a/'] instead of
           empty list when pathname is like `a/**`, recursive is True and
           directory 'a' doesn't exist. sftp_glob behaves like ``glob.glob`` in
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
        for path_obj in self.iglob(
            pattern=pattern, recursive=recursive, missing_ok=missing_ok
        ):
            yield FileEntry(path_obj.name, path_obj.path, path_obj.lstat())

    def iglob(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> Iterator["SftpPath"]:
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
        glob_path = self.path_with_protocol
        if pattern:
            glob_path = self.joinpath(pattern).path_with_protocol

        def _scandir(dirname: str) -> Iterator[Tuple[str, bool]]:
            result = []
            for entry in self.from_path(dirname).scandir():
                result.append((entry.name, entry.is_dir()))
            for name, is_dir in sorted(result):
                yield name, is_dir

        def _exist(path: PathLike, followlinks: bool = False):
            return self.from_path(path).exists(followlinks=followlinks)

        def _is_dir(path: PathLike, followlinks: bool = False):
            return self.from_path(path).is_dir(followlinks=followlinks)

        fs = FSFunc(_exist, _is_dir, _scandir)
        for real_path in _create_missing_ok_generator(
            iglob(fspath(glob_path), recursive=recursive, fs=fs),
            missing_ok,
            FileNotFoundError("No match any file: %r" % glob_path),
        ):
            yield self.from_path(real_path)

    def is_dir(self, followlinks: bool = False) -> bool:
        """
        Test if a path is directory

        .. note::

            The difference between this function and ``os.path.isdir`` is that
            this function regard symlink as file

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path is a directory, else False

        """
        try:
            stat = self.stat(follow_symlinks=followlinks)
            if S_ISDIR(stat.st_mode):
                return True
        except FileNotFoundError:
            pass
        return False

    def is_file(self, followlinks: bool = False) -> bool:
        """
        Test if a path is file

        .. note::

            The difference between this function and ``os.path.isfile`` is that
            this function regard symlink as file

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path is a file, else False

        """
        try:
            stat = self.stat(follow_symlinks=followlinks)
            if S_ISREG(stat.st_mode):
                return True
        except FileNotFoundError:
            pass
        return False

    def listdir(self) -> List[str]:
        """
        Get all contents of given sftp path.
        The result is in ascending alphabetical order.

        :returns: All contents have in the path in ascending alphabetical order
        """
        with self.scandir() as entries:
            return sorted([entry.name for entry in entries])

    def iterdir(self) -> Iterator["SftpPath"]:
        """
        Get all contents of given sftp path. The order of result is in arbitrary order.

        :returns: All contents have in the path.
        """
        with self.scandir() as entries:
            for entry in entries:
                yield self.joinpath(entry.name)

    def load(self) -> BinaryIO:
        """Read all content on specified path and write into memory

        User should close the BinaryIO manually

        :returns: Binary stream
        """
        with self.open(mode="rb") as f:
            data = f.read()
        return io.BytesIO(data)

    def mkdir(self, mode=0o777, parents: bool = False, exist_ok: bool = False):
        """
        make a directory on sftp, including parent directory.
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

        if parents:
            parent_path_objects = []
            for parent_path_object in self.parents:
                if parent_path_object.exists():
                    break
                else:
                    parent_path_objects.append(parent_path_object)
            for parent_path_object in parent_path_objects[::-1]:
                parent_path_object.mkdir(mode=mode, parents=False, exist_ok=True)
        try:
            self._client.mkdir(path=self._real_path, mode=mode)
        except OSError:
            # catch OSError when mkdir concurrently
            if not self.exists():
                raise

    def realpath(self) -> str:
        """Return the real path of given path

        :returns: Real path of given path
        """
        return self.resolve().path_with_protocol

    def _is_same_backend(self, other: "SftpPath") -> bool:
        return (
            self._urlsplit_parts.hostname == other._urlsplit_parts.hostname
            and self._urlsplit_parts.username == other._urlsplit_parts.username
            and self._urlsplit_parts.password == other._urlsplit_parts.password
            and self._urlsplit_parts.port == other._urlsplit_parts.port
        )

    def _is_same_protocol(self, path):
        return is_sftp(path)

    def rename(self, dst_path: PathLike, overwrite: bool = True) -> "SftpPath":
        """
        rename file on sftp

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        if not self._is_same_protocol(dst_path):
            raise OSError("Not a %s path: %r" % (self.protocol, dst_path))

        dst_path = self.from_path(str(dst_path).rstrip("/"))

        src_stat = self.stat()

        if self._is_same_backend(dst_path):
            if overwrite:
                dst_path.remove(missing_ok=True)
                self._client.rename(self._real_path, dst_path._real_path)
            else:
                self.sync(dst_path, overwrite=overwrite)
                self.remove(missing_ok=True)
        else:
            if self.is_dir():
                for file_entry in self.scandir():
                    self.from_path(file_entry.path).rename(
                        dst_path.joinpath(file_entry.name)
                    )
                self._client.rmdir(self._real_path)
            else:
                if overwrite or not dst_path.exists():
                    with self.open("rb") as fsrc:
                        with dst_path.open("wb") as fdst:
                            length = 16 * 1024
                            while True:
                                buf = fsrc.read(length)
                                if not buf:
                                    break
                                fdst.write(buf)
                self.unlink()

        dst_path.utime(src_stat.st_atime, src_stat.st_mtime)
        dst_path.chmod(src_stat.st_mode)
        return dst_path

    def replace(self, dst_path: PathLike, overwrite: bool = True) -> "SftpPath":
        """
        move file on sftp

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        return self.rename(dst_path=dst_path, overwrite=overwrite)

    def remove(self, missing_ok: bool = False) -> None:
        """
        Remove the file or directory on sftp

        :param missing_ok: if False and target file/directory not exists,
            raise FileNotFoundError
        """
        if missing_ok and not self.exists():
            return
        if self.is_dir():
            for file_entry in self.scandir():
                self.from_path(file_entry.path).remove(missing_ok=missing_ok)
            self._client.rmdir(self._real_path)
        else:
            self._client.unlink(self._real_path)

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

        def create_generator() -> Iterator[FileEntry]:
            try:
                stat = self.stat(follow_symlinks=followlinks)
            except FileNotFoundError:
                return
            if S_ISREG(stat.st_mode):
                yield FileEntry(
                    self.name,
                    self.path_with_protocol,
                    self.stat(follow_symlinks=followlinks),
                )
                return

            for name in self.listdir():
                current_path = self.joinpath(name)
                if current_path.is_dir():
                    yield from current_path.scan_stat(
                        missing_ok=missing_ok, followlinks=followlinks
                    )
                else:
                    yield FileEntry(
                        current_path.name,
                        current_path.path_with_protocol,
                        current_path.stat(follow_symlinks=followlinks),
                    )

        return _create_missing_ok_generator(
            create_generator(),
            missing_ok,
            FileNotFoundError("No match any file in: %r" % self.path_with_protocol),
        )

    def scandir(self) -> ContextIterator:
        """
        Get all content of given file path.

        :returns: An iterator contains all contents have prefix path
        """
        real_path = self._real_path
        stat = self.stat(follow_symlinks=False)
        if stat.is_symlink():
            real_path = self.readlink()._real_path
        elif not stat.is_dir():
            raise NotADirectoryError(f"Not a directory: '{self.path_with_protocol}'")

        def create_generator():
            for name in self._client.listdir(real_path):
                current_path = self.joinpath(name)
                yield FileEntry(
                    current_path.name,
                    current_path.path_with_protocol,
                    current_path.lstat(),
                )

        return ContextIterator(create_generator())

    def stat(self, follow_symlinks=True) -> StatResult:
        """
        Get StatResult of file on sftp, including file size and mtime,
        referring to fs_getsize and fs_getmtime

        :returns: StatResult
        """
        if follow_symlinks:
            result = _make_stat(self._client.stat(self._real_path))
        else:
            result = _make_stat(self._client.lstat(self._real_path))
        return result

    def unlink(self, missing_ok: bool = False) -> None:
        """
        Remove the file on sftp

        :param missing_ok: if False and target file not exists, raise FileNotFoundError
        """
        if missing_ok and not self.exists():
            return
        self._client.unlink(self._real_path)

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

            yield self._generate_path_object(root).path_with_protocol, dirs, files

            stack.extend(
                (os.path.join(root, directory) for directory in reversed(dirs))
            )

    def resolve(self, strict=False) -> "SftpPath":
        """Equal to sftp_realpath

        :param strict: Ignore this parameter, just for compatibility
        :return: Return the canonical path of the specified filename,
            eliminating any symbolic links encountered in the path.
        :rtype: SftpPath
        """
        path = self._client.normalize(self._real_path)
        return self._generate_path_object(path, resolve=True)

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
        with self.open("rb") as src:
            md5 = calculate_md5(src)
        return md5

    def symlink(self, dst_path: PathLike) -> None:
        """
        Create a symbolic link pointing to src_path named dst_path.

        :param dst_path: Destination path
        """
        dst_path = self.from_path(dst_path)
        if dst_path.exists(followlinks=False):
            raise FileExistsError(f"File exists: '{dst_path.path_with_protocol}'")
        return self._client.symlink(self._real_path, dst_path._real_path)

    def readlink(self) -> "SftpPath":
        """
        Return a SftpPath instance representing the path to
        which the symbolic link points.
        """
        if not self.is_symlink():
            raise OSError("Not a symlink: %s" % self.path_with_protocol)
        path = self._client.readlink(self._real_path)
        if not path:
            raise OSError("Not a symlink: %s" % self.path_with_protocol)
        if not path.startswith("/"):
            return self.parent.joinpath(path)
        return self._generate_path_object(path)

    def is_symlink(self) -> bool:
        """Test whether a path is a symbolic link

        :return: If path is a symbolic link return True, else False
        :rtype: bool
        """
        return self.lstat().is_symlink()

    def cwd(self) -> "SftpPath":
        """Return current working directory

        returns: Current working directory
        """
        return self._generate_path_object(self._client.normalize("."))

    def save(self, file_object: BinaryIO):
        """Write the opened binary stream to path
        If parent directory of path doesn't exist, it will be created.

        :param file_object: stream to be read
        """
        with self.open(mode="wb") as output:
            output.write(file_object.read())

    def open(
        self,
        mode: str = "r",
        *,
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
        if "w" in mode or "x" in mode or "a" in mode:
            if self.is_dir():
                raise IsADirectoryError("Is a directory: %r" % self.path_with_protocol)
            self.parent.mkdir(parents=True, exist_ok=True)
        elif not self.exists():
            raise FileNotFoundError("No such file: %r" % self.path_with_protocol)
        fileobj = self._client.open(self._real_path, mode, bufsize=buffering)
        fileobj.name = self.path
        if "r" in mode and "b" not in mode:
            return io.TextIOWrapper(
                fileobj, encoding=encoding, errors=errors
            )  # pytype: disable=wrong-arg-types
        return fileobj  # pytype: disable=bad-return-type

    def chmod(self, mode: int, *, follow_symlinks: bool = True):
        """
        Change the file mode and permissions, like os.chmod().

        :param mode: the file mode you want to change
        :param followlinks: Ignore this parameter, just for compatibility
        """
        return self._client.chmod(path=self._real_path, mode=mode)

    def absolute(self) -> "SftpPath":
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object
        """
        return self.resolve()

    def rmdir(self):
        """
        Remove this directory. The directory must be empty.
        """
        if len(self.listdir()) > 0:
            raise OSError(f"Directory not empty: '{self.path_with_protocol}'")
        return self._client.rmdir(self._real_path)

    def _exec_command(
        self,
        command: List[str],
        bufsize: int = -1,
        timeout: Optional[int] = None,
        environment: Optional[dict] = None,
    ) -> subprocess.CompletedProcess:
        with get_ssh_session(
            hostname=self._urlsplit_parts.hostname,
            port=self._urlsplit_parts.port,
            username=self._urlsplit_parts.username,
            password=self._urlsplit_parts.password,
            default_policy=self.default_policy,
        ) as chan:
            chan.settimeout(timeout)
            if environment:
                chan.update_environment(environment)
            chan.exec_command(" ".join([shlex.quote(arg) for arg in command]))  # nosec B601
            stdout = (
                chan.makefile("r", bufsize).read().decode(errors="backslashreplace")
            )
            stderr = (
                chan.makefile_stderr("r", bufsize)
                .read()
                .decode(errors="backslashreplace")
            )
            returncode = chan.recv_exit_status()
        return subprocess.CompletedProcess(
            args=command, returncode=returncode, stdout=stdout, stderr=stderr
        )

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
        if followlinks and self.is_symlink():
            return self.readlink().copy(dst_path=dst_path, callback=callback)

        if not self._is_same_protocol(dst_path):
            raise OSError("Not a %s path: %r" % (self.protocol, dst_path))
        if str(dst_path).endswith("/"):
            raise IsADirectoryError("Is a directory: %r" % dst_path)

        if self.is_dir():
            raise IsADirectoryError("Is a directory: %r" % self.path_with_protocol)

        if not overwrite and self.from_path(dst_path).exists():
            return

        self.from_path(os.path.dirname(fspath(dst_path))).makedirs(exist_ok=True)
        dst_path = self.from_path(dst_path)
        if self._is_same_backend(dst_path):
            if self._real_path == dst_path._real_path:
                raise SameFileError(
                    f"'{self.path}' and '{dst_path.path}' are the same file"
                )
            exec_result = self._exec_command(
                ["cp", self._real_path, dst_path._real_path]
            )
            if exec_result.returncode != 0:
                _logger.error(exec_result.stderr)
                raise OSError(f"Copy file error, returncode: {exec_result.returncode}")
            if callback:
                callback(self.stat(follow_symlinks=followlinks).size)
        else:
            with self.open("rb") as fsrc:
                with dst_path.open("wb") as fdst:
                    length = 16 * 1024
                    while True:
                        buf = fsrc.read(length)
                        if not buf:
                            break
                        fdst.write(buf)
                        if callback:
                            callback(len(buf))

        src_stat = self.stat()
        dst_path.utime(src_stat.st_atime, src_stat.st_mtime)
        dst_path._client.chmod(dst_path._real_path, src_stat.st_mode)

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
        if not self._is_same_protocol(dst_path):
            raise OSError("Not a %s path: %r" % (self.protocol, dst_path))

        for src_file_path, dst_file_path in _sftp_scan_pairs(
            self.path_with_protocol, dst_path
        ):
            dst_path = self.from_path(dst_file_path)
            src_path = self.from_path(src_file_path)

            if force:
                pass
            elif not overwrite and dst_path.exists():
                continue
            elif dst_path.exists() and is_same_file(
                src_path.stat(), dst_path.stat(), "copy"
            ):
                continue

            src_path.copy(
                dst_file_path,
                followlinks=followlinks,
                overwrite=True,
            )

    def utime(self, atime: Union[float, int], mtime: Union[float, int]) -> None:
        """
        Set the access and modified times of the file specified by path.

        :param atime: The access time to be set.
        :type atime: Union[float, int]
        :param mtime: The modification time to be set.
        :type mtime: Union[float, int]
        :return: None
        """
        return self._client.utime(self._real_path, (atime, mtime))

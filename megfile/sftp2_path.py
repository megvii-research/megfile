import getpass
import hashlib
import io
import os
import shlex
import socket
import subprocess
from functools import cached_property
from logging import getLogger as get_logger
from stat import S_ISDIR, S_ISLNK, S_ISREG
from typing import IO, BinaryIO, Callable, Iterator, List, Optional, Tuple, Union
from urllib.parse import urlsplit, urlunsplit

import ssh2.session  # type: ignore
import ssh2.sftp  # type: ignore
from ssh2.exceptions import SFTPProtocolError  # type: ignore
from ssh2.sftp_handle import SFTPAttributes  # type: ignore

from megfile.config import SFTP_MAX_RETRY_TIMES
from megfile.errors import SameFileError, _create_missing_ok_generator
from megfile.interfaces import ContextIterator, FileEntry, PathLike, StatResult
from megfile.lib.compare import is_same_file
from megfile.lib.compat import fspath
from megfile.lib.glob import FSFunc, iglob
from megfile.pathlike import URIPath
from megfile.smart_path import SmartPath
from megfile.utils import calculate_md5, copyfileobj, thread_local

_logger = get_logger(__name__)

__all__ = [
    "Sftp2Path",
    "is_sftp2",
]

SFTP_USERNAME = "SFTP_USERNAME"
SFTP_PASSWORD = "SFTP_PASSWORD"
SFTP_PRIVATE_KEY_PATH = "SFTP_PRIVATE_KEY_PATH"
SFTP_PRIVATE_KEY_TYPE = "SFTP_PRIVATE_KEY_TYPE"
SFTP_PRIVATE_KEY_PASSWORD = "SFTP_PRIVATE_KEY_PASSWORD"
SFTP_MAX_UNAUTH_CONN = "SFTP_MAX_UNAUTH_CONN"
MAX_RETRIES = SFTP_MAX_RETRY_TIMES
DEFAULT_SSH_CONNECT_TIMEOUT = 5
DEFAULT_SSH_KEEPALIVE_INTERVAL = 15

# SFTP2-specific buffer sizes and chunk sizes
SFTP2_BUFFER_SIZE = 1 * 2**20  # 1MB buffer for file operations


def _make_stat(stat) -> StatResult:
    """Convert ssh2.sftp stats to StatResult"""
    # ssh2-python uses different attribute names than paramiko
    size = getattr(stat, "filesize", 0) if stat else 0
    mtime = getattr(stat, "mtime", 0.0) if stat else 0.0
    # ssh2-python uses 'permissions' instead of 'st_mode'
    mode = getattr(stat, "permissions", 0) if stat else 0

    return StatResult(
        size=size,
        mtime=mtime,
        isdir=S_ISDIR(mode),
        islnk=S_ISLNK(mode),
        extra=stat,
    )


def get_private_key():
    """Get private key for SSH authentication"""
    private_key_path = os.getenv(SFTP_PRIVATE_KEY_PATH)
    if private_key_path:
        if not os.path.exists(private_key_path):
            raise FileNotFoundError(f"Private key file not exist: '{private_key_path}'")
        private_key_password = os.getenv(SFTP_PRIVATE_KEY_PASSWORD)
        if private_key_password:
            return private_key_path, private_key_password
        return private_key_path, ""
    return None


def provide_connect_info(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
):
    """Provide connection information"""
    if not port:
        port = 22
    if not username:
        username = os.getenv(SFTP_USERNAME)
        if not username:
            # 如果没有指定用户名，使用当前系统用户名
            username = getpass.getuser()
    if not password:
        password = os.getenv(SFTP_PASSWORD)
    private_key = get_private_key()
    return hostname, port, username, password, private_key


def sftp2_should_retry(error: Exception) -> bool:
    """Determine if an error should trigger a retry"""
    if isinstance(error, (ConnectionError, socket.timeout)):
        return True
    elif isinstance(error, OSError):
        for err_msg in ["Socket is closed", "Cannot assign requested address"]:
            if err_msg in str(error):
                return True
    return False


def _get_ssh2_session(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> ssh2.session.Session:
    """Create SSH2 session"""
    hostname, port, username, password, private_key = provide_connect_info(
        hostname=hostname, port=port, username=username, password=password
    )

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(DEFAULT_SSH_CONNECT_TIMEOUT)
    sock.connect((hostname, port))

    session = ssh2.session.Session()
    session.handshake(sock)

    # 尝试多种认证方法
    authenticated = False

    # 1. 如果提供了私钥，优先使用私钥认证
    if private_key and username:
        try:
            # For ssh2-python, we need to handle key authentication differently
            key_path, passphrase = private_key
            result = session.userauth_publickey_fromfile(
                username,
                key_path,
                passphrase=passphrase,
            )
            if result == 0:  # 0 indicates success in ssh2-python
                authenticated = True
                _logger.debug(f"Authentication successed with key: {key_path}")
        except Exception as e:
            _logger.debug(f"Private key authentication failed: {type(e).__name__}: {e}")

    # 2. 如果提供了密码，尝试密码认证
    if not authenticated and password and username:
        try:
            result = session.userauth_password(username, password)
            if result == 0:
                authenticated = True
                _logger.debug("Authentication successed with password")
        except Exception as e:
            _logger.debug(f"Password authentication failed: {type(e).__name__}: {e}")

    # 3. 尝试使用 SSH agent 认证
    if not authenticated and username:
        try:
            # ssh2-python 使用 agent_init() 和 agent_auth() 方法
            session.agent_init()
            session.agent_auth(username)
            authenticated = True
            _logger.debug("Successfully authenticated with SSH agent")
        except Exception as e:
            _logger.debug(f"SSH agent authentication failed: {type(e).__name__}: {e}")

    # 4. 尝试使用默认的公钥认证 (~/.ssh/id_rsa, ~/.ssh/id_dsa 等)
    if not authenticated and username:
        default_key_paths = [
            os.path.expanduser("~/.ssh/id_rsa"),
            os.path.expanduser("~/.ssh/id_dsa"),
            os.path.expanduser("~/.ssh/id_ecdsa"),
            os.path.expanduser("~/.ssh/id_ed25519"),
        ]

        for key_path in default_key_paths:
            if os.path.exists(key_path):
                try:
                    result = session.userauth_publickey_fromfile(
                        username,
                        key_path,  # 私钥文件路径
                    )

                    if result == 0:
                        authenticated = True
                        _logger.debug(
                            f"Successfully authenticated with key: {key_path}"
                        )
                        break
                except Exception as e:
                    _logger.debug(
                        f"Public key authentication with {key_path} failed: {e}"
                    )

    if not authenticated:
        sock.close()
        raise ValueError(
            f"Authentication failed for {username}@{hostname}. "
            "Please check your SSH configuration, SSH agent, or provide "
            "explicit credentials."
        )

    return session


def get_ssh2_session(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> ssh2.session.Session:
    """Get cached SSH2 session"""
    return thread_local(
        f"ssh2_session:{hostname},{port},{username},{password}",
        _get_ssh2_session,
        hostname,
        port,
        username,
        password,
    )


def _get_sftp2_client(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> ssh2.sftp.SFTP:
    """Get SFTP2 client"""
    session = get_ssh2_session(hostname, port, username, password)
    sftp = session.sftp_init()
    return sftp


def get_sftp2_client(
    hostname: str,
    port: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> ssh2.sftp.SFTP:
    """Get cached SFTP2 client"""
    return thread_local(
        f"sftp2_client:{hostname},{port},{username},{password}",
        _get_sftp2_client,
        hostname,
        port,
        username,
        password,
    )


def is_sftp2(path: PathLike) -> bool:
    """Test if a path is sftp2 path

    :param path: Path to be tested
    :returns: True of a path is sftp2 path, else False
    """
    path = fspath(path)
    parts = urlsplit(path)
    return parts.scheme == "sftp2"


def _sftp2_scan_pairs(
    src_url: PathLike, dst_url: PathLike
) -> Iterator[Tuple[PathLike, PathLike]]:
    for src_file_path in Sftp2Path(src_url).scan():
        content_path = src_file_path[len(fspath(src_url)) :]
        if len(content_path) > 0:
            dst_file_path = Sftp2Path(dst_url).joinpath(content_path).path_with_protocol
        else:
            dst_file_path = dst_url
        yield src_file_path, dst_file_path


class Sftp2RawFile(io.RawIOBase):
    """Raw SFTP file wrapper - implements only readinto for BufferedReader"""

    def __init__(self, sftp_handle, path: str, mode: str = "r"):
        self.sftp_handle = sftp_handle
        self.path = path
        self.mode = mode
        self.name = path
        self._closed = False

    def readable(self) -> bool:
        return "r" in self.mode

    def writable(self) -> bool:
        return "w" in self.mode or "a" in self.mode or "x" in self.mode

    def seekable(self) -> bool:
        return True

    @property
    def closed(self) -> bool:
        return self._closed

    def readinto(self, buffer) -> int:
        """Read into a pre-allocated buffer. Required by BufferedReader."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        # ssh2-python returns (bytes_read, data)
        bytes_read, chunk = self.sftp_handle.read(len(buffer))
        if bytes_read > 0:
            # Direct memory copy should be faster
            buffer[:bytes_read] = chunk
            return bytes_read
        return 0

    def read(self, size: int = -1) -> bytes:
        """Fallback read method - optimized for direct use"""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        if size <= 0:
            # For read-all, use readinto with BytesIO for consistency
            result = io.BytesIO()
            buffer = bytearray(SFTP2_BUFFER_SIZE)
            while True:
                n = self.readinto(buffer)
                if n == 0:
                    break
                result.write(buffer[:n])
            return result.getvalue()
        else:
            # For fixed size reads, use readinto
            buffer = bytearray(size)
            n = self.readinto(buffer)
            return bytes(buffer[:n])

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        _, bytes_written = self.sftp_handle.write(bytes(data))
        return bytes_written

    def close(self):
        if not self._closed:
            self.sftp_handle.close()
            self._closed = True

    def flush(self):
        """Flush the file. This is a no-op for SFTP files."""
        pass

    def tell(self) -> int:
        """Return current position. Uses SFTP handle tell methods."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        # Use SFTP handle's tell method
        if hasattr(self.sftp_handle, "tell64"):
            return self.sftp_handle.tell64()
        else:
            # If SFTP tell is not available or fails, raise error
            raise OSError("tell not supported for this SFTP implementation")

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seek to position. Uses SFTP handle seek methods."""
        if self._closed:
            raise ValueError("I/O operation on closed file")

        # Try to use SFTP handle's native seek functionality
        if hasattr(self.sftp_handle, "seek64"):
            # Calculate absolute position based on whence
            if whence == 0:  # SEEK_SET
                target_pos = offset
            elif whence == 1:  # SEEK_CUR
                current_pos = self.tell()
                target_pos = current_pos + offset
            elif whence == 2:  # SEEK_END
                # For SEEK_END, we need file size - not commonly supported
                raise OSError("SEEK_END not supported for SFTP files")
            else:
                raise OSError(f"invalid whence ({whence}, should be 0, 1, or 2)")

            if target_pos < 0:
                raise OSError("negative seek position")

            # Perform the seek
            self.sftp_handle.seek64(target_pos)
            return target_pos
        else:
            # Fallback: SFTP doesn't support seek
            raise OSError("seek not supported for this SFTP implementation")

    def fileno(self) -> int:
        """Return file descriptor. Not supported for SFTP."""
        # Return -1 to indicate no file descriptor (standard practice)
        return -1

    def isatty(self) -> bool:
        """Return whether this is a tty. Always False for SFTP files."""
        return False

    def truncate(self, size: Optional[int] = None) -> int:
        """Truncate file. Not supported for SFTP."""
        raise OSError("truncate not supported for SFTP files")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


@SmartPath.register
class Sftp2Path(URIPath):
    """sftp2 protocol

    uri format:
        - sftp2://[username[:password]@]hostname[:port]/file_path
    """

    protocol = "sftp2"

    def __init__(self, path: "PathLike", *other_paths: "PathLike"):
        super().__init__(path, *other_paths)
        parts = urlsplit(self.path)
        self._urlsplit_parts = parts
        self._remote_path = parts.path or "/"

    @cached_property
    def parts(self) -> Tuple[str, ...]:
        """A tuple giving access to the path's various components"""
        parts = [urlunsplit(self._urlsplit_parts._replace(path=""))]
        path = self._urlsplit_parts.path.lstrip("/")
        if path != "":
            parts.extend(path.split("/"))
        return tuple(parts)

    @property
    def _client(self):
        return get_sftp2_client(
            hostname=self._urlsplit_parts.hostname,
            port=self._urlsplit_parts.port,
            username=self._urlsplit_parts.username,
            password=self._urlsplit_parts.password,
        )

    @property
    def _session(self):
        """Get SSH session for executing server-side commands"""
        return get_ssh2_session(
            hostname=self._urlsplit_parts.hostname,
            port=self._urlsplit_parts.port,
            username=self._urlsplit_parts.username,
            password=self._urlsplit_parts.password,
        )

    def _exec_command(self, command: List[str]) -> subprocess.CompletedProcess:
        """Execute a command on the remote server via SSH

        Returns:
            subprocess.CompletedProcess object
        """
        session = self._session
        channel = session.open_session()

        # Execute the command
        channel.execute(shlex.join(command))

        # Read output
        stdout = io.BytesIO()
        stderr = io.BytesIO()

        while True:
            # Read stdout
            size, data = channel.read()
            if size > 0:
                stdout.write(data)

            # Read stderr
            size, data = channel.read_stderr()
            if size > 0:
                stderr.write(data)

            # Check if finished
            if channel.eof():
                break

        # Get exit status
        exit_code = channel.get_exit_status()
        channel.close()

        return subprocess.CompletedProcess(
            args=command,
            returncode=exit_code,
            stdout=stdout.getvalue().decode("utf-8", errors="replace"),
            stderr=stderr.getvalue().decode("utf-8", errors="replace"),
        )

    def _generate_path_object(self, sftp_local_path: str, resolve: bool = False):
        new_parts = self._urlsplit_parts._replace(path=sftp_local_path)
        return self.from_path(urlunsplit(new_parts))

    def exists(self, followlinks: bool = False) -> bool:
        """
        Test if the path exists

        :param followlinks: False if regard symlink as file, else True
        :returns: True if the path exists, else False
        """
        try:
            self.stat(follow_symlinks=followlinks)
            return True
        except FileNotFoundError:
            return False

    def getmtime(self, follow_symlinks: bool = False) -> float:
        """Get last-modified time of the file on the given path"""
        return self.stat(follow_symlinks=follow_symlinks).mtime

    def getsize(self, follow_symlinks: bool = False) -> int:
        """Get file size on the given file path (in bytes)"""
        return self.stat(follow_symlinks=follow_symlinks).size

    def glob(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> List["Sftp2Path"]:
        """Return path list in ascending alphabetical order"""
        return list(
            self.iglob(pattern=pattern, recursive=recursive, missing_ok=missing_ok)
        )

    def glob_stat(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> Iterator[FileEntry]:
        """Return a list contains tuples of path and file stat"""
        for path_obj in self.iglob(
            pattern=pattern, recursive=recursive, missing_ok=missing_ok
        ):
            yield FileEntry(path_obj.name, path_obj.path, path_obj.lstat())

    def iglob(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> Iterator["Sftp2Path"]:
        """Return path iterator in ascending alphabetical order"""
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
        for remote_path in _create_missing_ok_generator(
            iglob(fspath(glob_path), recursive=recursive, fs=fs),
            missing_ok,
            FileNotFoundError(f"No match any file: {glob_path!r}"),
        ):
            yield self.from_path(remote_path)

    def is_dir(self, followlinks: bool = False) -> bool:
        """Test if a path is directory"""
        try:
            stat = self.stat(follow_symlinks=followlinks)
            return stat.is_dir()
        except FileNotFoundError:
            return False

    def is_file(self, followlinks: bool = False) -> bool:
        """Test if a path is file"""
        try:
            stat = self.stat(follow_symlinks=followlinks)
            return (
                S_ISREG(stat.st_mode) if hasattr(stat, "st_mode") else not stat.is_dir()
            )
        except FileNotFoundError:
            return False

    def listdir(self) -> List[str]:
        """Get all contents of given sftp2 path"""
        with self.scandir() as entries:
            return sorted([entry.name for entry in entries])

    def iterdir(self) -> Iterator["Sftp2Path"]:
        """Get all contents of given sftp2 path"""
        with self.scandir() as entries:
            for entry in entries:
                yield self.joinpath(entry.name)

    def load(self) -> BinaryIO:
        """Read all content on specified path and write into memory"""
        with self.open(mode="rb") as f:
            data = f.read()
        return io.BytesIO(data)

    def mkdir(self, mode=0o777, parents: bool = False, exist_ok: bool = False):
        """Make a directory on sftp2"""
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
            self._client.mkdir(self._remote_path, mode)
        except OSError:
            if not self.exists():
                raise

    def realpath(self) -> str:
        """Return the real path of given path"""
        return self.resolve().path_with_protocol

    def _is_same_backend(self, other: "Sftp2Path") -> bool:
        return (
            self._urlsplit_parts.hostname == other._urlsplit_parts.hostname
            and self._urlsplit_parts.username == other._urlsplit_parts.username
            and self._urlsplit_parts.password == other._urlsplit_parts.password
            and self._urlsplit_parts.port == other._urlsplit_parts.port
        )

    def _is_same_protocol(self, path):
        return is_sftp2(path)

    def rename(self, dst_path: PathLike, overwrite: bool = True) -> "Sftp2Path":
        """Rename file on sftp2"""
        if not self._is_same_protocol(dst_path):
            raise OSError(f"Not a {self.protocol} path: {dst_path!r}")

        dst_path = self.from_path(str(dst_path).rstrip("/"))
        src_stat = self.stat()

        if self._is_same_backend(dst_path):
            if overwrite:
                dst_path.remove(missing_ok=True)
                self._client.rename(self._remote_path, dst_path._remote_path)
            else:
                self.sync(dst_path, overwrite=overwrite)
                self.remove(missing_ok=True)
        else:
            if self.is_dir():
                for file_entry in self.scandir():
                    self.from_path(file_entry.path).rename(
                        dst_path.joinpath(file_entry.name)
                    )
                self._client.rmdir(self._remote_path)
            else:
                if overwrite or not dst_path.exists():
                    with self.open("rb") as fsrc:
                        with dst_path.open("wb") as fdst:
                            copyfileobj(fsrc, fdst)
                self.unlink()

        dst_path.utime(src_stat.st_atime, src_stat.st_mtime)
        dst_path.chmod(src_stat.st_mode)
        return dst_path

    def replace(self, dst_path: PathLike, overwrite: bool = True) -> "Sftp2Path":
        """Move file on sftp2"""
        return self.rename(dst_path=dst_path, overwrite=overwrite)

    def remove(self, missing_ok: bool = False) -> None:
        """Remove the file or directory on sftp2"""
        if missing_ok and not self.exists():
            return
        if self.is_dir():
            for file_entry in self.scandir():
                self.from_path(file_entry.path).remove(missing_ok=missing_ok)
            self._client.rmdir(self._remote_path)
        else:
            self._client.unlink(self._remote_path)

    def scan(self, missing_ok: bool = True, followlinks: bool = False) -> Iterator[str]:
        """Iteratively traverse only files in given directory"""
        scan_stat_iter = self.scan_stat(missing_ok=missing_ok, followlinks=followlinks)
        for file_entry in scan_stat_iter:
            yield file_entry.path

    def scan_stat(
        self, missing_ok: bool = True, followlinks: bool = False
    ) -> Iterator[FileEntry]:
        """Iteratively traverse only files in given directory"""

        def create_generator() -> Iterator[FileEntry]:
            try:
                stat = self.stat(follow_symlinks=followlinks)
            except FileNotFoundError:
                return
            if not stat.is_dir():
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
            FileNotFoundError(f"No match any file in: {self.path_with_protocol!r}"),
        )

    def scandir(self) -> ContextIterator:
        """Get all content of given file path"""
        remote_path = self._remote_path
        stat_result = None
        try:
            stat_result = self.stat(follow_symlinks=False)
        except Exception:
            raise NotADirectoryError(f"Not a directory: '{self.path_with_protocol}'")

        if stat_result.is_symlink():
            remote_path = self.readlink()._remote_path
        elif not stat_result.is_dir():
            raise NotADirectoryError(f"Not a directory: '{self.path_with_protocol}'")

        def create_generator():
            # Use opendir and readdir from ssh2-python
            dir_handle = self._client.opendir(remote_path)
            try:
                # ssh2-python's readdir returns a generator
                # First call returns all entries, subsequent calls return empty
                entries_gen = dir_handle.readdir()
                entries = list(entries_gen) if entries_gen else []

                for name_len, name_bytes, stat_obj in entries:
                    name = name_bytes.decode("utf-8")
                    if name in (".", ".."):
                        continue

                    # Convert stat_obj to StatResult
                    stat_info = _make_stat(stat_obj)
                    yield FileEntry(
                        name,
                        self.joinpath(name).path_with_protocol,
                        stat_info,
                    )
            finally:
                dir_handle.close()

        return ContextIterator(create_generator())

    def stat(self, follow_symlinks=True) -> StatResult:
        """Get StatResult of file on sftp2"""
        try:
            if follow_symlinks:
                stat = self._client.stat(self._remote_path)
            else:
                stat = self._client.lstat(self._remote_path)
            return _make_stat(stat)
        except SFTPProtocolError as e:  # pytype: disable=mro-error
            raise FileNotFoundError(
                f"No such file or directory: {self.path_with_protocol!r}"
            ) from e

    def lstat(self) -> StatResult:
        """Get StatResult without following symlinks"""
        return self.stat(follow_symlinks=False)

    def unlink(self, missing_ok: bool = False) -> None:
        """Remove the file on sftp2"""
        if missing_ok and not self.exists():
            return
        self._client.unlink(self._remote_path)

    def walk(
        self, followlinks: bool = False
    ) -> Iterator[Tuple[str, List[str], List[str]]]:
        """Generate the file names in a directory tree by walking the tree top-down"""
        if not self.exists(followlinks=followlinks):
            return

        if self.is_file(followlinks=followlinks):
            return

        stack = [self._remote_path]
        while stack:
            root = stack.pop()
            dirs, files = [], []

            # Use scandir instead of readdir for consistency
            root_path = self._generate_path_object(root)
            with root_path.scandir() as entries:
                for entry in entries:
                    if entry.is_dir():
                        dirs.append(entry.name)
                    elif entry.is_file():
                        files.append(entry.name)

            dirs = sorted(dirs)
            files = sorted(files)

            yield self._generate_path_object(root).path_with_protocol, dirs, files

            stack.extend(
                (os.path.join(root, directory) for directory in reversed(dirs))
            )

    def resolve(self, strict=False) -> "Sftp2Path":
        """Return the canonical path"""
        path = self._client.realpath(self._remote_path)
        return self._generate_path_object(path)

    def md5(self, recalculate: bool = False, followlinks: bool = False):
        """Calculate the md5 value of the file"""
        if self.is_dir():
            hash_md5 = hashlib.md5()
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
        """Create a symbolic link pointing to src_path named dst_path"""
        dst_path = self.from_path(dst_path)
        if dst_path.exists(followlinks=False):
            raise FileExistsError(f"File exists: '{dst_path.path_with_protocol}'")
        return self._client.symlink(self._remote_path, dst_path._remote_path)

    def readlink(self) -> "Sftp2Path":
        """Return a Sftp2Path instance representing the path to which the
        symbolic link points"""
        if not self.exists():
            raise FileNotFoundError(
                f"No such file or directory: '{self.path_with_protocol}'"
            )
        if not self.is_symlink():
            raise OSError(f"Not a symlink: {self.path_with_protocol!r}")
        try:
            path = self._client.realpath(self._remote_path)
            if not path:
                raise OSError(f"Not a symlink: {self.path_with_protocol!r}")
            if not path.startswith("/"):
                return self.parent.joinpath(path)
            return self._generate_path_object(path)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"No such file or directory: '{self.path_with_protocol}'"
            )
        except Exception:
            raise OSError(f"Not a symlink: {self.path_with_protocol!r}")

    def is_symlink(self) -> bool:
        """Test whether a path is a symbolic link"""
        try:
            return self.lstat().is_symlink()
        except FileNotFoundError:
            return False

    def cwd(self) -> "Sftp2Path":
        """Return current working directory"""
        path = self._client.realpath(".")
        return self._generate_path_object(path)

    def save(self, file_object: BinaryIO):
        """Write the opened binary stream to path"""
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
        """Open a file on the path"""
        if "w" in mode or "x" in mode or "a" in mode:
            if self.is_dir():
                raise IsADirectoryError(f"Is a directory: {self.path_with_protocol!r}")
            self.parent.mkdir(parents=True, exist_ok=True)
        elif not self.exists():
            raise FileNotFoundError(f"No such file: {self.path_with_protocol!r}")

        # Convert mode for ssh2-python
        ssh2_mode = 0
        if "r" in mode:
            ssh2_mode |= ssh2.sftp.LIBSSH2_FXF_READ
        if "w" in mode:
            ssh2_mode |= (
                ssh2.sftp.LIBSSH2_FXF_WRITE
                | ssh2.sftp.LIBSSH2_FXF_CREAT
                | ssh2.sftp.LIBSSH2_FXF_TRUNC
            )
        if "a" in mode:
            ssh2_mode |= (
                ssh2.sftp.LIBSSH2_FXF_WRITE
                | ssh2.sftp.LIBSSH2_FXF_CREAT
                | ssh2.sftp.LIBSSH2_FXF_APPEND
            )

        sftp_handle = self._client.open(self._remote_path, ssh2_mode, 0o644)

        # Create raw file wrapper
        raw_file = Sftp2RawFile(sftp_handle, self.path, mode)

        if "r" in mode:
            if "b" in mode:
                # Binary read mode - use BufferedReader for optimal performance
                fileobj = io.BufferedReader(raw_file, buffer_size=SFTP2_BUFFER_SIZE)
            else:
                # Text read mode - wrap BufferedReader with TextIOWrapper
                buffered = io.BufferedReader(raw_file, buffer_size=SFTP2_BUFFER_SIZE)
                fileobj = io.TextIOWrapper(buffered, encoding=encoding, errors=errors)
        elif "w" in mode or "a" in mode:
            if "b" in mode:
                # Binary write mode - use BufferedWriter for optimal performance
                fileobj = io.BufferedWriter(raw_file, buffer_size=SFTP2_BUFFER_SIZE)
            else:
                # Text write mode - wrap BufferedWriter with TextIOWrapper
                buffered = io.BufferedWriter(raw_file, buffer_size=SFTP2_BUFFER_SIZE)
                fileobj = io.TextIOWrapper(buffered, encoding=encoding, errors=errors)
        else:
            raise ValueError(f"Invalid mode: {mode}")

        return fileobj

    def chmod(self, mode: int, *, follow_symlinks: bool = True):
        """Change the file mode and permissions"""
        stat = SFTPAttributes()
        stat.permissions = int(mode)
        return self._client.setstat(self._remote_path, stat)

    def absolute(self) -> "Sftp2Path":
        """Make the path absolute"""
        return self.resolve()

    def rmdir(self):
        """Remove this directory. The directory must be empty"""
        if len(self.listdir()) > 0:
            raise OSError(f"Directory not empty: '{self.path_with_protocol}'")
        return self._client.rmdir(self._remote_path)

    def copy(
        self,
        dst_path: PathLike,
        callback: Optional[Callable[[int], None]] = None,
        followlinks: bool = False,
        overwrite: bool = True,
    ):
        """Copy the file to the given destination path"""
        if followlinks and self.is_symlink():
            return self.readlink().copy(dst_path=dst_path, callback=callback)

        if not self._is_same_protocol(dst_path):
            raise OSError(f"Not a {self.protocol} path: {dst_path!r}")
        if str(dst_path).endswith("/"):
            raise IsADirectoryError(f"Is a directory: {dst_path!r}")

        if self.is_dir():
            raise IsADirectoryError(f"Is a directory: {self.path_with_protocol!r}")

        if not overwrite and self.from_path(dst_path).exists():
            return

        self.from_path(os.path.dirname(fspath(dst_path))).makedirs(exist_ok=True)
        dst_path = self.from_path(dst_path)

        if self._is_same_backend(dst_path):
            if self._remote_path == dst_path._remote_path:
                raise SameFileError(
                    f"'{self.path}' and '{dst_path.path}' are the same file"
                )
            # Same server - use server-side copy command for efficiency
            exec_result = self._exec_command(
                [
                    "cp",
                    self._remote_path,
                    dst_path._remote_path,
                ]
            )

            if exec_result.returncode != 0:
                _logger.error(exec_result.stderr)
                raise OSError(
                    f"Failed to copy file, returncode: {exec_result.returncode}, "
                    f"{exec_result.stderr}"
                )

            if callback:
                callback(self.stat(follow_symlinks=followlinks).size)

        else:
            # Fallback to traditional SFTP copy (download then upload)
            with self.open("rb") as fsrc:
                with dst_path.open("wb") as fdst:
                    copyfileobj(fsrc, fdst, callback)

        src_stat = self.stat()
        dst_path.utime(src_stat.st_atime, src_stat.st_mtime)
        dst_path.chmod(src_stat.st_mode)

    def sync(
        self,
        dst_path: PathLike,
        followlinks: bool = False,
        force: bool = False,
        overwrite: bool = True,
    ):
        """Copy file/directory on src_url to dst_url"""
        if not self._is_same_protocol(dst_path):
            raise OSError(f"Not a {self.protocol} path: {dst_path!r}")

        for src_file_path, dst_file_path in _sftp2_scan_pairs(
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
        """Set the access and modified times of the file"""
        stat = SFTPAttributes()
        stat.atime = int(atime)
        stat.mtime = int(mtime)
        self._client.setstat(self._remote_path, stat)

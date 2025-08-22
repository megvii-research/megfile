import getpass
import hashlib
import io
import os
import socket
from functools import cached_property
from logging import getLogger as get_logger
from stat import S_ISDIR, S_ISLNK, S_ISREG
from typing import IO, BinaryIO, Callable, Iterator, List, Optional, Tuple, Union
from urllib.parse import urlsplit, urlunsplit

import ssh2.session
import ssh2.sftp
import ssh2.utils

from megfile.config import SFTP_MAX_RETRY_TIMES
from megfile.errors import SameFileError, _create_missing_ok_generator
from megfile.interfaces import ContextIterator, FileEntry, PathLike, StatResult
from megfile.lib.compare import is_same_file
from megfile.lib.compat import fspath
from megfile.lib.glob import FSFunc, iglob
from megfile.pathlike import URIPath
from megfile.smart_path import SmartPath
from megfile.utils import calculate_md5, thread_local

_logger = get_logger(__name__)

__all__ = [
    "Sftp2Path",
    "is_sftp2",
]

SFTP2_USERNAME = "SFTP2_USERNAME"
SFTP2_PASSWORD = "SFTP2_PASSWORD"
SFTP2_PRIVATE_KEY_PATH = "SFTP2_PRIVATE_KEY_PATH"
SFTP2_PRIVATE_KEY_TYPE = "SFTP2_PRIVATE_KEY_TYPE"
SFTP2_PRIVATE_KEY_PASSWORD = "SFTP2_PRIVATE_KEY_PASSWORD"
SFTP2_MAX_UNAUTH_CONN = "SFTP2_MAX_UNAUTH_CONN"
MAX_RETRIES = SFTP_MAX_RETRY_TIMES
DEFAULT_SSH_CONNECT_TIMEOUT = 5
DEFAULT_SSH_KEEPALIVE_INTERVAL = 15


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
    private_key_path = os.getenv(SFTP2_PRIVATE_KEY_PATH)
    if private_key_path:
        if not os.path.exists(private_key_path):
            raise FileNotFoundError(f"Private key file not exist: '{private_key_path}'")
        with open(private_key_path, "rb") as f:
            return f.read()
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
        username = os.getenv(SFTP2_USERNAME)
        if not username:
            # 如果没有指定用户名，使用当前系统用户名
            username = getpass.getuser()
    if not password:
        password = os.getenv(SFTP2_PASSWORD)
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
            try:
                result = session.userauth_publickey_frommemory(
                    username,
                    private_key,
                    private_key_password=os.getenv(SFTP2_PRIVATE_KEY_PASSWORD, ""),
                )
                if result == 0:  # 0 indicates success in ssh2-python
                    authenticated = True
            except AttributeError:
                # Fallback for different ssh2-python versions
                result = session.userauth_publickey_fromstring(
                    username,
                    private_key,
                    passphrase=os.getenv(SFTP2_PRIVATE_KEY_PASSWORD, ""),
                )
                if result == 0:
                    authenticated = True
        except Exception as e:
            _logger.debug(f"Private key authentication failed: {e}")

    # 2. 如果提供了密码，尝试密码认证
    if not authenticated and password and username:
        try:
            result = session.userauth_password(username, password)
            if result == 0:
                authenticated = True
        except Exception as e:
            _logger.debug(f"Password authentication failed: {e}")

    # 3. 尝试使用 SSH agent 认证
    if not authenticated and username:
        try:
            # ssh2-python 使用 agent_init() 和 agent_auth() 方法
            session.agent_init()
            session.agent_auth(username)
            authenticated = True
            _logger.debug("Successfully authenticated with SSH agent")
        except Exception as e:
            _logger.debug(f"SSH agent authentication failed: {e}")
            # 尝试旧的 API 作为备选
            try:
                if hasattr(session, "userauth_agent"):
                    result = session.userauth_agent(username)
                    if result == 0:
                        authenticated = True
                        _logger.debug(
                            "Successfully authenticated with SSH agent (fallback)"
                        )
            except Exception as e2:
                _logger.debug(f"SSH agent fallback authentication failed: {e2}")

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
                    # ssh2-python 需要公钥和私钥文件路径
                    pub_key_path = key_path + ".pub"
                    if not os.path.exists(pub_key_path):
                        # 如果没有 .pub 文件，尝试让 ssh2-python 自动推断
                        pub_key_path = key_path

                    result = session.userauth_publickey_fromfile(
                        username,
                        pub_key_path,  # 公钥文件路径
                        key_path,  # 私钥文件路径
                        "",  # 私钥密码（空字符串表示无密码）
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
                    # 如果上面的方法失败，尝试只用私钥文件
                    try:
                        result = session.userauth_publickey_fromfile(
                            username,
                            key_path,  # 有时候公钥和私钥是同一个文件
                            key_path,
                            "",
                        )
                        if result == 0:
                            authenticated = True
                            _logger.debug(
                                f"Successfully authenticated with key (single file): "
                                f"{key_path}"
                            )
                            break
                    except Exception as e2:
                        _logger.debug(
                            f"Fallback public key authentication with {key_path} "
                            f"failed: {e2}"
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


class Sftp2File:
    """File-like object for SFTP2 operations"""

    def __init__(self, sftp_handle, path: str, mode: str = "r"):
        self.sftp_handle = sftp_handle
        self.path = path
        self.mode = mode
        self.name = path
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    def read(self, size: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if size == -1:
            # Read all data
            data = b""
            while True:
                try:
                    chunk, bytes_read = self.sftp_handle.read(8192)
                    if bytes_read == 0:
                        break
                    data += chunk[:bytes_read]
                except Exception:
                    break
            return data
        else:
            try:
                chunk, bytes_read = self.sftp_handle.read(size)
                return chunk[:bytes_read] if bytes_read > 0 else b""
            except Exception:
                return b""

    def write(self, data: bytes) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if isinstance(data, str):
            data = data.encode("utf-8")
        try:
            bytes_written = self.sftp_handle.write(data)
            return bytes_written
        except Exception:
            return 0

    def close(self):
        if not self._closed:
            try:
                self.sftp_handle.close()
            except Exception:
                pass
            self._closed = True

    def flush(self):
        """Flush the file. This is a no-op for SFTP files."""
        pass

    def readable(self) -> bool:
        return "r" in self.mode

    def writable(self) -> bool:
        return "w" in self.mode or "a" in self.mode

    def seekable(self) -> bool:
        return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


@SmartPath.register
class Sftp2Path(URIPath):
    """sftp2 protocol

    uri format:
        - absolute path
            - sftp2://[username[:password]@]hostname[:port]//file_path
        - relative path
            - sftp2://[username[:password]@]hostname[:port]/file_path
    """

    protocol = "sftp2"

    def __init__(self, path: "PathLike", *other_paths: "PathLike"):
        super().__init__(path, *other_paths)
        parts = urlsplit(self.path)
        self._urlsplit_parts = parts
        self._real_path = parts.path
        if parts.path.startswith("//"):
            self._root_dir = "/"
        else:
            self._root_dir = "/"  # Default to absolute path for ssh2
        self._real_path = (
            parts.path.lstrip("/")
            if not parts.path.startswith("//")
            else parts.path[2:]
        )
        if not self._real_path.startswith("/"):
            self._real_path = "/" + self._real_path

    @cached_property
    def parts(self) -> Tuple[str, ...]:
        """A tuple giving access to the path's various components"""
        if self._urlsplit_parts.path.startswith("//"):
            new_parts = self._urlsplit_parts._replace(path="//")
        else:
            new_parts = self._urlsplit_parts._replace(path="/")
        parts = [urlunsplit(new_parts)]
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

    def _generate_path_object(self, sftp_local_path: str, resolve: bool = False):
        if resolve or self._root_dir == "/":
            sftp_local_path = f"//{sftp_local_path.lstrip('/')}"
        else:
            sftp_local_path = os.path.relpath(sftp_local_path, start=self._root_dir)
            if sftp_local_path == ".":
                sftp_local_path = "/"
        new_parts = self._urlsplit_parts._replace(path=sftp_local_path)
        return self.from_path(urlunsplit(new_parts))

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
        except Exception:
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
        for real_path in _create_missing_ok_generator(
            iglob(fspath(glob_path), recursive=recursive, fs=fs),
            missing_ok,
            FileNotFoundError("No match any file: %r" % glob_path),
        ):
            yield self.from_path(real_path)

    def is_dir(self, followlinks: bool = False) -> bool:
        """Test if a path is directory"""
        try:
            stat = self.stat(follow_symlinks=followlinks)
            return stat.is_dir()
        except Exception:
            return False

    def is_file(self, followlinks: bool = False) -> bool:
        """Test if a path is file"""
        try:
            stat = self.stat(follow_symlinks=followlinks)
            return (
                S_ISREG(stat.st_mode) if hasattr(stat, "st_mode") else not stat.is_dir()
            )
        except Exception:
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
            self._client.mkdir(self._real_path, mode)
        except Exception:
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
            self._client.rmdir(self._real_path)
        else:
            self._client.unlink(self._real_path)

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
            FileNotFoundError("No match any file in: %r" % self.path_with_protocol),
        )

    def scandir(self) -> ContextIterator:
        """Get all content of given file path"""
        real_path = self._real_path
        stat_result = None
        try:
            stat_result = self.stat(follow_symlinks=False)
        except Exception:
            raise NotADirectoryError(f"Not a directory: '{self.path_with_protocol}'")

        if stat_result.is_symlink():
            real_path = self.readlink()._real_path
        elif not stat_result.is_dir():
            raise NotADirectoryError(f"Not a directory: '{self.path_with_protocol}'")

        def create_generator():
            try:
                # Use opendir and readdir from ssh2-python
                dir_handle = self._client.opendir(real_path)
                try:
                    # ssh2-python's readdir returns a generator
                    # First call returns all entries, subsequent calls return empty
                    entries_gen = dir_handle.readdir()
                    entries = list(entries_gen) if entries_gen else []
                    
                    for name_len, name_bytes, stat_obj in entries:
                        name = name_bytes.decode('utf-8')
                        if name in (".", ".."):
                            continue
                        
                        try:
                            # Convert stat_obj to StatResult
                            stat_info = _make_stat(stat_obj)
                            yield FileEntry(
                                name,
                                self.joinpath(name).path_with_protocol,
                                stat_info,
                            )
                        except Exception:
                            continue
                finally:
                    dir_handle.close()
            except Exception:
                # If directory listing fails, return empty
                pass

        return ContextIterator(create_generator())

    def stat(self, follow_symlinks=True) -> StatResult:
        """Get StatResult of file on sftp2"""
        try:
            if follow_symlinks:
                stat = self._client.stat(self._real_path)
            else:
                stat = self._client.lstat(self._real_path)
            return _make_stat(stat)
        except Exception as e:
            raise FileNotFoundError(f"No such file: '{self.path_with_protocol}'") from e

    def lstat(self) -> StatResult:
        """Get StatResult without following symlinks"""
        return self.stat(follow_symlinks=False)

    def unlink(self, missing_ok: bool = False) -> None:
        """Remove the file on sftp2"""
        if missing_ok and not self.exists():
            return
        self._client.unlink(self._real_path)

    def walk(
        self, followlinks: bool = False
    ) -> Iterator[Tuple[str, List[str], List[str]]]:
        """Generate the file names in a directory tree by walking the tree top-down"""
        if not self.exists(followlinks=followlinks):
            return

        if self.is_file(followlinks=followlinks):
            return

        stack = [self._real_path]
        while stack:
            root = stack.pop()
            dirs, files = [], []

            try:
                dir_handle = self._client.opendir(root)
                while True:
                    entry = self._client.readdir(dir_handle)
                    if not entry:
                        break
                    name, stat = entry
                    if name in (".", ".."):
                        continue
                    current_path = self._generate_path_object(os.path.join(root, name))
                    if current_path.is_file(followlinks=followlinks):
                        files.append(name)
                    elif current_path.is_dir(followlinks=followlinks):
                        dirs.append(name)
                self._client.close(dir_handle)
            except Exception:
                pass

            dirs = sorted(dirs)
            files = sorted(files)

            yield self._generate_path_object(root).path_with_protocol, dirs, files

            stack.extend(
                (os.path.join(root, directory) for directory in reversed(dirs))
            )

    def resolve(self, strict=False) -> "Sftp2Path":
        """Return the canonical path"""
        try:
            path = self._client.realpath(self._real_path)
            return self._generate_path_object(path, resolve=True)
        except Exception:
            return self

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
        return self._client.symlink(self._real_path, dst_path._real_path)

    def readlink(self) -> "Sftp2Path":
        """Return a Sftp2Path instance representing the path to which the
        symbolic link points"""
        if not self.exists():
            raise FileNotFoundError(
                f"No such file or directory: '{self.path_with_protocol}'"
            )
        if not self.is_symlink():
            raise OSError("Not a symlink: %s" % self.path_with_protocol)
        try:
            path = self._client.readlink(self._real_path)
            if not path:
                raise OSError("Not a symlink: %s" % self.path_with_protocol)
            if not path.startswith("/"):
                return self.parent.joinpath(path)
            return self._generate_path_object(path)
        except FileNotFoundError:
            raise FileNotFoundError(
                f"No such file or directory: '{self.path_with_protocol}'"
            )
        except Exception:
            raise OSError("Not a symlink: %s" % self.path_with_protocol)

    def is_symlink(self) -> bool:
        """Test whether a path is a symbolic link"""
        try:
            return self.lstat().is_symlink()
        except Exception:
            return False

    def cwd(self) -> "Sftp2Path":
        """Return current working directory"""
        try:
            path = self._client.realpath(".")
            return self._generate_path_object(path)
        except Exception:
            return self._generate_path_object("/")

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
                raise IsADirectoryError("Is a directory: %r" % self.path_with_protocol)
            self.parent.mkdir(parents=True, exist_ok=True)
        elif not self.exists():
            raise FileNotFoundError("No such file: %r" % self.path_with_protocol)

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

        try:
            sftp_handle = self._client.open(self._real_path, ssh2_mode, 0o644)
            fileobj = Sftp2File(sftp_handle, self.path, mode)
        except Exception as e:
            # Fallback: try using mode string directly for compatibility
            try:
                sftp_handle = self._client.open(self._real_path, mode, 0o644)
                fileobj = Sftp2File(sftp_handle, self.path, mode)
            except Exception:
                raise e

        if "r" in mode and "b" not in mode:
            return io.TextIOWrapper(fileobj, encoding=encoding, errors=errors)
        return fileobj

    def chmod(self, mode: int, *, follow_symlinks: bool = True):
        """Change the file mode and permissions"""
        return self._client.setstat(self._real_path, mode)

    def absolute(self) -> "Sftp2Path":
        """Make the path absolute"""
        return self.resolve()

    def rmdir(self):
        """Remove this directory. The directory must be empty"""
        if len(self.listdir()) > 0:
            raise OSError(f"Directory not empty: '{self.path_with_protocol}'")
        return self._client.rmdir(self._real_path)

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
            raise OSError("Not a %s path: %r" % (self.protocol, dst_path))

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
        try:
            self._client.utime(self._real_path, (atime, mtime))
        except Exception:
            # ssh2-python may not support utime directly
            pass

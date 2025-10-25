import hashlib
import io
import os
import re
from functools import cached_property
from logging import getLogger as get_logger
from typing import IO, BinaryIO, Callable, Iterable, Iterator, List, Optional, Tuple
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import dateutil.parser
from webdav3.client import Client as WebdavClient
from webdav3.client import WebDavXmlUtils
from webdav3.exceptions import RemoteResourceNotFound, WebDavException
from webdav3.urn import Urn

from megfile.errors import SameFileError, _create_missing_ok_generator
from megfile.interfaces import (
    ContextIterator,
    FileEntry,
    PathLike,
    Readable,
    Seekable,
    StatResult,
    Writable,
)
from megfile.lib.compare import is_same_file
from megfile.lib.compat import fspath
from megfile.lib.fnmatch import translate
from megfile.lib.glob import has_magic
from megfile.lib.joinpath import uri_join, uri_norm
from megfile.pathlike import URIPath
from megfile.smart_path import SmartPath
from megfile.utils import calculate_md5, copyfileobj, get_binary_mode, thread_local

_logger = get_logger(__name__)

__all__ = [
    "WebdavPath",
    "is_webdav",
]

WEBDAV_USERNAME = "WEBDAV_USERNAME"
WEBDAV_PASSWORD = "WEBDAV_PASSWORD"
WEBDAV_TOKEN = "WEBDAV_TOKEN"
WEBDAV_TIMEOUT = "WEBDAV_TIMEOUT"


def _make_stat(info: dict) -> StatResult:
    """Convert WebDAV info dict to StatResult"""
    size = int(info.get("size") or 0)
    # WebDAV returns datetime objects, convert to timestamp
    mtime_str = info.get("modified", "")
    try:
        mtime = dateutil.parser.parse(mtime_str).timestamp()
    except Exception:
        mtime = 0.0

    isdir = info.get("is_dir", False)

    return StatResult(
        size=size,
        mtime=mtime,
        isdir=isdir,
        islnk=False,  # WebDAV doesn't support symlinks
        extra=info,
    )


def _make_entry(info: dict, root_relative: str, root_absolute: str) -> FileEntry:
    path = info.get("path", "").rstrip("/")
    name = os.path.basename(path)
    return FileEntry(
        name,
        os.path.join(root_absolute, os.path.relpath(path, root_relative)),
        _make_stat(info),
    )


def provide_connect_info(
    hostname: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> dict:
    """Provide connection info for WebDAV client"""
    if not username:
        username = os.getenv(WEBDAV_USERNAME)
    if not password:
        password = os.getenv(WEBDAV_PASSWORD)
    if not token:
        token = os.getenv(WEBDAV_TOKEN)

    timeout = int(os.getenv(WEBDAV_TIMEOUT, "30"))

    options = {
        "webdav_hostname": hostname,
        "webdav_timeout": timeout,
        "webdav_disable_check": True,
    }

    if token:
        options["webdav_token"] = token
    elif username and password:
        options["webdav_login"] = username
        options["webdav_password"] = password

    return options


def _get_webdav_client(
    hostname: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> WebdavClient:
    """Get WebDAV client"""
    options = provide_connect_info(hostname, username, password, token)
    return WebdavClient(options)


def get_webdav_client(
    hostname: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    token: Optional[str] = None,
) -> WebdavClient:
    """Get cached WebDAV client"""
    return thread_local(
        f"webdav_client:{hostname},{username},{password},{token}",
        _get_webdav_client,
        hostname,
        username,
        password,
        token,
    )


def is_webdav(path: PathLike) -> bool:
    """Test if a path is WebDAV path

    :param path: Path to be tested
    :returns: True if a path is WebDAV path, else False
    """
    path = fspath(path)
    parts = urlsplit(path)
    return parts.scheme in ("webdav", "webdavs")


def _webdav_scan_pairs(
    src_url: PathLike, dst_url: PathLike
) -> Iterator[Tuple[PathLike, PathLike]]:
    for src_file_path in WebdavPath(src_url).scan():
        content_path = src_file_path[len(fspath(src_url)) :]
        if len(content_path) > 0:
            dst_file_path = (
                WebdavPath(dst_url).joinpath(content_path).path_with_protocol
            )
        else:
            dst_file_path = dst_url
        yield src_file_path, dst_file_path


def _webdav_stat(client: WebdavClient, remote_path: str):
    urn = Urn(remote_path)
    client._check_remote_resource(remote_path, urn)

    response = client.execute_request(
        action="info", path=urn.quote(), headers_ext=["Depth: 0"]
    )
    path = client.get_full_path(urn)
    info = WebDavXmlUtils.parse_info_response(
        response.content, path, client.webdav.hostname
    )
    info["is_dir"] = WebDavXmlUtils.parse_is_dir_response(
        response.content, path, client.webdav.hostname
    )
    return info


def _webdav_scan(client: WebdavClient, remote_path: str) -> List[dict]:
    directory_urn = Urn(remote_path, directory=True)
    if directory_urn.path() != WebdavClient.root and not client.check(
        directory_urn.path()
    ):
        raise RemoteResourceNotFound(directory_urn.path())

    path = Urn.normalize_path(client.get_full_path(directory_urn))
    response = client.execute_request(
        action="list", path=directory_urn.quote(), headers_ext=["Depth: infinity"]
    )
    subfiles = WebDavXmlUtils.parse_get_list_info_response(response.content)
    return [
        subfile
        for subfile in subfiles
        if Urn.compare_path(path, subfile.get("path")) is False
    ]


def _webdav_scandir(client: WebdavClient, remote_path: str) -> List[dict]:
    return client.list(remote_path, get_info=True)


def _webdav_split_magic(path: str) -> Tuple[str, str]:
    parts = path.split("/")
    for i in range(0, len(parts)):
        if has_magic(parts[i]):
            return "/".join(parts[:i]), "/".join(parts[i:])
    return path, ""


class WebdavMemoryHandler(Readable[bytes], Seekable, Writable[bytes]):  # noqa: F821
    def __init__(
        self,
        remote_path: str,
        mode: str,
        *,
        webdav_client: WebdavClient,
        name: str,
    ):
        self._remote_path = remote_path
        self._mode = mode
        self._client = webdav_client
        self._name = name

        if mode not in ("rb", "wb", "ab", "rb+", "wb+", "ab+"):
            raise ValueError("unacceptable mode: %r" % mode)

        self._fileobj = io.BytesIO()
        self._download_fileobj()

    @property
    def name(self) -> str:
        return self._name

    @property
    def mode(self) -> str:
        return self._mode

    def tell(self) -> int:
        return self._fileobj.tell()

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return self._fileobj.seek(offset, whence)

    def readable(self) -> bool:
        return self._mode[0] == "r" or self._mode[-1] == "+"

    def read(self, size: Optional[int] = None) -> bytes:
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        return self._fileobj.read(size)

    def readline(self, size: Optional[int] = None) -> bytes:
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        if size is None:
            size = -1
        return self._fileobj.readline(size)

    def readlines(self, hint: Optional[int] = None) -> List[bytes]:
        if not self.readable():
            raise io.UnsupportedOperation("not readable")
        if hint is None:
            hint = -1
        return self._fileobj.readlines(hint)

    def writable(self) -> bool:
        return self._mode[0] == "w" or self._mode[0] == "a" or self._mode[-1] == "+"

    def flush(self):
        self._fileobj.flush()

    def write(self, data: bytes) -> int:
        if not self.writable():
            raise io.UnsupportedOperation("not writable")
        if self._mode[0] == "a":
            self.seek(0, os.SEEK_END)
        return self._fileobj.write(data)

    def writelines(self, lines: Iterable[bytes]):
        if not self.writable():
            raise io.UnsupportedOperation("not writable")
        if self._mode[0] == "a":
            self.seek(0, os.SEEK_END)
        self._fileobj.writelines(lines)

    def _file_exists(self) -> bool:
        try:
            return not self._client.is_dir(self._remote_path)
        except RemoteResourceNotFound:
            return False

    def _download_fileobj(self):
        need_download = self._mode[0] == "r" or (
            self._mode[0] == "a" and self._file_exists()
        )
        if not need_download:
            return
        # directly download to the file handle
        self._client.download_from(self._fileobj, self._remote_path)
        if self._mode[0] == "r":
            self.seek(0, os.SEEK_SET)

    def _upload_fileobj(self):
        need_upload = self.writable()
        if not need_upload:
            return
        # directly upload from file handle
        self.seek(0, os.SEEK_SET)
        self._client.upload_to(self._fileobj, self._remote_path)

    def _close(self, need_upload: bool = True):
        if hasattr(self, "_fileobj"):
            if need_upload:
                self._upload_fileobj()
            self._fileobj.close()


@SmartPath.register
class WebdavPath(URIPath):
    """WebDAV protocol

    uri format:
        - webdav://[username[:password]@]hostname[:port]/file_path
        - webdavs://[username[:password]@]hostname[:port]/file_path
    """

    protocol = "webdav"

    def __init__(self, path: "PathLike", *other_paths: "PathLike"):
        super().__init__(path, *other_paths)
        parts = urlsplit(self.path)
        self._urlsplit_parts = parts

        # Normalize scheme to webdav/webdavs
        if parts.scheme == "http":
            self._webdav_scheme = "webdav"
        elif parts.scheme == "https":
            self._webdav_scheme = "webdavs"
        else:
            self._webdav_scheme = parts.scheme

        # Build hostname with scheme
        scheme_for_hostname = "https" if self._webdav_scheme == "webdavs" else "http"
        self._hostname = f"{scheme_for_hostname}://{parts.hostname}"
        if parts.port:
            self._hostname += f":{parts.port}"

        self._remote_path = unquote(parts.path) if parts.path else "/"

    @cached_property
    def parts(self) -> Tuple[str, ...]:
        """A tuple giving access to the path's various components"""
        parts = [urlunsplit(self._urlsplit_parts._replace(path=""))]
        path = self._urlsplit_parts.path.lstrip("/")
        if path != "":
            parts.extend(path.split("/"))
        return tuple(parts)

    @property
    def _client(self) -> WebdavClient:
        return get_webdav_client(
            hostname=self._hostname,
            username=self._urlsplit_parts.username,
            password=self._urlsplit_parts.password,
        )

    def _generate_path_object(self, webdav_path: str):
        """Generate a new WebdavPath object with the given path"""
        # Ensure path starts with /
        if not webdav_path.startswith("/"):
            webdav_path = "/" + webdav_path

        new_parts = self._urlsplit_parts._replace(
            scheme=self._webdav_scheme, path=quote(webdav_path, safe="/")
        )
        return self.from_path(urlunsplit(new_parts))  # pyre-ignore[6]

    def exists(self, followlinks: bool = False) -> bool:
        """
        Test if the path exists

        :param followlinks: Ignored for WebDAV (no symlink support)
        :returns: True if the path exists, else False
        """
        try:
            _webdav_stat(self._client, self._remote_path)
            return True
        except RemoteResourceNotFound:
            return False

    def getmtime(self, follow_symlinks: bool = False) -> float:
        """
        Get last-modified time of the file on the given path (in Unix timestamp format).

        :returns: last-modified time
        """
        return self.stat(follow_symlinks=follow_symlinks).mtime

    def getsize(self, follow_symlinks: bool = False) -> int:
        """
        Get file size on the given file path (in bytes).

        :returns: File size
        """
        return self.stat(follow_symlinks=follow_symlinks).size

    def glob(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> List["WebdavPath"]:
        """Return path list in ascending alphabetical order,
        in which path matches glob pattern

        :param pattern: Glob the given relative pattern
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :returns: A list contains paths match `pathname`
        """
        return list(
            sorted(
                self.iglob(pattern=pattern, recursive=recursive, missing_ok=missing_ok)
            )
        )

    def glob_stat(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> Iterator[FileEntry]:
        """Return a list contains tuples of path and file stat,
        in ascending alphabetical order, in which path matches glob pattern

        :param pattern: Glob the given relative pattern
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :returns: An iterator contains tuples of path and file stat
        """
        remote_path = self._remote_path
        if pattern:
            remote_path = os.path.join(remote_path, pattern)
        remote_path, pattern = _webdav_split_magic(remote_path)
        root = os.path.relpath(remote_path, self._remote_path)
        root = uri_join(self.path_with_protocol, root)
        root = uri_norm(root)
        pattern = re.compile(translate(pattern))
        scan_func = _webdav_scan if recursive else _webdav_scandir
        for info in scan_func(self._client, remote_path):
            entry = _make_entry(info, remote_path, root)
            relative = os.path.relpath(entry.path, root)
            if not pattern.match(relative):
                continue
            yield entry

    def iglob(
        self, pattern, recursive: bool = True, missing_ok: bool = True
    ) -> Iterator["WebdavPath"]:
        """Return path iterator in ascending alphabetical order,
        in which path matches glob pattern

        :param pattern: Glob the given relative pattern
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :returns: An iterator contains paths match `pathname`
        """
        for file_entry in self.glob_stat(
            pattern=pattern,
            recursive=recursive,
            missing_ok=missing_ok,
        ):
            yield self.from_path(file_entry.path)

    def is_dir(self, followlinks: bool = False) -> bool:
        """
        Test if a path is directory

        :param followlinks: Ignored for WebDAV
        :returns: True if the path is a directory, else False
        """
        try:
            return _webdav_stat(self._client, self._remote_path)["is_dir"]
        except RemoteResourceNotFound:
            return False

    def is_file(self, followlinks: bool = False) -> bool:
        """
        Test if a path is file

        :param followlinks: Ignored for WebDAV
        :returns: True if the path is a file, else False
        """
        try:
            return not _webdav_stat(self._client, self._remote_path)["is_dir"]
        except RemoteResourceNotFound:
            return False

    def listdir(self) -> List[str]:
        """
        Get all contents of given WebDAV path.
        The result is in ascending alphabetical order.

        :returns: All contents in ascending alphabetical order
        """
        with self.scandir() as entries:
            return sorted([entry.name for entry in entries])

    def iterdir(self) -> Iterator["WebdavPath"]:
        """
        Get all contents of given WebDAV path.

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
        Make a directory on WebDAV

        :param mode: Ignored for WebDAV
        :param parents: If parents is true, any missing parents are created
        :param exist_ok: If False and target directory exists, raise FileExistsError
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
            self._client.mkdir(self._remote_path)
        except WebDavException:
            # Catch exception when mkdir concurrently
            if not self.exists():
                raise

    def realpath(self) -> str:
        """Return the real path of given path

        :returns: Real path of given path
        """
        return self.path_with_protocol

    def _is_same_backend(self, other: "WebdavPath") -> bool:
        """Check if two paths are on the same WebDAV backend"""
        return (
            self._hostname == other._hostname
            and self._urlsplit_parts.username == other._urlsplit_parts.username
            and self._urlsplit_parts.password == other._urlsplit_parts.password
        )

    def _is_same_protocol(self, path):
        """Check if path is a WebDAV path"""
        return is_webdav(path)

    def rename(self, dst_path: PathLike, overwrite: bool = True) -> "WebdavPath":
        """
        Rename file on WebDAV

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        if not self._is_same_protocol(dst_path):
            raise OSError("Not a %s path: %r" % (self.protocol, dst_path))

        dst_path = self.from_path(str(dst_path).rstrip("/"))

        if self._is_same_backend(dst_path):
            if overwrite:
                dst_path.remove(missing_ok=True)
            self._client.move(
                self._remote_path, dst_path._remote_path, overwrite=overwrite
            )
        else:
            if self.is_dir():
                for file_entry in self.scandir():
                    self.from_path(file_entry.path).rename(
                        dst_path.joinpath(file_entry.name), overwrite=overwrite
                    )
                self.rmdir()
            else:
                if overwrite or not dst_path.exists():
                    with self.open("rb") as fsrc:
                        with dst_path.open("wb") as fdst:
                            copyfileobj(fsrc, fdst)
                self.unlink()

        return dst_path

    def replace(self, dst_path: PathLike, overwrite: bool = True) -> "WebdavPath":
        """
        Move file on WebDAV

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        return self.rename(dst_path=dst_path, overwrite=overwrite)

    def remove(self, missing_ok: bool = False) -> None:
        """
        Remove the file or directory on WebDAV

        :param missing_ok: if False and target file/directory not exists,
            raise FileNotFoundError
        """
        if missing_ok and not self.exists():
            return
        try:
            self._client.clean(self._remote_path)
        except RemoteResourceNotFound:
            if not missing_ok:
                raise FileNotFoundError(f"No such file: '{self.path_with_protocol}'")

    def scan(self, missing_ok: bool = True, followlinks: bool = False) -> Iterator[str]:
        """
        Iteratively traverse only files in given directory, in alphabetical order.

        :param missing_ok: If False and there's no file, raise FileNotFoundError
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

        :param missing_ok: If False and there's no file, raise FileNotFoundError
        :returns: A file path generator yielding FileEntry objects
        """

        def create_generator() -> Iterator[FileEntry]:
            if not self.exists():
                return

            if self.is_file():
                yield FileEntry(
                    self.name,
                    self.path_with_protocol,
                    self.stat(),
                )
                return

            for info in _webdav_scan(self._client, self._remote_path):
                entry = _make_entry(info, self._remote_path, self.path_with_protocol)
                if entry.is_dir():
                    continue
                yield entry

        return _create_missing_ok_generator(
            create_generator(),
            missing_ok,
            FileNotFoundError("No match any file in: %r" % self.path_with_protocol),
        )

    def scandir(self) -> ContextIterator:
        """
        Get all content of given file path.

        :returns: An iterator contains all contents
        """
        if not self.exists():
            raise FileNotFoundError(
                f"No such file or directory: '{self.path_with_protocol}'"
            )
        if not self.is_dir():
            raise NotADirectoryError(f"Not a directory: '{self.path_with_protocol}'")

        def create_generator():
            for info in _webdav_scandir(self._client, self._remote_path):
                yield _make_entry(info, self._remote_path, self.path_with_protocol)

        return ContextIterator(create_generator())

    def stat(self, follow_symlinks=True) -> StatResult:
        """
        Get StatResult of file on WebDAV

        :returns: StatResult
        """
        try:
            info = _webdav_stat(self._client, self._remote_path)
            return _make_stat(info)
        except RemoteResourceNotFound:
            raise FileNotFoundError(f"No such file: '{self.path_with_protocol}'")

    def unlink(self, missing_ok: bool = False) -> None:
        """
        Remove the file on WebDAV

        :param missing_ok: if False and target file not exists, raise FileNotFoundError
        """
        if missing_ok and not self.exists():
            return
        try:
            self._client.clean(self._remote_path)
        except RemoteResourceNotFound:
            if not missing_ok:
                raise FileNotFoundError(f"No such file: '{self.path_with_protocol}'")

    def walk(
        self, followlinks: bool = False
    ) -> Iterator[Tuple[str, List[str], List[str]]]:
        """
        Generate the file names in a directory tree by walking the tree top-down.

        :param followlinks: Ignored for WebDAV
        :returns: A 3-tuple generator (root, dirs, files)
        """
        if not self.exists():
            return
        if self.is_file():
            return

        stack = [self._remote_path]
        while stack:
            root = stack.pop()
            dirs, files = [], []

            root_path = self._generate_path_object(root)
            for entry in root_path.scandir():
                if entry.is_dir():
                    dirs.append(entry.name)
                else:
                    files.append(entry.name)

            dirs = sorted(dirs)
            files = sorted(files)

            yield root_path.path_with_protocol, dirs, files

            stack.extend(
                (os.path.join(root, directory) for directory in reversed(dirs))
            )

    def resolve(self, strict=False) -> "WebdavPath":
        """Return the absolute path

        :param strict: Ignored for WebDAV
        :return: Absolute path
        """
        return self

    def md5(self, recalculate: bool = False, followlinks: bool = False):
        """
        Calculate the md5 value of the file

        :param recalculate: Ignored for WebDAV
        :param followlinks: Ignored for WebDAV
        :returns: md5 of file
        """
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

    def is_symlink(self) -> bool:
        """WebDAV doesn't support symlinks

        :return: Always False
        """
        return False

    def cwd(self) -> "WebdavPath":
        """Return current working directory (root path)

        :returns: Root WebDAV path
        """
        return self._generate_path_object("/")

    def save(self, file_object: BinaryIO):
        """Write the opened binary stream to path

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
        :param buffering: buffering policy
        :param encoding: encoding for text mode
        :param errors: error handling for text mode
        :returns: File-Like object
        """
        if "x" in mode:
            if self.exists():
                raise FileExistsError("File exists: %r" % self.path_with_protocol)
        if "w" in mode or "x" in mode or "a" in mode:
            if self.is_dir():
                raise IsADirectoryError("Is a directory: %r" % self.path_with_protocol)
            self.parent.mkdir(parents=True, exist_ok=True)
        elif not self.exists():
            raise FileNotFoundError("No such file: %r" % self.path_with_protocol)

        buffer = WebdavMemoryHandler(
            self._remote_path,
            get_binary_mode(mode),
            webdav_client=self._client,
            name=self.path_with_protocol,
        )
        if "b" not in mode:
            return io.TextIOWrapper(buffer, encoding=encoding, errors=errors)
        return buffer

    def chmod(self, mode: int, *, follow_symlinks: bool = True):
        """
        WebDAV doesn't support chmod

        :param mode: Ignored
        :param follow_symlinks: Ignored
        """
        _logger.warning("WebDAV does not support chmod operation")

    def absolute(self) -> "WebdavPath":
        """
        Make the path absolute

        :returns: Absolute path
        """
        return self

    def rmdir(self):
        """
        Remove this directory. The directory must be empty.
        """
        if len(self.listdir()) > 0:
            raise OSError(f"Directory not empty: '{self.path_with_protocol}'")
        self._client.clean(self._remote_path)

    def copy(
        self,
        dst_path: PathLike,
        callback: Optional[Callable[[int], None]] = None,
        followlinks: bool = False,
        overwrite: bool = True,
    ):
        """
        Copy the file to the given destination path.

        :param dst_path: The destination path
        :param callback: Optional callback for progress
        :param followlinks: Ignored for WebDAV
        :param overwrite: whether to overwrite existing file
        """
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
            if self._remote_path == dst_path._remote_path:
                raise SameFileError(
                    f"'{self.path}' and '{dst_path.path}' are the same file"
                )
            self._client.copy(self._remote_path, dst_path._remote_path)
            if callback:
                callback(self.stat().size)
        else:
            with self.open("rb") as fsrc:
                with dst_path.open("wb") as fdst:
                    copyfileobj(fsrc, fdst, callback)

    def sync(
        self,
        dst_path: PathLike,
        followlinks: bool = False,
        force: bool = False,
        overwrite: bool = True,
    ):
        """Copy file/directory to dst_path

        :param dst_path: Given destination path
        :param followlinks: Ignored for WebDAV
        :param force: Sync forcibly
        :param overwrite: whether to overwrite existing file
        """
        if not self._is_same_protocol(dst_path):
            raise OSError("Not a %s path: %r" % (self.protocol, dst_path))

        for src_file_path, dst_file_path in _webdav_scan_pairs(
            self.path_with_protocol, dst_path
        ):
            dst_path_obj = self.from_path(dst_file_path)
            src_path_obj = self.from_path(src_file_path)

            if force:
                pass
            elif not overwrite and dst_path_obj.exists():
                continue
            elif dst_path_obj.exists() and is_same_file(
                src_path_obj.stat(), dst_path_obj.stat(), "copy"
            ):
                continue

            src_path_obj.copy(
                dst_file_path,
                followlinks=followlinks,
                overwrite=True,
            )


@SmartPath.register
class WebdavsPath(WebdavPath):
    protocol = "webdavs"

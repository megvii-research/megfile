# pyre-ignore-all-errors[16]
import os
import stat
from collections.abc import Sequence
from enum import Enum
from functools import cached_property
from typing import (
    IO,
    Any,
    BinaryIO,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

from megfile.lib.compat import PathLike as _PathLike
from megfile.lib.compat import fspath
from megfile.lib.fnmatch import _compile_pattern
from megfile.lib.joinpath import uri_join
from megfile.utils import classproperty

Self = TypeVar("Self")


class Access(Enum):
    READ = 1
    WRITE = 2


class StatResult(NamedTuple):
    size: int = 0
    ctime: float = 0.0
    mtime: float = 0.0
    isdir: bool = False
    islnk: bool = False
    extra: Any = None

    def is_file(self) -> bool:
        return not self.isdir or self.islnk

    def is_dir(self) -> bool:
        return self.isdir and not self.islnk

    def is_symlink(self) -> bool:
        return self.islnk

    @property
    def st_mode(self) -> int:
        """
        File mode: file type and file mode bits (permissions).
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_mode"):
            return self.extra.st_mode
        if self.is_symlink():
            return stat.S_IFLNK
        elif self.is_dir():
            return stat.S_IFDIR
        return stat.S_IFREG

    @property
    def st_ino(self) -> int:
        """
        Platform dependent, but if non-zero, uniquely identifies the file for
        a given value of st_dev. Typically:

        the inode number on Unix,
        the file index on Windows,
        the decimal of etag on oss.
        """
        if self.extra:
            if hasattr(self.extra, "st_ino"):
                return self.extra.st_ino
            elif isinstance(self.extra, dict) and self.extra.get("ETag"):
                return int(self.extra["ETag"][1:-1], 16)
        return 0

    @property
    def st_dev(self) -> int:
        """
        Identifier of the device on which this file resides.
        """
        if self.extra:
            if hasattr(self.extra, "st_dev"):
                return self.extra.st_dev
        return 0

    @property
    def st_nlink(self) -> int:
        """
        Number of hard links.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_nlink"):
            return self.extra.st_nlink
        return 0

    @property
    def st_uid(self) -> int:
        """
        User identifier of the file owner.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_uid"):
            return self.extra.st_uid
        return 0

    @property
    def st_gid(self) -> int:
        """
        Group identifier of the file owner.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_gid"):
            return self.extra.st_gid
        return 0

    @property
    def st_size(self) -> int:
        """
        Size of the file in bytes.
        """
        if self.extra and hasattr(self.extra, "st_size"):
            return self.extra.st_size
        return self.size

    @property
    def st_atime(self) -> float:
        """
        Time of most recent access expressed in seconds.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_atime"):
            return self.extra.st_atime
        return 0.0

    @property
    def st_mtime(self) -> float:
        """
        Time of most recent content modification expressed in seconds.
        """
        if self.extra and hasattr(self.extra, "st_mtime"):
            return self.extra.st_mtime
        return self.mtime

    @property
    def st_ctime(self) -> float:
        """
        Platform dependent:

            the time of most recent metadata change on Unix,
            the time of creation on Windows, expressed in seconds,
            the time of file created on oss;
            if is dir, return the latest ctime of the files in dir.
        """
        if self.extra and hasattr(self.extra, "st_ctime"):
            return self.extra.st_ctime
        return self.ctime

    @property
    def st_atime_ns(self) -> int:
        """
        Time of most recent access expressed in nanoseconds as an integer.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_atime_ns"):
            return self.extra.st_atime_ns
        return 0

    @property
    def st_mtime_ns(self) -> int:
        """
        Time of most recent content modification expressed in nanoseconds as an integer.
        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_mtime_ns"):
            return self.extra.st_mtime_ns
        return 0

    @property
    def st_ctime_ns(self) -> int:
        """
        Platform dependent:

            the time of most recent metadata change on Unix,
            the time of creation on Windows, expressed in nanoseconds as an integer.

        Only support fs.
        """
        if self.extra and hasattr(self.extra, "st_ctime_ns"):
            return self.extra.st_ctime_ns
        return 0


class FileEntry(NamedTuple):
    name: str
    path: str
    stat: StatResult

    def inode(self) -> Optional[Union[int, str]]:
        return self.stat.st_ino

    def is_file(self) -> bool:
        return self.stat.is_file()

    def is_dir(self) -> bool:
        return self.stat.is_dir()

    def is_symlink(self) -> bool:
        return self.stat.is_symlink()


class BasePath:
    def __init__(self, path: "PathLike"):
        self.path = str(path)

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, str(self))

    def __bytes__(self) -> bytes:
        return str(self).encode()

    def __fspath__(self) -> str:
        return self.path

    def __hash__(self) -> int:
        return hash(fspath(self))

    def __eq__(self, other_path: "BasePath") -> bool:
        return fspath(self) == fspath(other_path)

    def is_dir(self, followlinks: bool = False) -> bool:
        """Return True if the path points to a directory."""
        raise NotImplementedError('method "is_dir" not implemented: %r' % self)

    def is_file(self, followlinks: bool = False) -> bool:
        """Return True if the path points to a regular file."""
        raise NotImplementedError('method "is_file" not implemented: %r' % self)

    def is_symlink(self) -> bool:
        return False

    def access(self, mode: Access) -> bool:
        """Return True if the path has access permission described by mode."""
        raise NotImplementedError('method "access" not implemented: %r' % self)

    def exists(self, followlinks: bool = False) -> bool:
        """Whether the path points to an existing file or directory."""
        raise NotImplementedError('method "exists" not implemented: %r' % self)

    def listdir(self) -> List[str]:
        """Return the names of the entries in the directory the path points to."""
        raise NotImplementedError('method "listdir" not implemented: %r' % self)

    def scandir(self) -> Iterator[FileEntry]:
        """
        Return an iterator of FileEntry objects corresponding to the entries
        in the directory.
        """
        raise NotImplementedError('method "scandir" not implemented: %r' % self)

    def getsize(self, follow_symlinks: bool = True) -> int:
        """Return the size, in bytes."""
        raise NotImplementedError('method "getsize" not implemented: %r' % self)

    def getmtime(self, follow_symlinks: bool = True) -> float:
        """Return the time of last modification."""
        raise NotImplementedError('method "getmtime" not implemented: %r' % self)

    def stat(self, follow_symlinks=True) -> StatResult:
        """Get the status of the path."""
        raise NotImplementedError('method "stat" not implemented: %r' % self)

    def remove(self, missing_ok: bool = False) -> None:
        """Remove (delete) the file."""
        raise NotImplementedError('method "remove" not implemented: %r' % self)

    def unlink(self, missing_ok: bool = False) -> None:
        """Remove (delete) the file."""
        raise NotImplementedError('method "unlink" not implemented: %r' % self)

    def mkdir(self, mode=0o777, parents: bool = False, exist_ok: bool = False) -> None:
        """Create a directory."""
        raise NotImplementedError('method "mkdir" not implemented: %r' % self)

    def rmdir(self) -> None:
        """Remove (delete) the directory."""
        raise NotImplementedError('method "rmdir" not implemented: %r' % self)

    def open(self, mode: str = "r", **kwargs) -> IO:
        """Open the file with mode."""
        raise NotImplementedError('method "open" not implemented: %r' % self)

    def walk(
        self, followlinks: bool = False
    ) -> Iterator[Tuple[str, List[str], List[str]]]:
        """Generate the file names in a directory tree by walking the tree."""
        raise NotImplementedError('method "walk" not implemented: %r' % self)

    def scan(self, missing_ok: bool = True, followlinks: bool = False) -> Iterator[str]:
        """Iterate through the files in the directory."""
        raise NotImplementedError('method "scan" not implemented: %r' % self)

    def scan_stat(
        self, missing_ok: bool = True, followlinks: bool = False
    ) -> Iterator[FileEntry]:
        """Iterate through the files in the directory, with file stat."""
        raise NotImplementedError('method "scan_stat" not implemented: %r' % self)

    def glob(
        self: Self, pattern: str, recursive: bool = True, missing_ok: bool = True
    ) -> List[Self]:
        """Return files whose paths match the glob pattern."""
        raise NotImplementedError('method "glob" not implemented: %r' % self)

    def iglob(
        self: Self, pattern: str, recursive: bool = True, missing_ok: bool = True
    ) -> Iterator[Self]:
        """Return an iterator of files whose paths match the glob pattern."""
        raise NotImplementedError('method "iglob" not implemented: %r' % self)

    def glob_stat(
        self, pattern: str, recursive: bool = True, missing_ok: bool = True
    ) -> Iterator[FileEntry]:
        """Return an iterator of files with stat whose paths match the glob pattern."""
        raise NotImplementedError('method "glob_stat" not implemented: %r' % self)

    def load(self) -> BinaryIO:
        """Read all content in binary."""
        raise NotImplementedError('method "load" not implemented: %r' % self)

    def save(self, file_object: BinaryIO):
        """Write the opened binary stream to the path."""
        raise NotImplementedError('method "save" not implemented: %r' % self)

    def joinpath(self: Self, *other_paths: "PathLike") -> Self:
        """Join or or more path."""
        raise NotImplementedError('method "joinpath" not implemented: %r' % self)

    def abspath(self):
        """Return a normalized absolute version of the path."""
        raise NotImplementedError('method "abspath" not implemented: %r' % self)

    def realpath(self):
        """Return the canonical path of the path."""
        raise NotImplementedError('method "realpath" not implemented: %r' % self)

    def relpath(self, start=None):
        """Return the relative path."""
        raise NotImplementedError('method "relpath" not implemented: %r' % self)

    def is_absolute(self) -> bool:
        """Return True if the path is an absolute pathname."""
        raise NotImplementedError('method "is_absolute" not implemented: %r' % self)

    def is_mount(self) -> bool:
        """Return True if the path is a mount point."""
        raise NotImplementedError('method "is_mount" not implemented: %r' % self)

    def resolve(self):
        """Alias of realpath."""
        raise NotImplementedError('method "resolve" not implemented: %r' % self)

    def touch(self):
        with self.open("w"):
            pass

    # TODO: will be deleted in next version
    def is_link(self) -> bool:
        return self.is_symlink()

    def makedirs(self, exist_ok: bool = False) -> None:
        """
        Recursive directory creation function. Like mkdir(), but makes all
        intermediate-level directories needed to contain the leaf directory.
        """
        self.mkdir(parents=True, exist_ok=exist_ok)


PathLike = Union[str, BasePath, _PathLike]


class BaseURIPath(BasePath):
    # #####
    # TODO: Backwards compatible API, will be removed in megfile 1.0
    @classmethod
    def get_protocol(self) -> Optional[str]:
        pass  # pragma: no cover

    @classproperty
    def protocol(cls) -> str:
        return cls.get_protocol() or ""

    def make_uri(self) -> str:
        return self.path_with_protocol

    def as_uri(self) -> str:
        return self.make_uri()

    # #####

    @cached_property
    def path_with_protocol(self) -> str:
        """Return path with protocol, like file:///root, s3://bucket/key"""
        path = self.path
        protocol_prefix = self.protocol + "://"  # pyre-ignore[58]
        if path.startswith(protocol_prefix):
            return path
        return protocol_prefix + path.lstrip("/")

    @cached_property
    def path_without_protocol(self) -> str:
        """
        Return path without protocol, example: if path is s3://bucket/key,
        return bucket/key
        """
        path = self.path
        protocol_prefix = self.protocol + "://"  # pyre-ignore[58]
        if path.startswith(protocol_prefix):
            path = path[len(protocol_prefix) :]
        return path

    def as_posix(self) -> str:
        """Return a string representation of the path with forward slashes (/)"""
        return self.path_with_protocol

    def __fspath__(self) -> str:
        return self.as_uri()

    def __lt__(self, other_path: "BaseURIPath") -> bool:
        if not isinstance(other_path, BaseURIPath):
            raise TypeError("%r is not 'URIPath'" % other_path)
        if self.protocol != other_path.protocol:
            raise TypeError(
                "'<' not supported between instances of %r and %r"
                % (type(self), type(other_path))
            )
        return fspath(self) < fspath(other_path)

    def __le__(self, other_path: "BaseURIPath") -> bool:
        if not isinstance(other_path, BaseURIPath):
            raise TypeError("%r is not 'URIPath'" % other_path)
        if self.protocol != other_path.protocol:
            raise TypeError(
                "'<=' not supported between instances of %r and %r"
                % (type(self), type(other_path))
            )
        return str(self) <= str(other_path)

    def __gt__(self, other_path: "BaseURIPath") -> bool:
        if not isinstance(other_path, BaseURIPath):
            raise TypeError("%r is not 'URIPath'" % other_path)
        if self.protocol != other_path.protocol:
            raise TypeError(
                "'>' not supported between instances of %r and %r"
                % (type(self), type(other_path))
            )
        return str(self) > str(other_path)

    def __ge__(self, other_path: "BaseURIPath") -> bool:
        if not isinstance(other_path, BaseURIPath):
            raise TypeError("%r is not 'URIPath'" % other_path)
        if self.protocol != other_path.protocol:
            raise TypeError(
                "'>=' not supported between instances of %r and %r"
                % (type(self), type(other_path))
            )
        return str(self) >= str(other_path)

    @classproperty
    def drive(self) -> str:
        return ""

    @classproperty
    def root(self) -> str:
        return self.protocol + "://"  # pyre-ignore[58]

    @classproperty
    def anchor(self) -> str:
        return self.root  # pyre-ignore[7]


class URIPath(BaseURIPath):
    def __init__(self, path: "PathLike", *other_paths: "PathLike"):
        if len(other_paths) > 0:
            path = self.from_path(path).joinpath(*other_paths)
        self.path = str(path)

    @classmethod
    def from_path(cls: Type[Self], path: PathLike) -> Self:
        """Return new instance of this class

        :param path: new path

        :return: new instance of new path
        :rtype: Self
        """
        return cls(path)  # pyre-ignore[19]

    @classmethod
    def from_uri(cls: Type[Self], path: PathLike) -> Self:
        path = fspath(path)
        protocol_prefix = cls.protocol + "://"
        if path[: len(protocol_prefix)] != protocol_prefix:
            raise ValueError(
                "protocol not match, expected: %r, got: %r" % (cls.protocol, path)
            )
        return cls.from_path(path[len(protocol_prefix) :])

    def __truediv__(self: Self, other_path: PathLike) -> Self:
        if isinstance(other_path, BaseURIPath):
            if self.protocol != other_path.protocol:
                raise TypeError(
                    "'/' not supported between instances of %r and %r"
                    % (type(self), type(other_path))
                )
        elif not isinstance(other_path, str):
            raise TypeError("%r is not 'str' nor 'URIPath'" % other_path)
        return self.joinpath(other_path)

    def joinpath(self: Self, *other_paths: PathLike) -> Self:
        """
        Calling this method is equivalent to combining the path
        with each of the other arguments in turn
        """
        return self.from_path(uri_join(str(self), *map(str, other_paths)))

    @cached_property
    def parts(self) -> Tuple[str, ...]:
        """A tuple giving access to the path’s various components"""
        parts = [self.root]
        path = self.path_without_protocol
        path = path.lstrip("/")
        if path != "":
            parts.extend(path.split("/"))
        return tuple(parts)  # pyre-ignore[7]

    @cached_property
    def parents(self) -> "URIPathParents":
        """
        An immutable sequence providing access to the logical ancestors of the path
        """
        return URIPathParents(self)

    @cached_property
    def parent(self: Self) -> Self:
        """The logical parent of the path"""
        if self.path_without_protocol == "/":
            return self
        elif len(self.parents) > 0:
            return self.parents[0]
        return self.from_path("")

    @cached_property
    def name(self) -> str:
        """
        A string representing the final path component, excluding the drive and root
        """
        parts = self.parts
        if len(parts) == 1 and parts[0] == self.protocol + "://":  # pyre-ignore[58]
            return ""
        return parts[-1]

    @cached_property
    def suffix(self) -> str:
        """The file extension of the final component"""
        name = self.name
        i = name.rfind(".")
        if 0 < i < len(name) - 1:
            return name[i:]
        return ""

    @cached_property
    def suffixes(self) -> List[str]:
        """A list of the path’s file extensions"""
        name = self.name
        if name.endswith("."):
            return []
        name = name.lstrip(".")
        return ["." + suffix for suffix in name.split(".")[1:]]

    @cached_property
    def stem(self) -> str:
        """The final path component, without its suffix"""
        name = self.name
        i = name.rfind(".")
        if 0 < i < len(name) - 1:
            return name[:i]
        return name

    def is_reserved(self) -> bool:
        return False

    def match(self, pattern) -> bool:
        """
        Match this path against the provided glob-style pattern.
        Return True if matching is successful, False otherwise
        """
        match = _compile_pattern(pattern)
        for index in range(len(self.parts), 0, -1):
            path = "/".join(self.parts[index:])
            if match(path) is not None:
                return True
        return match(self.path_with_protocol) is not None

    def is_relative_to(self, *other) -> bool:
        try:
            self.relative_to(*other)
            return True
        except Exception:
            return False

    def relative_to(self: Self, *other: str) -> Self:
        """
        Compute a version of this path relative to the path represented by other.
        If it’s impossible, ValueError is raised.
        """
        if not other:
            raise TypeError("need at least one argument")

        other_path = self.from_path(other[0])
        if len(other) > 0:
            other_path = other_path.joinpath(*other[1:])
        other_path = other_path.path_with_protocol
        path = self.path_with_protocol

        if path.startswith(other_path):
            relative = path[len(other_path) :]
            relative = relative.lstrip("/")
            return type(self)(relative)  # pyre-ignore[19]

        raise ValueError("%r does not start with %r" % (path, other))

    def with_name(self: Self, name: str) -> Self:
        """Return a new path with the name changed"""
        path = str(self)
        raw_name = self.name
        return self.from_path(path[: len(path) - len(raw_name)] + name)

    def with_stem(self: Self, stem: str) -> Self:
        """Return a new path with the stem changed"""
        return self.with_name("".join([stem, self.suffix]))

    def with_suffix(self: Self, suffix: str) -> Self:
        """Return a new path with the suffix changed"""
        path = str(self)
        raw_suffix = self.suffix
        return self.from_path(path[: len(path) - len(raw_suffix)] + suffix)

    def relpath(self, start=None):
        """Return the relative path."""
        if start is None:
            raise TypeError("start is required")

        other_path = self.from_path(start).path_with_protocol
        path = self.path_with_protocol

        if path.startswith(other_path):
            relative = path[len(other_path) :]
            relative = relative.lstrip("/")
            return relative

        raise ValueError("%r does not start with %r" % (path, other_path))

    def is_absolute(self) -> bool:
        return True

    def is_mount(self) -> bool:
        """Test whether a path is a mount point

        :returns: True if a path is a mount point, else False
        """
        return False

    def is_socket(self) -> bool:
        """
        Return True if the path points to a Unix socket (or a symbolic link pointing
        to a Unix socket), False if it points to another kind of file.

        False is also returned if the path doesn’t exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        return False

    def is_fifo(self) -> bool:
        """
        Return True if the path points to a FIFO (or a symbolic link pointing to a
        FIFO), False if it points to another kind of file.

        False is also returned if the path doesn’t exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        return False

    def is_block_device(self) -> bool:
        """
        Return True if the path points to a block device (or a symbolic link pointing
        to a block device), False if it points to another kind of file.

        False is also returned if the path doesn’t exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        return False

    def is_char_device(self) -> bool:
        """
        Return True if the path points to a character device (or a symbolic link
        pointing to a character device), False if it points to another kind of file.

        False is also returned if the path doesn’t exist or is a broken symlink;
        other errors (such as permission errors) are propagated.
        """
        return False

    def abspath(self) -> str:
        """Return a normalized absolute version of the path."""
        return self.path_with_protocol

    def realpath(self) -> str:
        """Return the canonical path of the path."""
        return self.path_with_protocol

    def resolve(self):
        """Alias of realpath."""
        return self.path_with_protocol

    def chmod(self, mode: int, *, follow_symlinks: bool = True):
        raise NotImplementedError(f"'chmod' is unsupported on '{type(self)}'")

    def lchmod(self, mode: int):
        """
        Like chmod() but, if the path points to a symbolic link, the symbolic
        link’s mode is changed rather than its target’s.
        """
        return self.chmod(mode=mode, follow_symlinks=False)

    def read_bytes(self) -> bytes:
        """Return the binary contents of the pointed-to file as a bytes object"""
        with self.open(mode="rb") as f:
            return f.read()  # pytype: disable=bad-return-type

    def read_text(self) -> str:
        """Return the decoded contents of the pointed-to file as a string"""
        with self.open(mode="r") as f:
            return f.read()  # pytype: disable=bad-return-type

    def rename(self: Self, dst_path: PathLike, overwrite: bool = True) -> Self:
        """
        rename file

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        raise NotImplementedError(f"'rename' is unsupported on '{type(self)}'")

    def replace(self: Self, dst_path: PathLike, overwrite: bool = True) -> Self:
        """
        move file

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        return self.rename(dst_path=dst_path, overwrite=overwrite)

    def rglob(self: Self, pattern) -> List[Self]:
        """
        This is like calling Path.glob() with “**/” added in front of
        the given relative pattern
        """
        if not pattern:
            pattern = ""
        pattern = "**/" + pattern.lstrip("/")
        return self.glob(pattern=pattern)

    def md5(self, recalculate: bool = False, followlinks: bool = False) -> str:
        raise NotImplementedError(f"'md5' is unsupported on '{type(self)}'")

    def samefile(self, other_path) -> bool:
        """
        Return whether this path points to the same file
        """
        if hasattr(other_path, "protocol"):
            if other_path.protocol != self.protocol:
                return False

        stat = self.stat()
        if hasattr(other_path, "stat"):
            other_path_stat = other_path.stat()
        else:
            other_path_stat = self.from_path(other_path).stat()

        return (
            stat.st_ino == other_path_stat.st_ino
            and stat.st_dev == other_path_stat.st_dev
        )

    def symlink(self, dst_path: PathLike) -> None:
        raise NotImplementedError(f"'symlink' is unsupported on '{type(self)}'")

    def symlink_to(self, target, target_is_directory=False):
        """
        Make this path a symbolic link to target.
        symlink_to's arguments is the reverse of symlink's.
        Target_is_directory’s value is ignored, only be compatible with pathlib.Path
        """
        return self.from_path(target).symlink(dst_path=self.path)

    def hardlink_to(self, target):
        """
        Make this path a hard link to the same file as target.
        """
        raise NotImplementedError(f"'hardlink_to' is unsupported on '{type(self)}'")

    def write_bytes(self, data: bytes):
        """
        Open the file pointed to in bytes mode, write data to it, and close the file
        """
        with self.open(mode="wb") as f:
            return f.write(data)

    def write_text(self, data: str, encoding=None, errors=None, newline=None):
        """
        Open the file pointed to in text mode, write data to it, and close the file.
        The optional parameters have the same meaning as in open().
        """
        with self.open(
            mode="w", encoding=encoding, errors=errors, newline=newline
        ) as f:
            return f.write(data)

    def home(self):
        """Return the home directory

        returns: Home directory path
        """
        raise NotImplementedError(f"'home' is unsupported on '{type(self)}'")

    def group(self):
        """
        Return the name of the group owning the file.
        """
        raise NotImplementedError(f"'group' is unsupported on '{type(self)}'")

    def expanduser(self):
        """
        Return a new path with expanded ~ and ~user constructs, as returned by
        os.path.expanduser().

        Only fs path support this method.
        """
        raise NotImplementedError(f"'expanduser' is unsupported on '{type(self)}'")

    def cwd(self: Self) -> Self:
        """Return current working directory

        returns: Current working directory
        """
        raise NotImplementedError(f"'cwd' is unsupported on '{type(self)}'")

    def iterdir(self: Self) -> Iterator[Self]:
        """
        Get all contents of given fs path.
        The result is in ascending alphabetical order.

        :returns: All contents have in the path in ascending alphabetical order
        """
        raise NotImplementedError(f"'iterdir' is unsupported on '{type(self)}'")

    def owner(self) -> str:
        """
        Return the name of the user owning the file.
        """
        raise NotImplementedError(f"'owner' is unsupported on '{type(self)}'")

    def absolute(self: Self) -> Self:
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object
        """
        raise NotImplementedError(f"'absolute' is unsupported on '{type(self)}'")

    def utime(self, atime: Union[float, int], mtime: Union[float, int]):
        """
        Sets the access and modified times of the file specified by path to
        the specified values.

        :param atime: The access time to be set.
        :type atime: Union[float, int]
        :param mtime: The modification time to be set.
        :type mtime: Union[float, int]
        :raises NotImplementedError: Always raised,
            since the functionality is unsupported.
        """
        raise NotImplementedError(f"'utime' is unsupported on '{type(self)}'")

    def lstat(self) -> StatResult:
        """
        Like stat() but, if the path points to a symbolic link,
        return the symbolic link’s information rather than its target’s.
        """
        return self.stat(follow_symlinks=False)


class URIPathParents(Sequence):
    def __init__(self, path):
        # We don't store the instance to avoid reference cycles
        self.cls = type(path)
        parts = path.parts
        if len(parts) > 0 and parts[0] == path.protocol + "://":
            self.prefix = parts[0]
            self.parts = parts[1:]
        else:
            self.prefix = ""
            self.parts = parts

    def __len__(self):
        return max(len(self.parts) - 1, 0)

    def __getitem__(self, idx):
        if idx < 0 or idx > len(self):
            raise IndexError(idx)

        if len(self.parts[: -idx - 1]) > 1:
            other_path = os.path.join(*self.parts[: -idx - 1])
        elif len(self.parts[: -idx - 1]) == 1:
            other_path = self.parts[: -idx - 1][0]
        else:
            other_path = ""
        return self.cls(self.prefix + other_path)

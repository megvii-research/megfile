from collections.abc import Sequence
from enum import Enum
from functools import wraps
from typing import IO, Any, AnyStr, BinaryIO, Callable, Iterator, List, NamedTuple, Optional, Tuple, Union

from megfile.lib.compat import PathLike as _PathLike
from megfile.lib.compat import fspath
from megfile.lib.fnmatch import _compile_pattern
from megfile.lib.joinpath import uri_join
from megfile.utils import cachedproperty, classproperty

# Python 3.5+ compatible
'''
class StatResult(NamedTuple):

    size: int = 0
    ctime: float = 0.0
    mtime: float = 0.0
    isdir: bool = False
    islnk: bool = False
    extra: Any = None  # raw stat info

in Python 3.6+
'''

_StatResult = NamedTuple(
    'StatResult', [
        ('size', int), ('ctime', float), ('mtime', float), ('isdir', bool),
        ('islnk', bool), ('extra', Any)
    ])
_StatResult.__new__.__defaults__ = (0, 0.0, 0.0, False, False, None)


class Access(Enum):
    READ = 1
    WRITE = 2


class StatResult(_StatResult):

    def is_file(self) -> bool:
        return not self.isdir or self.islnk

    def is_dir(self) -> bool:
        return self.isdir and not self.islnk

    def is_symlink(self) -> bool:
        return self.islnk


'''
class FileEntry(NamedTuple):

    name: str
    stat: StatResult

in Python 3.6+
'''

_FileEntry = NamedTuple('FileEntry', [('name', str), ('stat', StatResult)])


class FileEntry(_FileEntry):

    def is_file(self) -> bool:
        return self.stat.is_file()

    def is_dir(self) -> bool:
        return self.stat.is_dir()

    def is_symlink(self) -> bool:
        return self.stat.is_symlink()


def method_not_implemented(func):

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        raise NotImplementedError(
            'method %r not implemented: %r' % (func.__name__, self))

    return wrapper


class BasePath:

    def __init__(self, path: "PathLike"):
        self.path = str(path)

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return '%s(%r)' % (self.__class__.__name__, str(self))

    def __bytes__(self) -> bytes:
        return str(self).encode()

    def __fspath__(self) -> str:
        return self.path

    def __hash__(self) -> int:
        return hash(fspath(self))

    def __eq__(self, other_path: "BasePath") -> bool:
        return fspath(self) == fspath(other_path)

    # pytype: disable=bad-return-type

    @method_not_implemented
    def is_dir(self) -> bool:  # type: ignore
        """Return True if the path points to a directory."""

    @method_not_implemented
    def is_file(self, followlinks: bool = False) -> bool:  # type: ignore
        """Return True if the path points to a regular file."""

    def is_symlink(self) -> bool:
        return False

    @method_not_implemented
    def access(self, mode: Access) -> bool:  # type: ignore
        """Return True if the path has access permission described by mode."""

    @method_not_implemented
    def exists(self, followlinks: bool = False) -> bool:  # type: ignore
        """Whether the path points to an existing file or directory."""

    # listdir or iterdir?
    @method_not_implemented
    def listdir(self) -> List[str]:  # type: ignore
        """Return the names of the entries in the directory the path points to."""

    @method_not_implemented
    def scandir(self) -> Iterator[FileEntry]:  # type: ignore
        """Return an iterator of FileEntry objects corresponding to the entries in the directory."""

    @method_not_implemented
    def getsize(self) -> int:  # type: ignore
        """Return the size, in bytes."""

    @method_not_implemented
    def getmtime(self) -> float:  # type: ignore
        """Return the time of last modification."""

    @method_not_implemented
    def stat(self) -> StatResult:  # type: ignore
        """Get the status of the path."""

    @method_not_implemented
    def remove(self, missing_ok: bool = False, **kwargs) -> None:
        """Remove (delete) the file."""

    @method_not_implemented
    def unlink(self, missing_ok: bool = False) -> None:
        """Remove (delete) the file."""

    @method_not_implemented
    def mkdir(self, exist_ok: bool = False) -> None:
        """Create a directory."""

    @method_not_implemented
    def rmdir(self) -> None:
        """Remove (delete) the directory."""

    @method_not_implemented
    def open(self, mode: str, s3_open_func: Callable[[str, str], BinaryIO]
            ) -> IO[AnyStr]:  # type: ignore
        """Open the file with mode."""

    @method_not_implemented
    def walk(self, **kwargs
            ) -> Iterator[Tuple[str, List[str], List[str]]]:  # type: ignore
        """Generate the file names in a directory tree by walking the tree."""

    @method_not_implemented
    def scan(self, missing_ok: bool = True,
             followlinks: bool = False) -> Iterator[str]:  # type: ignore
        """Iterate through the files in the directory."""

    @method_not_implemented
    def scan_stat(self, missing_ok: bool = True, followlinks: bool = False
                 ) -> Iterator[FileEntry]:  # type: ignore
        """Iterate through the files in the directory, with file stat."""

    @method_not_implemented
    def glob(self, recursive: bool = True,
             missing_ok: bool = True) -> List[str]:  # type: ignore
        """Return files whose paths match the glob pattern."""

    @method_not_implemented
    def iglob(self, recursive: bool = True,
              missing_ok: bool = True) -> Iterator[str]:  # type: ignore
        """Return an iterator of files whose paths match the glob pattern."""

    @method_not_implemented
    def glob_stat(self, recursive: bool = True, missing_ok: bool = True
                 ) -> Iterator[FileEntry]:  # type: ignore
        """Return an iterator of files with stat whose paths match the glob pattern."""

    @method_not_implemented
    def load(self) -> BinaryIO:  # type: ignore
        """Read all content in binary."""

    @method_not_implemented
    def save(self, file_object: BinaryIO):
        """Write the opened binary stream to the path."""

    @method_not_implemented
    def joinpath(self, *other_paths: "PathLike") -> str:  # type: ignore
        """Join or or more path."""

    @method_not_implemented
    def abspath(self):  # type: ignore
        """Return a normalized absolutized version of the path."""

    @method_not_implemented
    def realpath(self):  # type: ignore
        """Return the canonical path of the path."""

    @method_not_implemented
    def relpath(self, start=None):  # type: ignore
        """Return the relative path."""

    @method_not_implemented
    def is_absolute(self) -> bool:  # type: ignore
        """Return True if the path is an absolute pathname."""

    @method_not_implemented
    def is_mount(self) -> bool:  # type: ignore
        """Return True if the path is a mount point."""

    @method_not_implemented
    def resolve(self):  # type: ignore
        """Alias of realpath."""

    def touch(self):
        with self.open('w'):
            pass

    # will be deleted in next version
    def is_link(self) -> bool:
        return self.is_symlink()

    def makedirs(self, exist_ok: bool = False) -> None:
        self.mkdir(exist_ok=exist_ok)

    # pytype: enable=bad-return-type


PathLike = Union[str, BasePath, _PathLike]


class BaseURIPath(BasePath):

    # #####
    # Backwards compatible API, will be removed in megfile 1.0
    @classmethod
    def get_protocol(self) -> Optional[str]:
        pass  # pragma: no cover

    @classproperty
    def protocol(cls) -> str:
        return cls.get_protocol()

    def make_uri(self) -> str:
        return self.path_with_protocol

    def as_uri(self) -> str:
        return self.make_uri()

    # #####

    @cachedproperty
    def path_with_protocol(self) -> str:
        path = self.path
        if path.startswith(self.anchor):
            return path
        return self.anchor + path.lstrip('/')

    @cachedproperty
    def path_without_protocol(self) -> str:
        path = self.path
        if path.startswith(self.anchor):
            path = path[len(self.anchor):]
        return path

    def as_posix(self) -> str:
        return self.path_with_protocol

    @classmethod
    def from_path(cls, path: str) -> "BaseURIPath":
        return cls(path)

    @classmethod
    def from_uri(cls, path: str) -> "BaseURIPath":
        if path[:len(cls.anchor)] != cls.anchor:
            raise ValueError(
                "protocol not match, expected: %r, got: %r" %
                (cls.protocol, path))
        return cls.from_path(path[len(cls.anchor):])

    def __fspath__(self) -> str:
        return self.as_uri()

    def __lt__(self, other_path: "BaseURIPath") -> bool:
        if not isinstance(other_path, BaseURIPath):
            raise TypeError("%r is not 'URIPath'" % other_path)
        if self.protocol != other_path.protocol:
            raise TypeError(
                "'<' not supported between instances of %r and %r" %
                (type(self), type(other_path)))
        return fspath(self) < fspath(other_path)

    def __le__(self, other_path: "BaseURIPath") -> bool:
        if not isinstance(other_path, BaseURIPath):
            raise TypeError("%r is not 'URIPath'" % other_path)
        if self.protocol != other_path.protocol:
            raise TypeError(
                "'<=' not supported between instances of %r and %r" %
                (type(self), type(other_path)))
        return str(self) <= str(other_path)

    def __gt__(self, other_path: "BaseURIPath") -> bool:
        if not isinstance(other_path, BaseURIPath):
            raise TypeError("%r is not 'URIPath'" % other_path)
        if self.protocol != other_path.protocol:
            raise TypeError(
                "'>' not supported between instances of %r and %r" %
                (type(self), type(other_path)))
        return str(self) > str(other_path)

    def __ge__(self, other_path: "BaseURIPath") -> bool:
        if not isinstance(other_path, BaseURIPath):
            raise TypeError("%r is not 'URIPath'" % other_path)
        if self.protocol != other_path.protocol:
            raise TypeError(
                "'>=' not supported between instances of %r and %r" %
                (type(self), type(other_path)))
        return str(self) >= str(other_path)

    @classproperty
    def drive(self) -> str:
        return ''

    @classproperty
    def root(self) -> str:
        return self.protocol + '://'

    @classproperty
    def anchor(self) -> str:
        return self.root


class URIPath(BaseURIPath):

    def __init__(self, path: "PathLike", *other_paths: "PathLike"):
        if len(other_paths) > 0:
            path = self.from_path(path).joinpath(*other_paths)
        self.path = str(path)

    def __truediv__(self, other_path: PathLike) -> "BaseURIPath":
        if isinstance(other_path, BaseURIPath):
            if self.protocol != other_path.protocol:
                raise TypeError(
                    "'/' not supported between instances of %r and %r" %
                    (type(self), type(other_path)))
        elif not isinstance(other_path, str):
            raise TypeError("%r is not 'str' nor 'URIPath'" % other_path)
        return self.joinpath(other_path)

    def joinpath(self, *other_paths: PathLike) -> "BaseURIPath":
        return self.from_path(uri_join(str(self), *map(str, other_paths)))

    @cachedproperty
    def parts(self) -> Tuple[str]:
        parts = [self.root]
        path = self.path_without_protocol
        path = path.lstrip('/')
        if path != '':
            parts.extend(path.split('/'))
        return tuple(parts)

    @cachedproperty
    def parents(self) -> "URIPathParents":
        return URIPathParents(self)

    @cachedproperty
    def parent(self) -> "BaseURIPath":
        parts = self.parts[1:]
        if len(parts) == 0:
            return self
        return self.parents[0]

    @cachedproperty
    def name(self) -> str:
        parts = self.parts
        if len(parts) == 1:
            return ''
        return parts[-1]

    @cachedproperty
    def suffix(self) -> str:
        name = self.name
        i = name.rfind('.')
        if 0 < i < len(name) - 1:
            return name[i:]
        return ''

    @cachedproperty
    def suffixes(self) -> List[str]:
        name = self.name
        if name.endswith('.'):
            return []
        name = name.lstrip('.')
        return ['.' + suffix for suffix in name.split('.')[1:]]

    @cachedproperty
    def stem(self) -> str:
        name = self.name
        i = name.rfind('.')
        if 0 < i < len(name) - 1:
            return name[:i]
        return name

    def is_reserved(self) -> bool:
        return False

    def match(self, pattern) -> bool:
        match = _compile_pattern(pattern)
        for index in range(len(self.parts), 0, -1):
            path = '/'.join(self.parts[index:])
            if match(path) is not None:
                return True
        return match(self.path_with_protocol) is not None

    def relative_to(self, other) -> "BaseURIPath":
        if not other:
            raise TypeError("need at least one argument")
        if not isinstance(other, str):
            raise TypeError("%r is not 'str'" % (type(other)))

        path = self.path_without_protocol
        if other.startswith(self.root):
            path = self.path_with_protocol

        if path.startswith(other):
            relative = path[len(other):]
            relative = relative.lstrip('/')
            return type(self)(relative)
        else:
            raise ValueError("%r does not start with %r" % (path, other))

    def with_name(self, name) -> "BaseURIPath":
        path = str(self)
        raw_name = self.name
        return type(self)(path[:len(path) - len(raw_name)] + name)

    def with_suffix(self, suffix) -> "BaseURIPath":
        path = str(self)
        raw_suffix = self.suffix
        return type(self)(path[:len(path) - len(raw_suffix)] + suffix)

    def is_absolute(self) -> bool:
        return True

    def is_mount(self) -> bool:
        return False

    def abspath(self) -> str:
        return self.path_with_protocol

    def realpath(self) -> str:
        return self.path_with_protocol

    def relpath(self, start=None) -> str:
        return self.path_with_protocol

    def resolve(self):
        return self.path_with_protocol


class URIPathParents(Sequence):

    def __init__(self, path):
        # We don't store the instance to avoid reference cycles
        self.cls = type(path)
        self.parts = path.parts[1:]
        self.prefix = path.parts[0]
        if not str(path).startswith(path.parts[0]):
            if str(path).startswith('/'):
                self.prefix = '/'
            else:
                self.prefix = ''

    def __len__(self):
        return len(self.parts) - 1

    def __getitem__(self, idx):
        if idx < 0 or idx > len(self):
            raise IndexError(idx)
        return self.cls(self.prefix + '/'.join(self.parts[:-idx - 1]))

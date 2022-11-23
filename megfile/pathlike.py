import os
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
    def glob(self, pattern, recursive: bool = True,
             missing_ok: bool = True) -> List['BasePath']:  # type: ignore
        """Return files whose paths match the glob pattern."""

    @method_not_implemented
    def iglob(self, pattern, recursive: bool = True,
              missing_ok: bool = True) -> Iterator['BasePath']:  # type: ignore
        """Return an iterator of files whose paths match the glob pattern."""

    @method_not_implemented
    def glob_stat(
            self, pattern, recursive: bool = True,
            missing_ok: bool = True) -> Iterator[FileEntry]:  # type: ignore
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
        if self.path_without_protocol == "/":
            return self
        elif len(self.parents) > 0:
            return self.parents[0]
        return self.from_path("")

    @cachedproperty
    def name(self) -> str:
        parts = self.parts
        if len(parts) == 1 and parts[0] == self.root:
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

    def is_relative_to(self, *other) -> bool:
        try:
            self.relative_to(*other)
            return True
        except Exception:
            return False

    def relative_to(self, *other) -> "BaseURIPath":
        if not other:
            raise TypeError("need at least one argument")
        other = str(self.from_path(other[0]).joinpath(*other[1:]))

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
        return self.from_path(path[:len(path) - len(raw_name)] + name)

    def with_stem(self, stem) -> "BaseURIPath":
        return self.with_name("".join([stem, self.suffix]))

    def with_suffix(self, suffix) -> "BaseURIPath":
        path = str(self)
        raw_suffix = self.suffix
        return self.from_path(path[:len(path) - len(raw_suffix)] + suffix)

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

    def lstat(self) -> StatResult:
        return self.stat(followlinks=False)

    def chmod(self, mode: int, *, follow_symlinks: bool = True):
        raise NotImplementedError

    def lchmod(self, mode: int):
        '''
        Like chmod() but, if the path points to a symbolic link, the symbolic link’s mode is changed rather than its target’s.
        '''
        return self.chmod(mode=mode, follow_symlinks=False)

    def read_bytes(self) -> bytes:
        with self.open(mode='rb') as f:
            return f.read()

    def read_text(self) -> str:
        with self.open(mode='r') as f:
            return f.read()

    def rename(self, dst_path: PathLike) -> 'URIPath':
        raise NotImplementedError

    def replace(self, dst_path: PathLike) -> None:
        '''
        move file

        :param dst_path: Given destination path
        '''
        self.rename(dst_path=dst_path)

    def rglob(self, patten) -> List['URIPath']:
        if not patten:
            patten = ""
        patten += '**/'
        return self.glob(patten=patten)

    def md5(self, recalculate: bool = False, followlinks: bool = False) -> str:
        raise NotImplementedError

    def samefile(self, other_path) -> bool:
        '''
        Compare files have the same md5
        '''
        from megfile.smart_path import SmartPath
        other_path = SmartPath(other_path)
        return self.md5(recalculate=True) == other_path.md5(recalculate=True)

    def symlink(self, dst_path: PathLike) -> None:
        raise NotImplementedError

    def symlink_to(self, target, target_is_directory=False):
        '''
        Make this path a symbolic link to target. 
        Target_is_directory’s value is ignored, only be compatible with pathlib.Path
        '''
        return self.symlink(dst_path=target)

    def write_bytes(self, data: bytes):
        with self.open(mode='wb') as f:
            f.write(data)

    def write_text(self, data: str, encoding=None, errors=None, newline=None):
        with self.open(mode='w', encoding=encoding, errors=errors,
                       newline=newline) as f:
            return f.write(data)


class URIPathParents(Sequence):

    def __init__(self, path):
        # We don't store the instance to avoid reference cycles
        self.cls = type(path)
        parts = path.parts
        if len(parts) > 0 and parts[0] == path.root:
            self.prefix = parts[0]
            self.parts = parts[1:]
        else:
            self.prefix = ''
            self.parts = parts

    def __len__(self):
        return max(len(self.parts) - 1, 0)

    def __getitem__(self, idx):
        if idx < 0 or idx > len(self):
            raise IndexError(idx)

        if len(self.parts[:-idx - 1]) > 1:
            other_path = os.path.join(*self.parts[:-idx - 1])
        elif len(self.parts[:-idx - 1]) == 1:
            other_path = self.parts[:-idx - 1][0]
        else:
            other_path = ""
        return self.cls(self.prefix + other_path)

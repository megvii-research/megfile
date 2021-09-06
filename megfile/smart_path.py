from pathlib import PurePath
from typing import Tuple
from urllib.parse import urlsplit

from megfile.lib.compat import fspath

from .errors import ProtocolExistsError, ProtocolNotFoundError
from .interfaces import BasePath, BaseURIPath, PathLike


def _bind_function(name):

    def smart_method(self, *args, **kwargs):
        return getattr(self.pathlike, name)(*args, **kwargs)

    smart_method.__name__ = name

    return smart_method


def _bind_property(name):

    @property
    def smart_property(self):
        return getattr(self.pathlike, name)

    return smart_property


class SmartPath(BasePath):
    _registered_protocols = dict()

    def __init__(self, path: PathLike, *other_paths: PathLike):
        self.path = str(path)
        pathlike = path
        if not isinstance(pathlike, BaseURIPath):
            pathlike = self._create_pathlike(path)
        if len(other_paths) > 0:
            pathlike = pathlike.joinpath(*other_paths)
            self.path = str(pathlike)
        self.pathlike = pathlike

    @staticmethod
    def _extract_protocol(path: PathLike) -> Tuple[str, str]:
        if isinstance(path, str):
            protocol = urlsplit(path).scheme
            if protocol == "":
                protocol = "file"
                path_without_protocol = path
            else:
                path_without_protocol = path[len(protocol) + 3:]
        elif isinstance(path, (BaseURIPath, SmartPath)):
            protocol = path.protocol
            path_without_protocol = str(path)
        elif isinstance(path, PurePath):
            protocol = "file"
            path_without_protocol = str(path)
        else:
            raise ProtocolNotFoundError('protocol not found: %r' % path)
        return protocol, path_without_protocol

    @classmethod
    def _create_pathlike(cls, path: PathLike) -> BasePath:
        protocol, path_without_protocol = cls._extract_protocol(path)
        if protocol not in cls._registered_protocols:
            raise ProtocolNotFoundError(
                'protocol %r not found: %r' % (protocol, path))
        path_class = cls._registered_protocols[protocol]
        return path_class(path_without_protocol)

    @classmethod
    def register(cls, path_class, override_ok: bool = False):
        protocol = path_class.protocol
        if protocol in cls._registered_protocols and not override_ok:
            raise ProtocolExistsError('protocol already exists: %r' % protocol)
        cls._registered_protocols[protocol] = path_class
        return path_class

    is_dir = _bind_function('is_dir')
    is_file = _bind_function('is_file')
    is_symlink = _bind_function('is_symlink')
    access = _bind_function('access')
    exists = _bind_function('exists')
    listdir = _bind_function('listdir')
    scandir = _bind_function('scandir')
    getsize = _bind_function('getsize')
    getmtime = _bind_function('getmtime')
    stat = _bind_function('stat')
    remove = _bind_function('remove')
    rmdir = _bind_function('remove')
    rename = _bind_function('rename')
    replace = _bind_function('replace')
    unlink = _bind_function('unlink')
    mkdir = _bind_function('makedirs')
    open = _bind_function('open')
    touch = _bind_function('touch')
    walk = _bind_function('walk')
    scan = _bind_function('scan')
    scan_stat = _bind_function('scan_stat')
    glob = _bind_function('glob')
    iglob = _bind_function('iglob')
    glob_stat = _bind_function('glob_stat')
    load = _bind_function('load')
    save = _bind_function('save')
    joinpath = _bind_function('joinpath')
    abspath = _bind_function('abspath')
    realpath = _bind_function('realpath')
    relpath = _bind_function('relpath')
    is_absolute = _bind_function('is_absolute')
    is_mount = _bind_function('is_mount')
    md5 = _bind_function('md5')

    @property
    def protocol(self) -> str:
        return self.pathlike.protocol

    @classmethod
    def from_uri(cls, path: str):
        return cls(path)

    as_uri = _bind_function('as_uri')
    as_posix = _bind_function('as_posix')
    __lt__ = _bind_function('__lt__')
    __le__ = _bind_function('__le__')
    __gt__ = _bind_function('__gt__')
    __ge__ = _bind_function('__ge__')
    __fspath__ = _bind_function('__fspath__')
    __truediv__ = _bind_function('__truediv__')

    joinpath = _bind_function('joinpath')
    is_reserved = _bind_function('is_reserved')
    match = _bind_function('match')
    relative_to = _bind_function('relative_to')
    with_name = _bind_function('with_name')
    with_suffix = _bind_function('with_suffix')
    is_absolute = _bind_function('is_absolute')
    is_mount = _bind_function('is_mount')
    abspath = _bind_function('abspath')
    realpath = _bind_function('realpath')
    relpath = _bind_function('relpath')

    drive = _bind_property('drive')
    root = _bind_property('root')
    anchor = _bind_property('anchor')
    parts = _bind_property('parts')
    parents = _bind_property('parents')
    parent = _bind_property('parent')
    name = _bind_property('name')
    suffix = _bind_property('suffix')
    suffixes = _bind_property('suffixes')
    stem = _bind_property('stem')


def get_traditional_path(path: str):
    return fspath(SmartPath(path).path)

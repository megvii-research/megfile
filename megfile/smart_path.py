import os
from functools import cached_property
from pathlib import PurePath
from typing import Dict, Optional, Tuple, Union

from megfile.config import load_megfile_config
from megfile.lib.compat import fspath
from megfile.lib.url import get_url_scheme
from megfile.pathlike import URIPathParents
from megfile.utils import cached_classproperty

from .config import CaseSensitiveConfigParser
from .errors import ProtocolExistsError, ProtocolNotFoundError
from .interfaces import BasePath, PathLike

LEGACY_ALIASES_CONFIG = "~/.config/megfile/aliases.conf"


def _bind_function(name, after_callback=None, before_callback=None):
    if before_callback is None and after_callback is None:

        def smart_method(self, *args, **kwargs):
            return getattr(self.pathlike, name)(*args, **kwargs)

    else:

        def smart_method(self, *args, **kwargs):
            if before_callback is not None and len(args) > 0:
                first_arg = before_callback(self, args[0])
                args = (first_arg, *args[1:])
            result = getattr(self.pathlike, name)(*args, **kwargs)
            if after_callback is not None:
                return after_callback(self, result)
            return result

    smart_method.__name__ = name
    smart_method.__doc__ = f"Dynamically bound method for {name}"

    return smart_method


def _bind_property(name, callback=None):
    if callback is None:

        @property
        def smart_property(self):
            return getattr(self.pathlike, name)

    else:

        @property
        def smart_property(self):
            return callback(self, getattr(self.pathlike, name))

    return smart_property


def _load_aliases_config() -> Dict[str, Dict[str, str]]:
    configs = {}
    config_path = os.path.expanduser(LEGACY_ALIASES_CONFIG)
    if os.path.isfile(config_path):
        parser = CaseSensitiveConfigParser()
        parser.read(config_path)
        for section in parser.sections():
            configs[section] = dict(parser.items(section))
    for name, protocol_or_path in load_megfile_config("alias").items():
        if "://" in protocol_or_path:
            protocol, prefix = protocol_or_path.split("://", maxsplit=1)
            configs[name] = {"protocol": protocol, "prefix": prefix}
        else:
            configs[name] = {"protocol": protocol_or_path}
    return configs


def _to_aliased_path(pathlike, other_path: str) -> str:
    """Convert path string to aliased path string"""
    if pathlike.protocol == pathlike._unaliased_protocol:
        return other_path
    aliases: Dict[str, Dict[str, str]] = pathlike._aliases
    unaliased_prefix = aliases[pathlike.protocol].get("prefix", "")
    unaliased_prefix = "%s://%s" % (pathlike._unaliased_protocol, unaliased_prefix)
    if not other_path.startswith(unaliased_prefix):
        return other_path
    path_without_protocol = other_path[len(unaliased_prefix) :]
    return f"{pathlike.protocol}://{path_without_protocol}"


def _to_aliased_pathlike(pathlike, other_pathlike) -> BasePath:
    """Convert pathlike object to aliased SmartPath object"""
    other_path = str(other_pathlike)
    if pathlike.protocol != pathlike._unaliased_protocol:
        other_path = _to_aliased_path(pathlike, other_path)
    return SmartPath(other_path)


def _to_aliased_path_list(pathlike, other_paths):
    """Convert list of path strings to aliased path strings"""
    return [_to_aliased_path(pathlike, s) for s in other_paths]


def _to_aliased_pathlike_list(pathlike, other_pathlikes):
    """Convert list of pathlike objects to aliased SmartPath objects"""
    return [_to_aliased_pathlike(pathlike, p) for p in other_pathlikes]


def _to_aliased_path_iterator(pathlike, other_paths):
    """Convert iterator of path strings to aliased path strings"""
    for s in other_paths:
        yield _to_aliased_path(pathlike, s)


def _to_aliased_pathlike_iterator(pathlike, other_pathlikes):
    """Convert iterator of pathlike objects to aliased SmartPath objects"""
    for p in other_pathlikes:
        yield _to_aliased_pathlike(pathlike, p)


def _to_aliased_file_entry_iterator(pathlike, file_entries):
    """Convert iterator of FileEntry objects with aliased paths"""
    for entry in file_entries:
        yield entry._replace(path=_to_aliased_path(pathlike, entry.path))


def _to_aliased_walk_iterator(pathlike, walk_iterator):
    """Convert walk iterator with aliased paths"""
    for dirpath, dirnames, filenames in walk_iterator:
        aliased_dirpath = _to_aliased_path(pathlike, dirpath)
        yield (aliased_dirpath, dirnames, filenames)


def _to_unaliased_path(pathlike, path):
    """Convert path string to unaliased path string"""
    aliases: Dict[str, Dict[str, str]] = pathlike._aliases
    protocol, path_without_protocol = pathlike._split_protocol(path)
    if protocol in aliases:
        prefix = aliases[protocol].get("prefix", "")
        protocol = aliases[protocol]["protocol"]
        return "%s://%s%s" % (protocol, prefix, path_without_protocol)
    return path


class SmartPath(BasePath):
    _registered_protocols = dict()

    def __init__(self, path: Union[PathLike, int], *other_paths: PathLike):
        self.path = str(path) if not isinstance(path, int) else path
        self.protocol = self._extract_protocol(path)
        self._unaliased_path = _to_unaliased_path(self, path)
        self._unaliased_protocol = self._extract_protocol(self._unaliased_path)

        pathlike = path
        if not isinstance(pathlike, BasePath):
            pathlike = self._create_pathlike(self._unaliased_path)
        if len(other_paths) > 0:
            pathlike = pathlike.joinpath(*other_paths)
            self.path = str(pathlike)
        self.pathlike = pathlike

    @cached_classproperty
    def _aliases(cls) -> Dict[str, Dict[str, str]]:
        return _load_aliases_config()

    @classmethod
    def _split_protocol(cls, path: Union[PathLike, int]) -> Tuple[str, Union[str, int]]:
        if isinstance(path, int):
            return "file", path
        elif isinstance(path, str):
            protocol = get_url_scheme(path)
            if not protocol:
                protocol = "file"
                path_without_protocol = path
            else:
                path_without_protocol = path[len(protocol) + 3 :]
            return protocol, path_without_protocol
        elif isinstance(path, (BasePath, SmartPath)):
            return str(path.protocol), path.path_without_protocol
        elif isinstance(path, (PurePath, BasePath)):
            return SmartPath._split_protocol(fspath(path))
        raise ProtocolNotFoundError("protocol not found: %r" % path)

    @classmethod
    def _extract_protocol(cls, path: Union[PathLike, int]) -> str:
        return cls._split_protocol(path)[0]

    @classmethod
    def _create_pathlike(cls, path: Union[PathLike, int]) -> BasePath:
        protocol = cls._extract_protocol(path)
        if protocol.startswith("s3+"):
            protocol = "s3"
        if protocol not in cls._registered_protocols:
            raise ProtocolNotFoundError("protocol %r not found: %r" % (protocol, path))
        path_class = cls._registered_protocols[protocol]
        return path_class(path)

    @classmethod
    def register(cls, path_class, override_ok: bool = False):
        protocol = path_class.protocol
        if protocol in cls._registered_protocols and not override_ok:
            raise ProtocolExistsError("protocol already exists: %r" % protocol)
        cls._registered_protocols[protocol] = path_class
        return path_class

    @classmethod
    def from_uri(cls, path: PathLike):
        return cls(path)

    def relpath(self, start: Optional[str] = None) -> str:
        """Return the relative path of given path

        :param start: Given start directory
        :returns: Relative path from start
        """
        if start is not None:
            start = _to_unaliased_path(self, start)
        return self.pathlike.relpath(start=start)

    @cached_property
    def parts(self) -> Tuple[str, ...]:
        """A tuple giving access to the path’s various components"""
        parts = self.pathlike.parts
        parts = (_to_aliased_path(self, parts[0]), *parts[1:])
        return parts

    @cached_property
    def parents(self) -> "URIPathParents":
        """
        An immutable sequence providing access to the logical ancestors of the path
        """
        return URIPathParents(self)

    symlink = _bind_function("symlink", before_callback=_to_unaliased_path)
    symlink_to = _bind_function("symlink_to", before_callback=_to_unaliased_path)
    hardlink_to = _bind_function("hardlink_to", before_callback=_to_unaliased_path)
    readlink = _bind_function("readlink", _to_aliased_pathlike)
    is_dir = _bind_function("is_dir")
    is_file = _bind_function("is_file")
    is_symlink = _bind_function("is_symlink")
    access = _bind_function("access")
    exists = _bind_function("exists")
    listdir = _bind_function("listdir", _to_aliased_path_list)
    scandir = _bind_function("scandir", _to_aliased_file_entry_iterator)
    getsize = _bind_function("getsize")
    getmtime = _bind_function("getmtime")
    stat = _bind_function("stat")
    lstat = _bind_function("lstat")
    remove = _bind_function("remove")
    rename = _bind_function("rename", _to_aliased_pathlike, _to_unaliased_path)
    replace = _bind_function("replace", _to_aliased_pathlike, _to_unaliased_path)
    unlink = _bind_function("unlink")
    mkdir = _bind_function("mkdir")
    open = _bind_function("open")
    touch = _bind_function("touch")
    walk = _bind_function("walk", _to_aliased_walk_iterator)
    scan = _bind_function("scan", _to_aliased_path_iterator)
    scan_stat = _bind_function("scan_stat", _to_aliased_file_entry_iterator)
    glob = _bind_function("glob", _to_aliased_pathlike_list)
    iglob = _bind_function("iglob", _to_aliased_pathlike_iterator)
    glob_stat = _bind_function("glob_stat", _to_aliased_file_entry_iterator)
    load = _bind_function("load")
    save = _bind_function("save")
    joinpath = _bind_function("joinpath", _to_aliased_pathlike)
    abspath = _bind_function("abspath", _to_aliased_path)
    realpath = _bind_function("realpath", _to_aliased_path)
    is_absolute = _bind_function("is_absolute")
    is_mount = _bind_function("is_mount")
    md5 = _bind_function("md5")

    as_uri = _bind_function("as_uri", _to_aliased_path)
    as_posix = _bind_function("as_posix", _to_aliased_path)
    __fspath__ = _bind_function("__fspath__", _to_aliased_path)
    __truediv__ = _bind_function("__truediv__", _to_aliased_pathlike)

    is_reserved = _bind_function("is_reserved")
    match = _bind_function("match", before_callback=_to_unaliased_path)
    relative_to = _bind_function("relative_to", before_callback=_to_unaliased_path)
    with_name = _bind_function("with_name", _to_aliased_pathlike)
    with_suffix = _bind_function("with_suffix", _to_aliased_pathlike)
    with_stem = _bind_function("with_stem", _to_aliased_pathlike)
    iterdir = _bind_function("iterdir", _to_aliased_pathlike_iterator)
    cwd = _bind_function("cwd", _to_aliased_pathlike)
    home = _bind_function("home")
    expanduser = _bind_function("expanduser")
    resolve = _bind_function("resolve", _to_aliased_pathlike)
    chmod = _bind_function("chmod")
    lchmod = _bind_function("lchmod")
    group = _bind_function("group")
    is_socket = _bind_function("is_socket")
    is_fifo = _bind_function("is_fifo")
    is_block_device = _bind_function("is_block_device")
    is_char_device = _bind_function("is_char_device")
    owner = _bind_function("owner")
    absolute = _bind_function("absolute", _to_aliased_pathlike)
    rmdir = _bind_function("rmdir")
    is_relative_to = _bind_function(
        "is_relative_to", before_callback=_to_unaliased_path
    )
    read_bytes = _bind_function("read_bytes")
    read_text = _bind_function("read_text")
    rglob = _bind_function("rglob", _to_aliased_pathlike_list)
    samefile = _bind_function("samefile")
    write_bytes = _bind_function("write_bytes")
    write_text = _bind_function("write_text")
    utime = _bind_function("utime")

    drive = _bind_property("drive")
    root = _bind_property("root", _to_aliased_path)
    anchor = _bind_property("anchor", _to_aliased_path)
    parent = _bind_property("parent", _to_aliased_pathlike)
    name = _bind_property("name")
    suffix = _bind_property("suffix")
    suffixes = _bind_property("suffixes")
    stem = _bind_property("stem")


def get_traditional_path(path: PathLike) -> str:
    return fspath(SmartPath(path).pathlike.path)

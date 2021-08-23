import io
import os
from functools import wraps
from typing import IO, AnyStr, BinaryIO

from megfile import fs

from .interfaces import MegfilePathLike, URIPath
from .lib.compat import fspath
from .lib.joinpath import path_join
from .smart_path import SmartPath


def _bind_function(name):

    @wraps(getattr(fs, name))
    def fs_method(self, *args, **kwargs):
        return getattr(fs, name)(self.path_without_protocol, *args, **kwargs)

    return fs_method


@SmartPath.register
class FSPath(URIPath):
    """file protocol
    e.g. file:///data/test/ or /data/test
    """

    protocol = "file"

    def __fspath__(self) -> str:
        return os.path.normpath(self.path_without_protocol)

    @classmethod
    def from_uri(cls, path: str) -> "FSPath":
        return cls.from_path(path)

    @property
    def path_with_protocol(self) -> str:
        if self.path.startswith(self.anchor):
            return self.path
        return self.anchor + self.path

    abspath = _bind_function('fs_abspath')
    access = _bind_function('fs_access')
    exists = _bind_function('fs_exists')
    getmtime = _bind_function('fs_getmtime')
    getsize = _bind_function('fs_getsize')
    glob = _bind_function('fs_glob')
    glob_stat = _bind_function('fs_glob_stat')
    expanduser = _bind_function('fs_expanduser')
    iglob = _bind_function('fs_iglob')
    is_absolute = _bind_function('fs_isabs')
    is_dir = _bind_function('fs_isdir')
    is_file = _bind_function('fs_isfile')
    is_symlink = _bind_function('fs_islink')
    is_mount = _bind_function('fs_ismount')
    listdir = _bind_function('fs_listdir')
    load = _bind_function('fs_load_from')
    mkdir = _bind_function('fs_makedirs')
    realpath = _bind_function('fs_realpath')
    relpath = _bind_function('fs_relpath')
    remove = _bind_function('fs_remove')
    rename = _bind_function('fs_move')
    replace = _bind_function('fs_move')
    rmdir = _bind_function('fs_remove')
    scan = _bind_function('fs_scan')
    scan_stat = _bind_function('fs_scan_stat')
    scandir = _bind_function('fs_scandir')
    stat = _bind_function('fs_stat')
    unlink = _bind_function('fs_unlink')
    walk = _bind_function('fs_walk')
    resolve = _bind_function('fs_resolve')
    md5 = _bind_function('fs_getmd5')
    copy = _bind_function('fs_copy')
    sync = _bind_function('fs_sync')
    cwd = fs.fs_cwd
    home = fs.fs_home

    def joinpath(self, *other_paths: MegfilePathLike) -> "FSPath":
        path = fspath(self)
        if path == '.':
            path = ''
        return self.from_path(path_join(path, *map(fspath, other_paths)))

    def save(self, file_object: BinaryIO):
        return fs.fs_save_as(file_object, self.path)

    def open(self, mode: str, **kwargs) -> IO[AnyStr]:
        if 'w' in mode or 'x' in mode or 'a' in mode:
            fs.fs_makedirs(os.path.dirname(self.path), exist_ok=True)
        return io.open(self.path, mode)
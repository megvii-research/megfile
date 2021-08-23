from functools import wraps
from typing import IO, AnyStr, BinaryIO, Callable

from megfile import s3

from .interfaces import URIPath
from .smart_path import SmartPath
from .utils import necessary_params

__all__ = [
    'S3Path',
]


def _bind_function(name):

    @wraps(getattr(s3, name))
    def s3_method(self, *args, **kwargs):
        return getattr(s3, name)(self.path_with_protocol, *args, **kwargs)

    return s3_method


@SmartPath.register
class S3Path(URIPath):

    protocol = "s3"

    access = _bind_function('s3_access')
    exists = _bind_function('s3_exists')
    getmtime = _bind_function('s3_getmtime')
    getsize = _bind_function('s3_getsize')
    glob = _bind_function('s3_glob')
    glob_stat = _bind_function('s3_glob_stat')
    iglob = _bind_function('s3_iglob')
    is_dir = _bind_function('s3_isdir')
    is_file = _bind_function('s3_isfile')
    listdir = _bind_function('s3_listdir')
    load = _bind_function('s3_load_from')
    mkdir = _bind_function('s3_makedirs')
    move = _bind_function('s3_move')
    remove = _bind_function('s3_remove')
    rename = _bind_function('s3_rename')
    rmdir = _bind_function('s3_remove')
    scan = _bind_function('s3_scan')
    scan_stat = _bind_function('s3_scan_stat')
    scandir = _bind_function('s3_scandir')
    stat = _bind_function('s3_stat')
    unlink = _bind_function('s3_unlink')
    walk = _bind_function('s3_walk')
    md5 = _bind_function('s3_getmd5')
    copy = _bind_function('s3_copy')
    sync = _bind_function('s3_sync')

    def save(self, file_object: BinaryIO):
        return s3.s3_save_as(file_object, self.path_with_protocol)

    def open(
            self,
            mode: str = 'r',
            *,
            s3_open_func: Callable[[str, str], BinaryIO] = s3.s3_open,
            **kwargs) -> IO[AnyStr]:
        return s3_open_func(
            self.path_with_protocol, mode,
            **necessary_params(s3_open_func, **kwargs))

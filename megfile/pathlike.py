from enum import Enum
from functools import wraps
from os import PathLike as _PathLike
from typing import Any, NamedTuple, Union

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


PathLike = Union[str, _PathLike]

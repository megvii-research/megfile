import sys

__all__ = ['PathLike', 'fspath']

from os import PathLike
from os import fspath as _fspath


def fspath(path) -> str:
    result = _fspath(path)
    if isinstance(result, bytes):
        return result.decode()
    return result

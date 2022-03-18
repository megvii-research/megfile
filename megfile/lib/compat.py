import sys

__all__ = ['fspath']

from os import fspath as _fspath


def fspath(path) -> str:
    result = _fspath(path)
    if isinstance(result, bytes):
        return result.decode()
    return result

from os import PathLike
from os import fspath as _fspath

__all__ = ['PathLike', 'fspath']


def fspath(path) -> str:
    result = _fspath(path)
    if isinstance(result, bytes):
        return result.decode()
    return result

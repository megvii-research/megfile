import sys

__all__ = ['PathLike', 'fspath']

if sys.version_info < (3, 6):  # pragma: no cover

    from pathlib import PurePath as PathLike

    def fspath(path) -> str:
        """os.fspath replacement, useful to point out when we should replace it by the
        real function once we drop py35.
        """
        if hasattr(path, '__fspath__'):
            return path.__fspath__()
        elif isinstance(path, PathLike):
            return str(path)
        elif isinstance(path, bytes):
            return path.decode()
        elif isinstance(path, str):
            return path
        raise TypeError(
            'expected str, bytes or PathLike object, not %s' %
            type(path).__name__)

else:
    from os import PathLike
    from os import fspath as _fspath

    def fspath(path) -> str:
        result = _fspath(path)
        if isinstance(result, bytes):
            return result.decode()
        return result

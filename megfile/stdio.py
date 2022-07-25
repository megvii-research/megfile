from typing import IO, AnyStr

from megfile.interfaces import PathLike
from megfile.stdio_path import StdioPath, is_stdio

__all__ = [
    'StdioPath',
    'is_stdio',
    'stdio_open',
]


def stdio_open(path: PathLike, mode: str) -> IO[AnyStr]:
    '''Used to read or write stdio

    .. note ::

        Essentially invoke sys.stdin.buffer | sys.stdout.buffer to read or write

    :param path: Given path
    :param mode: Only supports 'rb' and 'wb' now
    :return: STDReader, STDWriter
    '''
    return StdioPath(path).open(mode)

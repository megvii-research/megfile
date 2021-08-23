from typing import Union
from urllib.parse import urlsplit

from megfile.interfaces import MegfilePathLike
from megfile.lib.compat import fspath
from megfile.lib.stdio_handler import STDReader, STDWriter


def is_stdio(path: MegfilePathLike) -> bool:
    '''stdio scheme definition: stdio://-

    .. note ::

        Only tests protocol

    :param path: Path to be tested
    :returns: True of a path is stdio url, else False
    '''

    path = fspath(path)
    if not isinstance(path, str) or not path.startswith('stdio://'):
        return False

    parts = urlsplit(path)
    return parts.scheme == 'stdio'


def stdio_open(path: str, mode: str = 'rb') -> Union[STDReader, STDWriter]:
    '''Used to read or write stdio

    .. note ::

        Essentially invoke sys.stdin.buffer | sys.stdout.buffer to read or write

    :param path: stdio path, stdio://- or stdio://0 stdio://1 stdio://2
    :param mode: Only supports 'rb' and 'wb' now
    :return: STDReader, STDWriter
    '''

    if mode not in ('rb', 'wb', 'rt', 'wt', 'r', 'w'):
        raise ValueError('unacceptable mode: %r' % mode)

    if path not in ('stdio://-', 'stdio://0', 'stdio://1', 'stdio://2'):
        raise ValueError('unacceptable path: %r' % path)

    if path in ('stdio://1', 'stdio://2') and 'r' in mode:
        raise ValueError('cannot open for reading: %r' % path)

    if path == 'stdio://0' and 'w' in mode:
        raise ValueError('cannot open for writing: %r' % path)

    if 'r' in mode:
        return STDReader(mode)
    return STDWriter(path, mode)

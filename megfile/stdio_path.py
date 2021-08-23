import io
from typing import IO, AnyStr

from megfile.interfaces import BaseURIPath
from megfile.utils import get_binary_mode

from .smart_path import SmartPath
from .stdio import stdio_open


@SmartPath.register
class StdioPath(BaseURIPath):

    protocol = "stdio"

    def open(self, mode: str, **kwargs) -> IO[AnyStr]:
        binary_mode = get_binary_mode(mode)
        fileobj = stdio_open(self.path_with_protocol, binary_mode)

        if 'b' not in mode:
            fileobj = io.TextIOWrapper(fileobj)  # pytype: disable=wrong-arg-types
            fileobj.mode = mode

        return fileobj  # pytype: disable=bad-return-type

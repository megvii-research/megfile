import os
from functools import cached_property
from typing import AnyStr, Callable, Optional

from megfile.interfaces import Readable, Seekable, Writable
from megfile.utils import get_content_size


class LazyHandler(Readable, Seekable, Writable):
    """Create a File-Like Object, maintaining file pointer,
    to avoid misunderstanding the position when read / write / seek.

    It can be roughly regarded as the copy function of the file handle,
    but you need to be careful with the write handle,
    because no matter which copy will modify the data itself.
    """

    def __init__(self, path: str, mode: str, open_func: Callable, **options):
        self._open_func = open_func
        self._path = path
        self._mode = mode
        self._options = options

    @property
    def name(self) -> str:
        return self._path

    @property
    def mode(self) -> str:
        return self._mode

    @cached_property
    def _file_object(self):
        return self._open_func(self._path, self._mode, **self._options)

    @property
    def _content_size(self) -> int:
        return get_content_size(self._file_object)

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return self._file_object.seek(offset, whence)

    def tell(self) -> int:
        return self._file_object.tell()

    def readable(self) -> bool:
        return self._file_object.readable()

    def read(self, size: Optional[int] = None) -> AnyStr:  # pyre-ignore[34]
        return self._file_object.read(size)

    def readline(self, size: Optional[int] = None) -> AnyStr:  # pyre-ignore[34]
        return self._file_object.readline(size)

    def writable(self) -> bool:
        return self._file_object.writable()

    def write(self, data: AnyStr):
        return self._file_object.write(data)

    def _close(self):
        return self._file_object.close()

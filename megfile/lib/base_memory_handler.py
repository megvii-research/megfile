import os
from abc import ABC, abstractmethod
from io import BytesIO, UnsupportedOperation
from typing import Iterable, List, Optional

from megfile.interfaces import Readable, Seekable, Writable


class BaseMemoryHandler(Readable[bytes], Seekable, Writable[bytes], ABC):
    def __init__(
        self,
        mode: str,
        *,
        atomic: bool = False,
    ):
        self._mode = mode

        if mode not in ("rb", "wb", "ab", "rb+", "wb+", "ab+"):
            raise ValueError("unacceptable mode: %r" % mode)

        self._fileobj = BytesIO()
        self._download_fileobj()

        if atomic:
            self.__atomic__ = True

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @property
    def mode(self) -> str:
        return self._mode

    def tell(self) -> int:
        return self._fileobj.tell()

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        return self._fileobj.seek(offset, whence)

    def readable(self) -> bool:
        return self._mode[0] == "r" or "+" in self._mode

    def read(self, size: Optional[int] = None) -> bytes:
        if not self.readable():
            raise UnsupportedOperation("not readable")
        return self._fileobj.read(size)

    def readline(self, size: Optional[int] = None) -> bytes:
        if not self.readable():
            raise UnsupportedOperation("not readable")
        if size is None:
            size = -1
        return self._fileobj.readline(size)

    def readlines(self, hint: Optional[int] = None) -> List[bytes]:
        if not self.readable():
            raise UnsupportedOperation("not readable")
        if hint is None:
            hint = -1
        return self._fileobj.readlines(hint)

    def writable(self) -> bool:
        return self._mode[0] == "w" or self._mode[0] == "a" or "+" in self._mode

    def flush(self):
        self._fileobj.flush()

    def write(self, data: bytes) -> int:
        if not self.writable():
            raise UnsupportedOperation("not writable")
        if self._mode[0] == "a":
            self.seek(0, os.SEEK_END)
        return self._fileobj.write(data)

    def writelines(self, lines: Iterable[bytes]):
        if not self.writable():
            raise UnsupportedOperation("not writable")
        if self._mode[0] == "a":
            self.seek(0, os.SEEK_END)
        self._fileobj.writelines(lines)

    @abstractmethod
    def _download_fileobj(self):
        pass

    @abstractmethod
    def _upload_fileobj(self):
        pass

    def _close(self, need_upload: bool = True):
        if hasattr(self, "_fileobj"):
            need_upload = need_upload and self.writable()
            if need_upload:
                self._upload_fileobj()
            self._fileobj.close()

    def _abort(self):
        if hasattr(self, "_fileobj"):
            self._fileobj.close()

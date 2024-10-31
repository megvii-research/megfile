import os
from abc import ABC, abstractmethod
from io import IOBase, UnsupportedOperation
from typing import IO, AnyStr, Iterable, List, Optional

from megfile.pathlike import (
    Access,
    BasePath,
    FileEntry,
    PathLike,
    Self,
    StatResult,
    URIPath,
)

__all__ = [
    "Access",
    "BasePath",
    "FileEntry",
    "PathLike",
    "StatResult",
    "fullname",
    "Closable",
    "FileLike",
    "Seekable",
    "Readable",
    "Writable",
    "FileCacher",
    "NullCacher",
    "ContextIterator",
    "URIPath",
]


def fullname(o):
    klass = o.__class__
    module = klass.__module__
    if module == "builtins":
        return klass.__qualname__  # avoid outputs like 'builtins.str'
    return module + "." + klass.__qualname__


# 1. Default value of closed is False
# 2. closed is set to True when close() are called
# 3. close() will only be called once
class Closable(ABC):
    @property
    def closed(self) -> bool:
        """Return True if the file-like object is closed."""
        return getattr(self, "__closed__", False)

    @abstractmethod
    def _close(self) -> None:
        pass  # pragma: no cover

    def close(self) -> None:
        """Flush and close the file-like object.

        This method has no effect if the file is already closed.
        """
        if not getattr(self, "__closed__", False):
            self._close()
            setattr(self, "__closed__", True)

    def __enter__(self: Self) -> Self:
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()


class FileLike(Closable, IOBase, IO[AnyStr], ABC):  # pytype: disable=signature-mismatch
    def fileno(self) -> int:
        raise UnsupportedOperation("not a local file")

    def isatty(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "<%s name=%r mode=%r>" % (
            fullname(self),
            self.name,
            self.mode,
        )  # pragma: no cover

    def seekable(self) -> bool:
        """Return True if the file-like object can be sought."""
        return False

    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Change stream position.

        Seek to byte `offset` relative to position indicated by `whence`:
            0  Start of stream (the default).  `offset` should be >= 0;
            1  Current position - `offset` may be negative;
            2  End of stream - `offset` usually negative.

        Return the new absolute position.
        """
        raise UnsupportedOperation("not seekable")  # pragma: no cover

    def readable(self) -> bool:
        """Return True if the file-like object can be read."""
        return False  # pragma: no cover

    def writable(self) -> bool:
        """Return True if the file-like object can be written."""
        return False

    def flush(self) -> None:
        """Flush write buffers, if applicable.

        This is not implemented for read-only and non-blocking streams.
        """


class Seekable(FileLike, ABC):
    def seekable(self) -> bool:
        """Return True if the file-like object can be sought."""
        return True

    @abstractmethod
    def seek(self, offset: int, whence: int = os.SEEK_SET) -> int:
        """Change stream position.

        Seek to byte offset `cookie` relative to position indicated by `whence`:
            0  Start of stream (the default).  `cookie` should be >= 0;
            1  Current position - `cookie` may be negative;
            2  End of stream - `cookie` usually negative.

        Return the new absolute position.
        """


class Readable(FileLike[AnyStr], ABC):
    def readable(self) -> bool:
        """Return True if the file-like object can be read."""
        return True

    @abstractmethod
    def read(self, size: Optional[int] = None) -> AnyStr:
        """Read at most `size` bytes or string, returned as a bytes or string object.

        If the `size` argument is negative, read until EOF is reached.
        Return an empty bytes or string object at EOF.
        """

    @abstractmethod
    def readline(self, size: Optional[int] = None) -> AnyStr:  # pyre-ignore[15]
        """Next line from the file, as a bytes or string object.

        Retain newline. A non-negative `size` argument limits the maximum number of
        bytes or string to return (an incomplete line may be returned then).
        Return an empty bytes object at EOF.
        """

    def readlines(self, hint: Optional[int] = None) -> List[AnyStr]:  # pyre-ignore[15]
        """Return a list of lines from the stream."""
        return self.read(size=hint).splitlines(True)  # pyre-ignore[7]

    def readinto(self, buffer: bytearray) -> int:
        """Read bytes into buffer.

        Returns number of bytes read (0 for EOF), or None if the object
        is set not to block and has no data to read.
        """
        if "b" not in self.mode:
            raise OSError("'readinto' only works on binary files")

        data = self.read(len(buffer))
        size = len(data)
        buffer[:size] = data  # pyre-ignore[6]
        return size

    def __next__(self) -> AnyStr:  # pyre-ignore[15]
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def __iter__(self: Self) -> Self:  # pyre-ignore[15]
        return self

    def truncate(self, size: Optional[int] = None) -> int:
        raise OSError("not writable")

    def write(self, data: AnyStr) -> int:
        raise OSError("not writable")

    def writelines(  # pyre-ignore[14]  # pytype: disable=signature-mismatch
        self, lines: Iterable[AnyStr]
    ) -> None:
        raise OSError("not writable")


class Writable(FileLike[AnyStr], ABC):
    def writable(self) -> bool:
        """Return True if the file-like object can be written."""
        return True

    @abstractmethod
    def write(self, data: AnyStr) -> int:
        """Write bytes or string to file.

        Return the number of bytes or string written.
        """

    def writelines(  # pyre-ignore[14]  # pytype: disable=signature-mismatch
        self, lines: Iterable[AnyStr]
    ) -> None:
        """Write `lines` to the file.

        Note that newlines are not added.
        `lines` can be any iterable object producing bytes-like or string-like objects.
        This is equivalent to calling write() for each element.
        """
        for line in lines:
            self.write(line)

    def truncate(self, size: Optional[int] = None) -> int:
        """
        Resize the stream to the given size in bytes.

        :param size: resize size, defaults to None
        :type size: int, optional

        :raises OSError: When the stream is not support truncate.
        :return: The new file size.
        :rtype: int
        """
        raise UnsupportedOperation("not support truncate")

    def read(self, size: Optional[int] = None) -> AnyStr:
        raise OSError("not readable")

    def readline(self, size: Optional[int] = None) -> AnyStr:  # pyre-ignore[15]
        raise OSError("not readable")

    def readlines(self, hint: Optional[int] = None) -> List[AnyStr]:  # pyre-ignore[15]
        raise OSError("not readable")


class FileCacher(ABC):
    @property
    @abstractmethod
    def cache_path(self) -> str:
        pass  # pragma: no cover

    @property
    def closed(self) -> bool:
        """Return True if the file-like object is closed."""
        return getattr(self, "__closed__", False)

    @abstractmethod
    def _close(self) -> None:
        pass  # pragma: no cover

    def close(self) -> None:
        """Flush and close the file-like object.

        This method has no effect if the file is already closed.
        """
        if not getattr(self, "__closed__", False):
            self._close()
            setattr(self, "__closed__", True)

    def __enter__(self) -> str:
        return self.cache_path

    def __exit__(self, type, value, traceback) -> None:
        self.close()

    def __del__(self):
        self.close()


class NullCacher(FileCacher):
    cache_path = None

    def __init__(self, path):
        self.cache_path = path

    def _close(self):
        pass


class ContextIterator(Closable):
    def __init__(self, iterable: Iterable) -> None:
        self._iter = iter(iterable)

    def _close(self) -> None:
        pass

    def __next__(self):
        return next(self._iter)

    def __iter__(self):
        return self

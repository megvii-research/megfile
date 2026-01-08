import typing as T
from io import TextIOWrapper
from logging import getLogger

from megfile.interfaces import FileLike

_logger = getLogger(__name__)


class FSFuncForAtomic(T.NamedTuple):
    exists: T.Callable[[str], bool]
    copy: T.Callable[[str, str], T.Any]
    replace: T.Callable[[str, str], T.Any]
    open: T.Callable[..., T.IO]
    unlink: T.Callable[[str], T.Any]


class WrapAtomic(FileLike):
    """Wrap a file object to provide atomic close/abort semantics."""

    __atomic__ = True

    def __init__(
        self,
        path: str,
        mode: str,
        fs_func: FSFuncForAtomic,
        *,
        buffering: int = -1,
        encoding: T.Optional[str] = None,
        errors: T.Optional[str] = None,
        newline: T.Optional[str] = None,
        closefd: bool = True,
    ):
        self.fs_func = fs_func
        if "x" in mode and self.fs_func.exists(path):
            raise FileExistsError(f"File exists: {path}")

        self._path = path
        self._mode = mode
        self._temp_path = self._path + ".temp"

        if self._should_copy():
            self.fs_func.copy(self._path, self._temp_path)

        # Open temp file with the same mode/encoding parameters.
        open_mode = mode.replace("x", "w", 1) if "x" in mode else mode

        self.fileobj = self.fs_func.open(
            self._temp_path,
            open_mode,
            buffering=buffering,
            encoding=encoding,
            errors=errors,
            newline=newline,
            closefd=closefd,
        )

        self.read = self.fileobj.read
        self.readline = self.fileobj.readline
        self.readlines = self.fileobj.readlines
        self.write = self.fileobj.write
        self.writelines = self.fileobj.writelines
        self.truncate = self.fileobj.truncate
        self.seek = self.fileobj.seek
        self.tell = self.fileobj.tell
        self.flush = self.fileobj.flush
        self.readable = self.fileobj.readable
        self.writable = self.fileobj.writable
        self.seekable = self.fileobj.seekable

    @property
    def name(self):
        return self._path

    @property
    def mode(self):
        return self._mode

    def _should_copy(self) -> bool:
        if self.fs_func.exists(self._path):
            return True
        return False

    def _close(self):
        self.fileobj.close()
        self.fs_func.replace(self._temp_path, self._path)

    def _abort(self):
        try:
            self.fileobj.close()
        except Exception:
            pass
        try:
            self.fs_func.unlink(self._temp_path)
        except FileNotFoundError:
            pass


class AtomicTextIOWrapper(TextIOWrapper):
    """TextIOWrapper that keeps atomic semantics of the underlying raw object."""

    def __init__(self, buffer, *args, **kwargs):
        # Keep a reference to the raw object so we can call abort later.
        self._raw = buffer
        super().__init__(buffer, *args, **kwargs)

    @property
    def atomic(self) -> bool:
        return getattr(self._raw, "atomic", False)

    def abort(self) -> bool:
        """Abort the atomic operation.

        Returns:
            bool: True if the abort was performed, False otherwise.
        """
        if hasattr(self._raw, "abort"):
            return self._raw.abort()
        return False

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.atomic and exc_val is not None:
            if self.abort():
                from megfile.errors import full_error_message

                _logger.warning(
                    f"skip closing atomic file-like object: {self}, "
                    f"since error encountered: {full_error_message(exc_val)}"
                )
            return

        super().__exit__(exc_type, exc_val, exc_tb)

    def __del__(self):
        if self.closed:
            return

        if self.atomic:
            if self.abort():
                _logger.warning(
                    f"skip closing atomic file-like object before deletion: {self}"
                )
            return

        self.close()

import typing as T

from megfile.interfaces import FileLike


class FSFunc(T.NamedTuple):
    exists: T.Callable[[str], bool]
    copy: T.Callable[[str, str], None]
    replace: T.Callable[[str, str], None]
    open: T.Callable[..., T.IO]
    unlink: T.Callable[[str], None]


class WrapAtomic(FileLike):
    """Wrap a file object to provide atomic close/abort semantics."""

    __atomic__ = True

    def __init__(
        self,
        path: str,
        mode: str,
        fs_func: FSFunc,
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

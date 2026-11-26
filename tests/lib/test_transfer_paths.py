import multiprocessing
import os
import sys
from multiprocessing.connection import Connection
from pathlib import Path, PureWindowsPath

import pytest

from megfile import FSPath, SmartPath, smart_path_join
from megfile.lib.compat.file_lock import file_lock
from megfile.lib.glob import get_non_glob_dir
from megfile.smart import _join_transfer_path


def test_windows_path_join() -> None:
    part = PureWindowsPath(r"one\two")
    root = SmartPath("s3://bucket")
    assert str(root / part) == "s3://bucket/one/two"
    assert smart_path_join(root, part) == "s3://bucket/one/two"
    assert smart_path_join(root, str(part)) == r"s3://bucket/one\two"


def test_remote_components() -> None:
    assert _join_transfer_path("s3://b", r"a/x\y", local_source=False) == (
        r"s3://b/a/x\y"
    )
    with pytest.raises(ValueError):
        _join_transfer_path("target", "../outside", local_source=False)


@pytest.mark.skipif(sys.platform != "win32", reason="native Windows path rules")
def test_native_windows_paths() -> None:
    assert FSPath(r"C:\one\two").as_uri() == "file:///C:/one/two"
    for path in (r"C:\one\two", r"\\server\share\two", r"one\two"):
        assert os.fspath(FSPath(FSPath(path).as_uri())) == path
    assert get_non_glob_dir(r"C:\one\*.py") == "C:/one"
    with pytest.raises(ValueError):
        _join_transfer_path("target", r"a/x\y", local_source=False)


def test_lock_exception(tmp_path: Path) -> None:
    path = tmp_path / "slot"
    with path.open("wb") as first, path.open("rb+") as second:
        with pytest.raises(RuntimeError), file_lock(first.fileno()):
            raise RuntimeError
        with file_lock(second.fileno()):
            pass


def _acquire_in_child(path: Path, status: Connection) -> None:
    """Report when the child attempts and then acquires its own lock."""
    with status, path.open("rb+") as stream:
        status.send("waiting")
        with file_lock(stream.fileno()):
            status.send("acquired")


def test_lock_between_processes(tmp_path: Path) -> None:
    path = tmp_path / "slot"
    context = multiprocessing.get_context("spawn")
    reader, writer = context.Pipe(duplex=False)
    with reader, writer, path.open("wb") as stream:
        child = context.Process(target=_acquire_in_child, args=(path, writer))
        try:
            with file_lock(stream.fileno()):
                child.start()
                assert reader.poll(15) and reader.recv() == "waiting"
                assert not reader.poll(0.2)
            assert reader.poll(15) and reader.recv() == "acquired"
        finally:
            if child.pid is not None:
                child.join(5)
                if child.is_alive():
                    child.terminate()
                    child.join()
        assert child.exitcode == 0


def test_sftp_absolute_glob_root() -> None:
    assert get_non_glob_dir("sftp://host//*.txt") == "sftp://host//"

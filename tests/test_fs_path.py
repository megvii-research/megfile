import os

import pytest

from megfile.fs_path import FSPath
from megfile.lib.compat import fspath

FS_PROTOCOL_PREFIX = FSPath.protocol + "://"
TEST_PATH = "/test/dir/file"
TEST_PATH_WITH_PROTOCOL = FS_PROTOCOL_PREFIX + TEST_PATH


def test_from_uri():
    assert FSPath.from_uri(TEST_PATH).path == TEST_PATH
    assert FSPath.from_uri(TEST_PATH_WITH_PROTOCOL).path == TEST_PATH_WITH_PROTOCOL


def test_path_with_protocol():
    path = FSPath(TEST_PATH)
    assert path.path_with_protocol == TEST_PATH_WITH_PROTOCOL

    int_path = FSPath(1)
    assert int_path.path_with_protocol == 1


def test_int_path():
    int_path = FSPath(1)
    assert int_path.path == 1

    with pytest.raises(TypeError):
        fspath(int_path)

    assert int_path.root == "/"
    assert int_path.anchor == "/"
    assert int_path.drive == ""
    assert int_path.path_without_protocol == 1
    assert int_path.is_absolute() is False
    assert int_path.is_socket() is False
    assert int_path.is_fifo() is False
    assert int_path.is_block_device() is False
    assert int_path.is_char_device() is False


def test_rename(fs):
    os.makedirs("test/dir1", exist_ok=True)
    with open("test/dir1/file", "w") as f:
        f.write("test")
    with open("test/file", "w") as f:
        f.write("test")

    os.makedirs("test1/dir1", exist_ok=True)
    FSPath("test").rename("test1", overwrite=False)
    assert os.path.exists("test1")
    assert not os.path.exists("test")
    with open("test1/dir1/file", "r") as f:
        assert f.read() == "test"

    os.makedirs("test2", exist_ok=True)
    with open("test2/file", "w") as f:
        f.write("test1")
    FSPath("test1/file").rename("test2/file", overwrite=False)
    assert os.path.exists("test2/file")
    assert not os.path.exists("test1/file")

    with open("test2/file", "r") as f:
        assert f.read() == "test1"


def test_copy_symlink(fs):
    os.symlink("test", "symlink")
    assert FSPath("symlink").is_symlink() is True

    with pytest.raises(FileNotFoundError):
        FSPath("symlink").copy("test2", followlinks=True)

    FSPath("symlink").copy("test1", followlinks=False)
    assert FSPath("test1").is_symlink() is True


def test_copy(fs):
    with open("test", "wb") as f:
        f.write(b"test")

    def callback(length):
        assert length == len("test")

    with open("test", "rb") as f:
        file_descriptor = f.fileno()
        FSPath(file_descriptor).copy("test2", callback=callback)

    with open("test2", "r") as f:
        assert f.read() == "test"


def test_sync(fs):
    os.makedirs("dir1")
    with open("dir1/test1", "w") as f:
        f.write("test1")

    os.makedirs("dir2")
    with open("dir2/test1", "w") as f:
        f.write("test2")

    FSPath("dir1").sync("dir2", overwrite=False)

    with open("dir2/test1", "r") as f:
        assert f.read() == "test2"

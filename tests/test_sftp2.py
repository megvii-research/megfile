import io
import os
import shutil
import stat
import subprocess
from typing import List

import pytest

from tests.compat import sftp2


class FakeSFTP2Client:
    def __init__(self):
        self._retry_times = 0

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        pass

    def listdir(self, path="."):
        return os.listdir(path)

    def open(self, filename, mode, attrs=0o644):
        return FakeSFTP2File(filename, mode)

    def remove(self, path):
        os.unlink(path)

    unlink = remove

    def rename(self, oldpath, newpath):
        if os.path.exists(newpath):
            raise OSError
        os.rename(oldpath, newpath)

    def posix_rename(self, oldpath, newpath):
        os.rename(oldpath, newpath)

    def mkdir(self, path, mode=0o777):
        os.mkdir(path=path, mode=mode)

    def rmdir(self, path):
        os.rmdir(path)

    def stat(self, path):
        return FakeSFTP2Stat(os.stat(path))

    def lstat(self, path):
        return FakeSFTP2Stat(os.lstat(path))

    def symlink(self, source, dest):
        os.symlink(source, dest)

    def setstat(self, path, stat):
        if stat.permissions != 0:
            os.chmod(path, stat.permissions)
        if stat.mtime != 0 or stat.atime != 0:
            os.utime(path, (stat.atime, stat.mtime))

    def realpath(self, path):
        return os.path.realpath(path)

    def opendir(self, path):
        return FakeSFTP2DirHandle(path)

    def readdir(self, handle):
        return handle.readdir()


class FakeSFTP2File:
    def __init__(self, filename, mode_str):
        self.filename = filename
        self.mode_str = mode_str
        # Convert ssh2 mode flags back to string for our fake implementation
        if isinstance(mode_str, int):
            # This is an ssh2 mode flag, convert to string mode
            import ssh2.sftp

            mode = ""
            if mode_str & ssh2.sftp.LIBSSH2_FXF_READ:
                mode += "r"
            if mode_str & ssh2.sftp.LIBSSH2_FXF_WRITE:
                if mode_str & ssh2.sftp.LIBSSH2_FXF_APPEND:
                    mode = "a"
                elif mode_str & ssh2.sftp.LIBSSH2_FXF_TRUNC:
                    mode = "w"
                else:
                    mode += "w"
            # Default to binary mode for consistency
            if "r" in mode or "w" in mode or "a" in mode:
                mode += "b"
            if not mode:
                mode = "rb"  # Default
        else:
            mode = mode_str
            # Keep original mode for string modes - this is key!
            # Don't force binary mode for string modes

        self.file_mode = mode  # Store the mode for our checks
        self._file = io.open(file=filename, mode=mode)

    def read(self, size=-1):
        data = self._file.read(size)
        if isinstance(data, str):
            encoded_data = data.encode("utf-8")
            # ssh2-python format: (bytes_read, data)
            return len(encoded_data), encoded_data
        return len(data), data  # ssh2-python format: (bytes_read, data)

    def write(self, data):
        written = 0
        try:
            if isinstance(data, str):
                written = self._file.write(data)
            else:
                # If we have bytes but file is in text mode, decode first
                if "b" not in self.file_mode:
                    if isinstance(data, bytes):
                        data = data.decode("utf-8")
                written = self._file.write(data)
            # Make sure data is flushed immediately for testing
            self._file.flush()
        except Exception:
            return (0, 0)  # ssh2-python returns (rc, bytes_written)
        return (0, written)  # ssh2-python returns (rc, bytes_written)

    def flush(self):
        self._file.flush()

    def tell(self):
        """Return current position in file"""
        return self._file.tell()

    def tell64(self):
        """Return current position in file (64-bit)"""
        return self._file.tell()

    def seek(self, offset, whence=0):
        """Seek to position in file"""
        return self._file.seek(offset, whence)

    def seek64(self, offset, whence=0):
        """Seek to position in file (64-bit)"""
        return self._file.seek(offset, whence)

    def close(self):
        if hasattr(self, "_file") and not self._file.closed:
            self._file.close()


class FakeSFTP2DirHandle:
    def __init__(self, path):
        self.path = path
        self.files = os.listdir(path) if os.path.exists(path) else []
        self.index = 0

    def readdir(self):
        """Return a generator that yields SSH2-python format entries"""

        def gen():
            for filename in self.files:
                if filename in (".", ".."):
                    continue
                full_path = os.path.join(self.path, filename)
                try:
                    stat_obj = FakeSFTP2Stat(os.lstat(full_path))
                    name_bytes = filename.encode("utf-8")
                    # SSH2-python format: (name_len, name_bytes, stat_obj)
                    yield (len(name_bytes), name_bytes, stat_obj)
                except OSError:
                    continue

        return gen()

    def close(self):
        pass


class FakeSFTP2Stat:
    def __init__(self, stat_result):
        # SSH2-python uses different attribute names
        self.filesize = stat_result.st_size
        self.mtime = stat_result.st_mtime
        self.permissions = stat_result.st_mode
        # Also keep the original attributes for compatibility
        self.st_size = stat_result.st_size
        self.st_mtime = stat_result.st_mtime
        self.st_mode = stat_result.st_mode


class FakeSSH2Session:
    def __init__(self):
        pass

    def sftp_init(self):
        return FakeSFTP2Client()

    def handshake(self, sock):
        pass

    def userauth_password(self, username, password):
        return True

    def userauth_publickey_frommemory(self, username, key, private_key_password=""):
        return True

    def userauth_publickey_fromstring(self, username, key, passphrase=""):
        return True


def _fake_exec_command(
    command: List[str],
) -> subprocess.CompletedProcess:
    if command[0] == "cp":
        shutil.copy(command[1], command[2])
    elif command[0] == "cat":
        with open(command[-1], "wb") as f:
            for file_name in command[1:-2]:
                with open(file_name, "rb") as f_src:
                    f.write(f_src.read())
            return subprocess.CompletedProcess(
                args=command, returncode=0, stdout=b"", stderr=b""
            )
    else:
        raise OSError("Nonsupport command")
    return subprocess.CompletedProcess(
        args=command, returncode=0, stdout=b"", stderr=b""
    )


@pytest.fixture
def sftp2_mocker(fs, mocker):
    client = FakeSFTP2Client()
    session = FakeSSH2Session()
    mocker.patch("megfile.sftp2_path.get_sftp2_client", return_value=client)
    mocker.patch("megfile.sftp2_path.get_ssh2_session", return_value=session)

    # Mock _exec_command to simulate server-side operations but force fallback
    mocker.patch(
        "megfile.sftp2_path.Sftp2Path._exec_command", side_effect=_fake_exec_command
    )
    yield client


def test_is_sftp2():
    assert sftp2.is_sftp2("sftp2://username@host/data") is True
    assert sftp2.is_sftp2("sftp://username@host/data") is False


def test_sftp2_readlink(sftp2_mocker):
    path = "sftp2://username@host/file"
    link_path = "sftp2://username@host/file.lnk"

    with sftp2.sftp2_open(path, "w") as f:
        f.write("test")

    sftp2.sftp2_symlink(path, link_path)
    assert sftp2.sftp2_readlink(link_path) == path

    path = "sftp2://username@host/notFound"
    link_path = "sftp2://username@host/notFound.lnk"
    sftp2.sftp2_symlink(path, link_path)
    assert sftp2.sftp2_readlink(link_path) == path

    with pytest.raises(FileNotFoundError):
        sftp2.sftp2_readlink("sftp2://username@host/notFound")

    with pytest.raises(OSError):
        sftp2.sftp2_readlink("sftp2://username@host/file")


def test_sftp2_absolute(sftp2_mocker):
    assert (
        sftp2.sftp2_absolute("sftp2://username@host/dir/../file").path_with_protocol
        == "sftp2://username@host/file"
    )


def test_sftp2_resolve(sftp2_mocker):
    assert (
        sftp2.sftp2_resolve("sftp2://username@host/dir/../file")
        == "sftp2://username@host/file"
    )


def test_sftp2_glob(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    sftp2.sftp2_makedirs("sftp2://username@host/A/a")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b/c")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")

    with sftp2.sftp2_open("sftp2://username@host/A/b/file.json", "w") as f:
        f.write("file")

    assert sftp2.sftp2_glob("sftp2://username@host/A/*") == [
        "sftp2://username@host/A/1.json",
        "sftp2://username@host/A/a",
        "sftp2://username@host/A/b",
    ]
    assert list(sftp2.sftp2_iglob("sftp2://username@host/A/*")) == [
        "sftp2://username@host/A/1.json",
        "sftp2://username@host/A/a",
        "sftp2://username@host/A/b",
    ]
    assert sftp2.sftp2_glob("sftp2://username@host/A/**/*.json") == [
        "sftp2://username@host/A/1.json",
        "sftp2://username@host/A/b/file.json",
    ]
    assert [
        file_entry.path
        for file_entry in sftp2.sftp2_glob_stat("sftp2://username@host/A/*")
    ] == [
        "sftp2://username@host/A/1.json",
        "sftp2://username@host/A/a",
        "sftp2://username@host/A/b",
    ]


def test_sftp2_isdir_sftp2_isfile(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A/B", parents=True)

    with sftp2.sftp2_open("sftp2://username@host/A/B/file", "w") as f:
        f.write("test")

    assert sftp2.sftp2_isdir("sftp2://username@host/A/B") is True
    assert sftp2.sftp2_isdir("sftp2://username@host/A/B/file") is False
    assert sftp2.sftp2_isfile("sftp2://username@host/A/B/file") is True
    assert sftp2.sftp2_isfile("sftp2://username@host/A/B") is False
    assert sftp2.sftp2_isfile("sftp2://username@host/A/C") is False


def test_sftp2_exists(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A/B", parents=True)

    with sftp2.sftp2_open("sftp2://username@host/A/B/file", "w") as f:
        f.write("test")

    assert sftp2.sftp2_exists("sftp2://username@host/A/B/file") is True

    sftp2.sftp2_symlink(
        "sftp2://username@host/A/B/file", "sftp2://username@host/A/B/file.lnk"
    )
    sftp2.sftp2_unlink("sftp2://username@host/A/B/file")
    assert sftp2.sftp2_exists("sftp2://username@host/A/B/file.lnk") is True
    assert (
        sftp2.sftp2_exists("sftp2://username@host/A/B/file.lnk", followlinks=True)
        is False
    )


def test_sftp2_scandir(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    sftp2.sftp2_makedirs("sftp2://username@host/A/a")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b/c")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")

    with sftp2.sftp2_open("sftp2://username@host/A/b/file.json", "w") as f:
        f.write("file")

    assert sorted(
        [
            file_entry.path
            for file_entry in sftp2.sftp2_scandir("sftp2://username@host/A")
        ]
    ) == [
        "sftp2://username@host/A/1.json",
        "sftp2://username@host/A/a",
        "sftp2://username@host/A/b",
    ]


def test_sftp2_stat(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test")
    sftp2.sftp2_symlink(
        "sftp2://username@host/A/test", "sftp2://username@host/A/test.lnk"
    )

    stat = sftp2.sftp2_stat("sftp2://username@host/A/test", follow_symlinks=True)
    os_stat = os.stat("/A/test")
    assert stat.size == os_stat.st_size
    assert stat.isdir is False
    assert stat.islnk is False
    assert stat.mtime == os_stat.st_mtime

    stat = sftp2.sftp2_stat("sftp2://username@host/A/test.lnk", follow_symlinks=True)
    os_stat = os.stat("/A/test")
    assert stat.size == os_stat.st_size
    assert stat.isdir is False
    assert stat.islnk is False
    assert stat.mtime == os_stat.st_mtime

    stat = sftp2.sftp2_lstat("sftp2://username@host/A/test.lnk")
    os_stat = os.lstat("/A/test.lnk")
    assert stat.size == os_stat.st_size
    assert stat.isdir is False
    assert stat.islnk is True
    assert stat.mtime == os_stat.st_mtime

    os_stat = os.stat("/A/test")
    assert sftp2.sftp2_getmtime("sftp2://username@host/A/test") == os_stat.st_mtime
    assert sftp2.sftp2_getsize("sftp2://username@host/A/test") == os_stat.st_size


def test_sftp2_listdir(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    sftp2.sftp2_makedirs("sftp2://username@host/A/a")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b/c")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")

    assert sftp2.sftp2_listdir("sftp2://username@host/A") == ["1.json", "a", "b"]


def test_sftp2_load_from(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test")
    assert sftp2.sftp2_load_from("sftp2://username@host/A/test").read() == b"test"


def test_sftp2_makedirs(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A/B/C", parents=True)
    assert sftp2.sftp2_exists("sftp2://username@host/A/B/C") is True

    with pytest.raises(FileExistsError):
        sftp2.sftp2_makedirs("sftp2://username@host/A/B/C")

    with pytest.raises(FileNotFoundError):
        sftp2.sftp2_makedirs("sftp2://username@host/D/B/C")


def test_sftp2_realpath(sftp2_mocker, mocker):
    assert (
        sftp2.sftp2_realpath("sftp2://username@host/A/../B/C")
        == "sftp2://username@host/B/C"
    )


def test_sftp2_rename(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test")

    sftp2.sftp2_rename("sftp2://username@host/A/test", "sftp2://username@host/A/test2")
    assert sftp2.sftp2_exists("sftp2://username@host/A/test") is False
    assert sftp2.sftp2_exists("sftp2://username@host/A/test2") is True


def test_sftp2_move(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test")

    sftp2.sftp2_move("sftp2://username@host/A/test", "sftp2://username@host/A/test2")
    assert sftp2.sftp2_exists("sftp2://username@host/A/test") is False
    assert sftp2.sftp2_exists("sftp2://username@host/A/test2") is True


def test_sftp2_open(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")

    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test")

    with sftp2.sftp2_open("sftp2://username@host/A/test", "r") as f:
        assert f.read() == "test"

    with sftp2.sftp2_open("sftp2://username@host/A/test", "rb") as f:
        data = f.read()
        assert data == b"test"

    with pytest.raises(FileNotFoundError):
        with sftp2.sftp2_open("sftp2://username@host/A/notFound", "r") as f:
            f.read()

    with pytest.raises(IsADirectoryError):
        with sftp2.sftp2_open("sftp2://username@host/A", "w") as f:
            f.read()


def test_sftp2_remove(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test")

    sftp2.sftp2_remove("sftp2://username@host/A/test")
    sftp2.sftp2_remove("sftp2://username@host/A/test", missing_ok=True)

    assert sftp2.sftp2_exists("sftp2://username@host/A/test") is False
    assert sftp2.sftp2_exists("sftp2://username@host/A") is True

    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test")
    sftp2.sftp2_remove("sftp2://username@host/A")
    assert sftp2.sftp2_exists("sftp2://username@host/A") is False


def test_sftp2_scan(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    sftp2.sftp2_makedirs("sftp2://username@host/A/a")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b/c")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")
    sftp2.sftp2_symlink(
        "sftp2://username@host/A/1.json", "sftp2://username@host/A/1.json.lnk"
    )

    with sftp2.sftp2_open("sftp2://username@host/A/b/file.json", "w") as f:
        f.write("file")

    assert list(sftp2.sftp2_scan("sftp2://username@host/A")) == [
        "sftp2://username@host/A/1.json",
        "sftp2://username@host/A/1.json.lnk",
        "sftp2://username@host/A/b/file.json",
    ]


def test_sftp2_unlink(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test")

    sftp2.sftp2_unlink("sftp2://username@host/A/test")
    sftp2.sftp2_unlink("sftp2://username@host/A/test", missing_ok=True)

    assert sftp2.sftp2_exists("sftp2://username@host/A/test") is False
    assert sftp2.sftp2_exists("sftp2://username@host/A") is True


def test_sftp2_walk(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    sftp2.sftp2_makedirs("sftp2://username@host/A/a")
    sftp2.sftp2_makedirs("sftp2://username@host/A/a/b")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b/c")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")
    with sftp2.sftp2_open("sftp2://username@host/A/a/2.json", "w") as f:
        f.write("2.json")
    with sftp2.sftp2_open("sftp2://username@host/A/b/3.json", "w") as f:
        f.write("3.json")

    assert list(sftp2.sftp2_walk("sftp2://username@host/A")) == [
        ("sftp2://username@host/A", ["a", "b"], ["1.json"]),
        ("sftp2://username@host/A/a", ["b"], ["2.json"]),
        ("sftp2://username@host/A/a/b", [], []),
        ("sftp2://username@host/A/b", ["c"], ["3.json"]),
        ("sftp2://username@host/A/b/c", [], []),
    ]


def test_sftp2_getmd5(sftp2_mocker):
    from tests.compat.fs import fs_getmd5

    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")
    assert sftp2.sftp2_getmd5("sftp2://username@host/A/1.json") == fs_getmd5(
        "/A/1.json"
    )
    assert sftp2.sftp2_getmd5("sftp2://username@host/A") == fs_getmd5("/A")


def test_sftp2_symlink(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")
    sftp2.sftp2_symlink(
        "sftp2://username@host/A/1.json", "sftp2://username@host/A/1.json.lnk"
    )
    assert sftp2.sftp2_islink("sftp2://username@host/A/1.json.lnk") is True
    assert sftp2.sftp2_islink("sftp2://username@host/A/1.json") is False

    with pytest.raises(FileExistsError):
        sftp2.sftp2_symlink(
            "sftp2://username@host/A/1.json", "sftp2://username@host/A/1.json.lnk"
        )


def test_sftp2_save_as(sftp2_mocker):
    sftp2.sftp2_save_as(io.BytesIO(b"test"), "sftp2://username@host/test")
    assert sftp2.sftp2_load_from("sftp2://username@host/test").read() == b"test"


def test_sftp2_chmod(sftp2_mocker):
    path = "sftp2://username@host/test"
    sftp2.sftp2_save_as(io.BytesIO(b"test"), path)

    sftp2.sftp2_chmod(path, mode=0o777)
    assert stat.S_IMODE(os.stat("/test").st_mode) == 0o777


def test_sftp2_rmdir(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")

    with pytest.raises(OSError):
        sftp2.sftp2_rmdir("sftp2://username@host/A")

    sftp2.sftp2_unlink("sftp2://username@host/A/1.json")
    sftp2.sftp2_rmdir("sftp2://username@host/A")
    assert sftp2.sftp2_exists("sftp2://username@host/A") is False


def test_sftp2_copy(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")
    sftp2.sftp2_symlink(
        "sftp2://username@host/A/1.json", "sftp2://username@host/A/1.json.lnk"
    )

    def callback(length):
        assert length == len("1.json")

    sftp2.sftp2_copy(
        "sftp2://username@host/A/1.json.lnk",
        "sftp2://username@host/A2/1.json.bak",
        followlinks=True,
        callback=callback,
    )

    assert (
        sftp2.sftp2_stat("sftp2://username@host/A/1.json").size
        == sftp2.sftp2_stat("sftp2://username@host/A2/1.json.bak").size
    )


def test_sftp2_concat(sftp2_mocker):
    with sftp2.sftp2_open("sftp2://username@host/1", "w") as f:
        f.write("1")
    with sftp2.sftp2_open("sftp2://username@host/2", "w") as f:
        f.write("2")
    with sftp2.sftp2_open("sftp2://username@host/3", "w") as f:
        f.write("3")

    sftp2.sftp2_concat(
        [
            "sftp2://username@host/1",
            "sftp2://username@host/2",
            "sftp2://username@host/3",
        ],
        "sftp2://username@host/4",
    )
    with sftp2.sftp2_open("sftp2://username@host/4", "r") as f:
        assert f.read() == "123"


def test_sftp2_download(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")

    sftp2.sftp2_download("sftp2://username@host/A/1.json", "/A2/1.json")
    assert (
        sftp2.sftp2_stat("sftp2://username@host/A/1.json").size
        == os.stat("/A2/1.json").st_size
    )

    with pytest.raises(OSError):
        sftp2.sftp2_download(
            "sftp2://username@host/A/1.json", "sftp2://username@host/1.json"
        )

    with pytest.raises(OSError):
        sftp2.sftp2_download("/1.json", "/1.json")

    with pytest.raises(IsADirectoryError):
        sftp2.sftp2_download("sftp2://username@host/A", "/1.json")

    with pytest.raises(IsADirectoryError):
        sftp2.sftp2_download("sftp2://username@host/A/1.json", "/1/")


def test_sftp2_upload(sftp2_mocker):
    with open("/1.json", "w") as f:
        f.write("1.json")

    sftp2.sftp2_upload("/1.json", "sftp2://username@host/A/1.json")
    assert (
        sftp2.sftp2_stat("sftp2://username@host/A/1.json").size
        == os.stat("/1.json").st_size
    )

    with pytest.raises(OSError):
        sftp2.sftp2_upload(
            "sftp2://username@host/A/1.json", "sftp2://username@host/1.json"
        )

    with pytest.raises(OSError):
        sftp2.sftp2_upload("/1.json", "/1.json")


def test_sftp2_path_join():
    assert (
        sftp2.sftp2_path_join("sftp2://username@host/A/", "a", "b")
        == "sftp2://username@host/A/a/b"
    )
    assert (
        sftp2.sftp2_path_join("sftp2://username@host/A/", "a", "b")
        == "sftp2://username@host/A/a/b"
    )


def test_sftp2_sync(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")

    sftp2.sftp2_sync("sftp2://username@host/A", "sftp2://username@host/A2")
    assert (
        sftp2.sftp2_stat("sftp2://username@host/A/1.json").size
        == sftp2.sftp2_stat("sftp2://username@host/A2/1.json").size
    )


def test_sftp2_rename_different_backend(sftp2_mocker):
    """Test rename between different backends falls back to copy"""
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    sftp2.sftp2_makedirs("sftp2://username@host/B")
    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test content")

    # When renaming between different backends, it should copy and delete
    # This is simulated by the mock, but tests the code path
    sftp2.sftp2_rename(
        "sftp2://username@host/A/test", "sftp2://username@host/B/test", overwrite=True
    )
    assert sftp2.sftp2_exists("sftp2://username@host/B/test") is True
    assert sftp2.sftp2_exists("sftp2://username@host/A/test") is False


def test_sftp2_rename_error(sftp2_mocker):
    """Test rename raises error for non-sftp2 destination"""
    from megfile.sftp2_path import Sftp2Path

    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test content")

    with pytest.raises(OSError, match="Not a sftp2 path"):
        Sftp2Path("sftp2://username@host/A/test").rename("/local/path")


def test_sftp2_copy_error(sftp2_mocker):
    """Test copy raises appropriate errors"""
    from megfile.sftp2_path import Sftp2Path

    sftp2.sftp2_makedirs("sftp2://username@host/A")
    with sftp2.sftp2_open("sftp2://username@host/A/test", "w") as f:
        f.write("test content")

    # Copy to non-sftp2 path should raise error
    with pytest.raises(OSError, match="Not a sftp2 path"):
        Sftp2Path("sftp2://username@host/A/test").copy("/local/path")

    # Copy to directory path should raise error
    with pytest.raises(IsADirectoryError):
        Sftp2Path("sftp2://username@host/A/test").copy("sftp2://username@host/B/")

    # Copy directory should raise error
    with pytest.raises(IsADirectoryError):
        Sftp2Path("sftp2://username@host/A").copy("sftp2://username@host/B/test")


def test_sftp2_scandir_not_directory(sftp2_mocker):
    """Test scandir raises error for non-directory"""
    from megfile.sftp2_path import Sftp2Path

    with sftp2.sftp2_open("sftp2://username@host/file", "w") as f:
        f.write("test")

    with pytest.raises(NotADirectoryError):
        list(Sftp2Path("sftp2://username@host/file").scandir())


def test_sftp2_remove_directory(sftp2_mocker):
    """Test remove works recursively on directories"""
    from megfile.sftp2_path import Sftp2Path

    sftp2.sftp2_makedirs("sftp2://username@host/A/B/C", parents=True)
    with sftp2.sftp2_open("sftp2://username@host/A/file1", "w") as f:
        f.write("test1")
    with sftp2.sftp2_open("sftp2://username@host/A/B/file2", "w") as f:
        f.write("test2")

    Sftp2Path("sftp2://username@host/A").remove()
    assert sftp2.sftp2_exists("sftp2://username@host/A") is False


def test_sftp2_scan_stat_file(sftp2_mocker):
    """Test scan_stat on a single file"""
    from megfile.sftp2_path import Sftp2Path

    with sftp2.sftp2_open("sftp2://username@host/file", "w") as f:
        f.write("test")

    result = list(Sftp2Path("sftp2://username@host/file").scan_stat())
    assert len(result) == 1
    assert result[0].name == "file"


def test_sftp2_walk_file(sftp2_mocker):
    """Test walk on a single file returns nothing"""
    from megfile.sftp2_path import Sftp2Path

    with sftp2.sftp2_open("sftp2://username@host/file", "w") as f:
        f.write("test")

    result = list(Sftp2Path("sftp2://username@host/file").walk())
    assert len(result) == 0


def test_sftp2_walk_nonexistent(sftp2_mocker):
    """Test walk on non-existent path returns nothing"""
    from megfile.sftp2_path import Sftp2Path

    result = list(Sftp2Path("sftp2://username@host/nonexistent").walk())
    assert len(result) == 0


def test_sftp2_readlink_errors(sftp2_mocker):
    """Test readlink raises appropriate errors"""
    from megfile.sftp2_path import Sftp2Path

    # Non-existent file
    with pytest.raises(FileNotFoundError):
        Sftp2Path("sftp2://username@host/nonexistent").readlink()

    # Regular file (not a symlink)
    with sftp2.sftp2_open("sftp2://username@host/file", "w") as f:
        f.write("test")

    with pytest.raises(OSError, match="Not a symlink"):
        Sftp2Path("sftp2://username@host/file").readlink()


def test_sftp2_utime(sftp2_mocker):
    """Test utime sets access and modification times"""
    from megfile.sftp2_path import Sftp2Path

    with sftp2.sftp2_open("sftp2://username@host/file", "w") as f:
        f.write("test")

    path = Sftp2Path("sftp2://username@host/file")
    path.utime(1000.0, 2000.0)

    stat = os.stat("/file")
    assert stat.st_atime == 1000
    assert stat.st_mtime == 2000


def test_sftp2_rename_cross_backend(sftp2_mocker):
    """Test rename when moving to different path falls back to copy"""
    from megfile.sftp2_path import Sftp2Path

    sftp2.sftp2_makedirs("sftp2://username@host/src")
    sftp2.sftp2_makedirs("sftp2://username@host/dst")  # Create destination dir
    with sftp2.sftp2_open("sftp2://username@host/src/file.txt", "w") as f:
        f.write("content")

    # Rename file to different location (same backend)
    Sftp2Path("sftp2://username@host/src/file.txt").rename(
        "sftp2://username@host/dst/file.txt", overwrite=True
    )

    assert sftp2.sftp2_exists("sftp2://username@host/dst/file.txt") is True


def test_sftp2_rename_directory(sftp2_mocker):
    """Test rename directory with contents"""
    from megfile.sftp2_path import Sftp2Path

    # Create a directory with files
    sftp2.sftp2_makedirs("sftp2://username@host/dir1/subdir", parents=True)
    with sftp2.sftp2_open("sftp2://username@host/dir1/file1.txt", "w") as f:
        f.write("file1")
    with sftp2.sftp2_open("sftp2://username@host/dir1/subdir/file2.txt", "w") as f:
        f.write("file2")

    # Rename directory
    Sftp2Path("sftp2://username@host/dir1").rename("sftp2://username@host/dir2")

    assert sftp2.sftp2_exists("sftp2://username@host/dir2/file1.txt") is True


def test_sftp2_copy_file(sftp2_mocker):
    """Test copy file"""
    from megfile.sftp2_path import Sftp2Path

    sftp2.sftp2_makedirs("sftp2://username@host/src")
    sftp2.sftp2_makedirs("sftp2://username@host/dst")
    with sftp2.sftp2_open("sftp2://username@host/src/file.txt", "w") as f:
        f.write("content")

    Sftp2Path("sftp2://username@host/src/file.txt").copy(
        "sftp2://username@host/dst/file.txt"
    )

    assert sftp2.sftp2_exists("sftp2://username@host/dst/file.txt") is True
    assert sftp2.sftp2_exists("sftp2://username@host/src/file.txt") is True


def test_sftp2_sync_directory(sftp2_mocker):
    """Test sync directory"""
    from megfile.sftp2_path import Sftp2Path

    sftp2.sftp2_makedirs("sftp2://username@host/src/subdir", parents=True)
    with sftp2.sftp2_open("sftp2://username@host/src/file1.txt", "w") as f:
        f.write("file1")
    with sftp2.sftp2_open("sftp2://username@host/src/subdir/file2.txt", "w") as f:
        f.write("file2")

    Sftp2Path("sftp2://username@host/src").sync("sftp2://username@host/dst")

    assert sftp2.sftp2_exists("sftp2://username@host/dst/file1.txt") is True
    assert sftp2.sftp2_exists("sftp2://username@host/dst/subdir/file2.txt") is True


def test_sftp2_walk_directory(sftp2_mocker):
    """Test walk directory"""
    from megfile.sftp2_path import Sftp2Path

    sftp2.sftp2_makedirs("sftp2://username@host/dir/subdir", parents=True)
    with sftp2.sftp2_open("sftp2://username@host/dir/file1.txt", "w") as f:
        f.write("file1")
    with sftp2.sftp2_open("sftp2://username@host/dir/subdir/file2.txt", "w") as f:
        f.write("file2")

    result = list(Sftp2Path("sftp2://username@host/dir").walk())
    assert len(result) >= 2  # At least 2 directories in walk result


def test_sftp2_is_file_and_is_dir(sftp2_mocker):
    """Test is_file and is_dir methods"""
    from megfile.sftp2_path import Sftp2Path

    sftp2.sftp2_makedirs("sftp2://username@host/dir")
    with sftp2.sftp2_open("sftp2://username@host/file.txt", "w") as f:
        f.write("test")

    assert Sftp2Path("sftp2://username@host/file.txt").is_file() is True
    assert Sftp2Path("sftp2://username@host/file.txt").is_dir() is False
    assert Sftp2Path("sftp2://username@host/dir").is_dir() is True
    assert Sftp2Path("sftp2://username@host/dir").is_file() is False

    # Non-existent should return False
    assert Sftp2Path("sftp2://username@host/nonexistent").is_file() is False
    assert Sftp2Path("sftp2://username@host/nonexistent").is_dir() is False


def test_sftp2_replace(sftp2_mocker):
    """Test replace method"""
    from megfile.sftp2_path import Sftp2Path

    with sftp2.sftp2_open("sftp2://username@host/src.txt", "w") as f:
        f.write("source")
    with sftp2.sftp2_open("sftp2://username@host/dst.txt", "w") as f:
        f.write("destination")

    Sftp2Path("sftp2://username@host/src.txt").replace("sftp2://username@host/dst.txt")

    assert sftp2.sftp2_exists("sftp2://username@host/dst.txt") is True
    assert sftp2.sftp2_exists("sftp2://username@host/src.txt") is False


def test_sftp2_glob_recursive(sftp2_mocker):
    """Test glob with recursive pattern"""
    from megfile.sftp2_path import Sftp2Path

    sftp2.sftp2_makedirs("sftp2://username@host/a/b/c", parents=True)
    with sftp2.sftp2_open("sftp2://username@host/a/file.txt", "w") as f:
        f.write("test1")
    with sftp2.sftp2_open("sftp2://username@host/a/b/file.txt", "w") as f:
        f.write("test2")
    with sftp2.sftp2_open("sftp2://username@host/a/b/c/file.txt", "w") as f:
        f.write("test3")

    result = list(Sftp2Path("sftp2://username@host/a").glob("**/*.txt"))
    assert len(result) == 3


def test_sftp2_symlink_operations(sftp2_mocker):
    """Test symlink-related operations"""
    from megfile.sftp2_path import Sftp2Path

    with sftp2.sftp2_open("sftp2://username@host/target.txt", "w") as f:
        f.write("target")

    # Create symlink
    sftp2.sftp2_symlink(
        "sftp2://username@host/target.txt", "sftp2://username@host/link.txt"
    )

    # Test is_symlink
    link_path = Sftp2Path("sftp2://username@host/link.txt")
    assert link_path.is_symlink() is True


def test_sftp2_scan_single_file(sftp2_mocker):
    """Test scan on single file"""
    with sftp2.sftp2_open("sftp2://username@host/file.txt", "w") as f:
        f.write("test")

    result = list(sftp2.sftp2_scan("sftp2://username@host/file.txt"))
    assert len(result) == 1


def test_sftp2_getmtime_getsize(sftp2_mocker):
    """Test getmtime and getsize"""
    with sftp2.sftp2_open("sftp2://username@host/file.txt", "w") as f:
        f.write("test content")

    size = sftp2.sftp2_getsize("sftp2://username@host/file.txt")
    mtime = sftp2.sftp2_getmtime("sftp2://username@host/file.txt")

    assert size == 12
    assert mtime > 0


def test_sftp2_listdir(sftp2_mocker):
    """Test listdir method"""
    from megfile.sftp2_path import Sftp2Path

    sftp2.sftp2_makedirs("sftp2://username@host/dir")
    with sftp2.sftp2_open("sftp2://username@host/dir/file1.txt", "w") as f:
        f.write("test1")
    with sftp2.sftp2_open("sftp2://username@host/dir/file2.txt", "w") as f:
        f.write("test2")

    result = Sftp2Path("sftp2://username@host/dir").listdir()
    assert len(result) == 2
    assert "file1.txt" in result
    assert "file2.txt" in result

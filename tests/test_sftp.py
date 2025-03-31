import io
import logging
import os
import shutil
import stat
import subprocess
import time
from typing import List, Optional

import paramiko
import pytest

from megfile import sftp, sftp_path
from megfile.errors import SameFileError


class FakeSFTPClient:
    def __init__(self):
        self._retry_times = 0
        self.sock = None

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        pass

    def listdir(self, path="."):
        return os.listdir(path)

    def listdir_attr(self, path="."):
        return list(self.listdir_iter(path=path))

    def listdir_iter(self, path=".", read_aheads=50):
        for filename in os.listdir(path):
            yield paramiko.SFTPAttributes.from_stat(
                os.stat(os.path.join(path, filename))
            )

    def open(self, filename, mode="r", bufsize=-1):
        if "r" in mode and "b" not in mode:
            mode = mode + "b"
        return io.open(file=filename, mode=mode, buffering=bufsize)

    # Python continues to vacillate about "open" vs "file"...
    file = open

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
        return paramiko.SFTPAttributes.from_stat(os.stat(path))

    def lstat(self, path):
        return paramiko.SFTPAttributes.from_stat(os.lstat(path))

    def symlink(self, source, dest):
        os.symlink(source, dest)

    def chmod(self, path, mode):
        os.chmod(path, mode)

    def chown(self, path, uid, gid):
        os.chown(path, uid, gid)

    def utime(self, path, times):
        if times is None:
            times = (time.time(), time.time())
        os.utime(path, times)

    def truncate(self, path, size):
        os.truncate(path, size)

    def readlink(self, path):
        path = os.readlink(path)
        if not os.path.exists(path):
            return None
        return path

    def normalize(self, path):
        return os.path.realpath(path)

    def chdir(self, path=None):
        os.chdir(path)

    def getcwd(self):
        return os.getcwd()

    def putfo(self, fl, remotepath, file_size=0, callback=None, confirm=True):
        with io.open(remotepath, "wb") as fdst:
            if file_size:
                while True:
                    buf = fl.read(file_size)
                    if not buf:
                        break
                    fdst.write(buf)
                    if callback:
                        callback(len(buf), len(buf))
            else:
                buf = fl.read()
                fdst.write(buf)

    def put(self, localpath, remotepath, callback=None, confirm=True):
        file_size = os.stat(localpath).st_size
        with io.open(localpath, "rb") as fl:
            return self.putfo(fl, remotepath, file_size, callback, confirm)

    def getfo(self, remotepath, fl, callback=None, prefetch=True):
        with io.open(remotepath, "rb") as fr:
            buf = fr.read()
            fl.write(buf)
            if callback:
                callback(len(buf), len(buf))

    def get(self, remotepath, localpath, callback=None, prefetch=True):
        with io.open(localpath, "wb") as fl:
            self.getfo(remotepath, fl, callback, prefetch)

    def _request(self, *args, **kwargs):
        self._retry_times += 1
        if self._retry_times <= 1:
            raise OSError("Socket is closed")
        elif self._retry_times <= 2:
            raise paramiko.SSHException()
        raise OSError("test error")


def _fake_exec_command(
    command: List[str],
    bufsize: int = -1,
    timeout: Optional[int] = None,
    environment: Optional[int] = None,
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
def sftp_mocker(fs, mocker):
    client = FakeSFTPClient()
    sftp_path._patch_sftp_client_request(client, "")
    mocker.patch("megfile.sftp_path._get_sftp_client", return_value=client)
    mocker.patch("megfile.sftp_path._get_ssh_client", return_value=client)
    mocker.patch(
        "megfile.sftp_path.SftpPath._exec_command", side_effect=_fake_exec_command
    )
    yield client


def test_sftp_retry(sftp_mocker):
    client = sftp_path.get_ssh_client("")

    with pytest.raises(OSError):
        client._request()
    assert client._retry_times > 2


def test_is_sftp():
    assert sftp.is_sftp("sftp://username@host//data") is True
    assert sftp.is_sftp("ftp://username@host/data") is False


def test_sftp_readlink(sftp_mocker):
    path = "sftp://username@host//file"
    link_path = "sftp://username@host//file.lnk"

    with sftp.sftp_open(path, "w") as f:
        f.write("test")

    sftp.sftp_symlink(path, link_path)
    assert sftp.sftp_readlink(link_path) == path

    with pytest.raises(FileNotFoundError):
        sftp.sftp_readlink("sftp://username@host//notFound")

    with pytest.raises(OSError):
        sftp.sftp_readlink("sftp://username@host//file")

    path = "sftp://username@host//notFound"
    link_path = "sftp://username@host//notFound.lnk"
    sftp.sftp_symlink(path, link_path)
    with pytest.raises(OSError):
        sftp.sftp_readlink("sftp://username@host//notFound.lnk")


def test_sftp_readlink_relative_path(fs, mocker):
    class FakeSFTPClient2(FakeSFTPClient):
        def normalize(self, path):
            if path == ".":
                return "/root"
            return os.path.relpath(path, start="/root")

    client = FakeSFTPClient2()
    mocker.patch("megfile.sftp_path.get_sftp_client", return_value=client)

    path = "sftp://username@host/file"
    link_path = "sftp://username@host/file.lnk"

    os.makedirs("/root")
    with open("/root/file", "w") as f:
        f.write("test")
    os.chdir("/root")
    os.symlink("file", "/root/file.lnk")

    assert sftp.sftp_readlink(link_path) == path
    assert (
        sftp.sftp_readlink("sftp://username@host//root/file.lnk")
        == "sftp://username@host//root/file"
    )


def test_sftp_absolute(sftp_mocker):
    assert (
        sftp.sftp_absolute("sftp://username@host//dir/../file")
        == "sftp://username@host//file"
    )


def test_sftp_resolve(sftp_mocker):
    assert (
        sftp.sftp_resolve("sftp://username@host//dir/../file")
        == "sftp://username@host//file"
    )


def test_sftp_glob(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    sftp.sftp_makedirs("sftp://username@host//A/a")
    sftp.sftp_makedirs("sftp://username@host//A/b")
    sftp.sftp_makedirs("sftp://username@host//A/b/c")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")

    with sftp.sftp_open("sftp://username@host//A/b/file.json", "w") as f:
        f.write("file")

    assert sftp.sftp_glob("sftp://username@host//A/*") == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/a",
        "sftp://username@host//A/b",
    ]
    assert list(sftp.sftp_iglob("sftp://username@host//A/*")) == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/a",
        "sftp://username@host//A/b",
    ]
    assert sftp.sftp_glob("sftp://username@host//A/**/*.json") == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/b/file.json",
    ]
    assert [
        file_entry.path
        for file_entry in sftp.sftp_glob_stat("sftp://username@host//A/*")
    ] == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/a",
        "sftp://username@host//A/b",
    ]
    assert sftp.sftp_glob("sftp://username@host//A/**/*") == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/a",
        "sftp://username@host//A/b",
        "sftp://username@host//A/b/c",
        "sftp://username@host//A/b/file.json",
    ]
    assert sftp.sftp_glob("sftp://username@host//A/") == ["sftp://username@host//A/"]


def test_sftp_isdir_sftp_isfile(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A/B", parents=True)

    with sftp.sftp_open("sftp://username@host//A/B/file", "w") as f:
        f.write("test")

    assert sftp.sftp_isdir("sftp://username@host//A/B") is True
    assert sftp.sftp_isdir("sftp://username@host//A/B/file") is False
    assert sftp.sftp_isfile("sftp://username@host//A/B/file") is True
    assert sftp.sftp_isfile("sftp://username@host//A/B") is False
    assert sftp.sftp_isfile("sftp://username@host//A/C") is False


def test_sftp_exists(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A/B", parents=True)

    with sftp.sftp_open("sftp://username@host//A/B/file", "w") as f:
        f.write("test")

    assert sftp.sftp_exists("sftp://username@host//A/B/file") is True

    sftp.sftp_symlink(
        "sftp://username@host//A/B/file", "sftp://username@host//A/B/file.lnk"
    )
    sftp.sftp_unlink("sftp://username@host//A/B/file")
    assert sftp.sftp_exists("sftp://username@host//A/B/file.lnk") is True
    assert (
        sftp.sftp_exists("sftp://username@host//A/B/file.lnk", followlinks=True)
        is False
    )


def test_sftp_scandir(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    sftp.sftp_makedirs("sftp://username@host//A/a")
    sftp.sftp_makedirs("sftp://username@host//A/b")
    sftp.sftp_makedirs("sftp://username@host//A/b/c")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")

    with sftp.sftp_open("sftp://username@host//A/b/file.json", "w") as f:
        f.write("file")

    assert sorted(
        [file_entry.path for file_entry in sftp.sftp_scandir("sftp://username@host//A")]
    ) == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/a",
        "sftp://username@host//A/b",
    ]

    sftp.sftp_symlink("sftp://username@host//A", "sftp://username@host//B")
    assert sorted(
        [file_entry.path for file_entry in sftp.sftp_scandir("sftp://username@host//B")]
    ) == [
        "sftp://username@host//B/1.json",
        "sftp://username@host//B/a",
        "sftp://username@host//B/b",
    ]

    with pytest.raises(FileNotFoundError):
        list(sftp.sftp_scandir("sftp://username@host//A/not_found"))

    with pytest.raises(NotADirectoryError):
        list(sftp.sftp_scandir("sftp://username@host//A/1.json"))


def test_sftp_stat(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/test", "w") as f:
        f.write("test")
    sftp.sftp_symlink(
        "sftp://username@host//A/test", "sftp://username@host//A/test.lnk"
    )

    stat = sftp.sftp_stat("sftp://username@host//A/test", follow_symlinks=True)
    os_stat = os.stat("/A/test")
    assert stat.size == os_stat.st_size
    assert stat.isdir is False
    assert stat.islnk is False
    assert stat.mtime == os_stat.st_mtime
    assert stat.ctime == 0

    stat = sftp.sftp_stat("sftp://username@host//A/test.lnk", follow_symlinks=True)
    os_stat = os.stat("/A/test")
    assert stat.size == os_stat.st_size
    assert stat.isdir is False
    assert stat.islnk is False
    assert stat.mtime == os_stat.st_mtime
    assert stat.ctime == 0

    stat = sftp.sftp_lstat("sftp://username@host//A/test.lnk")
    os_stat = os.lstat("/A/test.lnk")
    assert stat.size == os_stat.st_size
    assert stat.isdir is False
    assert stat.islnk is True
    assert stat.mtime == os_stat.st_mtime
    assert stat.ctime == 0

    os_stat = os.stat("/A/test")
    assert sftp.sftp_getmtime("sftp://username@host//A/test") == os_stat.st_mtime
    assert sftp.sftp_getsize("sftp://username@host//A/test") == os_stat.st_size


def test_sftp_listdir(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    sftp.sftp_makedirs("sftp://username@host//A/a")
    sftp.sftp_makedirs("sftp://username@host//A/b")
    sftp.sftp_makedirs("sftp://username@host//A/b/c")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")

    assert sftp.sftp_listdir("sftp://username@host//A") == ["1.json", "a", "b"]


def test_sftp_load_from(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/test", "w") as f:
        f.write("test")
    assert sftp.sftp_load_from("sftp://username@host//A/test").read() == b"test"


def test_sftp_makedirs(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A/B/C", parents=True)
    assert sftp.sftp_exists("sftp://username@host//A/B/C") is True

    with pytest.raises(FileExistsError):
        sftp.sftp_makedirs("sftp://username@host//A/B/C")

    with pytest.raises(FileNotFoundError):
        sftp.sftp_makedirs("sftp://username@host//D/B/C")


def test_sftp_realpath(sftp_mocker, mocker):
    assert (
        sftp.sftp_realpath("sftp://username@host//A/../B/C")
        == "sftp://username@host//B/C"
    )
    assert (
        sftp.sftp_realpath("sftp://username@host/A/B/C")
        == "sftp://username@host//A/B/C"
    )


def test_sftp_realpath_relative(fs, mocker):
    class FakeSFTPClient2(FakeSFTPClient):
        def normalize(self, path):
            if path == ".":
                return "/home/username"
            return os.path.join("/home/username", path)

    client = FakeSFTPClient2()
    mocker.patch("megfile.sftp_path.get_sftp_client", return_value=client)
    assert (
        sftp.sftp_realpath("sftp://username@host/A/B/C")
        == "sftp://username@host//home/username/A/B/C"
    )
    assert (
        sftp.sftp_realpath("sftp://username@host//A/B/C")
        == "sftp://username@host//A/B/C"
    )


def test_sftp_rename(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/test", "w") as f:
        f.write("test")

    sftp.sftp_rename("sftp://username@host//A/test", "sftp://username@host//A/test2")
    assert sftp.sftp_exists("sftp://username@host//A/test") is False
    assert sftp.sftp_exists("sftp://username@host//A/test2") is True

    sftp.sftp_rename("sftp://username@host//A", "sftp://username@host//A2")
    assert sftp.sftp_exists("sftp://username@host//A/test2") is False
    assert sftp.sftp_exists("sftp://username@host//A2/test2") is True

    sftp.sftp_rename("sftp://username@host//A2/test2", "sftp://username2@host2/A2/test")
    assert sftp.sftp_exists("sftp://username@host//A2/test2") is False
    assert sftp.sftp_exists("sftp://username2@host2/A2/test") is True

    sftp.sftp_rename("sftp://username@host//A2", "sftp://username2@host2/A")
    assert sftp.sftp_exists("sftp://username@host//A2/test") is False
    assert sftp.sftp_exists("sftp://username2@host2/A/test") is True

    with sftp.sftp_open("sftp://username@host//A/test2", "w") as f:
        f.write("test2")

    with pytest.raises(OSError):
        sftp.sftp_rename("sftp://username@host//A/test2", "/A/test")


def test_sftp_move(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/test", "w") as f:
        f.write("test")

    sftp.sftp_move("sftp://username@host//A/test", "sftp://username@host//A/test2")
    assert sftp.sftp_exists("sftp://username@host//A/test") is False
    assert sftp.sftp_exists("sftp://username@host//A/test2") is True

    sftp.sftp_move("sftp://username@host//A", "sftp://username@host//A2")
    assert sftp.sftp_exists("sftp://username@host//A/test2") is False
    assert sftp.sftp_exists("sftp://username@host//A2/test2") is True

    sftp.sftp_makedirs("sftp://username@host//A3")
    with sftp.sftp_open("sftp://username@host//A3/test2", "w") as f:
        f.write("test3")
    sftp.sftp_move(
        "sftp://username@host//A3", "sftp://username@host//A2", overwrite=False
    )
    assert sftp.sftp_exists("sftp://username@host//A3/test2") is False
    assert sftp.sftp_exists("sftp://username@host//A2/test2") is True
    with sftp.sftp_open("sftp://username@host//A2/test2", "r") as f:
        assert f.read() == "test"

    sftp.sftp_makedirs("sftp://username@host//A3")
    with sftp.sftp_open("sftp://username@host//A3/test2", "w") as f:
        f.write("test3")
    sftp.sftp_move(
        "sftp://username@host//A3", "sftp://username@host//A2", overwrite=True
    )
    assert sftp.sftp_exists("sftp://username@host//A3/test2") is False
    assert sftp.sftp_exists("sftp://username@host//A2/test2") is True
    with sftp.sftp_open("sftp://username@host//A2/test2", "r") as f:
        assert f.read() == "test3"


def test_sftp_open(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")

    with sftp.sftp_open("sftp://username@host//A/test", "w") as f:
        f.write("test")

    with sftp.sftp_open("sftp://username@host//A/test", "r") as f:
        assert f.read() == "test"

    with sftp.sftp_open("sftp://username@host//A/test", "rb") as f:
        assert f.read() == b"test"

    with pytest.raises(FileNotFoundError):
        with sftp.sftp_open("sftp://username@host//A/notFound", "r") as f:
            f.read()

    with pytest.raises(IsADirectoryError):
        with sftp.sftp_open("sftp://username@host//A", "r") as f:
            f.read()

    with pytest.raises(IsADirectoryError):
        with sftp.sftp_open("sftp://username@host//A", "w") as f:
            f.read()


def test_sftp_remove(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/test", "w") as f:
        f.write("test")

    sftp.sftp_remove("sftp://username@host//A/test")
    sftp.sftp_remove("sftp://username@host//A/test", missing_ok=True)

    assert sftp.sftp_exists("sftp://username@host//A/test") is False
    assert sftp.sftp_exists("sftp://username@host//A") is True

    with sftp.sftp_open("sftp://username@host//A/test", "w") as f:
        f.write("test")
    sftp.sftp_remove("sftp://username@host//A")
    assert sftp.sftp_exists("sftp://username@host//A") is False


def test_sftp_scan(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    sftp.sftp_makedirs("sftp://username@host//A/a")
    sftp.sftp_makedirs("sftp://username@host//A/b")
    sftp.sftp_makedirs("sftp://username@host//A/b/c")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")
    sftp.sftp_symlink(
        "sftp://username@host//A/1.json", "sftp://username@host//A/1.json.lnk"
    )

    with sftp.sftp_open("sftp://username@host//A/b/file.json", "w") as f:
        f.write("file")

    assert list(sftp.sftp_scan("sftp://username@host//A")) == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/1.json.lnk",
        "sftp://username@host//A/b/file.json",
    ]

    assert list(sftp.sftp_scan("sftp://username@host//A", followlinks=True)) == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/1.json.lnk",
        "sftp://username@host//A/b/file.json",
    ]

    assert [
        file_entry.path for file_entry in sftp.sftp_scan_stat("sftp://username@host//A")
    ] == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/1.json.lnk",
        "sftp://username@host//A/b/file.json",
    ]

    assert [
        file_entry.stat.size
        for file_entry in sftp.sftp_scan_stat(
            "sftp://username@host//A", followlinks=True
        )
    ] == [
        os.stat("/A/1.json").st_size,
        os.stat("/A/1.json").st_size,
        os.stat("/A/b/file.json").st_size,
    ]

    with pytest.raises(FileNotFoundError):
        list(sftp.sftp_scan_stat("sftp://username@host//B", missing_ok=False))


def test_sftp_unlink(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/test", "w") as f:
        f.write("test")

    sftp.sftp_unlink("sftp://username@host//A/test")
    sftp.sftp_unlink("sftp://username@host//A/test", missing_ok=True)

    assert sftp.sftp_exists("sftp://username@host//A/test") is False
    assert sftp.sftp_exists("sftp://username@host//A") is True

    with pytest.raises(IsADirectoryError):
        sftp.sftp_unlink("sftp://username@host//A")


def test_sftp_walk(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    sftp.sftp_makedirs("sftp://username@host//A/a")
    sftp.sftp_makedirs("sftp://username@host//A/a/b")
    sftp.sftp_makedirs("sftp://username@host//A/b")
    sftp.sftp_makedirs("sftp://username@host//A/b/c")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")
    with sftp.sftp_open("sftp://username@host//A/a/2.json", "w") as f:
        f.write("2.json")
    with sftp.sftp_open("sftp://username@host//A/b/3.json", "w") as f:
        f.write("3.json")

    assert list(sftp.sftp_walk("sftp://username@host//A")) == [
        ("sftp://username@host//A", ["a", "b"], ["1.json"]),
        ("sftp://username@host//A/a", ["b"], ["2.json"]),
        ("sftp://username@host//A/a/b", [], []),
        ("sftp://username@host//A/b", ["c"], ["3.json"]),
        ("sftp://username@host//A/b/c", [], []),
    ]

    assert list(sftp.sftp_walk("sftp://username@host//A/not_found")) == []
    assert list(sftp.sftp_walk("sftp://username@host//A/1.json")) == []


def test_sftp_getmd5(sftp_mocker):
    from megfile.fs import fs_getmd5

    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")
    assert sftp.sftp_getmd5("sftp://username@host//A/1.json") == fs_getmd5("/A/1.json")
    assert sftp.sftp_getmd5("sftp://username@host//A") == fs_getmd5("/A")


def test_sftp_symlink(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")
    sftp.sftp_symlink(
        "sftp://username@host//A/1.json", "sftp://username@host//A/1.json.lnk"
    )
    assert sftp.sftp_islink("sftp://username@host//A/1.json.lnk") is True
    assert sftp.sftp_islink("sftp://username@host//A/1.json") is False

    with pytest.raises(FileExistsError):
        sftp.sftp_symlink(
            "sftp://username@host//A/1.json", "sftp://username@host//A/1.json.lnk"
        )


def test_sftp_save_as(sftp_mocker):
    sftp.sftp_save_as(io.BytesIO(b"test"), "sftp://username@host//test")
    assert sftp.sftp_load_from("sftp://username@host//test").read() == b"test"


def test_sftp_chmod(sftp_mocker):
    path = "sftp://username@host//test"
    sftp.sftp_save_as(io.BytesIO(b"test"), path)

    sftp.sftp_chmod(path, mode=0o777)
    assert stat.S_IMODE(os.stat("/test").st_mode) == 0o777


def test_sftp_rmdir(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")

    with pytest.raises(OSError):
        sftp.sftp_rmdir("sftp://username@host//A")

    with pytest.raises(NotADirectoryError):
        sftp.sftp_rmdir("sftp://username@host//A/1.json")

    sftp.sftp_unlink("sftp://username@host//A/1.json")
    sftp.sftp_rmdir("sftp://username@host//A")
    assert sftp.sftp_exists("sftp://username@host//A") is False


def test_sftp_copy(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")
    sftp.sftp_symlink(
        "sftp://username@host//A/1.json", "sftp://username@host//A/1.json.lnk"
    )

    def callback(length):
        assert length == len("1.json")

    sftp.sftp_copy(
        "sftp://username@host//A/1.json.lnk",
        "sftp://username@host//A2/1.json.bak",
        followlinks=True,
        callback=callback,
    )

    assert (
        sftp.sftp_stat("sftp://username@host//A/1.json").size
        == sftp.sftp_stat("sftp://username@host//A2/1.json.bak").size
    )

    with sftp.sftp_open("sftp://username@host//A/2.json", "w") as f:
        f.write("2")
    sftp.sftp_copy(
        "sftp://username@host//A/2.json",
        "sftp://username@host//A2/1.json.bak",
        overwrite=False,
    )
    assert (
        sftp.sftp_stat("sftp://username@host//A/1.json").size
        == sftp.sftp_stat("sftp://username@host//A2/1.json.bak").size
    )

    sftp.sftp_copy(
        "sftp://username@host//A/2.json",
        "sftp://username@host//A2/1.json.bak",
        overwrite=True,
    )
    assert (
        sftp.sftp_stat("sftp://username@host//A/2.json").size
        == sftp.sftp_stat("sftp://username@host//A2/1.json.bak").size
    )


def test_sftp_copy_error(sftp_mocker, mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")

    with pytest.raises(IsADirectoryError):
        sftp.sftp_copy("sftp://username@host//A", "sftp://username@host//A2")

    with pytest.raises(IsADirectoryError):
        sftp.sftp_copy("sftp://username@host//A/1.json", "sftp://username@host//A2/")

    with pytest.raises(OSError):
        sftp.sftp_copy("sftp://username@host//A", "/A2")

    with pytest.raises(SameFileError):
        sftp.sftp_copy(
            "sftp://username@host//A/1.json", "sftp://username@host//A/1.json"
        )

    def _exec_command_error(*args, **kwargs):
        return subprocess.CompletedProcess(
            args=[], returncode=1, stdout=b"", stderr=b""
        )

    mocker.patch(
        "megfile.sftp_path.SftpPath._exec_command", side_effect=_exec_command_error
    )

    with pytest.raises(OSError):
        sftp.sftp_copy(
            "sftp://username@host//A/1.json", "sftp://username@host//A2/2.json"
        )


def test_sftp_copy_with_different_host(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")

    def callback(length):
        assert length == len("1.json")

    sftp.sftp_copy(
        "sftp://username@host//A/1.json",
        "sftp://username@host2/A/2.json",
        callback=callback,
    )

    assert (
        sftp.sftp_stat("sftp://username@host//A/1.json").size
        == sftp.sftp_stat("sftp://username@host2/A/2.json").size
    )


def test_sftp_sync(sftp_mocker, mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")

    sftp.sftp_sync("sftp://username@host//A", "sftp://username@host//A2")
    assert (
        sftp.sftp_stat("sftp://username@host//A/1.json").size
        == sftp.sftp_stat("sftp://username@host//A2/1.json").size
    )
    assert (
        sftp.sftp_stat("sftp://username@host//A/1.json").mtime
        == sftp.sftp_stat("sftp://username@host//A2/1.json").mtime
    )

    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("2")
    sftp.sftp_sync(
        "sftp://username@host//A", "sftp://username@host//A2", overwrite=False
    )
    assert sftp.sftp_stat("sftp://username@host//A2/1.json").size == 6

    sftp.sftp_sync("sftp://username@host//A", "sftp://username@host//A2", force=True)
    assert sftp.sftp_stat("sftp://username@host//A2/1.json").size == 1

    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("22")
    sftp.sftp_sync(
        "sftp://username@host//A", "sftp://username@host//A2", overwrite=True
    )
    assert sftp.sftp_stat("sftp://username@host//A2/1.json").size == 2

    sftp.sftp_sync(
        "sftp://username@host//A/1.json", "sftp://username@host//A/1.json.bak"
    )
    assert (
        sftp.sftp_stat("sftp://username@host//A/1.json").size
        == sftp.sftp_stat("sftp://username@host//A/1.json.bak").size
    )

    with pytest.raises(IsADirectoryError):
        sftp.sftp_sync("sftp://username@host//A/1.json", "sftp://username@host//A2/")

    func = mocker.patch("megfile.sftp_path.SftpPath.copy")
    sftp.sftp_sync("sftp://username@host//A2", "sftp://username@host//A")
    assert func.call_count == 0


def test_sftp_download(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")
    sftp.sftp_symlink(
        "sftp://username@host//A/1.json", "sftp://username@host//A/1.json.lnk"
    )

    sftp.sftp_download(
        "sftp://username@host//A/1.json.lnk", "/A2/1.json", followlinks=True
    )
    assert (
        sftp.sftp_stat("sftp://username@host//A/1.json").size
        == os.stat("/A2/1.json").st_size
    )

    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("2")
    sftp.sftp_download(
        sftp.SftpPath("sftp://username@host//A/1.json"), "/A2/1.json", overwrite=False
    )
    assert 6 == os.stat("/A2/1.json").st_size

    def callback(length):
        assert length == 1

    sftp.sftp_download(
        sftp.SftpPath("sftp://username@host//A/1.json"),
        "/A2/1.json",
        overwrite=True,
        callback=callback,
    )
    assert 1 == os.stat("/A2/1.json").st_size

    sftp.sftp_download(
        "sftp://username@host//A/1.json.lnk", "file:///A2/2.json", followlinks=True
    )
    assert (
        sftp.sftp_stat("sftp://username@host//A/1.json").size
        == os.stat("/A2/2.json").st_size
    )

    with pytest.raises(OSError):
        sftp.sftp_download(
            "sftp://username@host//A/1.json", "sftp://username@host//1.json"
        )

    with pytest.raises(OSError):
        sftp.sftp_download("/1.json", "/1.json")

    with pytest.raises(IsADirectoryError):
        sftp.sftp_download("sftp://username@host//A", "/1.json")

    with pytest.raises(IsADirectoryError):
        sftp.sftp_download("sftp://username@host//A/1.json", "/1/")


def test_sftp_upload(sftp_mocker):
    with sftp.sftp_open("/1.json", "w") as f:
        f.write("1.json")
    os.symlink("/1.json", "/1.json.lnk")

    sftp.sftp_upload("/1.json.lnk", "sftp://username@host//A/1.json", followlinks=True)
    assert (
        sftp.sftp_stat("sftp://username@host//A/1.json").size
        == os.stat("/1.json").st_size
    )

    with sftp.sftp_open("/1.json", "w") as f:
        f.write("2")
    sftp.sftp_upload("/1.json.lnk", "sftp://username@host//A/1.json", overwrite=False)
    assert sftp.sftp_stat("sftp://username@host//A/1.json").size == 6

    def callback(length):
        assert length == 1

    sftp.sftp_upload(
        "/1.json.lnk",
        sftp.SftpPath("sftp://username@host//A/1.json"),
        overwrite=True,
        callback=callback,
    )
    assert sftp.sftp_stat("sftp://username@host//A/1.json").size == 1

    sftp.sftp_upload("file:///1.json", "sftp://username@host//A/2.json")
    assert (
        sftp.sftp_stat("sftp://username@host//A/2.json").size
        == os.stat("/1.json").st_size
    )

    with pytest.raises(OSError):
        sftp.sftp_upload(
            "sftp://username@host//A/1.json", "sftp://username@host//1.json"
        )

    with pytest.raises(OSError):
        sftp.sftp_upload("/1.json", "/1.json")

    with pytest.raises(IsADirectoryError):
        sftp.sftp_upload("/", "sftp://username@host//A")

    with pytest.raises(IsADirectoryError):
        sftp.sftp_upload("/1.json", "sftp://username@host//A/")


def test_sftp_path_join():
    assert (
        sftp.sftp_path_join("sftp://username@host//A/", "a", "b")
        == "sftp://username@host//A/a/b"
    )
    assert (
        sftp.sftp_path_join("sftp://username@host/A/", "a", "b")
        == "sftp://username@host/A/a/b"
    )


def test_sftp_concat(sftp_mocker, mocker):
    with sftp.sftp_open("sftp://username@host//1", "w") as f:
        f.write("1")
    with sftp.sftp_open("sftp://username@host//2", "w") as f:
        f.write("2")
    with sftp.sftp_open("sftp://username@host//3", "w") as f:
        f.write("3")

    sftp.sftp_concat(
        [
            "sftp://username@host//1",
            "sftp://username@host//2",
            "sftp://username@host//3",
        ],
        "sftp://username@host//4",
    )
    with sftp.sftp_open("sftp://username@host//4", "r") as f:
        assert f.read() == "123"

    def _error_exec_command(
        command: List[str],
        bufsize: int = -1,
        timeout: Optional[int] = None,
        environment: Optional[int] = None,
    ):
        return subprocess.CompletedProcess(args=command, returncode=1)

    mocker.patch(
        "megfile.sftp_path.SftpPath._exec_command", side_effect=_error_exec_command
    )
    with pytest.raises(OSError):
        sftp.sftp_concat(
            [
                "sftp://username@host//1",
                "sftp://username@host//2",
                "sftp://username@host//3",
            ],
            "sftp://username@host//4",
        )


def test_sftp_add_host_key(fs, mocker):
    connect_times = 0

    class FakeKey:
        def get_name(self):
            return "ssh-ed25519"

        def asbytes(self):
            return b"test"

        def get_base64(self):
            return "test"

    class FakeTransport:
        def __init__(self, *args, **kwargs):
            pass

        def connect(self):
            nonlocal connect_times
            connect_times += 1

        def get_remote_server_key(self):
            return FakeKey()

        def close(self):
            pass

    mocker.patch("paramiko.Transport", return_value=FakeTransport())

    host_key_path = os.path.expanduser("~/.ssh/known_hosts")

    mocker.patch("builtins.input", return_value="no")
    sftp.sftp_add_host_key("127.0.0.1", prompt=True)
    with open(host_key_path, "r") as f:
        assert "" == f.read()
    assert connect_times == 1

    mocker.patch("builtins.input", return_value="yes")
    sftp.sftp_add_host_key("127.0.0.1", prompt=True)
    with open(host_key_path, "r") as f:
        assert "127.0.0.1 ssh-ed25519 test\n" == f.read()
    assert connect_times == 2

    file_stat = os.stat(host_key_path)
    assert stat.S_IMODE(file_stat.st_mode) == 0o600
    dir_stat = os.stat(os.path.dirname(host_key_path))
    assert stat.S_IMODE(dir_stat.st_mode) == 0o700

    mocker.patch("paramiko.hostkeys.HostKeys.lookup", return_value="ssh-rsa")
    sftp.sftp_add_host_key("127.0.0.1")
    assert connect_times == 2


def test__check_input(mocker, caplog):
    mocker.patch("builtins.input", return_value="xxx")
    with caplog.at_level(logging.WARNING, logger="megfile"):
        sftp._check_input("xxx", "fingerprint", times=9)
    assert "Retried more than 10 times, give up" in caplog.text

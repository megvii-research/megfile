import io
import os
import shutil
from datetime import datetime
from typing import Dict, Iterator, List

import pytest

from megfile import webdav
from megfile.errors import SameFileError


class FakeWebdavClient:
    """Mock WebDAV client that uses local filesystem"""

    def __init__(self, options: dict):
        self.options = options
        self.hostname = options.get("webdav_hostname", "")

    def _relative_path(self, path: str) -> str:
        if path.startswith("/"):
            return f".{path}"
        return path

    def check(self, path: str) -> bool:
        """Check if path exists"""
        path = self._relative_path(path)
        return os.path.exists(path)

    def is_dir(self, path: str) -> bool:
        """Check if path is a directory"""
        path = self._relative_path(path)
        return os.path.isdir(path)

    def list(self, path: str, get_info: bool = False) -> List:
        """List directory contents"""
        path = self._relative_path(path)

        if not get_info:
            return os.listdir(path)

        items = []
        for name in os.listdir(path):
            item_path = os.path.join(path, name)
            stat = os.stat(item_path)
            items.append(
                {
                    "path": item_path,
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    "is_dir": os.path.isdir(item_path),
                }
            )
        return items

    def info(self, path: str) -> Dict:
        """Get file/directory info"""
        path = self._relative_path(path)
        if not os.path.exists(path):
            from webdav3.exceptions import RemoteResourceNotFound

            raise RemoteResourceNotFound(path)

        stat = os.stat(path)
        return {
            "path": path,
            "size": stat.st_size,
            "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "is_dir": os.path.isdir(path),
        }

    def mkdir(self, path: str):
        """Create directory"""
        path = self._relative_path(path)
        os.mkdir(path)

    def clean(self, path: str):
        """Remove file or directory"""
        path = self._relative_path(path)
        if not os.path.exists(path):
            from webdav3.exceptions import RemoteResourceNotFound

            raise RemoteResourceNotFound(path)

        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.unlink(path)

    def copy(self, src: str, dst: str):
        """Copy file or directory"""
        src = self._relative_path(src)
        if os.path.isdir(src):
            shutil.copytree(src, dst)
        else:
            shutil.copy2(src, dst)

    def move(self, src: str, dst: str, overwrite: bool = False):
        """Move/rename file or directory"""
        src = self._relative_path(src)
        dst = self._relative_path(dst)
        if overwrite and os.path.exists(dst):
            if os.path.isdir(dst):
                shutil.rmtree(dst)
            else:
                os.unlink(dst)
        # Create parent directory if needed
        parent = os.path.dirname(dst)
        if parent and not os.path.exists(parent):
            os.makedirs(parent)
        shutil.move(src, dst)

    def download_from(self, buffer: io.BytesIO, remote_path: str):
        """Download file to buffer"""
        remote_path = self._relative_path(remote_path)
        with open(remote_path, "rb") as f:
            buffer.write(f.read())

    def upload_to(self, buffer: io.BytesIO, remote_path: str):
        """Upload buffer to file"""
        remote_path = self._relative_path(remote_path)
        # Create parent directory if needed
        parent = os.path.dirname(remote_path)
        if parent and not os.path.exists(parent):
            os.makedirs(parent)

        with open(remote_path, "wb") as f:
            f.write(buffer.read())


@pytest.fixture
def webdav_mocker(fs, mocker):
    """Mock WebDAV client to use local filesystem"""

    def fake_get_webdav_client(hostname, username=None, password=None, token=None):
        return FakeWebdavClient({"webdav_hostname": hostname})

    def fake_webdav_stat(client, path: str) -> Dict:
        return client.info(path)

    def fake_webdav_scan(client, path: str) -> Iterator[Dict]:
        for info in client.list(path, get_info=True):
            if info["is_dir"]:
                yield from fake_webdav_scan(client, info["path"])
            yield info

    mocker.patch(
        "megfile.webdav_path._get_webdav_client", side_effect=fake_get_webdav_client
    )
    mocker.patch(
        "megfile.webdav_path.get_webdav_client", side_effect=fake_get_webdav_client
    )
    mocker.patch("megfile.webdav_path._webdav_stat", side_effect=fake_webdav_stat)
    mocker.patch("megfile.webdav_path._webdav_scan", side_effect=fake_webdav_scan)
    yield


def test_is_webdav():
    assert webdav.is_webdav("webdav://host/data") is True
    assert webdav.is_webdav("webdavs://host/data") is True
    assert webdav.is_webdav("http://host/data") is False
    assert webdav.is_webdav("https://host/data") is False
    assert webdav.is_webdav("ftp://host/data") is False
    assert webdav.is_webdav("sftp://host/data") is False


def test_webdav_glob(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    webdav.webdav_makedirs("webdav://host/A/a")
    webdav.webdav_makedirs("webdav://host/A/b")
    webdav.webdav_makedirs("webdav://host/A/b/c")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    with webdav.webdav_open("webdav://host/A/b/file.json", "w") as f:
        f.write("file")

    assert sorted(webdav.webdav_glob("webdav://host/A/*")) == [
        "webdav://host/A/1.json",
        "webdav://host/A/a",
        "webdav://host/A/b",
    ]
    assert list(sorted(webdav.webdav_iglob("webdav://host/A/*"))) == [
        "webdav://host/A/1.json",
        "webdav://host/A/a",
        "webdav://host/A/b",
    ]
    assert sorted(webdav.webdav_glob("webdav://host/A/**/*.json")) == [
        "webdav://host/A/1.json",
        "webdav://host/A/b/file.json",
    ]
    assert [
        file_entry.path
        for file_entry in sorted(webdav.webdav_glob_stat("webdav://host/A/*"))
    ] == [
        "webdav://host/A/1.json",
        "webdav://host/A/a",
        "webdav://host/A/b",
    ]


def test_webdav_isdir_webdav_isfile(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A/B", parents=True)

    with webdav.webdav_open("webdav://host/A/B/file", "w") as f:
        f.write("test")

    assert webdav.webdav_isdir("webdav://host/A/B") is True
    assert webdav.webdav_isdir("webdav://host/A/B/file") is False
    assert webdav.webdav_isfile("webdav://host/A/B/file") is True
    assert webdav.webdav_isfile("webdav://host/A/B") is False
    assert webdav.webdav_isfile("webdav://host/A/C") is False


def test_webdav_exists(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A/B", parents=True)

    with webdav.webdav_open("webdav://host/A/B/file", "w") as f:
        f.write("test")

    assert webdav.webdav_exists("webdav://host/A/B/file") is True
    assert webdav.webdav_exists("webdav://host/A/B") is True
    assert webdav.webdav_exists("webdav://host/A/C") is False


def test_webdav_scandir(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    webdav.webdav_makedirs("webdav://host/A/a")
    webdav.webdav_makedirs("webdav://host/A/b")
    webdav.webdav_makedirs("webdav://host/A/b/c")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    with webdav.webdav_open("webdav://host/A/b/file.json", "w") as f:
        f.write("file")

    assert sorted(
        [file_entry.path for file_entry in webdav.webdav_scandir("webdav://host/A")]
    ) == [
        "webdav://host/A/1.json",
        "webdav://host/A/a",
        "webdav://host/A/b",
    ]

    with pytest.raises(FileNotFoundError):
        list(webdav.webdav_scandir("webdav://host/A/not_found"))

    with pytest.raises(NotADirectoryError):
        list(webdav.webdav_scandir("webdav://host/A/1.json"))


def test_webdav_stat(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/test", "w") as f:
        f.write("test")

    stat = webdav.webdav_stat("webdav://host/A/test")
    os_stat = os.stat("/A/test")
    assert stat.size == os_stat.st_size
    assert stat.isdir is False
    assert stat.islnk is False  # WebDAV doesn't support symlinks

    # Use approximate comparison for mtime due to floating point precision
    assert (
        abs(webdav.webdav_getmtime("webdav://host/A/test") - os_stat.st_mtime) < 0.001
    )
    assert webdav.webdav_getsize("webdav://host/A/test") == os_stat.st_size


def test_webdav_listdir(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    webdav.webdav_makedirs("webdav://host/A/a")
    webdav.webdav_makedirs("webdav://host/A/b")
    webdav.webdav_makedirs("webdav://host/A/b/c")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    assert webdav.webdav_listdir("webdav://host/A") == ["1.json", "a", "b"]


def test_webdav_load_from(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/test", "w") as f:
        f.write("test")
    assert webdav.webdav_load_from("webdav://host/A/test").read() == b"test"


def test_webdav_makedirs(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A/B/C", parents=True)
    assert webdav.webdav_exists("webdav://host/A/B/C") is True

    with pytest.raises(FileExistsError):
        webdav.webdav_makedirs("webdav://host/A/B/C")

    with pytest.raises(FileNotFoundError):
        webdav.webdav_makedirs("webdav://host/D/B/C")


def test_webdav_realpath(webdav_mocker):
    # WebDAV doesn't resolve paths like SFTP, just returns the path
    assert webdav.webdav_realpath("webdav://host/A/../B/C") == "webdav://host/A/../B/C"


def test_webdav_rename(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/test", "w") as f:
        f.write("test")

    webdav.webdav_rename("webdav://host/A/test", "webdav://host/A/test2")
    assert webdav.webdav_exists("webdav://host/A/test") is False
    assert webdav.webdav_exists("webdav://host/A/test2") is True

    webdav.webdav_rename("webdav://host/A", "webdav://host/A2")
    assert webdav.webdav_exists("webdav://host/A/test2") is False
    assert webdav.webdav_exists("webdav://host/A2/test2") is True

    with pytest.raises(OSError):
        webdav.webdav_rename("webdav://host/A2/test2", "/A2/test")


def test_webdav_move(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/test", "w") as f:
        f.write("test")

    webdav.webdav_move("webdav://host/A/test", "webdav://host/A/test2")
    assert webdav.webdav_exists("webdav://host/A/test") is False
    assert webdav.webdav_exists("webdav://host/A/test2") is True

    webdav.webdav_move("webdav://host/A", "webdav://host/A2")
    assert webdav.webdav_exists("webdav://host/A/test2") is False
    assert webdav.webdav_exists("webdav://host/A2/test2") is True


def test_webdav_open(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")

    with webdav.webdav_open("webdav://host/A/test", "w") as f:
        f.write("test")

    with webdav.webdav_open("webdav://host/A/test", "r") as f:
        assert f.read() == "test"

    with webdav.webdav_open("webdav://host/A/test", "rb") as f:
        assert f.read() == b"test"

    with pytest.raises(FileNotFoundError):
        with webdav.webdav_open("webdav://host/A/notFound", "r") as f:
            f.read()

    with pytest.raises(IsADirectoryError):
        with webdav.webdav_open("webdav://host/A", "w") as f:
            f.write("test")


def test_webdav_remove(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/test", "w") as f:
        f.write("test")

    webdav.webdav_remove("webdav://host/A/test")
    webdav.webdav_remove("webdav://host/A/test", missing_ok=True)

    assert webdav.webdav_exists("webdav://host/A/test") is False
    assert webdav.webdav_exists("webdav://host/A") is True

    with webdav.webdav_open("webdav://host/A/test", "w") as f:
        f.write("test")
    webdav.webdav_remove("webdav://host/A")
    assert webdav.webdav_exists("webdav://host/A") is False


def test_webdav_scan(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    webdav.webdav_makedirs("webdav://host/A/a")
    webdav.webdav_makedirs("webdav://host/A/b")
    webdav.webdav_makedirs("webdav://host/A/b/c")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    with webdav.webdav_open("webdav://host/A/b/file.json", "w") as f:
        f.write("file")

    assert list(sorted(webdav.webdav_scan("webdav://host/A"))) == [
        "webdav://host/A/1.json",
        "webdav://host/A/b/file.json",
    ]

    assert [
        file_entry.path
        for file_entry in sorted(webdav.webdav_scan_stat("webdav://host/A"))
    ] == [
        "webdav://host/A/1.json",
        "webdav://host/A/b/file.json",
    ]

    with pytest.raises(FileNotFoundError):
        list(webdav.webdav_scan_stat("webdav://host/B", missing_ok=False))


def test_webdav_unlink(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/test", "w") as f:
        f.write("test")

    webdav.webdav_unlink("webdav://host/A/test")
    webdav.webdav_unlink("webdav://host/A/test", missing_ok=True)

    assert webdav.webdav_exists("webdav://host/A/test") is False
    assert webdav.webdav_exists("webdav://host/A") is True


def test_webdav_walk(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    webdav.webdav_makedirs("webdav://host/A/a")
    webdav.webdav_makedirs("webdav://host/A/a/b")
    webdav.webdav_makedirs("webdav://host/A/b")
    webdav.webdav_makedirs("webdav://host/A/b/c")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")
    with webdav.webdav_open("webdav://host/A/a/2.json", "w") as f:
        f.write("2.json")
    with webdav.webdav_open("webdav://host/A/b/3.json", "w") as f:
        f.write("3.json")

    assert list(webdav.webdav_walk("webdav://host/A")) == [
        ("webdav://host/A", ["a", "b"], ["1.json"]),
        ("webdav://host/A/a", ["b"], ["2.json"]),
        ("webdav://host/A/a/b", [], []),
        ("webdav://host/A/b", ["c"], ["3.json"]),
        ("webdav://host/A/b/c", [], []),
    ]

    assert list(webdav.webdav_walk("webdav://host/A/not_found")) == []
    assert list(webdav.webdav_walk("webdav://host/A/1.json")) == []


def test_webdav_getmd5(webdav_mocker):
    from megfile.fs import fs_getmd5

    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")
    assert webdav.webdav_getmd5("webdav://host/A/1.json") == fs_getmd5("/A/1.json")
    assert webdav.webdav_getmd5("webdav://host/A") == fs_getmd5("/A")


def test_webdav_save_as(webdav_mocker):
    webdav.webdav_save_as(io.BytesIO(b"test"), "webdav://host/test")
    assert webdav.webdav_load_from("webdav://host/test").read() == b"test"


def test_webdav_rmdir(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    with pytest.raises(OSError):
        webdav.webdav_rmdir("webdav://host/A")

    webdav.webdav_unlink("webdav://host/A/1.json")
    webdav.webdav_rmdir("webdav://host/A")
    assert webdav.webdav_exists("webdav://host/A") is False


def test_webdav_copy(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    def callback(length):
        assert length == len("1.json")

    webdav.webdav_copy(
        "webdav://host/A/1.json",
        "webdav://host/A2/1.json.bak",
        callback=callback,
    )

    assert (
        webdav.webdav_stat("webdav://host/A/1.json").size
        == webdav.webdav_stat("webdav://host/A2/1.json.bak").size
    )

    with webdav.webdav_open("webdav://host/A/2.json", "w") as f:
        f.write("2")
    webdav.webdav_copy(
        "webdav://host/A/2.json",
        "webdav://host/A2/1.json.bak",
        overwrite=False,
    )
    assert (
        webdav.webdav_stat("webdav://host/A/1.json").size
        == webdav.webdav_stat("webdav://host/A2/1.json.bak").size
    )

    webdav.webdav_copy(
        "webdav://host/A/2.json",
        "webdav://host/A2/1.json.bak",
        overwrite=True,
    )
    assert (
        webdav.webdav_stat("webdav://host/A/2.json").size
        == webdav.webdav_stat("webdav://host/A2/1.json.bak").size
    )


def test_webdav_copy_error(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    with pytest.raises(IsADirectoryError):
        webdav.webdav_copy("webdav://host/A", "webdav://host/A2")

    with pytest.raises(IsADirectoryError):
        webdav.webdav_copy("webdav://host/A/1.json", "webdav://host/A2/")

    with pytest.raises(OSError):
        webdav.webdav_copy("webdav://host/A", "/A2")

    with pytest.raises(SameFileError):
        webdav.webdav_copy("webdav://host/A/1.json", "webdav://host/A/1.json")


def test_webdav_sync(webdav_mocker, mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    webdav.webdav_sync("webdav://host/A", "webdav://host/A2")
    assert (
        webdav.webdav_stat("webdav://host/A/1.json").size
        == webdav.webdav_stat("webdav://host/A2/1.json").size
    )

    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("2")
    webdav.webdav_sync("webdav://host/A", "webdav://host/A2", overwrite=False)
    assert webdav.webdav_stat("webdav://host/A2/1.json").size == 6

    webdav.webdav_sync("webdav://host/A", "webdav://host/A2", force=True)
    assert webdav.webdav_stat("webdav://host/A2/1.json").size == 1

    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("22")
    webdav.webdav_sync("webdav://host/A", "webdav://host/A2", overwrite=True)
    assert webdav.webdav_stat("webdav://host/A2/1.json").size == 2

    webdav.webdav_sync("webdav://host/A/1.json", "webdav://host/A/1.json.bak")
    assert (
        webdav.webdav_stat("webdav://host/A/1.json").size
        == webdav.webdav_stat("webdav://host/A/1.json.bak").size
    )


def test_webdav_download(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    webdav.webdav_download("webdav://host/A/1.json", "/A2/1.json")
    assert (
        webdav.webdav_stat("webdav://host/A/1.json").size
        == os.stat("/A2/1.json").st_size
    )

    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("2")
    webdav.webdav_download(
        webdav.WebdavPath("webdav://host/A/1.json"), "/A2/1.json", overwrite=False
    )
    assert 6 == os.stat("/A2/1.json").st_size

    def callback(length):
        assert length > 0

    webdav.webdav_download(
        webdav.WebdavPath("webdav://host/A/1.json"),
        "/A2/1.json",
        overwrite=True,
        callback=callback,
    )
    assert 1 == os.stat("/A2/1.json").st_size

    with pytest.raises(OSError):
        webdav.webdav_download("webdav://host/A/1.json", "webdav://host/1.json")

    with pytest.raises(OSError):
        webdav.webdav_download("/1.json", "/1.json")

    with pytest.raises(IsADirectoryError):
        webdav.webdav_download("webdav://host/A", "/1.json")

    with pytest.raises(IsADirectoryError):
        webdav.webdav_download("webdav://host/A/1.json", "/1/")


def test_webdav_upload(webdav_mocker):
    with open("/1.json", "w") as f:
        f.write("1.json")

    webdav.webdav_upload("/1.json", "webdav://host/A/1.json")
    assert (
        webdav.webdav_stat("webdav://host/A/1.json").size == os.stat("/1.json").st_size
    )

    with open("/1.json", "w") as f:
        f.write("2")
    webdav.webdav_upload("/1.json", "webdav://host/A/1.json", overwrite=False)
    assert webdav.webdav_stat("webdav://host/A/1.json").size == 6

    def callback(length):
        assert length > 0

    webdav.webdav_upload(
        "/1.json",
        webdav.WebdavPath("webdav://host/A/1.json"),
        overwrite=True,
        callback=callback,
    )
    assert webdav.webdav_stat("webdav://host/A/1.json").size == 1

    with pytest.raises(OSError):
        webdav.webdav_upload("webdav://host/A/1.json", "webdav://host/1.json")

    with pytest.raises(OSError):
        webdav.webdav_upload("/1.json", "/1.json")

    with pytest.raises(IsADirectoryError):
        webdav.webdav_upload("/A", "webdav://host/A")

    with pytest.raises(IsADirectoryError):
        webdav.webdav_upload("/1.json", "webdav://host/A/")


def test_webdav_path_join():
    assert (
        webdav.webdav_path_join("webdav://host/A/", "a", "b") == "webdav://host/A/a/b"
    )

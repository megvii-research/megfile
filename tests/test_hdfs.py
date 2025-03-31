import hashlib
import io
import os

import pytest

from megfile import hdfs
from megfile.lib.hdfs_tools import hdfs_api


@pytest.fixture
def config_mocker(mocker):
    mocker.patch(
        "megfile.hdfs_path.get_hdfs_config",
        return_value={
            "user": "user",
            "url": "http://127.0.0.1:8000",
            "root": "/",
            "timeout": 10,
            "token": "token",
        },
    )
    yield


@pytest.fixture
def http_mocker(mocker, requests_mock, config_mocker):
    """
    mocker path:
    root
    ├── 1.txt
    ├── a
    │   └── 2.txt
    └── b
        ├── 3.txt
        └── 4.json
    """
    mocker.patch(
        "megfile.hdfs_path.get_hdfs_config",
        return_value={
            "user": "user",
            "url": "http://127.0.0.1:8000",
            "root": "/",
            "timeout": 10,
            "token": "token",
        },
    )
    files = {
        "root": {
            "1.txt": "1",
            "a": {"2.txt": "22"},
            "b": {"3.txt": "333", "4.json": "4444"},
        }
    }

    def mock_dir(path):
        keys = path.split("/")
        sub_files = files
        for key in keys:
            sub_files = sub_files[key]
        requests_mock.get(
            f"http://127.0.0.1:8000/webhdfs/v1/{path}?op=GETFILESTATUS",
            json={
                "FileStatus": {
                    "accessTime": 0,
                    "blockSize": 0,
                    "group": "supergroup",
                    "length": 0,
                    "modificationTime": 1320173277227,
                    "owner": "webuser",
                    "pathSuffix": "",
                    "permission": "777",
                    "replication": 0,
                    "type": "DIRECTORY",
                }
            },
        )
        requests_mock.get(
            f"http://127.0.0.1:8000/webhdfs/v1/{path}?op=LISTSTATUS",
            json={
                "FileStatuses": {
                    "FileStatus": [
                        {
                            "accessTime": 1320171722771,
                            "blockSize": 1024,
                            "group": "supergroup",
                            "length": 0 if isinstance(content, dict) else len(content),
                            "modificationTime": 1320171722771,
                            "owner": "webuser",
                            "pathSuffix": name,
                            "permission": "644",
                            "replication": 1,
                            "type": "DIRECTORY"
                            if isinstance(content, dict)
                            else "FILE",
                        }
                        for name, content in sub_files.items()
                    ]
                }
            },
        )
        requests_mock.delete(
            f"http://127.0.0.1:8000/webhdfs/v1/{path}?op=DELETE&recursive=true",
            json={"boolean": True},
        )
        for name, content in sub_files.items():
            if isinstance(content, dict):
                mock_dir(f"{path}/{name}")
            else:
                mock_file(f"{path}/{name}")

    def mock_file(path):
        keys = path.split("/")
        content = files
        for key in keys:
            content = content[key]
        requests_mock.get(
            f"http://127.0.0.1:8000/webhdfs/v1/{path}?op=GETFILESTATUS",
            json={
                "FileStatus": {
                    "accessTime": 0,
                    "blockSize": 0,
                    "group": "supergroup",
                    "length": len(content),
                    "modificationTime": 1320173277227,
                    "owner": "webuser",
                    "pathSuffix": "",
                    "permission": "777",
                    "replication": 0,
                    "type": "FILE",
                }
            },
        )
        requests_mock.get(
            f"http://127.0.0.1:8000/webhdfs/v1/{path}?op=OPEN",
            text=content,
            status_code=200,
            headers={
                "Content-Length": str(len(content)),
                "Content-Type": "application/octet-stream",
            },
        )
        requests_mock.delete(
            f"http://127.0.0.1:8000/webhdfs/v1/{path}?op=DELETE&recursive=true",
            json={"boolean": True},
        )

    mock_dir("root")
    yield requests_mock


def test_is_hdfs(config_mocker):
    assert hdfs.is_hdfs("hdfs://A") is True
    assert hdfs.is_hdfs("hdfs1://A") is False


def test_hdfs_exists(http_mocker):
    http_mocker.get(
        "http://127.0.0.1:8000/webhdfs/v1/unknown?op=GETFILESTATUS",
        status_code=404,
        json={
            "RemoteException": {
                "exception": "FileNotFoundException",
                "javaClassName": "java.io.FileNotFoundException",
                "message": "File does not exist: /unknown",
            }
        },
    )

    assert hdfs.hdfs_exists("hdfs://root") is True
    assert hdfs.hdfs_exists("hdfs://root/1.txt") is True
    assert hdfs.hdfs_exists("hdfs://unknown") is False


def test_hdfs_stat(http_mocker):
    assert hdfs.hdfs_stat("hdfs://root/1.txt").size == 1
    assert hdfs.hdfs_getmtime("hdfs://root/1.txt") == 1320173277.227
    assert hdfs.hdfs_getsize("hdfs://root/a/2.txt") == 2


def test_hdfs_isdir(http_mocker):
    http_mocker.get(
        "http://127.0.0.1:8000/webhdfs/v1/root/2?op=GETFILESTATUS",
        status_code=404,
        json={
            "RemoteException": {
                "exception": "FileNotFoundException",
                "javaClassName": "java.io.FileNotFoundException",
                "message": "File does not exist: /unknown",
            }
        },
    )

    assert hdfs.hdfs_isdir("hdfs://root") is True
    assert hdfs.hdfs_isdir("hdfs://root/a") is True
    assert hdfs.hdfs_isdir("hdfs://root/1.txt") is False
    assert hdfs.hdfs_isdir("hdfs://root/2") is False


def test_hdfs_isfile(http_mocker):
    http_mocker.get(
        "http://127.0.0.1:8000/webhdfs/v1/root/2.txt?op=GETFILESTATUS",
        status_code=404,
        json={
            "RemoteException": {
                "exception": "FileNotFoundException",
                "javaClassName": "java.io.FileNotFoundException",
                "message": "File does not exist: /unknown",
            }
        },
    )

    assert hdfs.hdfs_isfile("hdfs://root") is False
    assert hdfs.hdfs_isfile("hdfs://root/a") is False
    assert hdfs.hdfs_isfile("hdfs://root/a/2.txt") is True
    assert hdfs.hdfs_isfile("hdfs://root/2.txt") is False


def test_hdfs_listdir(http_mocker):
    assert sorted(hdfs.hdfs_listdir("hdfs://root")) == ["1.txt", "a", "b"]


def test_hdfs_load_from(http_mocker):
    assert hdfs.hdfs_load_from("hdfs://root/1.txt").read() == b"1"
    assert hdfs.hdfs_load_from("hdfs://root/a/2.txt").read() == b"22"
    assert hdfs.hdfs_load_from("hdfs://root/b/3.txt").read() == b"333"


def test_hdfs_move(http_mocker, mocker):
    http_mocker.put(
        "http://127.0.0.1:8000/webhdfs/v1/a?op=RENAME&destination=%2Fb",
        json={"boolean": True},
    )
    http_mocker.get(
        "http://127.0.0.1:8000/webhdfs/v1/a?delegation=token&op=GETFILESTATUS",
        json={
            "FileStatus": {
                "accessTime": 0,
                "blockSize": 0,
                "group": "supergroup",
                "length": 4,
                "modificationTime": 1320173277227,
                "owner": "webuser",
                "pathSuffix": "",
                "permission": "777",
                "replication": 0,
                "type": "FILE",
            }
        },
    )
    remove_func = mocker.patch("megfile.hdfs_path.HdfsPath.remove")

    hdfs.hdfs_move("hdfs://a", "hdfs://b")

    remove_func.call_count == 2


def test_hdfs_move_dir(http_mocker, mocker):
    http_mocker.put(
        "http://127.0.0.1:8000/webhdfs/v1/root/a/2.txt?op=RENAME&destination=%2Froot%2Fb%2F2.txt",
        json={"boolean": True},
    )
    remove_func = mocker.patch("megfile.hdfs_path.HdfsPath.remove")

    hdfs.hdfs_move("hdfs://root/a/", "hdfs://root/b/")

    remove_func.call_count == 2


def test_hdfs_remove(http_mocker):
    hdfs.hdfs_remove("hdfs://root")

    http_mocker.delete(
        "http://127.0.0.1:8000/webhdfs/v1/unknown?op=DELETE&recursive=true",
        status_code=404,
        json={
            "RemoteException": {
                "exception": "FileNotFoundException",
                "javaClassName": "java.io.FileNotFoundException",
                "message": "File does not exist: /unknown",
            }
        },
    )

    with pytest.raises(FileNotFoundError):
        hdfs.hdfs_remove("hdfs://unknown")

    http_mocker.delete(
        "http://127.0.0.1:8000/webhdfs/v1/forbidden?op=DELETE&recursive=true",
        status_code=403,
        json={
            "RemoteException": {
                "exception": "SecurityException",
                "javaClassName": "java.lang.SecurityException",
                "message": "Failed to obtain user group information: ...",
            }
        },
    )
    with pytest.raises(PermissionError):
        hdfs.hdfs_remove("hdfs://forbidden")

    http_mocker.delete(
        "http://127.0.0.1:8000/webhdfs/v1/input_error?op=DELETE&recursive=true",
        status_code=400,
        json={
            "RemoteException": {
                "exception": "IllegalArgumentException",
                "javaClassName": "java.lang.IllegalArgumentException",
                "message": 'Invalid value for webhdfs parameter "permission": ...',
            }
        },
    )
    with pytest.raises(ValueError):
        hdfs.hdfs_remove("hdfs://input_error")

    http_mocker.delete(
        "http://127.0.0.1:8000/webhdfs/v1/error?op=DELETE&recursive=true",
        status_code=500,
    )
    with pytest.raises(hdfs_api.HdfsError):
        hdfs.hdfs_remove("hdfs://error")


def test_hdfs_scan(http_mocker):
    assert list(hdfs.hdfs_scan("hdfs://root")) == [
        "hdfs://root/1.txt",
        "hdfs://root/a/2.txt",
        "hdfs://root/b/3.txt",
        "hdfs://root/b/4.json",
    ]


def test_hdfs_scandir(http_mocker):
    filename = ["hdfs://root/1.txt", "hdfs://root/a", "hdfs://root/b"]
    for i, file_entry in enumerate(hdfs.hdfs_scandir("hdfs://root")):
        assert file_entry.name == os.path.basename(filename[i])
        assert file_entry.path == filename[i]
        assert file_entry.stat.mtime == 1320171722.771
        if i >= 1:
            assert file_entry.is_dir() is True
        else:
            assert file_entry.is_dir() is False


def test_hdfs_scan_stat(http_mocker):
    filename = [
        "hdfs://root/1.txt",
        "hdfs://root/a/2.txt",
        "hdfs://root/b/3.txt",
        "hdfs://root/b/4.json",
    ]
    for i, file_entry in enumerate(hdfs.hdfs_scan_stat("hdfs://root")):
        assert file_entry.name == os.path.basename(filename[i])
        assert file_entry.path == filename[i]
        assert file_entry.stat.mtime == 1320171722.771
        assert file_entry.is_dir() is False


def test_hdfs_unlink(http_mocker):
    hdfs.hdfs_unlink("hdfs://root/1.txt")
    with pytest.raises(IsADirectoryError):
        hdfs.hdfs_unlink("hdfs://root")


def test_hdfs_walk(http_mocker):
    assert list(hdfs.hdfs_walk("hdfs://root")) == [
        ("hdfs://root", ["a", "b"], ["1.txt"]),
        ("hdfs://root/a", [], ["2.txt"]),
        ("hdfs://root/b", [], ["3.txt", "4.json"]),
    ]


def test_hdfs_getmd5(http_mocker):
    http_mocker.get(
        "http://127.0.0.1:8000/webhdfs/v1/root/1.txt?op=GETFILECHECKSUM",
        json={
            "FileChecksum": {
                "algorithm": "MD5-of-1MD5-of-512CRC32",
                "bytes": "d41d8cd98f00b204e9800998ecf8427e",
                "length": 28,
            }
        },
    )
    assert hdfs.hdfs_getmd5("hdfs://root/1.txt") == "d41d8cd98f00b204e9800998ecf8427e"


def test_hdfs_getmd5_from_dir(http_mocker):
    http_mocker.get(
        "http://127.0.0.1:8000/webhdfs/v1/root/a/2.txt?op=GETFILECHECKSUM",
        json={
            "FileChecksum": {
                "algorithm": "MD5-of-1MD5-of-512CRC32",
                "bytes": "d41d8cd98f00b204e9800998ecf8427e",
                "length": 28,
            }
        },
    )
    hash_md5 = hashlib.md5()  # nosec
    hash_md5.update(b"d41d8cd98f00b204e9800998ecf8427e")

    assert hdfs.hdfs_getmd5("hdfs://root/a") == hash_md5.hexdigest()


def test_hdfs_save_as(http_mocker):
    http_mocker.put(
        "http://127.0.0.1:8000/webhdfs/v1/root/2.txt?op=CREATE",
        status_code=307,
        headers={
            "Location": "http://127.0.0.1:8001/webhdfs/v1/root/2.txt?op=CREATE",
            "Content-Length": "0",
        },
    )
    http_mocker.put(
        "http://127.0.0.1:8001/webhdfs/v1/root/2.txt?op=CREATE",
        status_code=201,
        headers={
            "Location": "webhdfs://127.0.0.1:8000/root/2.txt",
            "Content-Length": "0",
        },
    )
    hdfs.hdfs_save_as(io.BytesIO(b""), "hdfs://root/2.txt")


def test_hdfs_save_as(http_mocker):
    http_mocker.put(
        "http://127.0.0.1:8000/webhdfs/v1/root/2.txt?op=CREATE", status_code=401
    )
    with pytest.raises(PermissionError):
        hdfs.hdfs_save_as(io.BytesIO(b""), "hdfs://root/2.txt")


def test_hdfs_open(http_mocker):
    http_mocker.put(
        "http://127.0.0.1:8000/webhdfs/v1/root/2.txt?op=CREATE",
        status_code=307,
        headers={
            "Location": "http://127.0.0.1:8001/webhdfs/v1/root/2.txt?op=CREATE",
            "Content-Length": "0",
        },
    )
    http_mocker.put(
        "http://127.0.0.1:8001/webhdfs/v1/root/2.txt?op=CREATE",
        status_code=201,
        headers={
            "Location": "webhdfs://127.0.0.1:8000/root/2.txt",
            "Content-Length": "0",
        },
    )
    http_mocker.post(
        "http://127.0.0.1:8000/webhdfs/v1/root/2.txt?delegation=token&op=APPEND",
        status_code=201,
        headers={
            "Location": "http://127.0.0.1:8000/webhdfs/v1/root/2.txt?delegation=token&op=APPEND",
            "Content-Length": "0",
        },
    )

    with hdfs.hdfs_open("hdfs://root/2.txt", "wb") as f:
        f.write(b"")

    with hdfs.hdfs_open("hdfs://root/2.txt", "ab") as f:
        f.write(b"")

    with pytest.raises(ValueError):
        hdfs.hdfs_open("hdfs://root/2.txt", "wb+")

    with pytest.raises(ValueError):
        hdfs.hdfs_open("hdfs://root/2.txt", "unknown")

    with hdfs.hdfs_open("hdfs://root/a/2.txt", "r") as f:
        assert f.read() == "22"
        assert f.name == "hdfs://root/a/2.txt"


def test_hdfs_open_pickle(http_mocker):
    http_mocker.get(
        "http://127.0.0.1:8000/webhdfs/v1/root/1.pkl?op=GETFILESTATUS",
        json={
            "FileStatus": {
                "accessTime": 0,
                "blockSize": 0,
                "group": "supergroup",
                "length": 4,
                "modificationTime": 1320173277227,
                "owner": "webuser",
                "pathSuffix": "",
                "permission": "777",
                "replication": 0,
                "type": "FILE",
            }
        },
    )
    http_mocker.get(
        "http://127.0.0.1:8000/webhdfs/v1/root/1.pkl?op=OPEN",
        text="test",
        status_code=200,
        headers={"Content-Length": "4", "Content-Type": "application/octet-stream"},
    )

    with hdfs.hdfs_open("hdfs://root/1.pkl", "rb") as f:
        assert isinstance(f, io.BufferedReader)


def test_hdfs_glob(http_mocker):
    assert hdfs.hdfs_glob("hdfs://root/**/*.txt") == [
        "hdfs://root/1.txt",
        "hdfs://root/a/2.txt",
        "hdfs://root/b/3.txt",
    ]
    assert hdfs.hdfs_glob("hdfs://root/**/*.json") == ["hdfs://root/b/4.json"]

    assert list(hdfs.hdfs_iglob("hdfs://root/**/*.txt")) == [
        "hdfs://root/1.txt",
        "hdfs://root/a/2.txt",
        "hdfs://root/b/3.txt",
    ]

    files = ["hdfs://root/1.txt", "hdfs://root/a/2.txt", "hdfs://root/b/3.txt"]
    for i, file_entry in enumerate(hdfs.hdfs_glob_stat("hdfs://root/**/*.txt")):
        assert file_entry.path == files[i]


def test_hdfs_makedirs(http_mocker):
    with pytest.raises(FileExistsError):
        hdfs.hdfs_makedirs("hdfs://root/a")

    http_mocker.put(
        "http://127.0.0.1:8000/webhdfs/v1/root/c?op=MKDIRS",
        status_code=200,
        json={"boolean": True},
    )
    hdfs.hdfs_makedirs("hdfs://root/c", exist_ok=True)

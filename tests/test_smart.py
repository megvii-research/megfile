import os
from io import BytesIO, StringIO
from pathlib import Path

import boto3
import pytest
from mock import patch
from moto import mock_aws

import megfile
from megfile import smart
from megfile.errors import S3UnknownError
from megfile.interfaces import Access, FileEntry, StatResult
from megfile.s3_path import _s3_binary_mode
from megfile.smart_path import SmartPath


@pytest.fixture
def filesystem(fs):
    return fs


BUCKET = "bucket"


@pytest.fixture
def s3_empty_client(mocker):
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        mocker.patch("megfile.s3_path.get_s3_client", return_value=client)
        yield client


@patch.object(SmartPath, "listdir")
def test_smart_listdir(funcA):
    ret = ["ret value1", "ret value2"]
    funcA.return_value = ret
    res = smart.smart_listdir("Test Case")
    assert res == ret
    funcA.assert_called_once()


def test_smart_listdir2():
    sorted(smart.smart_listdir(None)) == sorted(os.listdir(None))


@patch.object(SmartPath, "scandir")
def test_smart_scandir(funcA):
    smart.smart_scandir("Test Case")
    funcA.assert_called_once()


def test_smart_scandir2():
    sorted([f.name for f in smart.smart_scandir(None)]) == sorted(os.listdir(None))


@patch.object(SmartPath, "getsize")
def test_smart_getsize(funcA):
    funcA.return_value = 0
    res = smart.smart_getsize("Test Case")
    assert res == 0
    funcA.assert_called_once()


@patch.object(SmartPath, "md5")
def test_smart_getmd5(funcA):
    funcA.return_value = "dcddb75469b4b4875094e14561e573d8"
    res = smart.smart_getmd5("Test Case")
    assert res == "dcddb75469b4b4875094e14561e573d8"
    funcA.assert_called_once()


@patch.object(SmartPath, "getmtime")
def test_smart_getmtime(funcA):
    funcA.return_value = 0.0
    res = smart.smart_getmtime("Test Case")
    assert res == 0.0
    funcA.assert_called_once()


@patch.object(SmartPath, "stat")
def test_smart_stat(funcA):
    funcA.return_value = StatResult()
    res = smart.smart_stat("Test Case")
    assert res == StatResult()
    funcA.assert_called_once()


@patch.object(SmartPath, "lstat")
def test_smart_stat(funcA):
    funcA.return_value = StatResult()
    res = smart.smart_lstat("Test Case")
    assert res == StatResult()
    funcA.assert_called_once()


@patch.object(SmartPath, "is_dir")
def test_smart_isdir(funcA):
    funcA.return_value = True
    res = smart.smart_isdir("True Case")
    assert res is True
    funcA.assert_called_once()
    res = smart.smart_isdir("True Case", followlinks=True)
    assert res is True
    assert funcA.call_count == 2
    funcA.return_value = False
    res = smart.smart_isdir("False Case")
    assert res is False
    res = smart.smart_isdir("False Case", followlinks=True)
    assert res is False
    assert funcA.call_count == 4
    res = smart.smart_isdir("s3://test", followlinks=True)
    assert res is False
    assert funcA.call_count == 5


def test_smart_isdir2(mocker):
    fs_dir = mocker.patch("megfile.fs_path.FSPath.is_dir")
    smart.smart_isdir("/test")
    fs_dir.assert_called_once_with(followlinks=False)

    s3_dir = mocker.patch("megfile.s3_path.S3Path.is_dir")
    smart.smart_isdir("s3://test")
    s3_dir.assert_called_once_with(followlinks=False)


@patch.object(SmartPath, "is_file")
def test_smart_isfile(funcA):
    funcA.return_value = True
    res = smart.smart_isfile("True Case")
    assert res is True
    funcA.assert_called_once()
    res = smart.smart_isfile("True Case", followlinks=True)
    assert res is True
    assert funcA.call_count == 2
    funcA.return_value = False
    res = smart.smart_isfile("False Case")
    assert res is False
    res = smart.smart_isfile("False Case", followlinks=True)
    assert res is False
    assert funcA.call_count == 4
    res = smart.smart_isfile("s3://test", followlinks=True)
    assert res is False
    assert funcA.call_count == 5


def test_smart_isfile2(mocker):
    fs_is_file = mocker.patch("megfile.fs_path.FSPath.is_file")
    smart.smart_isfile("/test")
    fs_is_file.assert_called_once_with(followlinks=False)

    s3_is_file = mocker.patch("megfile.s3_path.S3Path.is_file")
    smart.smart_isfile("s3://test")
    s3_is_file.assert_called_once_with(followlinks=False)


@patch.object(SmartPath, "exists")
def test_smart_exists(funcA):
    funcA.return_value = True
    res = smart.smart_exists("True Case")
    assert res is True
    funcA.assert_called_once()
    res = smart.smart_exists("True Case", followlinks=True)
    assert res is True
    assert funcA.call_count == 2
    funcA.return_value = False
    res = smart.smart_exists("False Case")
    assert res is False
    res = smart.smart_exists("False Case", followlinks=True)
    assert res is False
    assert funcA.call_count == 4
    res = smart.smart_exists("s3://test", followlinks=True)
    assert res is False
    assert funcA.call_count == 5


def test_smart_exists2(mocker):
    fs_exists = mocker.patch("megfile.fs_path.FSPath.exists")
    smart.smart_exists("/test")
    fs_exists.assert_called_once_with(followlinks=False)

    s3_exists = mocker.patch("megfile.s3_path.S3Path.exists")
    smart.smart_exists("s3://test")
    s3_exists.assert_called_once_with(followlinks=False)


@patch.object(SmartPath, "is_symlink")
def test_smart_islink(funcA):
    funcA.return_value = True
    res = smart.smart_islink("True Case")
    assert res is True
    funcA.assert_called_once()
    funcA.return_value = False
    res = smart.smart_islink("False Case")
    assert res is False
    assert funcA.call_count == 2


@patch.object(SmartPath, "access")
def test_smart_access(funcA):
    funcA.return_value = True
    s3_path = "s3://test"
    readBucket = smart.smart_access(s3_path, mode=Access.READ)
    writeBucket = smart.smart_access(s3_path, mode=Access.WRITE)
    file_path = "file"
    readFile = smart.smart_access(file_path, mode=Access.READ)
    writeFile = smart.smart_access(file_path, Access.WRITE)
    assert readBucket is True
    assert writeBucket is True
    assert readFile is True
    assert writeFile is True
    assert funcA.call_count == 4


def test_smart_copy(mocker):
    def is_symlink(path: str) -> bool:
        return path == "link"

    s3_copy = mocker.patch("megfile.smart.s3_copy")
    s3_download = mocker.patch("megfile.smart.s3_download")
    s3_upload = mocker.patch("megfile.smart.s3_upload")
    fs_copy = mocker.patch("megfile.smart.fs_copy")
    default_copy_func = mocker.patch("megfile.smart._default_copy_func")
    mocker.patch("megfile.fs_path.FSPath._copyfile")
    s3path_open = mocker.patch("megfile.s3_path.S3Path.open")

    mocker.patch("megfile.smart.smart_islink", side_effect=is_symlink)

    patch_dict = {
        "s3": {"s3": s3_copy, "file": s3_download},
        "file": {"s3": s3_upload, "file": fs_copy},
    }

    with patch("megfile.smart._copy_funcs", patch_dict) as _:
        smart.smart_copy("link", "s3://a/b")
        assert s3_copy.called is False
        assert s3_download.called is False
        assert s3_upload.called is False
        assert fs_copy.called is False

        src_path = "/tmp/src_file"
        smart.smart_copy("link", src_path, followlinks=True)
        assert s3_copy.called is False
        assert s3_download.called is False
        assert s3_upload.called is False
        assert fs_copy.called is True
        fs_copy.assert_called_once_with(
            "link", src_path, callback=None, followlinks=True, overwrite=True
        )
        fs_copy.reset_mock()

        smart.smart_copy("s3://a/b", "s3://a/b")
        s3_copy.assert_called_once_with(
            "s3://a/b", "s3://a/b", callback=None, followlinks=False, overwrite=True
        )

        smart.smart_copy("http://a/b", "fs")
        default_copy_func.assert_called_once_with(
            "http://a/b", "fs", callback=None, followlinks=False, overwrite=True
        )

        smart.smart_copy("s3://a/b", "fs")
        s3_download.assert_called_once_with(
            "s3://a/b", "fs", callback=None, followlinks=False, overwrite=True
        )

        smart.smart_copy("fs", "s3://a/b")
        s3_upload.assert_called_once_with(
            "fs", "s3://a/b", callback=None, followlinks=False, overwrite=True
        )

        fs_stat = mocker.patch(
            "megfile.fs.fs_stat", return_value=StatResult(islnk=False, size=10)
        )
        smart.smart_copy("fs", "fs", followlinks=False)
        fs_copy.assert_called_once_with(
            "fs", "fs", callback=None, followlinks=False, overwrite=True
        )
        fs_copy.reset_mock()
        fs_stat.stop()

        smart.smart_copy("s3+test1://a/b", "s3+test2://a/b")
        s3path_open.call_count == 2


def test_smart_copy_raise_error(mocker):
    def s3_upload_raise_error(*args, **kwargs):
        raise S3UnknownError(
            Exception("cannot schedule new futures after interpreter shutdown"),
            "s3://a/b",
        )

    _default_copy_func = mocker.patch("megfile.smart._default_copy_func")
    patch_dict = {
        "file": {"s3": s3_upload_raise_error},
    }

    with patch("megfile.smart._copy_funcs", patch_dict) as _:
        smart.smart_copy("/data/a", "s3://a/b")
        assert _default_copy_func.call_count == 1

    def s3_upload_raise_error(*args, **kwargs):
        raise S3UnknownError(Exception("test"), "s3://a/b")

    patch_dict = {
        "file": {"s3": s3_upload_raise_error},
    }

    with patch("megfile.smart._copy_funcs", patch_dict) as _:
        with pytest.raises(S3UnknownError):
            smart.smart_copy("/data/a", "s3://a/b")


def test_smart_copy_overwrite(fs, mocker):
    with open("file", "wb") as f:
        f.write(b"")
    with open("file1", "wb") as f:
        f.write(b"test")

    smart.smart_copy("/file", "/file1", overwrite=False)
    with open("/file1", "rb") as f:
        assert f.read() == b"test"
    smart.smart_copy("/file", "/file1", overwrite=True)
    with open("/file1", "rb") as f:
        assert f.read() == b""


def test_smart_copy_UP2UP(filesystem):
    patch_dict = {}
    with patch("megfile.smart._copy_funcs", patch_dict) as _:
        data = b"test"
        with smart.smart_open("a", "wb") as writer:
            writer.write(data)
        smart.smart_copy("a", "b")
        assert data == smart.smart_open("b", "rb").read()


def test_smart_sync(mocker):
    def isdir(path: str) -> bool:
        return os.path.basename(path).startswith("folder")

    def isfile(path: str) -> bool:
        return os.path.basename(path).startswith("file")

    smart_copy = mocker.patch("megfile.smart.smart_copy")
    mocker.patch("megfile.smart.smart_isdir", side_effect=isdir)
    mocker.patch("megfile.smart.smart_isfile", side_effect=isfile)
    smart_scan_stat = mocker.patch("megfile.smart.smart_scan_stat")
    """
      folder/
        - folderA/
          -fileB
        - fileA
      - a/
        - b/
          - c
        - d
      - a
    """

    def scan_stat(path: str, followlinks: bool, missing_ok: bool = True):
        if path == "folder":
            return [
                FileEntry(name="fileB", path="folder/folderA/fileB", stat=None),
                FileEntry(name="fileA", path="folder/fileA", stat=None),
            ]
        if path == "folder/fileA":
            return [FileEntry(name="fileA", path="folder/fileA", stat=None)]
        if path == "a":
            return [
                FileEntry(name="a", path="a", stat=None),
                FileEntry(name="c", path="a/b/c", stat=None),
                FileEntry(name="d", path="a/d", stat=None),
            ]

    def exists(path, followlinks: bool = False):
        return path in ("folder", "folder/fileA", "a")

    mocker.patch("megfile.smart.smart_exists", side_effect=exists)
    smart_scan_stat.side_effect = scan_stat

    smart.smart_sync("folder", "dst", followlinks=True)
    assert smart_copy.call_count == 2
    smart_copy.assert_any_call(
        "folder/fileA", "dst/fileA", callback=None, followlinks=True
    )
    smart_copy.assert_any_call(
        "folder/folderA/fileB", "dst/folderA/fileB", callback=None, followlinks=True
    )
    smart_copy.reset_mock()

    smart.smart_sync("folder/fileA", "dst/file", followlinks=True)
    assert smart_copy.call_count == 1
    smart_copy.assert_any_call(
        "folder/fileA", "dst/file", callback=None, followlinks=True
    )
    smart_copy.reset_mock()

    smart.smart_sync("a", "dst", followlinks=True)
    assert smart_copy.call_count == 3
    smart_copy.assert_any_call("a", "dst", callback=None, followlinks=True)
    smart_copy.assert_any_call("a/b/c", "dst/b/c", callback=None, followlinks=True)
    smart_copy.assert_any_call("a/d", "dst/d", callback=None, followlinks=True)

    with pytest.raises(FileNotFoundError):
        smart.smart_sync("not_exists", "dst")


def test_smart_sync_with_progress(mocker, fs):
    with pytest.raises(FileNotFoundError):
        smart.smart_sync_with_progress("not_exists", "dst")

    os.makedirs("/test")
    with open("/test/a", "w") as f:
        f.write("123")

    def callback(path, length):
        assert path == "/test/a"
        assert length == 3

    smart.smart_sync_with_progress("/test", "/test1", callback=callback)


def test_smart_sync_overwrite(fs):
    src = "/tmp/refiletest/src/test.txt"
    dst = "/tmp/refiletest/dst/test.txt"
    smart.smart_makedirs("/tmp/refiletest/src")
    with open(src, "w") as f:
        f.write("test")

    smart.smart_makedirs("/tmp/refiletest/dst")
    with open(dst, "w") as f:
        f.write("test1")

    smart.smart_sync(src, dst, overwrite=False)
    with open(dst, "r") as f:
        assert f.read() == "test1"

    smart.smart_sync(src, dst, overwrite=True)
    with open(dst, "r") as f:
        assert f.read() == "test"


def test_smart_sync_file(s3_empty_client, fs):
    smart.smart_makedirs("/A")
    smart.smart_touch("/A/file")

    smart.smart_sync("/A/file", "s3://bucket/A/file")
    assert smart.smart_exists("s3://bucket/A/file") is True

    with patch("megfile.smart.smart_copy") as smart_copy:
        file_stat = os.stat("/A/file")
        os.utime("/A/file", (file_stat.st_atime, file_stat.st_mtime - 1))
        smart.smart_sync("/A", "s3://bucket/A")
        assert smart_copy.call_count == 0

    with patch("megfile.smart.smart_copy") as smart_copy:
        file_stat = os.stat("/A/file")
        os.utime("/A/file", (file_stat.st_atime, file_stat.st_mtime - 1))
        smart.smart_sync("/A", "s3://bucket/A", force=True)
        assert smart_copy.call_count == 1

    patch_dict = {}

    with patch("megfile.smart._copy_funcs", patch_dict) as _:
        smart.smart_sync("/A", "s3://bucket/B")
        assert smart.smart_exists("s3://bucket/B/file") is True


@patch.object(SmartPath, "remove")
def test_smart_remove(funcA):
    funcA.return_value = None

    res = smart.smart_remove("False Case", missing_ok=False, followlinks=True)
    assert res is None
    funcA.assert_called_once_with(missing_ok=False, followlinks=True)

    res = smart.smart_remove("True Case", missing_ok=True, followlinks=True)
    assert res is None
    funcA.assert_called_with(missing_ok=True, followlinks=True)

    res = smart.smart_remove("s3://test", missing_ok=True, followlinks=True)
    assert res is None


def test_smart_remove(mocker):
    fs_remove = mocker.patch("megfile.fs_path.FSPath.remove")
    smart.smart_remove("/test")
    fs_remove.assert_called_once_with(missing_ok=False)

    s3_remove = mocker.patch("megfile.s3_path.S3Path.remove")
    smart.smart_remove("s3://test")
    s3_remove.assert_called_once_with(missing_ok=False)


def test_smart_move(mocker):
    funcA = mocker.patch("megfile.smart_path.SmartPath.rename")
    funcA.return_value = None
    res = smart.smart_move("s3://bucket/a", "s3://bucket/b")
    assert res is None
    funcA.assert_called_once_with("s3://bucket/b", overwrite=True)

    res = smart.smart_move("/bucket/a", "/bucket/b")
    assert res is None
    assert funcA.call_count == 2

    func_smart_sync = mocker.patch("megfile.smart.smart_sync")
    func_smart_remove = mocker.patch("megfile.smart.smart_remove")
    smart.smart_move("/bucket/a", "s3://bucket/b")
    func_smart_sync.assert_called_once_with(
        "/bucket/a", "s3://bucket/b", followlinks=True, overwrite=True
    )
    func_smart_remove.assert_called_once_with("/bucket/a")


@patch.object(SmartPath, "rename")
def test_smart_rename(funcA):
    funcA.return_value = None
    res = smart.smart_move("s3://bucket/a", "s3://bucket/b")
    assert res is None
    funcA.assert_called_once_with("s3://bucket/b", overwrite=True)


def test_smart_rename_fs(s3_empty_client, filesystem):
    """
    /tmp_rename/
        /src/
            -src_file
        /dst/
            -link --> tmp_rename/src/src_file
    """
    os.mkdir("tmp_rename")
    os.mkdir("tmp_rename/src")
    os.mkdir("tmp_rename/dst")
    with open("tmp_rename/src/src_file", "w") as f:
        f.write("")
    os.symlink("tmp_rename/src/src_file", "tmp_rename/dst/link")
    os.path.exists("tmp_rename/dst/link") is True
    assert os.path.exists("tmp_rename/src")
    smart.smart_rename("tmp_rename/src/src_file", "tmp_rename/src_copy")
    assert os.path.exists("tmp_rename/src_copy")
    assert not os.path.exists("tmp_rename/src/src_file")
    assert not os.path.exists("tmp_rename/dst/link")

    with pytest.raises(IsADirectoryError):
        smart.smart_rename("tmp_rename/src", "tmp_rename/src_copy")

    smart.smart_rename("tmp_rename/src_copy", "s3://bucket/src_copy")
    assert smart.smart_exists("s3://bucket/src_copy")
    assert not smart.smart_exists("tmp_rename/src_copy")


@patch.object(SmartPath, "unlink")
def test_smart_unlink(funcA):
    funcA.return_value = None

    res = smart.smart_unlink("False Case", False)
    assert res is None
    funcA.assert_called_once_with(missing_ok=False)

    res = smart.smart_unlink("True Case", True)
    assert res is None
    funcA.assert_called_with(missing_ok=True)


@patch.object(SmartPath, "makedirs")
def test_smart_makedirs(funcA):
    funcA.return_value = None
    res = smart.smart_makedirs("Test Case", exist_ok=True)
    assert res is None
    funcA.assert_called_once_with(True)


def test_smart_open_input_params(mocker, fs):
    s3_open = mocker.patch("megfile.s3_path.S3Path.open")
    with smart.smart_open("s3://test"):
        pass
    s3_open.assert_called_once()

    fs_open = mocker.patch("megfile.fs_path.FSPath.open")
    with smart.smart_open("/test"):
        pass
    fs_open.assert_called_once()

    http_open = mocker.patch("megfile.http_path.HttpPath.open")
    with smart.smart_open("http://test"):
        pass
    http_open.assert_called_once()


def test_smart_open(mocker, fs):
    """
    This test is pretty naïve. Feel free to improve it
    in order to ensure smart_open works as we expected.

    Even ourselves do not know what we expect up to now.
    """
    # s3_writer = mocker.patch('megfile.s3.S3BufferedWriter')
    # s3_reader = mocker.patch('megfile.s3.S3PrefetchReader')
    # fs_open = mocker.patch('io.open', side_effect=open)
    # text_wrapper = mocker.patch('io.TextIOWrapper')
    # is_s3_func = mocker.patch('megfile.smart.is_s3')
    # fs_isdir_func = mocker.patch('megfile.smart.fs_isdir')
    # s3_isdir_func = mocker.patch('megfile.smart.s3_isdir')
    # s3_isfile_func = mocker.patch('megfile.smart.s3_isfile')
    # parse_s3_url = mocker.patch('megfile.s3.parse_s3_url')
    # mocker.patch('megfile.s3.get_s3_client')

    # is_s3_func.return_value = False
    # fs_isdir_func.return_value = True

    # with pytest.raises(IsADirectoryError):
    #     smart.smart_open('folder')
    # is_s3_func.return_value = False
    # fs_isdir_func.return_value = False
    # with pytest.raises(FileNotFoundError):
    #     smart.smart_open('non-exist.file')
    # fs_open.side_effect = None
    # fs_open.reset_mock()

    # is_s3_func.return_value = False
    # fs_isdir_func.return_value = False
    # smart.smart_open('file', 'wb+')
    # fs_open.assert_called_once_with('file', 'wb+', encoding=None, errors=None)
    # fs_open.reset_mock()

    # is_s3_func.return_value = False
    # fs_isdir_func.return_value = False
    # smart.smart_open('non-exist/file', 'wb')
    # fs_open.assert_called_once_with(
    #     'non-exist/file', 'wb', encoding=None, errors=None)
    # fs_open.reset_mock()

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = True
    # s3_isfile_func.return_value = True
    # parse_s3_url.return_value = ('bucket', 'key')
    # smart.smart_open('s3://bucket/key')
    # s3_reader.side_effect = None
    # s3_reader.reset_mock()
    # text_wrapper.reset_mock()

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = True
    # s3_isfile_func.return_value = False
    # parse_s3_url.return_value = ('bucket', 'key')
    # with pytest.raises(IsADirectoryError) as e:
    #     smart.smart_open('s3://bucket/key')

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = False
    # s3_isfile_func.return_value = False
    # parse_s3_url.return_value = ('bucket', 'key')
    # with pytest.raises(FileNotFoundError) as e:
    #     smart.smart_open('s3://bucket/key')

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = True
    # s3_isfile_func.return_value = True
    # parse_s3_url.return_value = ('bucket', 'key')
    # with pytest.raises(FileExistsError) as e:
    #     smart.smart_open('s3://bucket/key', 'x')

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = False
    # s3_isfile_func.return_value = False
    # parse_s3_url.return_value = ('bucket', 'key')
    # with pytest.raises(ValueError) as e:
    #     smart.smart_open('s3://bucket/key', 'wb+')
    # assert 'wb+' in str(e.value)

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = False
    # s3_isfile_func.return_value = False
    # parse_s3_url.return_value = ('bucket', 'key')
    # smart.smart_open('s3://bucket/key', 'w')
    # # s3_writer.assert_called_once() in Python 3.6+
    # assert s3_writer.call_count == 1
    # # text_wrapper.assert_called_once() in Python 3.6+
    # assert text_wrapper.call_count == 1
    # s3_writer.reset_mock()
    # text_wrapper.reset_mock()

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = False
    # s3_isfile_func.return_value = False
    # parse_s3_url.return_value = ('bucket', 'key')
    # smart.smart_open('s3://bucket/key', 'xb')
    # # s3_writer.assert_called_once() in Python 3.6+
    # assert s3_writer.call_count == 1
    # # text_wrapper.assert_not_called() in Python 3.6+
    # assert text_wrapper.call_count == 0
    # s3_writer.reset_mock()
    # text_wrapper.reset_mock()

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = False
    # s3_isfile_func.return_value = True
    # parse_s3_url.return_value = ('bucket', 'key')
    # smart.smart_open('s3://bucket/key', 'r')
    # # s3_reader.assert_called_once() in Python 3.6+
    # assert s3_reader.call_count == 1
    # # text_wrapper.assert_called_once() in Python 3.6+
    # assert text_wrapper.call_count == 1
    # s3_reader.reset_mock()
    # text_wrapper.reset_mock()


def test_smart_open_custom_s3_open_func(mocker, fs):
    s3_open = mocker.Mock()
    s3_binary_open = _s3_binary_mode(s3_open)
    text_wrapper = mocker.patch("io.TextIOWrapper")
    s3_hasbucket_func = mocker.patch("megfile.s3_path.S3Path.hasbucket")
    s3_hasbucket_func.return_value = True
    s3_isfile_func = mocker.patch("megfile.s3_path.S3Path.is_file")
    s3_isfile_func.return_value = False
    parse_s3_url = mocker.patch("megfile.s3_path.parse_s3_url")
    parse_s3_url.return_value = ("bucket", "key")

    parse_s3_url = mocker.patch("megfile.s3_path.S3Path._s3_get_metadata")
    parse_s3_url.return_value = {}
    smart.smart_open("s3://bucket/key", "r", s3_open_func=s3_binary_open)
    s3_open.assert_called_once_with("s3://bucket/key", "rb")
    # text_wrapper.assert_called_once() in Python 3.6+
    assert text_wrapper.call_count == 1
    s3_open.reset_mock()
    text_wrapper.reset_mock()

    smart.smart_open("s3://bucket/key", "wt", s3_open_func=s3_binary_open)
    s3_open.assert_called_once_with("s3://bucket/key", "wb")
    # text_wrapper.assert_called_once() in Python 3.6+
    assert text_wrapper.call_count == 1
    s3_open.reset_mock()
    text_wrapper.reset_mock()

    smart.smart_open("s3://bucket/key", "wb", s3_open_func=s3_binary_open)
    s3_open.assert_called_once_with("s3://bucket/key", "wb")
    # text_wrapper.assert_not_called() in Python 3.6+
    assert text_wrapper.call_count == 0
    s3_open.reset_mock()
    text_wrapper.reset_mock()

    smart.smart_open("s3://bucket/key", "ab+", s3_open_func=s3_binary_open)
    s3_open.assert_called_once_with("s3://bucket/key", "ab+")
    # text_wrapper.assert_not_called() in Python 3.6+
    assert text_wrapper.call_count == 0
    s3_open.reset_mock()
    text_wrapper.reset_mock()

    smart.smart_open("s3://bucket/key", "x", s3_open_func=s3_binary_open)
    s3_open.assert_called_once_with("s3://bucket/key", "wb")
    # text_wrapper.assert_not_called() in Python 3.6+
    assert text_wrapper.call_count == 1
    s3_open.reset_mock()
    text_wrapper.reset_mock()

    with pytest.raises(FileExistsError):
        s3_isfile_func.return_value = True
        smart.smart_open("s3://bucket/key", "x", s3_open_func=s3_binary_open)


@patch.object(SmartPath, "joinpath", return_value=Path())
def test_smart_path_join(funcA):
    smart.smart_path_join("s3://Test Case1", "s3://Test Case2", "s3://Test Case3")
    funcA.assert_called_once_with("s3://Test Case2", "s3://Test Case3")


def test_smart_path_join_result():
    assert smart.smart_path_join("path") == "path"
    assert smart.smart_path_join("path", "to/file") == "path/to/file"
    assert smart.smart_path_join("path", "to//file") == "path/to/file"
    assert smart.smart_path_join("path", "to", "file") == "path/to/file"
    assert smart.smart_path_join("path", "to/", "file") == "path/to/file"
    assert smart.smart_path_join("path", "to", "/file") == "/file"
    assert smart.smart_path_join("path", "to", "file/") == "path/to/file"
    assert smart.smart_path_join("s3://") == "s3://"
    assert smart.smart_path_join("s3://", "bucket/key") == "s3://bucket/key"
    assert smart.smart_path_join("s3://", "bucket//key") == "s3://bucket//key"
    assert smart.smart_path_join("s3://", "bucket", "key") == "s3://bucket/key"
    assert smart.smart_path_join("s3://", "bucket/", "key") == "s3://bucket/key"
    assert smart.smart_path_join("s3://", "bucket", "/key") == "s3://bucket/key"
    assert smart.smart_path_join("s3://", "bucket", "key/") == "s3://bucket/key/"


@patch.object(SmartPath, "walk")
def test_smart_walk(funcA):
    funcA.return_value = None
    res = smart.smart_walk("Test Case", followlinks=True)
    assert res is None
    funcA.assert_called_once()

    res = smart.smart_walk("s3://test", followlinks=True)
    assert res is None
    funcA.call_count == 2


def test_smart_walk2(mocker):
    fs_walk = mocker.patch("megfile.fs_path.FSPath.walk")
    smart.smart_walk("/test")
    fs_walk.assert_called_once_with(followlinks=False)

    s3_walk = mocker.patch("megfile.s3_path.S3Path.walk")
    smart.smart_walk("s3://test")
    s3_walk.assert_called_once_with(followlinks=False)


@patch.object(SmartPath, "scan")
def test_smart_scan(funcA):
    smart.smart_scan("Test Case", followlinks=True)
    funcA.assert_called_once()

    smart.smart_scan("s3://test", followlinks=True)
    funcA.call_count == 2


def test_smart_scan2(mocker):
    fs_scan = mocker.patch("megfile.fs_path.FSPath.scan")
    smart.smart_scan("/test")
    fs_scan.assert_called_once_with(missing_ok=True, followlinks=False)

    s3_scan = mocker.patch("megfile.s3_path.S3Path.scan")
    smart.smart_scan("s3://test")
    s3_scan.assert_called_once_with(missing_ok=True, followlinks=False)


@patch.object(SmartPath, "scan_stat")
def test_smart_scan_stat(funcA):
    smart.smart_scan_stat("Test Case", followlinks=True)
    funcA.assert_called_once()

    smart.smart_scan_stat("s3://test", followlinks=True)
    funcA.call_count == 2


def test_smart_scan_stat2(mocker):
    fs_scan_stat = mocker.patch("megfile.fs_path.FSPath.scan_stat")
    smart.smart_scan_stat("/test")
    fs_scan_stat.assert_called_once_with(missing_ok=True, followlinks=False)

    s3_scan_stat = mocker.patch("megfile.s3_path.S3Path.scan_stat")
    smart.smart_scan_stat("s3://test")
    s3_scan_stat.assert_called_once_with(missing_ok=True, followlinks=False)


def test_smart_glob(s3_empty_client, fs):
    os.mkdir("A")
    os.mkdir("A/a")
    os.mkdir("A/b")
    os.mkdir("A/b/c")
    with open("A/1.json", "w") as f:
        f.write("1.json")

    with open("A/b/file.json", "w") as f:
        f.write("file")

    assert smart.smart_glob("A/*") == ["A/1.json", "A/a", "A/b"]
    assert list(smart.smart_iglob("A/*")) == ["A/1.json", "A/a", "A/b"]
    assert [file_entry.path for file_entry in smart.smart_glob_stat("A/*")] == [
        "A/1.json",
        "A/a",
        "A/b",
    ]

    for path in smart.smart_glob("A/*"):
        assert isinstance(path, str)

    SmartPath("s3://bucket/A/1").write_text("1")
    SmartPath("s3://bucket/A/2.json").write_text("2")
    SmartPath("s3://bucket/A/3").write_text("3")
    SmartPath("s3://bucket/A/4/5").write_text("5")
    SmartPath("s3://bucket/A/4/6.json").write_text("6")

    assert smart.smart_glob("s3://bucket/A/*") == [
        "s3://bucket/A/1",
        "s3://bucket/A/2.json",
        "s3://bucket/A/3",
        "s3://bucket/A/4",
    ]

    assert smart.smart_glob("s3+test://bucket/A/*") == [
        "s3+test://bucket/A/1",
        "s3+test://bucket/A/2.json",
        "s3+test://bucket/A/3",
        "s3+test://bucket/A/4",
    ]

    assert [
        file_entry.path for file_entry in smart.smart_glob_stat("s3://bucket/A/*")
    ] == [
        "s3://bucket/A/1",
        "s3://bucket/A/2.json",
        "s3://bucket/A/3",
        "s3://bucket/A/4",
    ]

    for path in smart.smart_glob("s3://bucket/A/*"):
        assert isinstance(path, str)
    pass


@patch.object(SmartPath, "glob")
def test_smart_glob_cross_backend(funcA):
    # sublist = [1,2,3]
    # funcA.return_value = iter(sublist)
    smart.smart_glob(r"{/a,s3://bucket/key}/filename")
    assert funcA.call_count == 2
    # assert list(res) == sublist*2


@patch.object(SmartPath, "iglob")
def test_smart_iglob(funcA):
    list(smart.smart_iglob("s3://bucket/*"))
    funcA.assert_called_once()


@patch.object(SmartPath, "iglob")
def test_smart_iglob_cross_backend(funcA):
    list(smart.smart_iglob(r"{/a,s3://bucket/key,s3://bucket2/key}/filename"))
    assert funcA.call_count == 2


@patch.object(SmartPath, "glob_stat")
def test_smart_glob_stat(funcA):
    list(smart.smart_glob_stat("s3://bucket/*"))
    funcA.assert_called_once()


@patch.object(SmartPath, "glob_stat")
def test_smart_glob_stat_cross_backend(funcA):
    list(smart.smart_glob_stat(r"{/a,s3://bucket/key,s3://bucket2/key}/filename"))
    assert funcA.call_count == 2


def test_smart_save_as(mocker):
    funcA = mocker.patch("megfile.s3_path.S3Path.save")
    funcB = mocker.patch("megfile.fs_path.FSPath.save")
    stream = BytesIO()
    smart.smart_save_as(stream, "s3://test/ture_case")
    funcA.assert_called_once_with(stream)
    smart.smart_save_as(stream, "/test/false_case")
    funcB.assert_called_once_with(stream)


@patch.object(SmartPath, "load")
def test_smart_load_from(funcA):
    smart.smart_load_from("Test Case")
    funcA.assert_called_once()


@pytest.fixture
def s3_path():
    yield "s3://bucket/test"


@pytest.fixture
def s3_bucket():
    yield "s3://bucket"


@pytest.fixture
def abs_path(fs):
    fs.create_file(os.path.join(os.path.dirname(__file__), "test"))
    yield os.path.join(os.path.dirname(__file__), "test")


@pytest.fixture
def rel_path(fs):
    fs.create_file("./test")
    yield "test"


@pytest.fixture
def link_path(fs, abs_path):
    fs.create_symlink("link", abs_path)
    yield "link"


@pytest.fixture
def mount_point(fs):
    fs.add_mount_point(os.path.dirname(__file__))
    yield os.path.dirname(__file__)


def test_smart_isabs(s3_path, abs_path, rel_path):
    assert smart.smart_isabs(s3_path) is True
    assert smart.smart_isabs(abs_path) is True
    assert smart.smart_isabs(rel_path) is False


def test_smart_ismount(mount_point, s3_path, abs_path):
    assert smart.smart_ismount(s3_path) is False
    assert smart.smart_ismount(abs_path) is False
    assert smart.smart_ismount(mount_point) is True


def test_smart_abspath(mocker, s3_path, abs_path, rel_path):
    mocker.patch("os.getcwd", return_value=os.path.dirname(__file__))
    assert smart.smart_abspath(s3_path) == s3_path
    assert smart.smart_abspath(rel_path) == abs_path


def test_smart_realpath(s3_path, abs_path, link_path):
    assert smart.smart_realpath(s3_path) == s3_path
    assert smart.smart_realpath(abs_path) == abs_path
    assert smart.smart_realpath(link_path) == abs_path


def test_smart_relpath(mocker, s3_path, s3_bucket, abs_path, rel_path):
    mocker.patch("os.getcwd", return_value=os.path.dirname(__file__))
    with pytest.raises(TypeError):
        assert smart.smart_relpath(s3_path) == s3_path
    with pytest.raises(ValueError):
        smart.smart_relpath(s3_path, abs_path)
    assert smart.smart_relpath(abs_path, os.path.dirname(__file__)) == rel_path
    assert smart.smart_relpath(s3_path, s3_bucket) == rel_path


def test_smart_open_stdin(mocker):
    stdin_buffer_read = mocker.patch("sys.stdin.buffer.read")
    stdin_buffer_read.return_value = b"test\ntest1\n"

    reader = megfile.smart_open("stdio://-", "r")
    assert reader.read() == "test\ntest1\n"


def test_smart_open_stdout(mocker):
    # TODO: 这里 pytest 会把 sys.stdout mocker 掉, 导致无法测试, 之后想办法解决
    return
    data = BytesIO()

    def fake_write(w_data):
        data.write(w_data)
        return len(w_data)

    mocker.patch("_io.FileIO", side_effect=fake_write)
    writer = megfile.smart_open("stdio://-", "w")
    writer.write("test")
    assert data.getvalue() == b"test"


@patch.object(smart, "s3_load_content")
def test_smart_load_content(funcA, fs):
    path = "test.txt"
    content = b"hello world"
    with open(path, "wb") as f:
        f.write(content)

    assert smart.smart_load_content(path) == content
    assert smart.smart_load_content(path, 1) == content[1:]
    assert smart.smart_load_content(path, 0, 1) == content[:1]
    assert smart.smart_load_content(path, 4, 7) == content[4:7]
    assert smart.smart_load_content(path, stop=5) == content[:5]
    assert smart.smart_load_content(path, stop=100) == content

    smart.smart_load_content("s3://bucket/test.txt")
    funcA.assert_called_once()

    with pytest.raises(ValueError):
        smart.smart_load_content(path, 5, 3)


def test_smart_save_content(mocker):
    content = b"test data for smart_save_content"
    smart_open = mocker.patch("megfile.smart.smart_open")

    smart.smart_save_content("s3://bucket/key", content)
    smart_open.assert_called_once_with("s3://bucket/key", "wb")


def test_smart_load_text(mocker):
    content = "test data for smart_load_text"
    smart_open = mocker.patch("megfile.smart.smart_open")

    def _fake_smart_open(*args, **kwargs):
        return StringIO(content)

    smart_open.side_effect = _fake_smart_open

    assert smart.smart_load_text("s3://bucket/key") == content

    with pytest.raises(Exception):
        smart.smart_load_text("s3://bucket/key", 5, 2)


def test_smart_save_text(mocker):
    content = "test data for smart_save_text"
    smart_open = mocker.patch("megfile.smart.smart_open")

    smart.smart_save_text("s3://bucket/key", content)
    smart_open.assert_called_once_with("s3://bucket/key", "w")


def test_register_copy_func():
    def test(*args, **kwargs):
        pass

    smart.register_copy_func("a", "b", test)
    assert smart._copy_funcs["a"]["b"] == test

    with pytest.raises(ValueError) as error:
        smart.register_copy_func("a", "b", test)

    assert error.value.args[0] == "Copy Function has already existed: a->b"


def test_smart_cache(mocker, s3_empty_client):
    smart_copy = mocker.patch("megfile.smart.smart_copy")
    smart_copy.return_value = None

    from megfile.interfaces import NullCacher
    from megfile.smart import SmartCacher

    cacher = megfile.smart_cache("/path/to/file")
    assert isinstance(cacher, NullCacher)
    with cacher as path:
        assert path == "/path/to/file"

    cacher = megfile.smart_cache("s3://path/to/file")
    assert isinstance(cacher, SmartCacher)
    assert smart_copy.call_count == 1
    cacher.close()
    assert cacher.closed is True

    cacher = megfile.smart_cache("s3://path/to/save", mode="w")
    assert isinstance(cacher, SmartCacher)
    assert smart_copy.call_count == 1
    with cacher as path:
        with open(path, "w") as f:
            f.write("test")
    assert cacher.closed is True
    assert smart_copy.call_count == 2
    assert os.path.exists(path) is False

    with pytest.raises(ValueError):
        megfile.smart_cache("s3://path/to/file", mode="x")


def test_smart_symlink(mocker, s3_empty_client, filesystem):
    src_path = "/tmp/src_file"
    dst_path = "/tmp/dst_file"
    smart.smart_symlink(src_path, dst_path)

    res = os.readlink(dst_path)
    assert res == src_path

    src_url = "s3://bucket/src"
    dst_url = "s3://bucket/dst"
    dst_dst_url = "s3://bucket/dst_dst"
    content = b"bytes"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="src", Body=content)
    smart.smart_symlink(src_url, dst_url)
    smart.smart_symlink(dst_url, dst_dst_url)

    res = smart.smart_readlink(dst_dst_url)
    assert res == src_url


def test_smart_readlink(filesystem):
    src_path = "/tmp/src_file"
    dst_path = "/tmp/dst_file"
    os.symlink(src_path, dst_path)

    res = smart.smart_readlink(dst_path)
    assert res == src_path


def test_default_copy_func(filesystem):
    content = "1234"

    def callback_func(count):
        assert count == len(content)

    with open("a.txt", "w") as f:
        f.write(content)

    smart._default_copy_func("a.txt", "b.txt", callback=callback_func)

    with open("b.txt", "r") as f:
        assert f.read() == content

    with open("c.txt", "w") as f:
        f.write("12345")

    smart._default_copy_func(
        "c.txt",
        "b.txt",
        callback=callback_func,
        overwrite=False,
    )

    with open("b.txt", "r") as f:
        assert f.read() == content


@patch.object(smart, "combine")
def test_smart_combine_open(funcA, mocker):
    mocker.patch("megfile.smart.smart_glob", return_value=[])
    smart.smart_combine_open("test path")
    funcA.assert_called_once()


@patch.object(smart, "smart_open")
def test_smart_save_content(funcA):
    smart.smart_save_content("test path", b"test")
    funcA.assert_called_once()


@patch.object(smart, "smart_open")
def test_smart_save_text(funcA):
    smart.smart_save_text("test path", "test")
    funcA.assert_called_once()


@patch.object(smart, "smart_open")
def test_smart_load_text(funcA):
    smart.smart_load_text("test path")
    funcA.assert_called_once()


def test_smart_concat(s3_empty_client, fs):
    smart.smart_save_content("a.txt", b"a")
    smart.smart_save_content("b.txt", b"b")
    smart.smart_concat(["a.txt", "b.txt"], "c.txt")
    assert smart.smart_load_content("c.txt") == b"ab"

    s3_empty_client.create_bucket(Bucket="bucket")
    smart.smart_save_content("s3://bucket/a.txt", b"a")
    smart.smart_save_content("b.txt", b"b")
    smart.smart_concat(["s3://bucket/a.txt", "b.txt"], "c.txt")
    assert smart.smart_load_content("c.txt") == b"ab"

    smart.smart_concat([], "c.txt")

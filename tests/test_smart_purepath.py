import os
from typing import Generator

import boto3
import pytest
from moto import mock_aws

from megfile.errors import S3FileExistsError, S3FileNotFoundError
from megfile.fs_path import FSPath
from megfile.lib.compat import fspath
from megfile.pathlike import StatResult
from megfile.s3_path import S3Path
from megfile.smart_path import SmartPath

from . import FakeStatResult, Now

BUCKET = "bucket"


@pytest.fixture
def s3_empty_client(mocker):
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        mocker.patch("megfile.s3_path.get_s3_client", return_value=client)
        yield client


def test_repr():
    assert repr(SmartPath("file://foo.bar")) == "SmartPath('file://foo.bar')"
    assert str(SmartPath("file://foo.bar")) == "file://foo.bar"
    assert bytes(SmartPath("file://foo.bar")) == b"file://foo.bar"

    assert repr(SmartPath("/foo.bar")) == "SmartPath('/foo.bar')"
    assert str(SmartPath("/foo.bar")) == "/foo.bar"
    assert bytes(SmartPath("/foo.bar")) == b"/foo.bar"


def test_fspath():
    assert fspath(SmartPath("file://bucket/key")) == "bucket/key"

    assert fspath(SmartPath("bucket/key")) == "bucket/key"
    assert fspath(SmartPath("/foo.bar")) == "/foo.bar"
    assert fspath(SmartPath("///foo.bar")) == "/foo.bar"

    assert fspath(SmartPath("s3://bucket/key")) == "s3://bucket/key"
    assert fspath(SmartPath("s3://foo.bar")) == "s3://foo.bar"


def test_join_strs():
    assert SmartPath("file://foo", "some/path", "bar") == SmartPath(
        "file://foo/some/path/bar"
    )
    assert SmartPath("file://foo", "", "bar") == SmartPath("file://foo//bar")
    assert SmartPath("file://foo", "/some/path", "bar") == SmartPath("/some/path/bar")

    assert SmartPath("foo", "some/path", "bar") == SmartPath("foo/some/path/bar")
    assert SmartPath("foo", "", "bar") == SmartPath("foo//bar")
    assert SmartPath("foo", "/some/path", "bar") == SmartPath("/some/path/bar")

    assert SmartPath("s3://foo", "some/path", "bar") == SmartPath(
        "s3://foo/some/path/bar"
    )
    assert SmartPath("s3://foo", "", "bar") == SmartPath("s3://foo//bar")
    assert SmartPath("s3://foo", "/some/path", "bar") == SmartPath(
        "s3://foo/some/path/bar"
    )


def test_join_paths():
    assert SmartPath(SmartPath("file://foo"), SmartPath("bar")) == SmartPath(
        "file://foo/bar"
    )

    assert SmartPath(SmartPath("foo"), SmartPath("bar")) == SmartPath("foo/bar")

    assert SmartPath(SmartPath("s3://foo"), SmartPath("bar")) == SmartPath(
        "s3://foo/bar"
    )


def test_slashes_single_double_dots():
    assert SmartPath("file://foo//bar") == SmartPath("file://foo//bar")
    assert SmartPath("file://foo/./bar") == SmartPath("file://foo/./bar")
    assert SmartPath("file://foo/../bar") == SmartPath("file://foo/../bar")
    assert SmartPath("file://../bar") == SmartPath("file://../bar")

    assert SmartPath("foo//bar") == SmartPath("foo//bar")
    assert SmartPath("foo/./bar") == SmartPath("foo/./bar")
    assert SmartPath("foo/../bar") == SmartPath("foo/../bar")
    assert SmartPath("../bar") == SmartPath("../bar")

    assert SmartPath("s3://foo//bar") == SmartPath("s3://foo//bar")
    assert SmartPath("s3://foo/./bar") == SmartPath("s3://foo/./bar")
    assert SmartPath("s3://foo/../bar") == SmartPath("s3://foo/../bar")
    assert SmartPath("s3://../bar") == SmartPath("s3://../bar")

    assert SmartPath("file://foo", "../bar") == SmartPath("file://foo/../bar")
    assert SmartPath("foo", "../bar") == SmartPath("foo/../bar")


def test_operators():
    assert SmartPath("file://foo") / "bar" / "baz" == SmartPath("file://foo/bar/baz")
    assert SmartPath("foo") / "bar" / "baz" == SmartPath("foo/bar/baz")
    assert SmartPath("file://foo") / "bar" / "baz" in {SmartPath("file://foo/bar/baz")}


def test_parts():
    assert SmartPath("file://foo//bar").parts == ("foo", "bar")
    assert SmartPath("file://foo/./bar").parts == ("foo", "bar")
    assert SmartPath("file://foo/../bar").parts == ("foo", "..", "bar")
    assert SmartPath("file://../bar").parts == ("..", "bar")
    assert (SmartPath("file://foo") / "../bar").parts == ("foo", "..", "bar")
    assert SmartPath("file://foo/bar").parts == ("foo", "bar")

    assert SmartPath("file://foo", "../bar").parts == ("foo", "..", "bar")
    assert SmartPath("file://", "foo", "bar").parts == ("foo", "bar")

    assert SmartPath("s3://foo//bar").parts == ("s3://", "foo", "", "bar")
    assert SmartPath("s3://foo/./bar").parts == ("s3://", "foo", ".", "bar")
    assert SmartPath("s3://foo/../bar").parts == ("s3://", "foo", "..", "bar")
    assert SmartPath("s3://../bar").parts == ("s3://", "..", "bar")
    assert (SmartPath("s3://foo") / "../bar").parts == ("s3://", "foo", "..", "bar")
    assert SmartPath("s3://foo/bar").parts == ("s3://", "foo", "bar")

    assert SmartPath("s3://foo", "../bar").parts == ("s3://", "foo", "..", "bar")
    assert SmartPath("s3://", "foo", "bar").parts == ("s3://", "foo", "bar")

    assert SmartPath("foo//bar").parts == ("foo", "bar")
    assert SmartPath("foo/./bar").parts == ("foo", "bar")
    assert SmartPath("foo/../bar").parts == ("foo", "..", "bar")
    assert SmartPath("../bar").parts == ("..", "bar")
    assert SmartPath("foo", "../bar").parts == ("foo", "..", "bar")
    assert SmartPath("foo/bar").parts == ("foo", "bar")
    assert SmartPath("/", "foo", "bar").parts == ("/", "foo", "bar")


def test_drive():
    assert SmartPath("file://foo/bar").drive == ""

    assert SmartPath("foo//bar").drive == ""
    assert SmartPath("foo/./bar").drive == ""
    assert SmartPath("foo/../bar").drive == ""
    assert SmartPath("../bar").drive == ""

    assert SmartPath("foo", "../bar").drive == ""

    assert SmartPath("s3://bucket/test").drive == ""


def test_root():
    assert SmartPath("foo/bar").root == ""

    assert SmartPath("/foo/bar").root == "/"
    assert SmartPath("foo//bar").root == ""
    assert SmartPath("foo/./bar").root == ""
    assert SmartPath("foo/../bar").root == ""
    assert SmartPath("../bar").root == ""

    assert SmartPath("s3://bucket/test").root == "s3://"


def test_anchor():
    assert SmartPath("/foo/bar").anchor == "/"

    assert SmartPath("foo//bar").anchor == ""
    assert SmartPath("foo/./bar").anchor == ""
    assert SmartPath("foo/../bar").anchor == ""
    assert SmartPath("../bar").anchor == ""

    assert SmartPath("s3://bucket/test").anchor == "s3://"


def test_parents():
    assert tuple(SmartPath("foo//bar").parents) == (SmartPath("foo"), SmartPath(""))
    assert tuple(SmartPath("foo/./bar").parents) == (SmartPath("foo"), SmartPath(""))
    assert tuple(SmartPath("foo/../bar").parents) == (
        SmartPath("foo/.."),
        SmartPath("foo"),
        SmartPath(""),
    )
    assert tuple(SmartPath("../bar").parents) == (SmartPath(".."), SmartPath(""))
    assert tuple(SmartPath("foo", "../bar").parents) == (
        SmartPath("foo/.."),
        SmartPath("foo"),
        SmartPath(""),
    )

    assert tuple(SmartPath("file://foo/bar").parents) == (
        SmartPath("foo"),
        SmartPath(""),
    )


def test_parent():
    assert SmartPath("foo//bar").parent == SmartPath("foo/")
    assert SmartPath("foo/./bar").parent == SmartPath("foo/.")
    assert SmartPath("foo/../bar").parent == SmartPath("foo/..")
    assert SmartPath("../bar").parent == SmartPath("..")
    assert SmartPath("/foo/bar").parent == SmartPath("/foo")
    assert SmartPath("file://").parent == SmartPath("file://")
    assert SmartPath("foo", "../bar").parent == SmartPath("foo/..")
    assert SmartPath("/").parent == SmartPath("/")


def test_name():
    assert SmartPath("file://foo/bar/baz.py").name == "baz.py"
    assert SmartPath("foo/bar/baz.py").name == "baz.py"
    assert SmartPath("s3://foo/bar/baz.py").name == "baz.py"


def test_suffix():
    assert SmartPath("file://foo/bar.tar.gz").suffix == ".gz"
    assert SmartPath("file://foo/bar").suffix == ""

    assert SmartPath("foo/bar/baz.py").suffix == ".py"
    assert SmartPath("foo/bar").suffix == ""

    assert SmartPath("s3://foo/bar/baz.py").suffix == ".py"
    assert SmartPath("s3://foo/bar").suffix == ""


def test_suffixes():
    assert SmartPath("file://foo/bar.tar.gar").suffixes == [".tar", ".gar"]
    assert SmartPath("file://foo/bar.tar.gz").suffixes == [".tar", ".gz"]
    assert SmartPath("file://foo/bar").suffixes == []

    assert SmartPath("foo/bar.tar.gar").suffixes == [".tar", ".gar"]
    assert SmartPath("foo/bar.tar.gz").suffixes == [".tar", ".gz"]
    assert SmartPath("foo/bar").suffixes == []

    assert SmartPath("s3://foo/bar.tar.gar").suffixes == [".tar", ".gar"]
    assert SmartPath("s3://foo/bar.tar.gz").suffixes == [".tar", ".gz"]
    assert SmartPath("s3://foo/bar").suffixes == []


def test_stem():
    assert SmartPath("foo/bar.tar.gar").stem == "bar.tar"
    assert SmartPath("foo/bar.tar").stem == "bar"
    assert SmartPath("foo/bar").stem == "bar"


def test_uri():
    assert SmartPath("/foo/bar").as_uri() == "file:///foo/bar"
    assert SmartPath("file:///foo/bar/baz").as_uri() == "file:///foo/bar/baz"
    assert SmartPath("file:///bucket/key").as_uri() == "file:///bucket/key"
    assert SmartPath("/buc:ket/ke@y").as_uri() == "file:///buc:ket/ke@y"
    assert SmartPath("file://foo/bar").as_uri() == "file://foo/bar"
    assert SmartPath("file://foo/bar/baz").as_uri() == "file://foo/bar/baz"
    assert SmartPath("file://bucket/key").as_uri() == "file://bucket/key"

    # no escape
    assert SmartPath("file://buc:ket/ke@y").as_uri() == "file://buc:ket/ke@y"


def test_absolute():
    assert not SmartPath("file://foo/bar").is_absolute()
    assert not SmartPath("foo/bar").is_absolute()


def test_reserved():
    assert not SmartPath("file://foo/bar").is_reserved()
    assert not SmartPath("foo/bar").is_reserved()


def test_is_relative_to():
    assert SmartPath("file://foo/bar").is_relative_to("foo")
    assert not SmartPath("file:///foo/bar").is_relative_to("foo")

    assert SmartPath("s3://foo/bar").is_relative_to("s3://foo")
    assert SmartPath("s3://foo/bar").is_relative_to("foo")
    assert not SmartPath("s3://foo/bar").is_relative_to("bar")


def test_joinpath():
    assert SmartPath("file://foo").joinpath("bar") == SmartPath("file://foo/bar")
    assert SmartPath("file://foo").joinpath(SmartPath("bar")) == SmartPath(
        "file://foo/bar"
    )
    assert SmartPath("file://foo").joinpath("bar", "baz") == SmartPath(
        "file://foo/bar/baz"
    )

    assert SmartPath("foo").joinpath("bar") == SmartPath("foo/bar")
    assert SmartPath("foo").joinpath(SmartPath("bar")) == SmartPath("foo/bar")
    assert SmartPath("foo").joinpath("bar", "baz") == SmartPath("foo/bar/baz")

    assert SmartPath("s3://foo").joinpath("bar") == SmartPath("s3://foo/bar")
    assert SmartPath("s3://foo").joinpath(SmartPath("bar")) == SmartPath("s3://foo/bar")
    assert SmartPath("s3://foo").joinpath("bar", "baz") == SmartPath("s3://foo/bar/baz")


def test_match():
    assert SmartPath("a/b.py").match("*.py")
    assert SmartPath("file://a/b/c.py").match("b/*.py")
    assert not SmartPath("file://a/b/c.py").match("file://a/*.py")
    assert SmartPath("file://a.py").match("file://*.py")
    assert SmartPath("a/b.py").match("file://a/b.py")
    assert not SmartPath("a/b.py").match("file://*.py")
    assert not SmartPath("a/b.py").match("*.Py")

    assert SmartPath("s3://a/b.py").match("*.py")
    assert SmartPath("s3://a/b.py").match("s3://a/b.py")
    assert not SmartPath("s3://a/b.py").match("s3://*.py")
    assert not SmartPath("s3://a/b.py").match("*.Py")


def test_relative_to():
    path = SmartPath("file://foo/bar")
    assert path.relative_to("file://") == SmartPath("foo/bar")
    assert path.relative_to("file://foo") == SmartPath("bar")
    with pytest.raises(ValueError):
        path.relative_to("file://baz")

    path = SmartPath("s3://foo/bar")
    assert path.relative_to("s3://") == S3Path("foo/bar")
    assert path.relative_to("s3://foo") == S3Path("bar")
    assert path.relative_to("foo") == S3Path("bar")
    with pytest.raises(ValueError):
        path.relative_to("s3://baz")


def test_relative_to_relative():
    path = SmartPath("foo/bar/baz")
    assert path.relative_to("foo/bar") == SmartPath("baz")
    assert path.relative_to("foo") == SmartPath("bar/baz")
    with pytest.raises(ValueError):
        path.relative_to("baz")


def test_with_name():
    path = SmartPath("file://foo/bar.tar.gz")
    assert path.with_name("baz.py") == SmartPath("file://foo/baz.py")

    path = SmartPath("foo/bar.tar.gz")
    assert path.with_name("baz.py") == SmartPath("foo/baz.py")

    path = SmartPath("s3://foo/bar.tar.gz")
    assert path.with_name("baz.py") == SmartPath("s3://foo/baz.py")


def test_with_stem():
    path = SmartPath("file://foo/bar.tar.gz")
    assert path.with_stem("baz") == SmartPath("file://foo/baz.gz")

    path = SmartPath("s3://foo/bar.tar.gz")
    assert path.with_stem("baz") == SmartPath("s3://foo/baz.gz")


def test_with_suffix():
    path = SmartPath("file://foo/bar.tar.gz")
    assert path.with_suffix(".bz2") == SmartPath("file://foo/bar.tar.bz2")
    path = SmartPath("baz")
    assert path.with_suffix(".txt") == SmartPath("baz.txt")
    path = SmartPath("baz.txt")
    assert path.with_suffix("") == SmartPath("baz")

    path = SmartPath("s3://foo/bar.tar.gz")
    assert path.with_suffix(".bz2") == SmartPath("s3://foo/bar.tar.bz2")


def test_cwd(fs):
    os.makedirs("/test")
    os.chdir("/test")
    path = SmartPath("/")
    assert path.cwd() == SmartPath("/test")

    path = SmartPath("s3://bucketA/test")
    assert path.cwd() == path

    with pytest.raises(NotImplementedError):
        SmartPath("http://test/test").cwd()


def test_home(mocker):
    mocker.patch("os.path.expanduser", return_value="/home/test")
    path = SmartPath("/")
    assert path.home() == SmartPath("/home/test")

    with pytest.raises(NotImplementedError):
        SmartPath("s3://bucketA/test").home()


def make_stat(size=0, time=Now(), isdir=False, islnk=False):
    return StatResult(size=size, ctime=time, mtime=time, isdir=isdir, islnk=islnk)


def test_stat(fs, mocker):
    mocker.patch("megfile.fs_path.StatResult", side_effect=FakeStatResult)

    with pytest.raises(FileNotFoundError):
        SmartPath("NotExist").stat()
    with pytest.raises(FileNotFoundError):
        SmartPath("").stat()

    assert SmartPath("/").stat() == make_stat(isdir=True)
    assert SmartPath(".").stat() == make_stat(isdir=True)

    os.mkdir("folderA")
    with open("/folderA/fileA", "wb") as fileA:
        fileA.write(b"fileA")
    os.symlink("/folderA/fileA", "/folderA/fileA.lnk")
    assert SmartPath("/").stat() == make_stat(size=19, isdir=True)
    assert SmartPath(".").stat() == make_stat(size=19, isdir=True)
    assert SmartPath("/folderA/fileA").stat() == make_stat(size=5, isdir=False)
    assert SmartPath("/folderA/fileA.lnk").stat() == make_stat(
        size=5, isdir=False, islnk=False
    )
    assert SmartPath("/folderA/fileA.lnk").lstat() == make_stat(
        size=14, isdir=False, islnk=True
    )


def test_chmod(fs):
    with pytest.raises(NotImplementedError):
        SmartPath("s3://bucketA/test").chmod(0o444)

    with pytest.raises(NotImplementedError):
        SmartPath("s3://bucketA/test").lchmod(0o444)

    path = "/test"
    with open(path, "w") as f:
        f.write("test")
    path_obj = SmartPath(path)
    path_obj.chmod(0o444)
    assert os.stat(path).st_mode == 33060
    # path_obj.lchmod(0o777)
    # assert os.stat(path).st_mode == 33279


def test_exists(s3_empty_client, fs):
    path = SmartPath("/test")
    path.write_text("test")
    assert path.exists()

    path = SmartPath("s3://bucket/test")
    path.write_text("test")
    assert path.exists()


def test_expanduser(fs):
    home = os.environ["HOME"]
    os.environ["HOME"] = "/home/test"

    with pytest.raises(NotImplementedError):
        SmartPath("s3://bucketA/test").expanduser()

    path = SmartPath("~/file")
    assert path.expanduser() == SmartPath("/home/test/file")
    os.environ["HOME"] = home


def test_glob(s3_empty_client, fs):
    os.mkdir("A")
    os.mkdir("A/a")
    os.mkdir("A/b")
    os.mkdir("A/b/c")
    with open("A/1.json", "w") as f:
        f.write("1.json")

    with open("A/b/file.json", "w") as f:
        f.write("file")

    assert SmartPath("A").glob("*") == [
        SmartPath("A/1.json"),
        SmartPath("A/a"),
        SmartPath("A/b"),
    ]
    assert SmartPath("A").rglob("*.json") == [
        SmartPath("A/1.json"),
        SmartPath("A/b/file.json"),
    ]
    assert SmartPath("A").rglob(None) == [
        SmartPath("A/"),
        SmartPath("A/a/"),
        SmartPath("A/b/"),
        SmartPath("A/b/c/"),
    ]
    assert [file_entry.path for file_entry in SmartPath("A").glob_stat("*")] == [
        SmartPath("A/1.json"),
        SmartPath("A/a"),
        SmartPath("A/b"),
    ]

    for path in SmartPath("A").glob("*"):
        assert isinstance(path, FSPath)

    SmartPath("s3://bucket/A/1").write_text("1")
    SmartPath("s3://bucket/A/2.json").write_text("2")
    SmartPath("s3://bucket/A/3").write_text("3")
    SmartPath("s3://bucket/A/4/5").write_text("5")
    SmartPath("s3://bucket/A/4/6.json").write_text("6")

    assert SmartPath("s3://bucket/A").glob("*") == [
        SmartPath("s3://bucket/A/1"),
        SmartPath("s3://bucket/A/2.json"),
        SmartPath("s3://bucket/A/3"),
        SmartPath("s3://bucket/A/4"),
    ]
    assert SmartPath("s3://bucket/A").rglob("*.json") == [
        SmartPath("s3://bucket/A/2.json"),
        SmartPath("s3://bucket/A/4/6.json"),
    ]

    assert [
        file_entry.path for file_entry in SmartPath("s3://bucket/A").glob_stat("*")
    ] == [
        SmartPath("s3://bucket/A/1"),
        SmartPath("s3://bucket/A/2.json"),
        SmartPath("s3://bucket/A/3"),
        SmartPath("s3://bucket/A/4"),
    ]

    for path in SmartPath("s3://bucket/A").glob("*"):
        assert isinstance(path, S3Path)
    pass


def test_group(mocker):
    mocker.patch("pathlib.Path.group", return_value="test_group")

    assert SmartPath("/test").group() == "test_group"

    with pytest.raises(NotImplementedError):
        SmartPath("s3://bucketA/test").group()
    pass


def test_is_dir(s3_empty_client, fs):
    os.mkdir("A")
    with open("A/1.json", "w") as f:
        f.write("1.json")

    assert SmartPath("A").is_dir() is True
    assert SmartPath("A/1.json").is_dir() is False

    SmartPath("s3://bucket/A/4/5").write_text("5")

    assert SmartPath("s3://bucket/A").is_dir() is True
    assert SmartPath("s3://bucket/A/4/5").is_dir() is False


def test_is_file(s3_empty_client, fs):
    os.mkdir("A")
    with open("A/1.json", "w") as f:
        f.write("1.json")

    assert SmartPath("A").is_file() is False
    assert SmartPath("A/1.json").is_file() is True

    SmartPath("s3://bucket/A/4/5").write_text("5")

    assert SmartPath("s3://bucket/A").is_file() is False
    assert SmartPath("s3://bucket/A/4/5").is_file() is True


def test_is_mount(mocker):
    mocker.patch("os.path.ismount", return_value=True)
    assert SmartPath("s3://bucket/A/4/5").is_mount() is False
    assert SmartPath("/A/4/5").is_mount() is True


def test_is_symlink(s3_empty_client, fs):
    os.mkdir("A")
    with open("A/1.json", "w") as f:
        f.write("1.json")
    os.symlink("A/1.json", "A/1.lnk")

    assert SmartPath("A/1.json").is_symlink() is False
    assert SmartPath("A/1.lnk").is_symlink() is True

    path = SmartPath("s3://bucket/file")
    path.write_text("5")
    path.symlink("s3://bucket/lnk")

    assert SmartPath("s3://bucket/file").is_symlink() is False
    assert SmartPath("s3://bucket/lnk").is_symlink() is True


def test_is_socket(mocker):
    mocker.patch("pathlib.Path.is_socket", return_value=True)
    assert SmartPath("A/1.json").is_socket() is True
    assert SmartPath("s3://bucket/file").is_socket() is False


def test_is_fifo(mocker):
    mocker.patch("pathlib.Path.is_fifo", return_value=True)
    assert SmartPath("A/1.json").is_fifo() is True
    assert SmartPath("s3://bucket/file").is_fifo() is False


def test_is_block_device(mocker):
    mocker.patch("pathlib.Path.is_block_device", return_value=True)
    assert SmartPath("A/1.json").is_block_device() is True
    assert SmartPath("s3://bucket/file").is_block_device() is False


def test_is_char_device(mocker):
    mocker.patch("pathlib.Path.is_char_device", return_value=True)
    assert SmartPath("A/1.json").is_char_device() is True
    assert SmartPath("s3://bucket/file").is_char_device() is False


def test_iterdir(s3_empty_client, fs):
    os.mkdir("A")
    os.mkdir("A/a")
    os.mkdir("A/b")
    os.mkdir("A/b/c")
    with open("A/1.json", "w") as f:
        f.write("1.json")

    with open("A/b/file.json", "w") as f:
        f.write("file")

    assert isinstance(SmartPath("A").iterdir(), Generator)
    assert sorted(list(SmartPath("A").iterdir())) == [
        SmartPath("A/1.json"),
        SmartPath("A/a"),
        SmartPath("A/b"),
    ]

    for path in SmartPath("A").iterdir():
        assert isinstance(path, FSPath)

    SmartPath("s3://bucket/A/1").write_text("1")
    SmartPath("s3://bucket/A/2.json").write_text("2")
    SmartPath("s3://bucket/A/3").write_text("3")
    SmartPath("s3://bucket/A/4/5").write_text("5")
    SmartPath("s3://bucket/A/4/6.json").write_text("6")

    assert sorted(list(SmartPath("s3://bucket/A").iterdir())) == [
        SmartPath("s3://bucket/A/1"),
        SmartPath("s3://bucket/A/2.json"),
        SmartPath("s3://bucket/A/3"),
        SmartPath("s3://bucket/A/4"),
    ]

    for path in SmartPath("s3://bucket/A").iterdir():
        assert isinstance(path, S3Path)

    with pytest.raises(NotImplementedError):
        list(SmartPath("http://test/test").iterdir())


def test_mkdir(s3_empty_client, fs):
    path = SmartPath("/test")
    path.mkdir()
    assert path.is_dir() is True

    with pytest.raises(FileExistsError):
        path.mkdir()

    path.mkdir(exist_ok=True)

    with pytest.raises(FileNotFoundError):
        SmartPath("/test/notExist/testA").mkdir()

    SmartPath("/test/notExist/testA").mkdir(parents=True)
    assert os.path.exists("/test/notExist/testA")

    path = SmartPath("s3://bucket/A/1")
    path.touch()
    with pytest.raises(S3FileExistsError):
        SmartPath("s3://bucket/A").mkdir()
    pass


def test_open(s3_empty_client, fs):
    path = SmartPath("/test")
    with path.open(mode="w") as f:
        f.write("1")

    with path.open(mode="r") as f:
        assert f.read() == "1"

    path = SmartPath("s3://bucket/A/1")
    with path.open(mode="w") as f:
        f.write("1")

    with path.open(mode="r") as f:
        assert f.read() == "1"

    with pytest.raises(ValueError):
        SmartPath("http://bucket/A").open("wb")
    pass


def test_owner(fs, mocker):
    class Fake:
        pw_name = "test_owner"

    mocker.patch("pwd.getpwuid", return_value=Fake)
    path = SmartPath("/test")
    path.touch()
    assert path.owner() == "test_owner"

    with pytest.raises(NotImplementedError):
        SmartPath("http://bucket/A").owner()


def test_read_bytes(s3_empty_client, fs):
    path = SmartPath("/test")
    with path.open(mode="w") as f:
        f.write("1")
    assert path.read_bytes() == b"1"

    path = SmartPath("s3://bucket/A/1")
    with path.open(mode="w") as f:
        f.write("1")
    assert path.read_bytes() == b"1"


def test_read_text(s3_empty_client, fs):
    path = SmartPath("/test")
    with path.open(mode="w") as f:
        f.write("1")
    assert path.read_text() == "1"

    path = SmartPath("s3://bucket/A/1")
    with path.open(mode="w") as f:
        f.write("1")
    assert path.read_text() == "1"


def test_readlink(s3_empty_client, fs):
    path = SmartPath("/test")
    path.touch()
    SmartPath("/test.lnk").symlink_to(path)
    assert SmartPath("/test.lnk").readlink() == path

    path = SmartPath("s3://bucket/A/1")
    path.touch()
    SmartPath("s3://bucket/A/1.lnk").symlink_to(path)
    assert SmartPath("s3://bucket/A/1.lnk").readlink() == path


def test_rename(s3_empty_client, fs):
    path = SmartPath("/test")
    path.touch()
    assert path.rename("/test-rename") == SmartPath("/test-rename")
    assert path.exists() is False

    path = SmartPath("s3://bucket/A/1")
    path.touch()
    assert path.rename("s3://bucket/A/1-rename") == SmartPath("s3://bucket/A/1-rename")
    assert path.exists() is False

    with pytest.raises(NotImplementedError):
        SmartPath("http://test/test").rename("test")


def test_replace(s3_empty_client, fs):
    path = SmartPath("/test")
    path.touch()
    assert path.replace("/test-rename") == SmartPath("/test-rename")
    assert path.exists() is False

    path = SmartPath("s3://bucket/A/1")
    path.touch()
    assert path.replace("s3://bucket/A/1-rename") == SmartPath("s3://bucket/A/1-rename")
    assert path.exists() is False


def test_absolute(s3_empty_client, fs):
    os.makedirs("/test/a")
    SmartPath("/test/a/file").touch()
    os.chdir("/test/a")
    assert SmartPath("file").absolute() == "/test/a/file"

    path = SmartPath("s3://bucket/A/1")
    assert path.absolute() == path

    with pytest.raises(NotImplementedError):
        SmartPath("http://test/test").absolute()


def test_resolve(fs):
    os.makedirs("/test/a")
    SmartPath("/test/a/file").touch()
    os.chdir("/test/a")
    os.symlink("/test/a/file", "/test/a/file.lnk")
    assert SmartPath("file").resolve() == "/test/a/file"
    assert SmartPath("/test/a/../a/file").resolve() == "/test/a/file"
    assert SmartPath("file.lnk").resolve() == "/test/a/file"


def test_rmdir(fs):
    os.makedirs("/test/a")
    SmartPath("/test/a/file").touch()

    with pytest.raises(OSError):
        SmartPath("/test/a").rmdir()

    with pytest.raises(NotADirectoryError):
        SmartPath("/test/a/file").rmdir()

    with pytest.raises(NotImplementedError):
        SmartPath("s3://bucket/A/1").rmdir()


def test_samefile(s3_empty_client, fs, mocker):
    from pathlib import Path

    from megfile.fs_path import FSPath
    from megfile.s3_path import S3Path
    from megfile.smart import smart_copy

    os.makedirs("/a")
    fs_path = FSPath("/a/test")
    with open(fs_path, "w") as f:
        f.write("test")

    with open("test", "w") as f:
        f.write("test1")

    assert fs_path.samefile(FSPath("/a/test")) is True
    assert fs_path.samefile(Path("/a/test")) is True
    assert fs_path.samefile("a/test") is True
    assert fs_path.samefile("/a/./test") is True
    assert fs_path.samefile("/a/../test") is False
    assert fs_path.samefile("/a/../a/test") is True

    s3_path = S3Path(f"s3://{BUCKET}/test")
    smart_copy(fs_path, str(s3_path))

    assert s3_path.samefile(FSPath("/a/test")) is False

    assert s3_path.samefile(Path("/a/test")) is False
    assert s3_path.samefile(f"s3://{BUCKET}/test") is True

    with pytest.raises(S3FileNotFoundError):
        s3_path.samefile("/a/not_found")

    with pytest.raises(S3FileNotFoundError):
        s3_path.samefile(f"s3://{BUCKET}/not_found")


def test_symlink_to(s3_empty_client, fs):
    SmartPath("/test").touch()
    SmartPath("/test.lnk").symlink_to("/test")
    assert SmartPath("/test.lnk").lstat().is_symlink()

    SmartPath("s3://bucket/A/1").touch()
    SmartPath("s3://bucket/A/1.lnk").symlink_to("s3://bucket/A/1")
    assert SmartPath("s3://bucket/A/1.lnk").lstat().is_symlink()

    with pytest.raises(NotImplementedError):
        SmartPath("http://test/test").symlink_to("http://test/test2")


def teste_hardlink_to(fs):
    SmartPath("/test").touch()
    SmartPath("/test.lnk").hardlink_to("/test")
    assert os.stat("/test.lnk").st_nlink == 2

    with pytest.raises(NotImplementedError):
        SmartPath("http://test/test").hardlink_to("http://test/test2")


def test_unlink(s3_empty_client, fs):
    path = SmartPath("/test")
    path.touch()
    assert path.exists()
    path.unlink()
    assert path.exists() is False

    path = SmartPath("s3://bucket/A/1")
    path.touch()
    assert path.exists()
    path.unlink()
    assert path.exists() is False

    with pytest.raises(NotImplementedError):
        SmartPath("http://test/test").unlink()


def test_write_bytes(s3_empty_client, fs):
    content = b"test"
    path = SmartPath("/test")
    path.write_bytes(content)
    assert path.read_bytes() == content

    path = SmartPath("s3://bucket/A/1")
    path.write_bytes(content)
    assert path.read_bytes() == content

    with pytest.raises(ValueError):
        SmartPath("http://test/test").write_bytes(content)


def test_write_text(s3_empty_client, fs):
    content = "test"
    path = SmartPath("/test")
    path.write_text(content)
    assert path.read_text() == content

    path = SmartPath("s3://bucket/A/1")
    path.write_text(content)
    assert path.read_text() == content

    with pytest.raises(ValueError):
        SmartPath("http://test/test").write_text(content)

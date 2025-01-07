import pathlib

import pytest

from megfile.fs_path import FSPath
from megfile.lib.compat import fspath


def test_repr():
    assert repr(FSPath("file://foo.bar")) == "FSPath('file://foo.bar')"
    assert str(FSPath("file://foo.bar")) == "file://foo.bar"
    assert bytes(FSPath("file://foo.bar")) == b"file://foo.bar"

    assert repr(FSPath("/foo.bar")) == "FSPath('/foo.bar')"
    assert str(FSPath("/foo.bar")) == "/foo.bar"
    assert bytes(FSPath("/foo.bar")) == b"/foo.bar"


def test_fspath():
    assert fspath(FSPath("file://bucket/key")) == "bucket/key"

    assert fspath(FSPath("bucket/key")) == "bucket/key"
    assert fspath(FSPath("/foo.bar")) == "/foo.bar"
    assert fspath(FSPath("///foo.bar")) == "/foo.bar"


def test_join_strs():
    assert FSPath("file://foo", "some/path", "bar") == FSPath(
        "file://foo/some/path/bar"
    )
    assert FSPath("file://foo", "", "bar") == FSPath("file://foo//bar")
    assert FSPath("file://foo", "/some/path", "bar") == FSPath("/some/path/bar")

    assert FSPath("foo", "some/path", "bar") == FSPath("foo/some/path/bar")
    assert FSPath("foo", "", "bar") == FSPath("foo//bar")
    assert FSPath("foo", "/some/path", "bar") == FSPath("/some/path/bar")


def test_join_paths():
    assert FSPath(FSPath("file://foo"), FSPath("bar")) == FSPath("file://foo/bar")

    assert FSPath(FSPath("foo"), FSPath("bar")) == FSPath("foo/bar")


def test_slashes_single_double_dots():
    assert FSPath("file://foo//bar") == FSPath("file://foo//bar")
    assert FSPath("file://foo/./bar") == FSPath("file://foo/./bar")
    assert FSPath("file://foo/../bar") == FSPath("file://foo/../bar")
    assert FSPath("file://../bar") == FSPath("file://../bar")

    assert FSPath("foo//bar") == FSPath("foo//bar")
    assert FSPath("foo/./bar") == FSPath("foo/./bar")
    assert FSPath("foo/../bar") == FSPath("foo/../bar")
    assert FSPath("../bar") == FSPath("../bar")

    assert FSPath("file://foo", "../bar") == FSPath("file://foo/../bar")
    assert FSPath("foo", "../bar") == FSPath("foo/../bar")


def test_operators():
    assert FSPath("file://foo") / "bar" / "baz" == FSPath("file://foo/bar/baz")
    assert FSPath("foo") / "bar" / "baz" == FSPath("foo/bar/baz")
    assert FSPath("file://foo") / "bar" / "baz" in {FSPath("file://foo/bar/baz")}
    assert FSPath("file://foo") / pathlib.Path("bar") / "baz" in {
        FSPath("file://foo/bar/baz")
    }


def test_parts():
    assert FSPath("file://foo//bar").parts == ("foo", "bar")
    assert FSPath("file://foo/./bar").parts == ("foo", "bar")
    assert FSPath("file://foo/../bar").parts == ("foo", "..", "bar")
    assert FSPath("file://../bar").parts == ("..", "bar")
    assert (FSPath("file://foo") / "../bar").parts == ("foo", "..", "bar")
    assert FSPath("file://foo/bar").parts == ("foo", "bar")

    assert FSPath("file://foo", "../bar").parts == ("foo", "..", "bar")
    assert FSPath("file://", "foo", "bar").parts == ("foo", "bar")

    assert FSPath("foo//bar").parts == ("foo", "bar")
    assert FSPath("foo/./bar").parts == ("foo", "bar")
    assert FSPath("foo/../bar").parts == ("foo", "..", "bar")
    assert FSPath("../bar").parts == ("..", "bar")
    assert FSPath("foo", "../bar").parts == ("foo", "..", "bar")
    assert FSPath("foo/bar").parts == ("foo", "bar")
    assert FSPath("/", "foo", "bar").parts == ("/", "foo", "bar")


def test_drive():
    assert FSPath("file://foo/bar").drive == ""

    assert FSPath("foo//bar").drive == ""
    assert FSPath("foo/./bar").drive == ""
    assert FSPath("foo/../bar").drive == ""
    assert FSPath("../bar").drive == ""

    assert FSPath("foo", "../bar").drive == ""


def test_root():
    assert FSPath("file://foo/bar").root == ""

    assert FSPath("/foo/bar").root == "/"
    assert FSPath("foo//bar").root == ""
    assert FSPath("foo/./bar").root == ""
    assert FSPath("foo/../bar").root == ""
    assert FSPath("../bar").root == ""


def test_anchor():
    assert FSPath("file://foo/bar").anchor == ""

    assert FSPath("foo//bar").anchor == ""
    assert FSPath("foo/./bar").anchor == ""
    assert FSPath("foo/../bar").anchor == ""
    assert FSPath("../bar").anchor == ""
    assert FSPath("/bar").anchor == "/"


def test_parents():
    assert tuple(FSPath("foo//bar").parents) == (FSPath("foo"), FSPath(""))
    assert tuple(FSPath("foo/./bar").parents) == (FSPath("foo"), FSPath(""))
    assert tuple(FSPath("foo/../bar").parents) == (
        FSPath("foo/.."),
        FSPath("foo"),
        FSPath(""),
    )
    assert tuple(FSPath("../bar").parents) == (FSPath(".."), FSPath(""))
    assert tuple(FSPath("foo", "../bar").parents) == (
        FSPath("foo/.."),
        FSPath("foo"),
        FSPath(""),
    )

    assert tuple(FSPath("file://foo/bar").parents) == (
        FSPath("file://foo"),
        FSPath("file://"),
    )


def test_parent():
    assert FSPath("foo//bar").parent == FSPath("foo/")
    assert FSPath("foo/./bar").parent == FSPath("foo/.")
    assert FSPath("foo/../bar").parent == FSPath("foo/..")
    assert FSPath("../bar").parent == FSPath("..")
    assert FSPath("/foo/bar").parent == FSPath("/foo")
    assert FSPath("file://").parent == FSPath("file://")
    assert FSPath("foo", "../bar").parent == FSPath("foo/..")
    assert FSPath("/").parent == FSPath("/")
    assert FSPath("").parent == FSPath("")
    assert FSPath("foo").parent == FSPath("")


def test_name():
    assert FSPath("file://foo/bar/baz.py").name == "baz.py"
    assert FSPath("foo/bar/baz.py").name == "baz.py"


def test_suffix():
    assert FSPath("file://foo/bar.tar.gz").suffix == ".gz"
    assert FSPath("file://foo/bar").suffix == ""

    assert FSPath("foo/bar/baz.py").suffix == ".py"
    assert FSPath("foo/bar").suffix == ""


def test_suffixes():
    assert FSPath("file://foo/bar.tar.gar").suffixes == [".tar", ".gar"]
    assert FSPath("file://foo/bar.tar.gz").suffixes == [".tar", ".gz"]
    assert FSPath("file://foo/bar").suffixes == []

    assert FSPath("foo/bar.tar.gar").suffixes == [".tar", ".gar"]
    assert FSPath("foo/bar.tar.gz").suffixes == [".tar", ".gz"]
    assert FSPath("foo/bar").suffixes == []


def test_stem():
    assert FSPath("foo/bar.tar.gar").stem == "bar.tar"
    assert FSPath("foo/bar.tar").stem == "bar"
    assert FSPath("foo/bar").stem == "bar"


def test_uri():
    assert FSPath("/foo/bar").as_uri() == "file:///foo/bar"
    assert FSPath("file:///foo/bar/baz").as_uri() == "file:///foo/bar/baz"
    assert FSPath("file:///bucket/key").as_uri() == "file:///bucket/key"
    assert FSPath("/buc:ket/ke@y").as_uri() == "file:///buc:ket/ke@y"
    assert FSPath("file://foo/bar").as_uri() == "file://foo/bar"
    assert FSPath("file://foo/bar/baz").as_uri() == "file://foo/bar/baz"
    assert FSPath("file://bucket/key").as_uri() == "file://bucket/key"

    # no escape
    assert FSPath("file://buc:ket/ke@y").as_uri() == "file://buc:ket/ke@y"


def test_absolute():
    assert not FSPath("file://foo/bar").is_absolute()
    assert not FSPath("foo/bar").is_absolute()


def test_reserved():
    assert not FSPath("file://foo/bar").is_reserved()
    assert not FSPath("foo/bar").is_reserved()


def test_joinpath():
    assert FSPath("file://foo").joinpath("bar") == FSPath("file://foo/bar")
    assert FSPath("file://foo").joinpath(FSPath("bar")) == FSPath("file://foo/bar")
    assert FSPath("file://foo").joinpath("bar", "baz") == FSPath("file://foo/bar/baz")

    assert FSPath("foo").joinpath("bar") == FSPath("foo/bar")
    assert FSPath("foo").joinpath(FSPath("bar")) == FSPath("foo/bar")
    assert FSPath("foo").joinpath("bar", "baz") == FSPath("foo/bar/baz")


def test_match():
    assert FSPath("a/b.py").match("*.py")
    assert FSPath("file://a/b/c.py").match("b/*.py")
    assert not FSPath("file://a/b/c.py").match("file://a/*.py")
    assert FSPath("file://a.py").match("file://*.py")
    assert FSPath("a/b.py").match("file://a/b.py")
    assert not FSPath("a/b.py").match("file://*.py")
    assert not FSPath("a/b.py").match("*.Py")


def test_relative_to():
    path = FSPath("file://foo/bar")
    assert path.relative_to("file://") == FSPath("foo/bar")
    assert path.relative_to("file://foo") == FSPath("bar")
    assert path.relative_to("foo") == FSPath("bar")
    with pytest.raises(ValueError):
        path.relative_to("file://baz")


def test_relative_to_relative():
    path = FSPath("foo/bar/baz")
    assert path.relative_to("foo/bar") == FSPath("baz")
    assert path.relative_to("foo") == FSPath("bar/baz")
    with pytest.raises(ValueError):
        path.relative_to("baz")


def test_with_name():
    path = FSPath("file://foo/bar.tar.gz")
    assert path.with_name("baz.py") == FSPath("file://foo/baz.py")
    path = FSPath("file://")

    # with pytest.raises(ValueError):
    #     path.with_name('baz.py')

    path = FSPath("foo/bar.tar.gz")
    assert path.with_name("baz.py") == FSPath("foo/baz.py")


def test_with_suffix():
    path = FSPath("file://foo/bar.tar.gz")
    assert path.with_suffix(".bz2") == FSPath("file://foo/bar.tar.bz2")
    path = FSPath("baz")
    assert path.with_suffix(".txt") == FSPath("baz.txt")
    path = FSPath("baz.txt")
    assert path.with_suffix("") == FSPath("baz")

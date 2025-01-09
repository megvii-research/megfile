import requests_mock  # noqa: F401

from megfile.http_path import HttpPath
from megfile.lib.compat import fspath


def test_repr():
    assert repr(HttpPath("http://foo.bar")) == "HttpPath('http://foo.bar')"
    assert str(HttpPath("http://foo.bar")) == "http://foo.bar"
    assert bytes(HttpPath("http://foo.bar")) == b"http://foo.bar"

    assert repr(HttpPath("/foo.bar")) == "HttpPath('/foo.bar')"
    assert str(HttpPath("/foo.bar")) == "/foo.bar"
    assert bytes(HttpPath("/foo.bar")) == b"/foo.bar"


def test_fspath():
    assert fspath(HttpPath("http://bucket/key")) == "http://bucket/key"

    assert fspath(HttpPath("bucket/key")) == "http://bucket/key"
    assert fspath(HttpPath("/foo.bar")) == "http://foo.bar"
    assert fspath(HttpPath("///foo.bar")) == "http://foo.bar"


def test_join_strs():
    assert HttpPath("http://foo", "some/path", "bar") == HttpPath(
        "http://foo/some/path/bar"
    )
    assert HttpPath("http://foo", "", "bar") == HttpPath("http://foo//bar")
    assert HttpPath("http://foo", "/some/path", "bar") == HttpPath(
        "http://foo/some/path/bar"
    )

    assert HttpPath("foo", "some/path", "bar") == HttpPath("foo/some/path/bar")
    assert HttpPath("foo", "", "bar") == HttpPath("foo//bar")
    assert HttpPath("foo", "/some/path", "bar") == HttpPath("foo/some/path/bar")


def test_join_paths():
    assert HttpPath(HttpPath("http://foo"), HttpPath("bar")) == HttpPath(
        "http://foo/bar"
    )

    assert HttpPath(HttpPath("foo"), HttpPath("bar")) == HttpPath("foo/bar")


def test_slashes_single_double_dots():
    assert HttpPath("http://foo//bar") == HttpPath("http://foo//bar")
    assert HttpPath("http://foo/./bar") == HttpPath("http://foo/./bar")
    assert HttpPath("http://foo/../bar") == HttpPath("http://foo/../bar")
    assert HttpPath("http://../bar") == HttpPath("http://../bar")

    assert HttpPath("foo//bar") == HttpPath("foo//bar")
    assert HttpPath("foo/./bar") == HttpPath("foo/./bar")
    assert HttpPath("foo/../bar") == HttpPath("foo/../bar")
    assert HttpPath("../bar") == HttpPath("../bar")

    assert HttpPath("http://foo", "../bar") == HttpPath("http://foo/../bar")
    assert HttpPath("foo", "../bar") == HttpPath("foo/../bar")


def test_operators():
    assert HttpPath("http://foo") / "bar" / "baz" == HttpPath("http://foo/bar/baz")
    assert HttpPath("foo") / "bar" / "baz" == HttpPath("foo/bar/baz")
    assert HttpPath("http://foo") / "bar" / "baz" in {HttpPath("http://foo/bar/baz")}


def test_parts():
    assert HttpPath("http://foo//bar").parts == ("http://", "foo", "", "bar")
    assert HttpPath("http://foo/./bar").parts == ("http://", "foo", ".", "bar")
    assert HttpPath("http://foo/../bar").parts == ("http://", "foo", "..", "bar")
    assert HttpPath("http://../bar").parts == ("http://", "..", "bar")
    assert (HttpPath("http://foo") / "../bar").parts == ("http://", "foo", "..", "bar")
    assert HttpPath("http://foo/bar").parts == ("http://", "foo", "bar")

    assert HttpPath("http://foo", "../bar").parts == ("http://", "foo", "..", "bar")
    assert HttpPath("http://", "foo", "bar").parts == ("http://", "foo", "bar")

    assert HttpPath("foo//bar").parts == ("http://", "foo", "", "bar")
    assert HttpPath("foo/./bar").parts == ("http://", "foo", ".", "bar")
    assert HttpPath("foo/../bar").parts == ("http://", "foo", "..", "bar")
    assert HttpPath("../bar").parts == ("http://", "..", "bar")
    assert HttpPath("foo", "../bar").parts == ("http://", "foo", "..", "bar")
    assert HttpPath("foo/bar").parts == ("http://", "foo", "bar")
    assert HttpPath("/", "foo", "bar").parts == ("http://", "foo", "bar")


def test_drive():
    assert HttpPath("http://foo/bar").drive == ""

    assert HttpPath("foo//bar").drive == ""
    assert HttpPath("foo/./bar").drive == ""
    assert HttpPath("foo/../bar").drive == ""
    assert HttpPath("../bar").drive == ""

    assert HttpPath("foo", "../bar").drive == ""


def test_root():
    assert HttpPath("http://foo/bar").root == "http://"

    assert HttpPath("/foo/bar").root == "http://"
    assert HttpPath("foo//bar").root == "http://"
    assert HttpPath("foo/./bar").root == "http://"
    assert HttpPath("foo/../bar").root == "http://"
    assert HttpPath("../bar").root == "http://"

    assert HttpPath("foo", "../bar").root == "http://"


def test_anchor():
    assert HttpPath("http://foo/bar").anchor == "http://"

    assert HttpPath("foo//bar").anchor == "http://"
    assert HttpPath("foo/./bar").anchor == "http://"
    assert HttpPath("foo/../bar").anchor == "http://"
    assert HttpPath("../bar").anchor == "http://"

    assert HttpPath("foo", "../bar").anchor == "http://"


def test_parents():
    assert tuple(HttpPath("foo//bar").parents) == (
        HttpPath("foo/"),
        HttpPath("foo"),
        HttpPath(""),
    )
    assert tuple(HttpPath("foo/./bar").parents) == (
        HttpPath("foo/."),
        HttpPath("foo"),
        HttpPath(""),
    )
    assert tuple(HttpPath("foo/../bar").parents) == (
        HttpPath("foo/.."),
        HttpPath("foo"),
        HttpPath(""),
    )
    assert tuple(HttpPath("../bar").parents) == (HttpPath(".."), HttpPath(""))

    assert tuple(HttpPath("foo", "../bar").parents) == (
        HttpPath("foo/.."),
        HttpPath("foo"),
        HttpPath(""),
    )

    assert tuple(HttpPath("http://foo/bar").parents) == (
        HttpPath("http://foo"),
        HttpPath("http://"),
    )


def test_parent():
    assert HttpPath("foo//bar").parent == HttpPath("foo/")
    assert HttpPath("foo/./bar").parent == HttpPath("foo/.")
    assert HttpPath("foo/../bar").parent == HttpPath("foo/..")
    assert HttpPath("../bar").parent == HttpPath("..")
    assert HttpPath("/foo/bar").parent == HttpPath("/foo")

    assert HttpPath("foo", "../bar").parent == HttpPath("foo/..")
    assert HttpPath("/").parent == HttpPath("")
    assert HttpPath("http://").parent == HttpPath("http://")


def test_name():
    assert HttpPath("http://foo/bar/baz.py").name == "baz.py"
    assert HttpPath("foo/bar/baz.py").name == "baz.py"


def test_suffix():
    assert HttpPath("http://foo/bar.tar.gz").suffix == ".gz"
    assert HttpPath("http://foo/bar").suffix == ""

    assert HttpPath("foo/bar/baz.py").suffix == ".py"
    assert HttpPath("foo/bar").suffix == ""


def test_suffixes():
    assert HttpPath("http://foo/bar.tar.gar").suffixes == [".tar", ".gar"]
    assert HttpPath("http://foo/bar.tar.gz").suffixes == [".tar", ".gz"]
    assert HttpPath("http://foo/bar").suffixes == []

    assert HttpPath("foo/bar.tar.gar").suffixes == [".tar", ".gar"]
    assert HttpPath("foo/bar.tar.gz").suffixes == [".tar", ".gz"]
    assert HttpPath("foo/bar").suffixes == []


def test_stem():
    assert HttpPath("foo/bar.tar.gar").stem == "bar.tar"
    assert HttpPath("foo/bar.tar").stem == "bar"
    assert HttpPath("foo/bar").stem == "bar"


def test_uri():
    assert HttpPath("http://foo/bar").as_uri() == "http://foo/bar"
    assert HttpPath("http://foo/bar/baz").as_uri() == "http://foo/bar/baz"
    assert HttpPath("http://bucket/key").as_uri() == "http://bucket/key"

    # no escape
    assert HttpPath("http://buc:ket/ke@y").as_uri() == "http://buc:ket/ke@y"

from megfile.lib.compat import fspath
from megfile.s3_path import S3Path


def test_repr():
    assert repr(S3Path("s3://foo.bar")) == "S3Path('s3://foo.bar')"
    assert str(S3Path("s3://foo.bar")) == "s3://foo.bar"
    assert bytes(S3Path("s3://foo.bar")) == b"s3://foo.bar"

    assert repr(S3Path("/foo.bar")) == "S3Path('/foo.bar')"
    assert str(S3Path("/foo.bar")) == "/foo.bar"
    assert bytes(S3Path("/foo.bar")) == b"/foo.bar"


def test_fspath():
    assert fspath(S3Path("s3://bucket/key")) == "s3://bucket/key"

    assert fspath(S3Path("bucket/key")) == "s3://bucket/key"
    assert fspath(S3Path("/foo.bar")) == "s3://foo.bar"
    assert fspath(S3Path("///foo.bar")) == "s3://foo.bar"


def test_join_strs():
    assert S3Path("s3://foo", "some/path", "bar") == S3Path("s3://foo/some/path/bar")
    assert S3Path("s3://foo", "", "bar") == S3Path("s3://foo//bar")
    assert S3Path("s3://foo", "/some/path", "bar") == S3Path("s3://foo/some/path/bar")

    assert S3Path("foo", "some/path", "bar") == S3Path("foo/some/path/bar")
    assert S3Path("foo", "", "bar") == S3Path("foo//bar")
    assert S3Path("foo", "/some/path", "bar") == S3Path("foo/some/path/bar")


def test_join_paths():
    assert S3Path(S3Path("s3://foo"), S3Path("bar")) == S3Path("s3://foo/bar")

    assert S3Path(S3Path("foo"), S3Path("bar")) == S3Path("foo/bar")


def test_slashes_single_double_dots():
    assert S3Path("s3://foo//bar") == S3Path("s3://foo//bar")
    assert S3Path("s3://foo/./bar") == S3Path("s3://foo/./bar")
    assert S3Path("s3://foo/../bar") == S3Path("s3://foo/../bar")
    assert S3Path("s3://../bar") == S3Path("s3://../bar")

    assert S3Path("foo//bar") == S3Path("foo//bar")
    assert S3Path("foo/./bar") == S3Path("foo/./bar")
    assert S3Path("foo/../bar") == S3Path("foo/../bar")
    assert S3Path("../bar") == S3Path("../bar")

    assert S3Path("s3://foo", "../bar") == S3Path("s3://foo/../bar")
    assert S3Path("foo", "../bar") == S3Path("foo/../bar")


def test_operators():
    assert S3Path("s3://foo") / "bar" / "baz" == S3Path("s3://foo/bar/baz")
    assert S3Path("foo") / "bar" / "baz" == S3Path("foo/bar/baz")
    assert S3Path("s3://foo") / "bar" / "baz" in {S3Path("s3://foo/bar/baz")}


def test_parts():
    assert S3Path("s3://foo//bar").parts == ("s3://", "foo", "", "bar")
    assert S3Path("s3://foo/./bar").parts == ("s3://", "foo", ".", "bar")
    assert S3Path("s3://foo/../bar").parts == ("s3://", "foo", "..", "bar")
    assert S3Path("s3://../bar").parts == ("s3://", "..", "bar")
    assert (S3Path("s3://foo") / "../bar").parts == ("s3://", "foo", "..", "bar")
    assert S3Path("s3://foo/bar").parts == ("s3://", "foo", "bar")

    assert S3Path("s3://foo", "../bar").parts == ("s3://", "foo", "..", "bar")
    assert S3Path("s3://", "foo", "bar").parts == ("s3://", "foo", "bar")

    assert S3Path("foo//bar").parts == ("s3://", "foo", "", "bar")
    assert S3Path("foo/./bar").parts == ("s3://", "foo", ".", "bar")
    assert S3Path("foo/../bar").parts == ("s3://", "foo", "..", "bar")
    assert S3Path("../bar").parts == ("s3://", "..", "bar")
    assert S3Path("foo", "../bar").parts == ("s3://", "foo", "..", "bar")
    assert S3Path("foo/bar").parts == ("s3://", "foo", "bar")
    assert S3Path("/", "foo", "bar").parts == ("s3://", "foo", "bar")
    assert S3Path("s3+a://foo/bar").parts == ("s3+a://", "foo", "bar")


def test_drive():
    assert S3Path("s3://foo/bar").drive == ""

    assert S3Path("foo//bar").drive == ""
    assert S3Path("foo/./bar").drive == ""
    assert S3Path("foo/../bar").drive == ""
    assert S3Path("../bar").drive == ""

    assert S3Path("foo", "../bar").drive == ""


def test_root():
    assert S3Path("s3://foo/bar").root == "s3://"

    assert S3Path("/foo/bar").root == "s3://"
    assert S3Path("foo//bar").root == "s3://"
    assert S3Path("foo/./bar").root == "s3://"
    assert S3Path("foo/../bar").root == "s3://"
    assert S3Path("../bar").root == "s3://"

    assert S3Path("foo", "../bar").root == "s3://"


def test_anchor():
    assert S3Path("s3://foo/bar").anchor == "s3://"

    assert S3Path("foo//bar").anchor == "s3://"
    assert S3Path("foo/./bar").anchor == "s3://"
    assert S3Path("foo/../bar").anchor == "s3://"
    assert S3Path("../bar").anchor == "s3://"

    assert S3Path("foo", "../bar").anchor == "s3://"


def test_parents():
    assert tuple(S3Path("foo//bar").parents) == (
        S3Path("foo/"),
        S3Path("foo"),
        S3Path(""),
    )
    assert tuple(S3Path("foo/./bar").parents) == (
        S3Path("foo/."),
        S3Path("foo"),
        S3Path(""),
    )
    assert tuple(S3Path("foo/../bar").parents) == (
        S3Path("foo/.."),
        S3Path("foo"),
        S3Path(""),
    )
    assert tuple(S3Path("../bar").parents) == (S3Path(".."), S3Path(""))

    assert tuple(S3Path("foo", "../bar").parents) == (
        S3Path("foo/.."),
        S3Path("foo"),
        S3Path(""),
    )

    assert tuple(S3Path("s3://foo/bar").parents) == (
        S3Path("s3://foo"),
        S3Path("s3://"),
    )


def test_parent():
    assert S3Path("foo//bar").parent == S3Path("foo/")
    assert S3Path("foo/./bar").parent == S3Path("foo/.")
    assert S3Path("foo/../bar").parent == S3Path("foo/..")
    assert S3Path("../bar").parent == S3Path("..")
    assert S3Path("/foo/bar").parent == S3Path("/foo")
    assert S3Path("/foo").parent == S3Path("/")
    assert S3Path("/").parent == S3Path("/")
    assert S3Path("foo").parent == S3Path("")

    assert S3Path("foo", "../bar").parent == "s3://foo/.."
    assert S3Path("/").parent == "s3://"
    assert S3Path("s3://").parent == "s3://"


def test_name():
    assert S3Path("s3://foo/bar/baz.py").name == "baz.py"
    assert S3Path("foo/bar/baz.py").name == "baz.py"


def test_suffix():
    assert S3Path("s3://foo/bar.tar.gz").suffix == ".gz"
    assert S3Path("s3://foo/bar").suffix == ""

    assert S3Path("foo/bar/baz.py").suffix == ".py"
    assert S3Path("foo/bar").suffix == ""


def test_suffixes():
    assert S3Path("s3://foo/bar.tar.gar").suffixes == [".tar", ".gar"]
    assert S3Path("s3://foo/bar.tar.gz").suffixes == [".tar", ".gz"]
    assert S3Path("s3://foo/bar").suffixes == []

    assert S3Path("foo/bar.tar.gar").suffixes == [".tar", ".gar"]
    assert S3Path("foo/bar.tar.gz").suffixes == [".tar", ".gz"]
    assert S3Path("foo/bar").suffixes == []


def test_stem():
    assert S3Path("foo/bar.tar.gar").stem == "bar.tar"
    assert S3Path("foo/bar.tar").stem == "bar"
    assert S3Path("foo/bar").stem == "bar"


def test_uri():
    assert S3Path("s3://foo/bar").as_uri() == "s3://foo/bar"
    assert S3Path("s3://foo/bar/baz").as_uri() == "s3://foo/bar/baz"
    assert S3Path("s3://bucket/key").as_uri() == "s3://bucket/key"

    # no escape
    assert S3Path("s3://buc:ket/ke@y").as_uri() == "s3://buc:ket/ke@y"

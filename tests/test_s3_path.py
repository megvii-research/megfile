import pytest

from megfile.s3_path import S3Path


def test_absolute():
    assert S3Path("s3://foo/bar").is_absolute()
    assert S3Path("foo/bar").is_absolute()


def test_reserved():
    assert not S3Path("s3://foo/bar").is_reserved()
    assert not S3Path("foo/bar").is_reserved()


def test_joinpath():
    assert S3Path("s3://foo").joinpath("bar") == S3Path("s3://foo/bar")
    assert S3Path("s3://foo").joinpath(S3Path("bar")) == S3Path("s3://foo/bar")
    assert S3Path("s3://foo").joinpath("bar", "baz") == S3Path("s3://foo/bar/baz")

    assert S3Path("foo").joinpath("bar") == S3Path("foo/bar")
    assert S3Path("foo").joinpath(S3Path("bar")) == S3Path("foo/bar")
    assert S3Path("foo").joinpath("bar", "baz") == S3Path("foo/bar/baz")


def test_match():
    assert S3Path("a/b.py").match("*.py")
    assert S3Path("s3://a/b/c.py").match("b/*.py")
    assert not S3Path("s3://a/b/c.py").match("s3://a/*.py")
    assert S3Path("s3://a.py").match("s3://*.py")
    assert S3Path("a/b.py").match("s3://a/b.py")
    assert not S3Path("a/b.py").match("s4://*.py")
    assert not S3Path("a/b.py").match("s3://*.py")
    assert not S3Path("a/b.py").match("*.Py")


def test_relative_to():
    path = S3Path("s3://foo/bar")
    assert path.relative_to("s3://") == S3Path("foo/bar")
    assert path.relative_to("s3://foo") == S3Path("bar")
    assert path.relative_to("foo") == S3Path("bar")
    with pytest.raises(ValueError):
        path.relative_to("s3://baz")


def test_relative_to_relative():
    path = S3Path("foo/bar/baz")
    assert path.relative_to("foo/bar") == S3Path("baz")
    assert path.relative_to("foo") == S3Path("bar/baz")
    with pytest.raises(ValueError):
        path.relative_to("baz")


def test_with_name():
    path = S3Path("s3://foo/bar.tar.gz")
    assert path.with_name("baz.py") == S3Path("s3://foo/baz.py")
    path = S3Path("s3://")

    # with pytest.raises(ValueError):
    #     path.with_name('baz.py')

    path = S3Path("foo/bar.tar.gz")
    assert path.with_name("baz.py") == S3Path("foo/baz.py")


def test_with_suffix():
    path = S3Path("s3://foo/bar.tar.gz")
    assert path.with_suffix(".bz2") == S3Path("s3://foo/bar.tar.bz2")
    path = S3Path("baz")
    assert path.with_suffix(".txt") == S3Path("baz.txt")
    path = S3Path("baz.txt")
    assert path.with_suffix("") == S3Path("baz")


def test_utime():
    with pytest.raises(NotImplementedError):
        S3Path("s3://foo/bar.tar.gz").utime(0, 0)

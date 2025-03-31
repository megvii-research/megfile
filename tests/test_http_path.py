import io
import logging
from copy import deepcopy

import pytest
import requests
import requests_mock  # noqa: F401

from megfile.http_path import HttpPath, Response, get_http_session, is_http


def test_absolute():
    assert HttpPath("http://foo/bar").is_absolute()
    assert HttpPath("foo/bar").is_absolute()


def test_reserved():
    assert not HttpPath("http://foo/bar").is_reserved()
    assert not HttpPath("foo/bar").is_reserved()


def test_joinpath():
    assert HttpPath("http://foo").joinpath("bar") == HttpPath("http://foo/bar")
    assert HttpPath("http://foo").joinpath(HttpPath("bar")) == HttpPath(
        "http://foo/bar"
    )
    assert HttpPath("http://foo").joinpath("bar", "baz") == HttpPath(
        "http://foo/bar/baz"
    )

    assert HttpPath("foo").joinpath("bar") == HttpPath("foo/bar")
    assert HttpPath("foo").joinpath(HttpPath("bar")) == HttpPath("foo/bar")
    assert HttpPath("foo").joinpath("bar", "baz") == HttpPath("foo/bar/baz")


def test_match():
    assert HttpPath("a/b.py").match("*.py")
    assert HttpPath("http://a/b/c.py").match("b/*.py")
    assert not HttpPath("http://a/b/c.py").match("http://a/*.py")
    assert HttpPath("http://a.py").match("http://*.py")
    assert HttpPath("a/b.py").match("http://a/b.py")
    assert not HttpPath("a/b.py").match("https://a/b.py")
    assert not HttpPath("a/b.py").match("http://*.py")
    assert not HttpPath("a/b.py").match("*.Py")


def test_relative_to():
    path = HttpPath("http://foo/bar")
    assert path.relative_to("http://") == HttpPath("foo/bar")
    assert path.relative_to("http://foo") == HttpPath("bar")
    with pytest.raises(ValueError):
        path.relative_to("http://baz")


def test_relative_to_relative():
    path = HttpPath("foo/bar/baz")
    assert path.relative_to("foo/bar") == HttpPath("baz")
    assert path.relative_to("foo") == HttpPath("bar/baz")
    with pytest.raises(ValueError):
        path.relative_to("baz")


def test_with_name():
    path = HttpPath("http://foo/bar.tar.gz")
    assert path.with_name("baz.py") == HttpPath("http://foo/baz.py")
    path = HttpPath("http://")

    # with pytest.raises(ValueError):
    #     path.with_name('baz.py')

    path = HttpPath("foo/bar.tar.gz")
    assert path.with_name("baz.py") == HttpPath("foo/baz.py")


def test_with_suffix():
    path = HttpPath("http://foo/bar.tar.gz")
    assert path.with_suffix(".bz2") == HttpPath("http://foo/bar.tar.bz2")
    path = HttpPath("baz")
    assert path.with_suffix(".txt") == HttpPath("baz.txt")
    path = HttpPath("baz.txt")
    assert path.with_suffix("") == HttpPath("baz")


def test_http_retry(requests_mock, mocker):
    max_retries = 2
    mocker.patch("megfile.http_path.HTTP_MAX_RETRY_TIMES", max_retries)
    requests_mock.post("http://foo", status_code=500)
    session = get_http_session()
    history_index = 0

    with pytest.raises(requests.exceptions.HTTPError):
        session.post("http://foo", files={"foo": "bar"})
    for _ in range(max_retries):
        assert b'name="foo"' in requests_mock.request_history[history_index].body
        assert b"bar" in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post("http://foo", files={"foo": io.BytesIO(b"bar")})
    for _ in range(max_retries):
        assert b"bar" in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post("http://foo", files={"foo": io.BytesIO(b"bar")})
    for _ in range(max_retries):
        assert b"bar" in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post("http://foo", files={"foo": ("filename", io.BytesIO(b"bar"))})
    for _ in range(max_retries):
        assert b'name="filename"' in requests_mock.request_history[history_index].body
        assert b"bar" in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post(
            "http://foo",
            files={"foo": ("filename", io.BytesIO(b"bar"), "application/vnd.ms-excel")},
        )
    for _ in range(max_retries):
        assert b'name="filename"' in requests_mock.request_history[history_index].body
        assert b"bar" in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post(
            "http://foo",
            files={"foo": ("filename", b"bar", "application/vnd.ms-excel")},
        )
    for _ in range(max_retries):
        assert b'name="filename"' in requests_mock.request_history[history_index].body
        assert b"bar" in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post(
            "http://foo",
            files={
                "foo": (
                    "filename",
                    io.BytesIO(b"bar"),
                    "application/vnd.ms-excel",
                    {"Expires": "0"},
                )
            },
        )
    for _ in range(max_retries):
        assert b'name="filename"' in requests_mock.request_history[history_index].body
        assert b"bar" in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post("http://foo", data=io.BytesIO(b"bar"))
    for _ in range(max_retries):
        assert (
            b"bar" == deepcopy(requests_mock.request_history[history_index].body).read()
        )
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post("http://foo", data=(s for s in ["a"]))
    assert history_index + 1 == len(requests_mock.request_history)


def test_http_retry_fileobj_without_seek(requests_mock, mocker, fs):
    max_retries = 2
    mocker.patch("megfile.http_path.HTTP_MAX_RETRY_TIMES", max_retries)
    requests_mock.post("http://foo", status_code=500)
    session = get_http_session()
    history_index = 0

    with open("foo.txt", "wb") as f:
        f.write(b"bar")

    class FakeFile:
        def __init__(self):
            self.name = "foo.txt"

        def read(self, size=-1, **kwargs):
            return b"bar"

    with pytest.raises(requests.exceptions.HTTPError):
        session.post(
            "http://foo",
            files={"foo": ("filename", FakeFile(), "application/vnd.ms-excel")},
        )
    for _ in range(max_retries):
        assert b'name="filename"' in requests_mock.request_history[history_index].body
        assert b"bar" in requests_mock.request_history[history_index].body
        history_index += 1


def test_http_retry_fileobj_without_name(requests_mock, mocker, fs, caplog):
    with caplog.at_level(logging.INFO, logger="megfile"):
        max_retries = 2
        mocker.patch("megfile.http_path.HTTP_MAX_RETRY_TIMES", max_retries)
        requests_mock.post("http://foo", status_code=500)
        session = get_http_session()

        class FakeFileWithoutName:
            def __init__(self):
                pass

            def read(self, size=-1, **kwargs):
                return b"bar"

        with pytest.raises(requests.exceptions.HTTPError):
            session.post(
                "http://foo",
                files={
                    "foo": (
                        "filename",
                        FakeFileWithoutName(),
                        "application/vnd.ms-excel",
                    )
                },
            )
        assert len(requests_mock.request_history) == 1
        assert (
            "Can not retry http request, because the file object "
            'is not seekable and not support "name"'
        ) in caplog.text


def test_response():
    fp = io.BytesIO(b"test")
    fp.name = "foo"
    real_read = fp.read
    fp.read = lambda size, **kwargs: real_read(size)

    resp = Response(fp)
    assert resp.mode == "rb"
    assert resp.name == "foo"
    assert resp.read(0) == b""
    assert resp.read(1) == b"t"
    assert resp.read(-1) == b"est"
    assert resp.tell() == 4

    fp = io.BytesIO(b"1\n2\n3\n4\n")
    fp.name = "foo"
    real_read = fp.read
    fp.read = lambda size, **kwargs: real_read(size)

    resp = Response(fp)
    assert resp.name == "foo"
    assert resp.readlines() == [b"1\n", b"2\n", b"3\n", b"4\n"]

    fp = io.BytesIO(b"1\n2\n3\n4\n")
    fp.name = "foo"
    real_read = fp.read
    fp.read = lambda size, **kwargs: real_read(size)

    resp = Response(fp)
    assert resp.name == "foo"
    lines = []
    for i in range(4):
        line = resp.readline(-1)
        assert resp.tell() == (i + 1) * 2
        if not line:
            break
        lines.append(line)
    assert lines == [b"1\n", b"2\n", b"3\n", b"4\n"]

    fp = io.BytesIO(b"11\n2\n3\n4\n")
    fp.name = "foo"
    real_read = fp.read
    fp.read = lambda size, **kwargs: real_read(size)

    resp = Response(fp)
    resp._block_size = 4
    assert resp.name == "foo"
    assert resp.readline(0) == b""
    assert resp.readline(1) == b"1"
    assert resp.readline(2) == b"1\n"
    assert resp.readline(1) == b"2"
    assert resp.readline(1) == b"\n"

    fp = io.BytesIO(b"123")
    fp.name = "foo"
    real_read = fp.read
    fp.read = lambda size, **kwargs: real_read(size)

    resp = Response(fp)
    resp._block_size = 2
    assert resp.readline(1) == b"1"
    assert resp.readline() == b"23"
    assert resp.readline() == b""


def test_is_http():
    assert is_http("http://foo") is True
    assert is_http("s3://foo") is False


def test_open_with_headers(requests_mock):
    requests_mock.get(
        "http://test", text="test", status_code=200, headers={"Content-Length": "4"}
    )
    headers = {"A": "a", "B": "b"}

    path = HttpPath("http://test")
    path.request_kwargs = {"headers": headers}
    with path.open("rb") as f:
        assert f.read() == b"test"

    for key, value in headers.items():
        assert requests_mock.request_history[0].headers[key] == value


def test_https_path():
    assert HttpPath("https://foo").protocol == "https"

import pickle
import time
from functools import cached_property
from io import BufferedReader, BytesIO
from typing import Optional

import pytest
import requests

from megfile.errors import HttpFileNotFoundError, HttpPermissionError, UnknownError
from megfile.http import (
    get_http_session,
    http_exists,
    http_getmtime,
    http_getsize,
    http_open,
    http_stat,
    is_http,
)


def test_is_http():
    assert is_http("http://www.baidu.com")
    assert is_http("https://www.baidu.com")
    assert not is_http("no-http://www.baidu.com")


class PatchedBytesIO(BytesIO):
    def read(self, size: Optional[int] = None, **kwargs) -> bytes:
        return super().read(size)


class FakeResponse:
    status_code = 0

    @cached_property
    def raw(self):
        return PatchedBytesIO(b"test")

    @property
    def headers(self):
        return {
            "Content-Length": "4",
            "Content-Type": "test/test",
            "Last-Modified": "Wed, 24 Nov 2021 07:18:41 GMT",
        }

    def raise_for_status(self):
        if self.status_code // 100 == 2:
            return
        error = requests.exceptions.HTTPError()
        error.response = self
        raise error

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()


def test_http_open(mocker):
    with pytest.raises(ValueError) as error:
        http_open("http://test", "w")

    requests_get_func = mocker.patch("requests.Session.get")

    class FakeResponse200(FakeResponse):
        status_code = 200

    requests_get_func.return_value = FakeResponse200()
    with http_open("http://test", "rb") as http_reader:
        assert http_reader.read() == b"test"
        assert http_reader.name == "http://test"

    class FakeResponse404(FakeResponse):
        status_code = 404

    requests_get_func.return_value = FakeResponse404()
    with pytest.raises(HttpFileNotFoundError) as error:
        http_open("http://test", "rb")

    class FakeResponse401(FakeResponse):
        status_code = 401

    requests_get_func.return_value = FakeResponse401()
    with pytest.raises(HttpPermissionError) as error:
        http_open("http://test", "rb")

    class FakeResponse502(FakeResponse):
        status_code = 502

    requests_get_func.return_value = FakeResponse502()
    with pytest.raises(UnknownError) as error:
        http_open("http://test", "rb")

    def fake_get(*args, **kwargs):
        raise requests.exceptions.ReadTimeout("test")

    requests_get_func.side_effect = fake_get
    with pytest.raises(UnknownError) as error:
        http_open("http://test", "rb")

    assert str(error.value) == (
        "Unknown error encountered: 'http://test', "
        "error: requests.exceptions.ReadTimeout('test')"
    )


def test_http_open_pickle(mocker):
    class PickleResponse(FakeResponse):
        status_code = 200

        @cached_property
        def raw(self):
            return BytesIO(pickle.dumps(b"test"))

        @cached_property
        def content(self):
            return pickle.dumps(b"test")

        @cached_property
        def cookies(self):
            return {}

        @property
        def headers(self):
            return {
                "Content-Length": str(len(pickle.dumps(b"test"))),
                "Content-Type": "test/test",
                "Last-Modified": "Wed, 24 Nov 2021 07:18:41 GMT",
                "Accept-Ranges": "bytes",
            }

    requests_get_func = mocker.patch("requests.Session.get")
    mocker.patch("requests.get", return_value=PickleResponse())

    requests_get_func.return_value = PickleResponse()
    with http_open("http://test", "rb", block_size=1) as http_reader:
        assert isinstance(http_reader, BufferedReader)


def test_http_open_range(mocker):
    requests_get_func = mocker.patch("requests.Session.get")
    requests_get_mocker = mocker.patch("requests.get")

    class FakeResponse200Range(FakeResponse):
        status_code = 200

        def __init__(self, headers=None):
            self._content = b"test"
            if headers and headers.get("Range"):
                start, end = list(
                    map(int, headers["Range"].replace("bytes=", "").split("-"))
                )
                self._content = self._content[start : end + 1]

        @property
        def raw(self):
            return BytesIO(self._content)

        @property
        def headers(self):
            return {
                "Content-Length": str(len(self._content)),
                "Content-Type": "test/test",
                "Last-Modified": "Wed, 24 Nov 2021 07:18:41 GMT",
                "Accept-Ranges": "bytes",
            }

        @property
        def content(self):
            return self._content

        @property
        def cookies(self):
            return {}

    def fake_get(*args, headers=None, **kwargs):
        return FakeResponse200Range(headers)

    requests_get_func.return_value = FakeResponse200Range()
    requests_get_mocker.side_effect = fake_get
    with http_open("http://test", "rb", block_size=1) as f:
        f.read() == b"test"


def test_http_getsize(mocker):
    requests_get_func = mocker.patch("requests.Session.get")

    class FakeResponse200(FakeResponse):
        status_code = 200

    requests_get_func.return_value = FakeResponse200()
    assert http_getsize("http://test") == 4


def test_http_getmtime(mocker):
    requests_get_func = mocker.patch("requests.Session.get")

    class FakeResponse200(FakeResponse):
        status_code = 200

    requests_get_func.return_value = FakeResponse200()
    assert http_getmtime("http://test") == time.mktime(
        time.strptime("Wed, 24 Nov 2021 07:18:41 GMT", "%a, %d %b %Y %H:%M:%S %Z")
    )


def test_http_getstat(mocker):
    requests_get_func = mocker.patch("requests.Session.get")

    class FakeResponse200(FakeResponse):
        status_code = 200

    requests_get_func.return_value = FakeResponse200()
    stat = http_stat("http://test")
    assert stat.mtime == time.mktime(
        time.strptime("Wed, 24 Nov 2021 07:18:41 GMT", "%a, %d %b %Y %H:%M:%S %Z")
    )
    assert stat.size == 4
    assert stat.st_ino == 0

    class FakeResponse404(FakeResponse):
        status_code = 404

    requests_get_func.return_value = FakeResponse404()
    with pytest.raises(HttpFileNotFoundError):
        http_stat("http://test")

    class FakeResponseWithoutStat(FakeResponse):
        status_code = 200

        @property
        def headers(self):
            return {
                "Content-Type": "test/test",
            }

    requests_get_func.return_value = FakeResponseWithoutStat()
    stat = http_stat("http://test")
    assert stat.size == 0
    assert stat.st_mtime == 0.0


def test_get_http_session(mocker):
    requests_request_func = mocker.patch("requests.Session.request")
    mocker.patch("megfile.http_path.HTTP_MAX_RETRY_TIMES", 1)

    class FakeResponse502(FakeResponse):
        status_code = 502

    requests_request_func.return_value = FakeResponse502()
    session = get_http_session()
    with pytest.raises(requests.exceptions.HTTPError):
        session.request("get", "http://test")

    class FakeResponse200(FakeResponse):
        status_code = 200

    requests_request_func.return_value = FakeResponse200()
    session = get_http_session()
    response = session.request("get", "http://test")
    assert response.status_code == 200


def test_http_exists(mocker):
    class FakeResponse200(FakeResponse):
        status_code = 200

    mocker.patch("requests.Session.get", return_value=FakeResponse200())
    assert http_exists("http://test")

    class FakeResponse404(FakeResponse):
        status_code = 404

    mocker.patch("requests.Session.get", return_value=FakeResponse404())
    assert http_exists("http://test") is False

    mocker.patch(
        "requests.Session.get", side_effect=requests.exceptions.ConnectionError()
    )
    assert http_exists("http://test") is False

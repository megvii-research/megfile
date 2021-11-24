import time
from io import BytesIO

import pytest
import requests

from megfile.errors import HttpFileNotFoundError, HttpPermissionError, UnknownError
from megfile.http import http_getmtime, http_getsize, http_open, is_http


def test_is_http():
    assert is_http("http://www.baidu.com")
    assert is_http("https://www.baidu.com")
    assert not is_http("no-http://www.baidu.com")


class FakeResponse:
    status_code = 0

    @property
    def raw(self):
        return BytesIO(b'test')

    @property
    def headers(self):
        return {
            "Content-Length": '999',
            'Content-Type': 'test/test',
            "Last-Modified": "Wed, 24 Nov 2021 07:18:41 GMT"
        }

    def raise_for_status(self):
        if self.status_code // 100 == 2:
            return
        error = requests.exceptions.HTTPError()
        error.response = self
        raise error


def test_http_open(mocker):

    with pytest.raises(ValueError) as error:
        http_open('http://test', 'w')

    requests_get_func = mocker.patch('megfile.http.requests.get')

    class FakeResponse200(FakeResponse):
        status_code = 200

    requests_get_func.return_value = FakeResponse200()
    assert http_open('http://test', 'rb').read() == b'test'

    class FakeResponse404(FakeResponse):
        status_code = 404

    requests_get_func.return_value = FakeResponse404()
    with pytest.raises(HttpFileNotFoundError) as error:
        http_open('http://test', 'rb')

    class FakeResponse401(FakeResponse):
        status_code = 401

    requests_get_func.return_value = FakeResponse401()
    with pytest.raises(HttpPermissionError) as error:
        http_open('http://test', 'rb')

    class FakeResponse502(FakeResponse):
        status_code = 502

    requests_get_func.return_value = FakeResponse502()
    with pytest.raises(UnknownError) as error:
        http_open('http://test', 'rb')

    def fake_get(*args, **kwargs):
        raise requests.exceptions.ReadTimeout('test')

    requests_get_func.side_effect = fake_get
    with pytest.raises(UnknownError) as error:
        http_open('http://test', 'rb')

    assert str(
        error.value
    ) == 'Unknown error encountered: \'http://test\', error: requests.exceptions.ReadTimeout(\'test\')'


def test_http_getsize(mocker):

    requests_get_func = mocker.patch('megfile.http.requests.get')

    class FakeResponse200(FakeResponse):
        status_code = 200

    requests_get_func.return_value = FakeResponse200()
    assert http_getsize('http://test') == 999


def test_http_getmtime(mocker):

    requests_get_func = mocker.patch('megfile.http.requests.get')

    class FakeResponse200(FakeResponse):
        status_code = 200

    requests_get_func.return_value = FakeResponse200()
    assert http_getmtime('http://test') == 1637709521.0

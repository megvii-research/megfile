import io
from copy import deepcopy
from typing import Optional

import pytest
import requests
import requests_mock  # noqa

from megfile.http_path import HttpPath, Response, get_http_session, is_http
from megfile.lib.compat import fspath


def test_repr():
    assert repr(HttpPath('http://foo.bar')) == "HttpPath('http://foo.bar')"
    assert str(HttpPath('http://foo.bar')) == 'http://foo.bar'
    assert bytes(HttpPath('http://foo.bar')) == b'http://foo.bar'

    assert repr(HttpPath('/foo.bar')) == "HttpPath('/foo.bar')"
    assert str(HttpPath('/foo.bar')) == '/foo.bar'
    assert bytes(HttpPath('/foo.bar')) == b'/foo.bar'


def test_fspath():
    assert fspath(HttpPath('http://bucket/key')) == 'http://bucket/key'

    assert fspath(HttpPath('bucket/key')) == 'http://bucket/key'
    assert fspath(HttpPath('/foo.bar')) == 'http://foo.bar'
    assert fspath(HttpPath('///foo.bar')) == 'http://foo.bar'


def test_join_strs():
    assert HttpPath('http://foo', 'some/path',
                    'bar') == HttpPath('http://foo/some/path/bar')
    assert HttpPath('http://foo', '', 'bar') == HttpPath('http://foo//bar')
    assert HttpPath('http://foo', '/some/path',
                    'bar') == HttpPath('http://foo/some/path/bar')

    assert HttpPath('foo', 'some/path', 'bar') == HttpPath('foo/some/path/bar')
    assert HttpPath('foo', '', 'bar') == HttpPath('foo//bar')
    assert HttpPath('foo', '/some/path', 'bar') == HttpPath('foo/some/path/bar')


def test_join_paths():
    assert HttpPath(HttpPath('http://foo'),
                    HttpPath('bar')) == HttpPath('http://foo/bar')

    assert HttpPath(HttpPath('foo'), HttpPath('bar')) == HttpPath('foo/bar')


def test_slashes_single_double_dots():
    assert HttpPath('http://foo//bar') == HttpPath('http://foo//bar')
    assert HttpPath('http://foo/./bar') == HttpPath('http://foo/./bar')
    assert HttpPath('http://foo/../bar') == HttpPath('http://foo/../bar')
    assert HttpPath('http://../bar') == HttpPath('http://../bar')

    assert HttpPath('foo//bar') == HttpPath('foo//bar')
    assert HttpPath('foo/./bar') == HttpPath('foo/./bar')
    assert HttpPath('foo/../bar') == HttpPath('foo/../bar')
    assert HttpPath('../bar') == HttpPath('../bar')

    assert HttpPath('http://foo', '../bar') == HttpPath('http://foo/../bar')
    assert HttpPath('foo', '../bar') == HttpPath('foo/../bar')


def test_operators():
    assert HttpPath('http://foo') / 'bar' / 'baz' == HttpPath(
        'http://foo/bar/baz')
    assert HttpPath('foo') / 'bar' / 'baz' == HttpPath('foo/bar/baz')
    assert HttpPath('http://foo') / 'bar' / 'baz' in {
        HttpPath('http://foo/bar/baz')
    }


def test_parts():
    assert HttpPath('http://foo//bar').parts == ('http://', 'foo', '', 'bar')
    assert HttpPath('http://foo/./bar').parts == ('http://', 'foo', '.', 'bar')
    assert HttpPath('http://foo/../bar').parts == (
        'http://', 'foo', '..', 'bar')
    assert HttpPath('http://../bar').parts == ('http://', '..', 'bar')
    assert (HttpPath('http://foo') /
            '../bar').parts == ('http://', 'foo', '..', 'bar')
    assert HttpPath('http://foo/bar').parts == ('http://', 'foo', 'bar')

    assert HttpPath('http://foo',
                    '../bar').parts == ('http://', 'foo', '..', 'bar')
    assert HttpPath('http://', 'foo', 'bar').parts == ('http://', 'foo', 'bar')

    assert HttpPath('foo//bar').parts == ('http://', 'foo', '', 'bar')
    assert HttpPath('foo/./bar').parts == ('http://', 'foo', '.', 'bar')
    assert HttpPath('foo/../bar').parts == ('http://', 'foo', '..', 'bar')
    assert HttpPath('../bar').parts == ('http://', '..', 'bar')
    assert HttpPath('foo', '../bar').parts == ('http://', 'foo', '..', 'bar')
    assert HttpPath('foo/bar').parts == ('http://', 'foo', 'bar')
    assert HttpPath('/', 'foo', 'bar').parts == ('http://', 'foo', 'bar')


def test_drive():
    assert HttpPath('http://foo/bar').drive == ''

    assert HttpPath('foo//bar').drive == ''
    assert HttpPath('foo/./bar').drive == ''
    assert HttpPath('foo/../bar').drive == ''
    assert HttpPath('../bar').drive == ''

    assert HttpPath('foo', '../bar').drive == ''


def test_root():
    assert HttpPath('http://foo/bar').root == 'http://'

    assert HttpPath('/foo/bar').root == 'http://'
    assert HttpPath('foo//bar').root == 'http://'
    assert HttpPath('foo/./bar').root == 'http://'
    assert HttpPath('foo/../bar').root == 'http://'
    assert HttpPath('../bar').root == 'http://'

    assert HttpPath('foo', '../bar').root == 'http://'


def test_anchor():
    assert HttpPath('http://foo/bar').anchor == 'http://'

    assert HttpPath('foo//bar').anchor == 'http://'
    assert HttpPath('foo/./bar').anchor == 'http://'
    assert HttpPath('foo/../bar').anchor == 'http://'
    assert HttpPath('../bar').anchor == 'http://'

    assert HttpPath('foo', '../bar').anchor == 'http://'


def test_parents():
    assert tuple(HttpPath('foo//bar').parents) == (
        HttpPath('foo/'), HttpPath('foo'), HttpPath(''))
    assert tuple(HttpPath('foo/./bar').parents) == (
        HttpPath('foo/.'), HttpPath('foo'), HttpPath(''))
    assert tuple(HttpPath('foo/../bar').parents) == (
        HttpPath('foo/..'), HttpPath('foo'), HttpPath(''))
    assert tuple(HttpPath('../bar').parents) == (HttpPath('..'), HttpPath(''))

    assert tuple(HttpPath('foo', '../bar').parents) == (
        HttpPath('foo/..'), HttpPath('foo'), HttpPath(''))

    assert tuple(HttpPath('http://foo/bar').parents) == (
        HttpPath('http://foo'), HttpPath('http://'))


def test_parent():
    assert HttpPath('foo//bar').parent == HttpPath('foo/')
    assert HttpPath('foo/./bar').parent == HttpPath('foo/.')
    assert HttpPath('foo/../bar').parent == HttpPath('foo/..')
    assert HttpPath('../bar').parent == HttpPath('..')
    assert HttpPath('/foo/bar').parent == HttpPath('/foo')

    assert HttpPath('foo', '../bar').parent == HttpPath('foo/..')
    assert HttpPath('/').parent == HttpPath('')
    assert HttpPath('http://').parent == HttpPath('http://')


def test_name():
    assert HttpPath('http://foo/bar/baz.py').name == 'baz.py'
    assert HttpPath('foo/bar/baz.py').name == 'baz.py'


def test_suffix():
    assert HttpPath('http://foo/bar.tar.gz').suffix == '.gz'
    assert HttpPath('http://foo/bar').suffix == ''

    assert HttpPath('foo/bar/baz.py').suffix == '.py'
    assert HttpPath('foo/bar').suffix == ''


def test_suffixes():
    assert HttpPath('http://foo/bar.tar.gar').suffixes == ['.tar', '.gar']
    assert HttpPath('http://foo/bar.tar.gz').suffixes == ['.tar', '.gz']
    assert HttpPath('http://foo/bar').suffixes == []

    assert HttpPath('foo/bar.tar.gar').suffixes == ['.tar', '.gar']
    assert HttpPath('foo/bar.tar.gz').suffixes == ['.tar', '.gz']
    assert HttpPath('foo/bar').suffixes == []


def test_stem():
    assert HttpPath('foo/bar.tar.gar').stem == 'bar.tar'
    assert HttpPath('foo/bar.tar').stem == 'bar'
    assert HttpPath('foo/bar').stem == 'bar'


def test_uri():
    assert HttpPath('http://foo/bar').as_uri() == 'http://foo/bar'
    assert HttpPath('http://foo/bar/baz').as_uri() == 'http://foo/bar/baz'
    assert HttpPath('http://bucket/key').as_uri() == 'http://bucket/key'

    # no escape
    assert HttpPath('http://buc:ket/ke@y').as_uri() == 'http://buc:ket/ke@y'


def test_absolute():
    assert HttpPath('http://foo/bar').is_absolute()
    assert HttpPath('foo/bar').is_absolute()


def test_reserved():
    assert not HttpPath('http://foo/bar').is_reserved()
    assert not HttpPath('foo/bar').is_reserved()


def test_joinpath():
    assert HttpPath('http://foo').joinpath('bar') == HttpPath('http://foo/bar')
    assert HttpPath('http://foo').joinpath(
        HttpPath('bar')) == HttpPath('http://foo/bar')
    assert HttpPath('http://foo').joinpath(
        'bar', 'baz') == HttpPath('http://foo/bar/baz')

    assert HttpPath('foo').joinpath('bar') == HttpPath('foo/bar')
    assert HttpPath('foo').joinpath(HttpPath('bar')) == HttpPath('foo/bar')
    assert HttpPath('foo').joinpath('bar', 'baz') == HttpPath('foo/bar/baz')


def test_match():
    assert HttpPath('a/b.py').match('*.py')
    assert HttpPath('http://a/b/c.py').match('b/*.py')
    assert not HttpPath('http://a/b/c.py').match('http://a/*.py')
    assert HttpPath('http://a.py').match('http://*.py')
    assert HttpPath('a/b.py').match('http://a/b.py')
    assert not HttpPath('a/b.py').match('https://a/b.py')
    assert not HttpPath('a/b.py').match('http://*.py')
    assert not HttpPath('a/b.py').match('*.Py')


def test_relative_to():
    path = HttpPath('http://foo/bar')
    assert path.relative_to('http://') == HttpPath('foo/bar')
    assert path.relative_to('http://foo') == HttpPath('bar')
    with pytest.raises(ValueError):
        path.relative_to('http://baz')


def test_relative_to_relative():
    path = HttpPath('foo/bar/baz')
    assert path.relative_to('foo/bar') == HttpPath('baz')
    assert path.relative_to('foo') == HttpPath('bar/baz')
    with pytest.raises(ValueError):
        path.relative_to('baz')


def test_with_name():
    path = HttpPath('http://foo/bar.tar.gz')
    assert path.with_name('baz.py') == HttpPath('http://foo/baz.py')
    path = HttpPath('http://')

    # with pytest.raises(ValueError):
    #     path.with_name('baz.py')

    path = HttpPath('foo/bar.tar.gz')
    assert path.with_name('baz.py') == HttpPath('foo/baz.py')


def test_with_suffix():
    path = HttpPath('http://foo/bar.tar.gz')
    assert path.with_suffix('.bz2') == HttpPath('http://foo/bar.tar.bz2')
    path = HttpPath('baz')
    assert path.with_suffix('.txt') == HttpPath('baz.txt')
    path = HttpPath('baz.txt')
    assert path.with_suffix('') == HttpPath('baz')


def test_http_retry(requests_mock, mocker):
    max_retries = 2
    mocker.patch('megfile.http_path.max_retries', max_retries)
    requests_mock.post('http://foo', status_code=500)
    session = get_http_session()
    history_index = 0

    with pytest.raises(requests.exceptions.HTTPError):
        session.post('http://foo', files={'foo': 'bar'})
    for _ in range(max_retries):
        assert b'name="foo"' in requests_mock.request_history[
            history_index].body
        assert b'bar' in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post('http://foo', files={'foo': io.BytesIO(b'bar')})
    for _ in range(max_retries):
        assert b'bar' in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post('http://foo', files={'foo': io.BytesIO(b'bar')})
    for _ in range(max_retries):
        assert b'bar' in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post(
            'http://foo', files={'foo': ('filename', io.BytesIO(b'bar'))})
    for _ in range(max_retries):
        assert b'name="filename"' in requests_mock.request_history[
            history_index].body
        assert b'bar' in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post(
            'http://foo',
            files={
                'foo':
                    (
                        'filename', io.BytesIO(b'bar'),
                        'application/vnd.ms-excel')
            })
    for _ in range(max_retries):
        assert b'name="filename"' in requests_mock.request_history[
            history_index].body
        assert b'bar' in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post(
            'http://foo',
            files={
                'foo':
                    (
                        'filename', io.BytesIO(b'bar'),
                        'application/vnd.ms-excel', {
                            'Expires': '0'
                        })
            })
    for _ in range(max_retries):
        assert b'name="filename"' in requests_mock.request_history[
            history_index].body
        assert b'bar' in requests_mock.request_history[history_index].body
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post('http://foo', data=io.BytesIO(b'bar'))
    for _ in range(max_retries):
        assert b'bar' == deepcopy(
            requests_mock.request_history[history_index].body).read()
        history_index += 1

    with pytest.raises(requests.exceptions.HTTPError):
        session.post('http://foo', data=(s for s in ['a']))
    assert history_index + 1 == len(requests_mock.request_history)


def test_response():
    fp = io.BytesIO(b'test')
    fp.name = 'foo'
    real_read = fp.read
    fp.read = lambda size, **kwargs: real_read(size)

    resp = Response(fp)
    assert resp.mode == 'rb'
    assert resp.name == 'foo'
    assert resp.read(0) == b''
    assert resp.read(1) == b't'
    assert resp.read(-1) == b'est'
    assert resp.tell() == 4

    fp = io.BytesIO(b'1\n2\n3\n4\n')
    fp.name = 'foo'
    real_read = fp.read
    fp.read = lambda size, **kwargs: real_read(size)

    resp = Response(fp)
    assert resp.name == 'foo'
    assert resp.readlines() == [b'1\n', b'2\n', b'3\n', b'4\n']

    fp = io.BytesIO(b'1\n2\n3\n4\n')
    fp.name = 'foo'
    real_read = fp.read
    fp.read = lambda size, **kwargs: real_read(size)

    resp = Response(fp)
    assert resp.name == 'foo'
    lines = []
    for i in range(4):
        line = resp.readline(-1)
        assert resp.tell() == (i + 1) * 2
        if not line:
            break
        lines.append(line)
    assert lines == [b'1\n', b'2\n', b'3\n', b'4\n']

    fp = io.BytesIO(b'11\n2\n3\n4\n')
    fp.name = 'foo'
    real_read = fp.read
    fp.read = lambda size, **kwargs: real_read(size)

    resp = Response(fp)
    resp._block_size = 4
    assert resp.name == 'foo'
    assert resp.readline(0) == b''
    assert resp.readline(1) == b'1'
    assert resp.readline(2) == b'1\n'
    assert resp.readline(1) == b'2'
    assert resp.readline(1) == b'\n'

    fp = io.BytesIO(b'123')
    fp.name = 'foo'
    real_read = fp.read
    fp.read = lambda size, **kwargs: real_read(size)

    resp = Response(fp)
    resp._block_size = 2
    assert resp.readline(1) == b'1'
    assert resp.readline() == b'23'
    assert resp.readline() == b''


def test_is_http():
    assert is_http('http://foo') is True
    assert is_http('s3://foo') is False


def test_open_with_headers(requests_mock):
    requests_mock.get(
        'http://test',
        text='test',
        status_code=200,
        headers={
            'Content-Length': '4',
        })
    headers = {
        'A': 'a',
        'B': 'b',
    }

    path = HttpPath('http://test')
    path.request_kwargs = {'headers': headers}
    with path.open('rb') as f:
        assert f.read() == b"test"

    for key, value in headers.items():
        assert requests_mock.request_history[0].headers[key] == value

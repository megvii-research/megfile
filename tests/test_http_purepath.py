from pathlib import Path

import pytest

from megfile.http_path import HttpPath
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

    # TODO: 下面是还暂不支持的用法
    # assert 'http://foo' / HttpPath('bar') == HttpPath('http://foo/bar')
    # assert 'foo' / HttpPath('bar') == HttpPath('foo/bar')


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

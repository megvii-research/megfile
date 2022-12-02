from pathlib import Path

import pytest

from megfile.lib.compat import fspath
from megfile.s3_path import S3Path
from megfile.smart_path import SmartPath


def test_repr():
    assert repr(SmartPath('file://foo.bar')) == "SmartPath('file://foo.bar')"
    assert str(SmartPath('file://foo.bar')) == 'file://foo.bar'
    assert bytes(SmartPath('file://foo.bar')) == b'file://foo.bar'

    assert repr(SmartPath('/foo.bar')) == "SmartPath('/foo.bar')"
    assert str(SmartPath('/foo.bar')) == '/foo.bar'
    assert bytes(SmartPath('/foo.bar')) == b'/foo.bar'


def test_fspath():
    assert fspath(SmartPath('file://bucket/key')) == 'bucket/key'

    assert fspath(SmartPath('bucket/key')) == 'bucket/key'
    assert fspath(SmartPath('/foo.bar')) == '/foo.bar'
    assert fspath(SmartPath('///foo.bar')) == '/foo.bar'

    assert fspath(SmartPath('s3://bucket/key')) == 's3://bucket/key'
    assert fspath(SmartPath('s3://foo.bar')) == 's3://foo.bar'


def test_join_strs():
    assert SmartPath('file://foo', 'some/path',
                     'bar') == SmartPath('file://foo/some/path/bar')
    assert SmartPath('file://foo', '', 'bar') == SmartPath('file://foo//bar')
    assert SmartPath('file://foo', '/some/path',
                     'bar') == SmartPath('/some/path/bar')

    assert SmartPath('foo', 'some/path',
                     'bar') == SmartPath('foo/some/path/bar')
    assert SmartPath('foo', '', 'bar') == SmartPath('foo//bar')
    assert SmartPath('foo', '/some/path', 'bar') == SmartPath('/some/path/bar')

    assert SmartPath('s3://foo', 'some/path',
                     'bar') == SmartPath('s3://foo/some/path/bar')
    assert SmartPath('s3://foo', '', 'bar') == SmartPath('s3://foo//bar')
    assert SmartPath('s3://foo', '/some/path',
                     'bar') == SmartPath('s3://foo/some/path/bar')


def test_join_paths():
    assert SmartPath(SmartPath('file://foo'),
                     SmartPath('bar')) == SmartPath('file://foo/bar')

    assert SmartPath(SmartPath('foo'), SmartPath('bar')) == SmartPath('foo/bar')

    assert SmartPath(SmartPath('s3://foo'),
                     SmartPath('bar')) == SmartPath('s3://foo/bar')


def test_slashes_single_double_dots():
    assert SmartPath('file://foo//bar') == SmartPath('file://foo//bar')
    assert SmartPath('file://foo/./bar') == SmartPath('file://foo/./bar')
    assert SmartPath('file://foo/../bar') == SmartPath('file://foo/../bar')
    assert SmartPath('file://../bar') == SmartPath('file://../bar')

    assert SmartPath('foo//bar') == SmartPath('foo//bar')
    assert SmartPath('foo/./bar') == SmartPath('foo/./bar')
    assert SmartPath('foo/../bar') == SmartPath('foo/../bar')
    assert SmartPath('../bar') == SmartPath('../bar')

    assert SmartPath('s3://foo//bar') == SmartPath('s3://foo//bar')
    assert SmartPath('s3://foo/./bar') == SmartPath('s3://foo/./bar')
    assert SmartPath('s3://foo/../bar') == SmartPath('s3://foo/../bar')
    assert SmartPath('s3://../bar') == SmartPath('s3://../bar')

    assert SmartPath('file://foo', '../bar') == SmartPath('file://foo/../bar')
    assert SmartPath('foo', '../bar') == SmartPath('foo/../bar')


def test_operators():
    assert SmartPath('file://foo') / 'bar' / 'baz' == SmartPath(
        'file://foo/bar/baz')
    assert SmartPath('foo') / 'bar' / 'baz' == SmartPath('foo/bar/baz')
    assert SmartPath('file://foo') / 'bar' / 'baz' in {
        SmartPath('file://foo/bar/baz')
    }

    # TODO: 下面是还暂不支持的用法
    # assert 'file://foo' / SmartPath('bar') == SmartPath('file://foo/bar')
    # assert 'foo' / SmartPath('bar') == SmartPath('foo/bar')


def test_parts():
    assert SmartPath('file://foo//bar').parts == ('foo', 'bar')
    assert SmartPath('file://foo/./bar').parts == ('foo', 'bar')
    assert SmartPath('file://foo/../bar').parts == ('foo', '..', 'bar')
    assert SmartPath('file://../bar').parts == ('..', 'bar')
    assert (SmartPath('file://foo') / '../bar').parts == ('foo', '..', 'bar')
    assert SmartPath('file://foo/bar').parts == ('foo', 'bar')

    assert SmartPath('file://foo', '../bar').parts == ('foo', '..', 'bar')
    assert SmartPath('file://', 'foo', 'bar').parts == ('foo', 'bar')

    assert SmartPath('s3://foo//bar').parts == ('s3://', 'foo', '', 'bar')
    assert SmartPath('s3://foo/./bar').parts == ('s3://', 'foo', '.', 'bar')
    assert SmartPath('s3://foo/../bar').parts == ('s3://', 'foo', '..', 'bar')
    assert SmartPath('s3://../bar').parts == ('s3://', '..', 'bar')
    assert (SmartPath('s3://foo') /
            '../bar').parts == ('s3://', 'foo', '..', 'bar')
    assert SmartPath('s3://foo/bar').parts == ('s3://', 'foo', 'bar')

    assert SmartPath('s3://foo',
                     '../bar').parts == ('s3://', 'foo', '..', 'bar')
    assert SmartPath('s3://', 'foo', 'bar').parts == ('s3://', 'foo', 'bar')

    assert SmartPath('foo//bar').parts == ('foo', 'bar')
    assert SmartPath('foo/./bar').parts == ('foo', 'bar')
    assert SmartPath('foo/../bar').parts == ('foo', '..', 'bar')
    assert SmartPath('../bar').parts == ('..', 'bar')
    assert SmartPath('foo', '../bar').parts == ('foo', '..', 'bar')
    assert SmartPath('foo/bar').parts == ('foo', 'bar')
    assert SmartPath('/', 'foo', 'bar').parts == ('/', 'foo', 'bar')


def test_drive():
    assert SmartPath('file://foo/bar').drive == ''

    assert SmartPath('foo//bar').drive == ''
    assert SmartPath('foo/./bar').drive == ''
    assert SmartPath('foo/../bar').drive == ''
    assert SmartPath('../bar').drive == ''

    assert SmartPath('foo', '../bar').drive == ''

    assert SmartPath('s3://bucket/test').drive == ''


def test_root():
    assert SmartPath('foo/bar').root == ''

    assert SmartPath('/foo/bar').root == '/'
    assert SmartPath('foo//bar').root == ''
    assert SmartPath('foo/./bar').root == ''
    assert SmartPath('foo/../bar').root == ''
    assert SmartPath('../bar').root == ''

    assert SmartPath('s3://bucket/test').root == 's3://'


def test_anchor():
    assert SmartPath('/foo/bar').anchor == '/'

    assert SmartPath('foo//bar').anchor == ''
    assert SmartPath('foo/./bar').anchor == ''
    assert SmartPath('foo/../bar').anchor == ''
    assert SmartPath('../bar').anchor == ''

    assert SmartPath('s3://bucket/test').anchor == 's3://'


def test_parents():
    assert tuple(
        SmartPath('foo//bar').parents) == (SmartPath('foo'), SmartPath(''))
    assert tuple(
        SmartPath('foo/./bar').parents) == (SmartPath('foo'), SmartPath(''))
    assert tuple(SmartPath('foo/../bar').parents) == (
        SmartPath('foo/..'), SmartPath('foo'), SmartPath(''))
    assert tuple(
        SmartPath('../bar').parents) == (SmartPath('..'), SmartPath(''))
    assert tuple(SmartPath('foo', '../bar').parents) == (
        SmartPath('foo/..'), SmartPath('foo'), SmartPath(''))

    assert tuple(SmartPath('file://foo/bar').parents) == (
        SmartPath('foo'), SmartPath(''))


def test_parent():
    assert SmartPath('foo//bar').parent == SmartPath('foo/')
    assert SmartPath('foo/./bar').parent == SmartPath('foo/.')
    assert SmartPath('foo/../bar').parent == SmartPath('foo/..')
    assert SmartPath('../bar').parent == SmartPath('..')
    assert SmartPath('/foo/bar').parent == SmartPath('/foo')
    assert SmartPath('file://').parent == SmartPath('file://')
    assert SmartPath('foo', '../bar').parent == SmartPath('foo/..')
    assert SmartPath('/').parent == SmartPath('/')


def test_name():
    assert SmartPath('file://foo/bar/baz.py').name == 'baz.py'
    assert SmartPath('foo/bar/baz.py').name == 'baz.py'
    assert SmartPath('s3://foo/bar/baz.py').name == 'baz.py'


def test_suffix():
    assert SmartPath('file://foo/bar.tar.gz').suffix == '.gz'
    assert SmartPath('file://foo/bar').suffix == ''

    assert SmartPath('foo/bar/baz.py').suffix == '.py'
    assert SmartPath('foo/bar').suffix == ''

    assert SmartPath('s3://foo/bar/baz.py').suffix == '.py'
    assert SmartPath('s3://foo/bar').suffix == ''


def test_suffixes():
    assert SmartPath('file://foo/bar.tar.gar').suffixes == ['.tar', '.gar']
    assert SmartPath('file://foo/bar.tar.gz').suffixes == ['.tar', '.gz']
    assert SmartPath('file://foo/bar').suffixes == []

    assert SmartPath('foo/bar.tar.gar').suffixes == ['.tar', '.gar']
    assert SmartPath('foo/bar.tar.gz').suffixes == ['.tar', '.gz']
    assert SmartPath('foo/bar').suffixes == []

    assert SmartPath('s3://foo/bar.tar.gar').suffixes == ['.tar', '.gar']
    assert SmartPath('s3://foo/bar.tar.gz').suffixes == ['.tar', '.gz']
    assert SmartPath('s3://foo/bar').suffixes == []


def test_stem():
    assert SmartPath('foo/bar.tar.gar').stem == 'bar.tar'
    assert SmartPath('foo/bar.tar').stem == 'bar'
    assert SmartPath('foo/bar').stem == 'bar'


def test_uri():
    assert SmartPath('/foo/bar').as_uri() == 'file:///foo/bar'
    assert SmartPath('file:///foo/bar/baz').as_uri() == 'file:///foo/bar/baz'
    assert SmartPath('file:///bucket/key').as_uri() == 'file:///bucket/key'
    assert SmartPath('/buc:ket/ke@y').as_uri() == 'file:///buc:ket/ke@y'
    assert SmartPath('file://foo/bar').as_uri() == 'file://foo/bar'
    assert SmartPath('file://foo/bar/baz').as_uri() == 'file://foo/bar/baz'
    assert SmartPath('file://bucket/key').as_uri() == 'file://bucket/key'

    # no escape
    assert SmartPath('file://buc:ket/ke@y').as_uri() == 'file://buc:ket/ke@y'


def test_absolute():
    assert not SmartPath('file://foo/bar').is_absolute()
    assert not SmartPath('foo/bar').is_absolute()


def test_reserved():
    assert not SmartPath('file://foo/bar').is_reserved()
    assert not SmartPath('foo/bar').is_reserved()


def test_is_relative_to():
    assert SmartPath('file://foo/bar').is_relative_to('foo')
    assert not SmartPath('file:///foo/bar').is_relative_to('foo')

    assert SmartPath('s3://foo/bar').is_relative_to('s3://foo')
    assert SmartPath('s3://foo/bar').is_relative_to('foo')
    assert not SmartPath('s3://foo/bar').is_relative_to('bar')


def test_joinpath():
    assert SmartPath('file://foo').joinpath('bar') == SmartPath(
        'file://foo/bar')
    assert SmartPath('file://foo').joinpath(
        SmartPath('bar')) == SmartPath('file://foo/bar')
    assert SmartPath('file://foo').joinpath(
        'bar', 'baz') == SmartPath('file://foo/bar/baz')

    assert SmartPath('foo').joinpath('bar') == SmartPath('foo/bar')
    assert SmartPath('foo').joinpath(SmartPath('bar')) == SmartPath('foo/bar')
    assert SmartPath('foo').joinpath('bar', 'baz') == SmartPath('foo/bar/baz')

    assert SmartPath('s3://foo').joinpath('bar') == SmartPath('s3://foo/bar')
    assert SmartPath('s3://foo').joinpath(
        SmartPath('bar')) == SmartPath('s3://foo/bar')
    assert SmartPath('s3://foo').joinpath(
        'bar', 'baz') == SmartPath('s3://foo/bar/baz')


def test_match():
    assert SmartPath('a/b.py').match('*.py')
    assert SmartPath('file://a/b/c.py').match('b/*.py')
    assert not SmartPath('file://a/b/c.py').match('file://a/*.py')
    assert SmartPath('file://a.py').match('file://*.py')
    assert SmartPath('a/b.py').match('file://a/b.py')
    assert not SmartPath('a/b.py').match('file://*.py')
    assert not SmartPath('a/b.py').match('*.Py')

    assert SmartPath('s3://a/b.py').match('*.py')
    assert SmartPath('s3://a/b.py').match('s3://a/b.py')
    assert not SmartPath('s3://a/b.py').match('s3://*.py')
    assert not SmartPath('s3://a/b.py').match('*.Py')


def test_relative_to():
    path = SmartPath('file://foo/bar')
    assert path.relative_to('file://') == SmartPath('foo/bar')
    assert path.relative_to('file://foo') == SmartPath('bar')
    with pytest.raises(ValueError):
        path.relative_to('file://baz')

    path = SmartPath('s3://foo/bar')
    assert path.relative_to('s3://') == S3Path('foo/bar')
    assert path.relative_to('s3://foo') == S3Path('bar')
    assert path.relative_to('foo') == S3Path('bar')
    with pytest.raises(ValueError):
        path.relative_to('s3://baz')


def test_relative_to_relative():
    path = SmartPath('foo/bar/baz')
    assert path.relative_to('foo/bar') == SmartPath('baz')
    assert path.relative_to('foo') == SmartPath('bar/baz')
    with pytest.raises(ValueError):
        path.relative_to('baz')


def test_with_name():
    path = SmartPath('file://foo/bar.tar.gz')
    assert path.with_name('baz.py') == SmartPath('file://foo/baz.py')

    path = SmartPath('foo/bar.tar.gz')
    assert path.with_name('baz.py') == SmartPath('foo/baz.py')

    path = SmartPath('s3://foo/bar.tar.gz')
    assert path.with_name('baz.py') == SmartPath('s3://foo/baz.py')


def test_with_stem():
    path = SmartPath('file://foo/bar.tar.gz')
    assert path.with_stem('baz') == SmartPath('file://foo/baz.gz')

    path = SmartPath('s3://foo/bar.tar.gz')
    assert path.with_stem('baz') == SmartPath('s3://foo/baz.gz')


def test_with_suffix():
    path = SmartPath('file://foo/bar.tar.gz')
    assert path.with_suffix('.bz2') == SmartPath('file://foo/bar.tar.bz2')
    path = SmartPath('baz')
    assert path.with_suffix('.txt') == SmartPath('baz.txt')
    path = SmartPath('baz.txt')
    assert path.with_suffix('') == SmartPath('baz')

    path = SmartPath('s3://foo/bar.tar.gz')
    assert path.with_suffix('.bz2') == SmartPath('s3://foo/bar.tar.bz2')

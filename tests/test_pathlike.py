import pytest

from megfile.pathlike import BasePath, BaseURIPath, FileEntry, StatResult, URIPath


def test_file_entry():
    stat_result = StatResult(
        size=100, ctime=1.1, mtime=1.2, isdir=False, islnk=False)
    file_entry = FileEntry(name='test', stat=stat_result)
    assert file_entry.isfile() is True
    assert file_entry.isdir() is False
    assert file_entry.is_symlink() is False


def test_base_path(mocker):
    path = '/test'
    base_path = BasePath(path)
    assert base_path.__fspath__() == path
    assert base_path.is_link() is False

    funcA = mocker.patch('megfile.pathlike.BasePath.open')
    base_path.touch()
    funcA.assert_called_once_with('w')

    with pytest.raises(NotImplementedError):
        base_path.isdir()


def test_base_uri_path_as_posix(mocker):
    path = '/test'
    base_uri_path = BaseURIPath(path)
    mocker.patch('megfile.pathlike.BaseURIPath.anchor', 'fs://')
    assert base_uri_path.as_posix() == 'fs://test'


def test_base_uri_path(mocker):
    from megfile.utils import classproperty
    mocker.patch('megfile.pathlike.BaseURIPath.protocol', 'fs')

    path = '/test'
    base_uri_path = BaseURIPath(path)

    other_path_a = 1
    with pytest.raises(TypeError):
        base_uri_path > other_path_a

    with pytest.raises(TypeError):
        base_uri_path < other_path_a

    with pytest.raises(TypeError):
        base_uri_path >= other_path_a

    with pytest.raises(TypeError):
        base_uri_path <= other_path_a

    class BaseURIPathB(BaseURIPath):

        def __init__(self, path: "PathLike"):
            super().__init__(path)

        @classproperty
        def protocol(cls) -> str:
            return 'test_b'

    other_path_b = BaseURIPathB('/test_b')
    with pytest.raises(TypeError):
        base_uri_path > other_path_b

    with pytest.raises(TypeError):
        base_uri_path < other_path_b

    with pytest.raises(TypeError):
        base_uri_path >= other_path_b

    with pytest.raises(TypeError):
        base_uri_path <= other_path_b

    other_path_c = BaseURIPath('/test2')
    assert (base_uri_path > other_path_c) is False
    assert (base_uri_path < other_path_c) is True
    assert (base_uri_path >= other_path_c) is False
    assert (base_uri_path <= other_path_c) is True


def test_uri_path(mocker):
    mocker.patch('megfile.pathlike.URIPath.protocol', 'fs')
    mocker.patch('megfile.pathlike.URIPath.path_without_protocol', '')

    path = '/test'
    uri_path = URIPath(path)
    assert uri_path.name == ''

    mocker.patch('megfile.pathlike.URIPath.name', '.')
    uri_path_b = URIPath(path)
    assert uri_path_b.suffixes == []

    with pytest.raises(TypeError):
        uri_path.relative_to(None)

    with pytest.raises(TypeError):
        uri_path.relative_to(1)
    assert uri_path.resolve() == 'fs://test'

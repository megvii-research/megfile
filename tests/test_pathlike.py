import pytest

from megfile.pathlike import (
    BasePath,
    FileEntry,
    PathLike,
    StatResult,
    URIPath,
)


def test_file_entry():
    stat_result = StatResult(size=100, ctime=1.1, mtime=1.2, isdir=False, islnk=False)
    file_entry = FileEntry(name="test", path="test", stat=stat_result)
    assert file_entry.is_file() is True
    assert file_entry.is_dir() is False
    assert file_entry.is_symlink() is False


def test_base_path(mocker):
    path = "/test"
    base_path = BasePath(path)
    base_path.protocol = "p"
    assert base_path.__fspath__() == "p://test"

    funcA = mocker.patch("megfile.pathlike.BasePath.open")
    base_path.touch()
    funcA.assert_called_once_with("w")

    with pytest.raises(NotImplementedError):
        base_path.is_dir()


def test_base_uri_path_as_posix(mocker):
    path = "/test"
    base_uri_path = BasePath(path)
    mocker.patch("megfile.pathlike.BasePath.protocol", "fs")
    assert base_uri_path.as_posix() == "fs://test"


def test_base_uri_path(mocker):
    from megfile.utils import classproperty

    mocker.patch("megfile.pathlike.BasePath.protocol", "fs")

    path = "/test"
    base_uri_path = BasePath(path)

    other_path_a = 1
    with pytest.raises(TypeError):
        base_uri_path > other_path_a

    with pytest.raises(TypeError):
        base_uri_path < other_path_a

    with pytest.raises(TypeError):
        base_uri_path >= other_path_a

    with pytest.raises(TypeError):
        base_uri_path <= other_path_a

    class BaseURIPathB(BasePath):
        def __init__(self, path: PathLike):
            super().__init__(path)

        @classproperty
        def protocol(cls) -> str:
            return "test_b"

    other_path_b = BaseURIPathB("/test_b")
    with pytest.raises(TypeError):
        base_uri_path > other_path_b

    with pytest.raises(TypeError):
        base_uri_path < other_path_b

    with pytest.raises(TypeError):
        base_uri_path >= other_path_b

    with pytest.raises(TypeError):
        base_uri_path <= other_path_b

    other_path_c = BasePath("/test2")
    assert (base_uri_path > other_path_c) is False
    assert (base_uri_path < other_path_c) is True
    assert (base_uri_path >= other_path_c) is False
    assert (base_uri_path <= other_path_c) is True


def test_uri_path(mocker):
    mocker.patch("megfile.pathlike.URIPath.protocol", "fs")
    mocker.patch("megfile.pathlike.URIPath.path_without_protocol", "")

    path = "/test"
    uri_path = URIPath(path)
    assert uri_path.name == ""

    mocker.patch("megfile.pathlike.URIPath.name", ".")
    uri_path_b = URIPath(path)
    assert uri_path_b.suffixes == []

    with pytest.raises(TypeError):
        uri_path.relative_to()

    with pytest.raises(ValueError):
        uri_path.relative_to(1)
    assert uri_path.resolve() == "fs://test"


def test_base_path_attr(mocker):
    path = BasePath("/test")
    with pytest.raises(NotImplementedError):
        path / "test"

    assert path.name == "/test"

    with pytest.raises(NotImplementedError):
        path.joinpath("test")

    with pytest.raises(NotImplementedError):
        path.parts

    with pytest.raises(NotImplementedError):
        path.parents

    with pytest.raises(NotImplementedError):
        path.parent

    with pytest.raises(NotImplementedError):
        path.is_dir()

    with pytest.raises(NotImplementedError):
        path.is_file()

    assert path.is_symlink() is False

    with pytest.raises(NotImplementedError):
        path.access(None)

    with pytest.raises(NotImplementedError):
        path.exists()

    with pytest.raises(NotImplementedError):
        path.listdir()

    with pytest.raises(NotImplementedError):
        path.scandir()

    with pytest.raises(NotImplementedError):
        path.getsize()

    with pytest.raises(NotImplementedError):
        path.getmtime()

    with pytest.raises(NotImplementedError):
        path.stat()

    with pytest.raises(NotImplementedError):
        path.match(r"*")

    with pytest.raises(NotImplementedError):
        path.remove()

    with pytest.raises(NotImplementedError):
        path.mkdir()

    with pytest.raises(NotImplementedError):
        path.rmdir()

    with pytest.raises(NotImplementedError):
        path.open()

    with pytest.raises(NotImplementedError):
        path.walk()

    with pytest.raises(NotImplementedError):
        path.scan()

    with pytest.raises(NotImplementedError):
        path.scan_stat()

    with pytest.raises(NotImplementedError):
        path.glob(None)

    with pytest.raises(NotImplementedError):
        path.iglob(None)

    with pytest.raises(NotImplementedError):
        path.glob_stat(None)

    with pytest.raises(NotImplementedError):
        path.load()

    with pytest.raises(NotImplementedError):
        path.save(None)

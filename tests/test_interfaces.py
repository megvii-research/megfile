from io import BytesIO, UnsupportedOperation

import pytest

from megfile.interfaces import (
    BasePath,
    Closable,
    Readable,
    URIPath,
    Writable,
    fullname,
)


class Klass1(Closable):
    pass


class Klass2(Closable):
    def __init__(self):
        self.outer_close_call_count = 0

    def _close(self):
        self.outer_close_call_count += 1


class Klass3(Klass2):
    def __init__(self):
        self.inner_close_call_count = 0
        super().__init__()

    def _close(self):
        self.inner_close_call_count += 1
        super()._close()


def test_fullname():
    assert fullname(Klass3) == "abc.ABCMeta"
    assert fullname(str) == "type"


def test_not_provide_close():
    with pytest.raises(TypeError):
        Klass1()


def test_with_context():
    with Klass2() as reader:
        assert reader.closed is False
    assert reader.closed is True
    assert reader.outer_close_call_count == 1


def test_only_close_once():
    reader = Klass2()
    reader.close()
    reader.close()
    assert reader.outer_close_call_count == 1


def test_subclass_only_close_once():
    reader = Klass3()
    reader.close()
    reader.close()
    assert reader.outer_close_call_count == 1
    assert reader.inner_close_call_count == 1


class Klass4(Readable[bytes]):
    name = "test"
    mode = "rb"

    def __init__(self, data):
        self._buffer = BytesIO(data)

    def tell(self):
        return self._buffer.tell()

    def read(self, size=None) -> bytes:
        return self._buffer.read(size)

    def readline(self) -> bytes:
        return self._buffer.readline()

    def _close(self):
        pass


def test_readable(mocker):
    r = Klass4(b"")
    assert r.readlines() == []
    assert r.isatty() is False

    r = Klass4(b"1\n2\n")
    assert r.readlines() == [b"1\n", b"2\n"]

    r = Klass4(b"1\n2\n")
    assert next(r) == b"1\n"
    assert list(r) == [b"2\n"]

    r = Klass4(b"1\n2\n")
    assert r.readinto(bytearray(b"123")) == 3

    r = Klass4(b"1\n2\n")
    r.mode = "r"

    with pytest.raises(OSError):
        r.readinto(bytearray(b"123"))

    with pytest.raises(OSError):
        r.truncate()

    with pytest.raises(OSError):
        r.write(b"123")

    with pytest.raises(OSError):
        r.writelines([b"123"])


class Klass5(Writable[bytes]):
    name = "test"
    mode = "w"

    def __init__(self):
        self._buffer = BytesIO()

    def tell(self):
        return self._buffer.tell()

    def write(self, data: bytes):
        return self._buffer.write(data)

    def getvalue(self) -> bytes:
        return self._buffer.getvalue()

    def _close(self):
        pass


def test_writable(mocker):
    w = Klass5()
    w.writelines([b"1", b"2"])
    assert w.getvalue() == b"12"

    with pytest.raises(UnsupportedOperation):
        w.truncate()

    with pytest.raises(OSError):
        w.read()

    with pytest.raises(OSError):
        w.readline()

    with pytest.raises(OSError):
        w.readlines()


TEST_PATH = "test/file"
TEST_URI = "test://test/file"


class Klass6(BasePath):
    def __init__(self, path):
        self.path = path


def test_basepath(mocker):
    b = Klass6(TEST_PATH)
    assert b.path == TEST_PATH
    assert str(b) == TEST_PATH


class Klass7(URIPath):
    protocol = "test"


def test_uripath_as_uri(mocker):
    assert Klass7.protocol == "test"
    u = Klass7(TEST_PATH)
    assert u.as_uri() == u.path_with_protocol


def test_uripath_from_uri(mocker):
    with pytest.raises(ValueError) as error:
        Klass7.from_uri(TEST_PATH)
    assert error.value.args[0].startswith("protocol not match")

    t = Klass7.from_uri(TEST_URI)
    assert isinstance(t, Klass7)
    assert t.path == TEST_PATH


class Klass8(URIPath):
    protocol = "s3"


class Klass9(URIPath):
    protocol = "s4"


class Klass10:
    protocol = "s3"

    def __init__(self, path) -> None:
        self.path = path

    def __repr__(self) -> str:
        return f"Klass10('{self.path}')"


def test_uripath_truediv(mocker):
    path = Klass8("s3://bucket/dir/")
    other_path = Klass9("file")
    with pytest.raises(TypeError) as error:
        path / other_path
    assert error.value.args[0].startswith("'/' not supported")

    other_path = Klass10("file")
    with pytest.raises(TypeError) as error:
        path / other_path
    assert error.value.args[0] == "Klass10('file') is not 'PathLike' object"

    assert (path / "file").path == "s3://bucket/dir/file"
    assert (path / "/file").path == "s3://bucket/dir/file"
    assert (path / Klass8("file")).path == "s3://bucket/dir/file"
    assert (path / Klass8("/file")).path == "s3://bucket/dir/file"

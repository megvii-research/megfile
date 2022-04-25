from io import BytesIO

import pytest

from megfile.interfaces import Closable, Readable, Writable, fullname


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
    assert fullname(Klass3) == 'abc.ABCMeta'
    assert fullname(str) == 'type'


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


class Klass4(Readable):

    name = 'test'
    mode = 'r'

    def __init__(self, data):
        self._buffer = BytesIO(data)

    def tell(self):
        return self._buffer.tell()

    def read(self, size=None):
        return self._buffer.read(size)

    def readline(self):
        return self._buffer.readline()

    def _close(self):
        pass


def test_readable(mocker):
    r = Klass4(b'')
    assert r.readlines() == []

    r = Klass4(b'1\n2\n')
    assert r.readlines() == [b'1\n', b'2\n']

    r = Klass4(b'1\n2\n')
    assert next(r) == b'1\n'
    assert list(r) == [b'2\n']

    r = Klass4(b'1\n2\n')
    assert r.readinto(bytearray(b'123')) == 3


class Klass5(Writable):

    name = 'test'
    mode = 'w'

    def __init__(self):
        self._buffer = BytesIO()

    def tell(self):
        return self._buffer.tell()

    def write(self, data):
        return self._buffer.write(data)

    def getvalue(self):
        return self._buffer.getvalue()

    def _close(self):
        pass


def test_writable(mocker):
    w = Klass5()
    w.writelines([b'1', b'2'])
    assert w.getvalue() == b'12'

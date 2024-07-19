from io import BytesIO, StringIO

from megfile.lib.shadow_handler import ShadowHandler


def test_shadow_handler_ability():
    io = BytesIO()
    fd = ShadowHandler(io)

    assert isinstance(fd.name, str)
    assert fd.mode == "rb+"
    assert io.readable() == fd.readable()  # True
    assert io.writable() == fd.writable()  # True
    assert io.seekable() == fd.seekable()  # True


def test_shadow_handler_read():
    io = BytesIO(b"abcde")
    fd = ShadowHandler(io)

    assert fd.read(1) == b"a"
    assert fd.tell() == 1
    assert io.read() == b"bcde"
    assert fd.tell() == 1
    assert fd.read(1) == b"b"
    assert fd.seek(-1, 2) == 4
    assert fd.read(1) == b"e"
    assert fd.tell() == 5


def test_shadow_handler_readline():
    io = BytesIO(b"ab\ncd\ne")
    fd = ShadowHandler(io)

    assert fd.readline(1) == b"a"
    fd.seek(0)
    assert fd.readline(2) == b"ab"
    fd.seek(0)
    assert fd.readline(4) == b"ab\n"
    fd.seek(0)
    assert fd.readline(5) == b"ab\n"


def test_shadow_handler_read_text():
    io = StringIO("abcde")
    fd = ShadowHandler(io)

    assert fd.read(1) == "a"
    assert io.read() == "bcde"
    assert fd.read(1) == "b"
    assert fd.seek(-1, 2) == 4
    assert fd.read(1) == "e"


def test_shadow_handler_read_non_intrusive():
    io = BytesIO(b"abcde")
    fd = ShadowHandler(io, intrusive=False)

    assert fd.read(1) == b"a"
    assert io.read() == b"abcde"
    assert fd.read(1) == b"b"


def test_shadow_handler_write():
    io = BytesIO()
    fd = ShadowHandler(io)

    assert fd.write(b"a") == 1
    assert io.write(b"b") == 1
    assert io.getvalue() == b"ab"
    assert fd.write(b"c") == 1
    assert io.getvalue() == b"ac"


def test_shadow_handler_write_non_intrusive():
    io = BytesIO()
    fd = ShadowHandler(io, intrusive=False)

    assert fd.write(b"a") == 1
    assert io.write(b"b") == 1
    assert io.getvalue() == b"b"
    assert fd.write(b"c") == 1
    assert io.getvalue() == b"bc"


def test_shadow_handler_cross_read_and_write():
    io = BytesIO(b"abc")
    fd1 = ShadowHandler(io)
    fd2 = ShadowHandler(io)
    fd3 = ShadowHandler(io)

    assert fd1.read(1) == b"a"
    assert fd2.read(1) == b"a"
    assert fd3.write(b"A") == 1
    assert fd1.read() == b"bc"
    assert fd2.read() == b"bc"
    assert fd3.write(b"B") == 1

    fd1.seek(0)
    fd2.seek(1)
    fd3.seek(2)
    assert fd1.read(1) == b"A"
    assert fd2.read(1) == b"B"
    assert fd3.write(b"C") == 1
    assert fd1.read() == b"BC"
    assert fd2.read() == b"C"

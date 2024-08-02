from io import BytesIO

import pytest

from megfile.stdio import is_stdio, stdio_open


def test_is_stdio():
    assert is_stdio("stdio://-")
    assert is_stdio("stdio://a")
    assert is_stdio("stdio://@")
    assert not is_stdio("s3://a,b")


def test_stdio_reader(mocker):
    with pytest.raises(ValueError) as error:
        stdio_open("stdio://-", "io")
    assert error.value.args[0] == "unacceptable mode: 'io'"

    reader = stdio_open("stdio://-", "rb")
    assert reader.name == "stdin"
    assert reader.mode == "rb"

    stdin_buffer_read = mocker.patch("sys.stdin.buffer.read")
    stdin_buffer_read.return_value = b"test\ntest1\n"
    assert reader.read() == b"test\ntest1\n"
    assert set(reader.readlines()) == set([b"test\n", b"test1\n"])

    stdin_buffer_readline = mocker.patch("sys.stdin.buffer.readline")
    stdin_buffer_readline.return_value = b"test\ntest1\n"
    assert reader.readline() == b"test\ntest1\n"

    with pytest.raises(IOError):
        reader.tell()

    reader.close()
    assert reader.closed

    with stdio_open("stdio://-", "rb") as reader:
        assert reader.read() == b"test\ntest1\n"


def test_stdio_writer(mocker):
    stdout_buffer_write = mocker.patch("sys.stdout.buffer.write")
    data = BytesIO()

    def fake_write(w_data):
        data.write(w_data)
        return len(w_data)

    stdout_buffer_write.side_effect = fake_write

    writer = stdio_open("stdio://-", "wb")
    assert writer.name == "stdout"
    assert writer.mode == "wb"

    writer.write(b"test")
    assert data.getvalue() == b"test"

    data = BytesIO()
    writer.writelines([b"test\n", b"test1\n"])
    assert data.getvalue() == b"test\ntest1\n"

    with pytest.raises(IOError):
        writer.tell()

    writer.close()
    assert writer.closed

    writer = stdio_open("stdio://2", "wb")
    assert writer.name == "stderr"
    assert writer.mode == "wb"

    data = BytesIO()
    with stdio_open("stdio://-", "wb") as writer:
        writer.write(b"test")
        assert data.getvalue() == b"test"


def test_stdio_open_error():
    with pytest.raises(ValueError):
        with stdio_open("test://-", "wb"):
            pass

    with pytest.raises(ValueError):
        with stdio_open("stdio://1", "rb"):
            pass

    with pytest.raises(ValueError):
        with stdio_open("stdio://0", "wb"):
            pass

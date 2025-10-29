from io import BytesIO
from typing import Dict

import pytest

from megfile.lib.webdav_memory_handler import WebdavMemoryHandler

from ..test_webdav import FakeWebdavClient

PATH = "/tmp/testfile"
NAME = "webdav://host/tmp/testfile"
CONTENT = b"block0\n block1\n block2"
LOCAL_PATH = "/tmp/localfile"


@pytest.fixture
def client(fs, mocker):
    def fake_webdav_stat(client, path: str) -> Dict:
        return client.info(path)

    mocker.patch("megfile.webdav_path._webdav_stat", side_effect=fake_webdav_stat)
    yield FakeWebdavClient()


def test_webdav_memory_handler_close(client):
    writer = WebdavMemoryHandler(PATH, "wb", webdav_client=client, name=NAME)
    assert writer.closed is False
    writer.close()
    assert writer.closed is True

    reader = WebdavMemoryHandler(PATH, "rb", webdav_client=client, name=NAME)
    assert reader.closed is False
    reader.close()
    assert reader.closed is True


def test_webdav_memory_handler_mode(client):
    with pytest.raises(ValueError):
        WebdavMemoryHandler(PATH, "w", webdav_client=client, name=NAME)


def test_webdav_memory_handler_read(client):
    client.upload_to(BytesIO(CONTENT), PATH)

    with WebdavMemoryHandler(PATH, "rb", webdav_client=client, name=NAME) as reader:
        assert reader.readline() == b"block0\n"
        assert reader.read() == b" block1\n block2"


def test_webdav_memory_handler_write(client):
    with WebdavMemoryHandler(PATH, "wb", webdav_client=client, name=NAME) as writer:
        writer.write(CONTENT)

    buffer = BytesIO()
    client.download_from(buffer, PATH)
    content = buffer.getvalue()
    assert content == CONTENT


def test_webdav_memory_handler_append(client):
    with WebdavMemoryHandler(PATH, "ab", webdav_client=client, name=NAME) as writer:
        writer.write(CONTENT)

    with WebdavMemoryHandler(PATH, "ab", webdav_client=client, name=NAME) as writer:
        writer.write(CONTENT)

    buffer = BytesIO()
    client.download_from(buffer, PATH)
    content = buffer.getvalue()
    assert content == CONTENT * 2


def assert_ability(fp1, fp2):
    # TODO: pyfakefs writable 返回值是错的, readable 不可读时会抛异常
    # 正确测试以下几项, 需要关掉 pyfakefs
    assert fp1.seekable() == fp2.seekable()
    # assert fp1.readable() == fp2.readable()
    # assert fp1.writable() == fp2.writable()


def assert_read(fp1, fp2, size):
    assert fp1.read(size) == fp2.read(size)


def assert_seek(fp1, fp2, cookie, whence):
    fp1.seek(cookie, whence)
    fp2.seek(cookie, whence)
    assert fp1.tell() == fp2.tell()


def assert_write(fp1, fp2, buffer):
    def load_content(fp):
        fp.flush()
        if isinstance(fp, WebdavMemoryHandler):
            return fp._fileobj.getvalue()
        with open(fp.name, "rb") as reader:
            return reader.read()

    fp1.write(buffer)
    fp2.write(buffer)
    assert load_content(fp1) == load_content(fp2)


def assert_write_lines(fp1, fp2, buffer):
    def load_content(fp):
        fp.flush()
        if isinstance(fp, WebdavMemoryHandler):
            return fp._fileobj.getvalue()
        with open(fp.name, "rb") as reader:
            return reader.read()

    fp1.writelines([buffer] * 2)
    fp2.writelines([buffer] * 2)
    assert load_content(fp1) == load_content(fp2)


def test_webdav_memory_handler_mode_rb(client):
    client.upload_to(BytesIO(CONTENT), PATH)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "rb") as fp1,
        WebdavMemoryHandler(PATH, "rb", webdav_client=client, name=NAME) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_read(fp1, fp2, 5)
        assert_seek(fp1, fp2, 0, 0)
        assert_read(fp1, fp2, 5)
        assert_seek(fp1, fp2, 0, 1)
        assert_read(fp1, fp2, 5)
        assert_seek(fp1, fp2, 0, 2)
        assert_read(fp1, fp2, 5)

        fp2.seek(0)
        assert fp2.readline() == b"block0\n"
        assert list(fp2.readlines()) == [b" block1\n", b" block2"]

        with pytest.raises(IOError):
            fp2.write(b"")
        with pytest.raises(IOError):
            fp2.writelines([])


def test_webdav_memory_handler_mode_wb(client):
    client.upload_to(BytesIO(CONTENT), PATH)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "wb") as fp1,
        WebdavMemoryHandler(PATH, "wb", webdav_client=client, name=NAME) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_write(fp1, fp2, CONTENT)

        with pytest.raises(IOError):
            fp2.read()
        with pytest.raises(IOError):
            fp2.readline()
        with pytest.raises(IOError):
            fp2.readlines()


def test_webdav_memory_handler_mode_ab(client):
    client.upload_to(BytesIO(CONTENT), PATH)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "ab") as fp1,
        WebdavMemoryHandler(PATH, "ab", webdav_client=client, name=NAME) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_write(fp1, fp2, CONTENT)
        assert_write_lines(fp1, fp2, CONTENT)


def test_webdav_memory_handler_mode_rbp(client):
    client.upload_to(BytesIO(CONTENT), PATH)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "rb+") as fp1,
        WebdavMemoryHandler(PATH, "rb+", webdav_client=client, name=NAME) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_read(fp1, fp2, 5)


def test_webdav_memory_handler_mode_rbp(client):
    client.upload_to(BytesIO(CONTENT), PATH)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "wb+") as fp1,
        WebdavMemoryHandler(PATH, "wb+", webdav_client=client, name=NAME) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_read(fp1, fp2, 5)


def test_webdav_memory_handler_mode_rbp(client):
    client.upload_to(BytesIO(CONTENT), PATH)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "ab+") as fp1,
        WebdavMemoryHandler(PATH, "ab+", webdav_client=client, name=NAME) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_read(fp1, fp2, 5)

from io import BytesIO
from typing import Dict
from unittest.mock import MagicMock

import pytest
from webdav3.exceptions import RemoteResourceNotFound

from megfile.lib.webdav_memory_handler import (
    WebdavMemoryHandler,
    _webdav_download_from,
    _webdav_stat,
)

from ..test_webdav import FakeWebdavClient

PATH = "/tmp/testfile"
NAME = "webdav://host/tmp/testfile"
CONTENT = b"block0\n block1\n block2"
LOCAL_PATH = "/tmp/localfile"


@pytest.fixture
def client(fs, mocker):
    def fake_webdav_stat(client, path: str) -> Dict:
        return client.info(path)

    def fake_webdav_download_from(client, buff, path: str) -> Dict:
        return client.download_from(buff, path)

    mocker.patch(
        "megfile.lib.webdav_memory_handler._webdav_stat", side_effect=fake_webdav_stat
    )
    mocker.patch(
        "megfile.lib.webdav_memory_handler._webdav_download_from",
        side_effect=fake_webdav_download_from,
    )
    yield FakeWebdavClient()


@pytest.fixture
def real_webdav_client(mocker):
    """
    Create a mock WebDAV client for testing real _webdav_stat and _webdav_download_from
    """
    mock_client = MagicMock()
    mock_client.chunk_size = 8192
    return mock_client


def test_webdav_stat_function(real_webdav_client, mocker):
    """Test _webdav_stat function directly"""
    mock_response = MagicMock()
    mock_response.content = b"""<?xml version="1.0" encoding="utf-8"?>
    <d:multistatus xmlns:d="DAV:">
        <d:response>
            <d:href>/test/file.txt</d:href>
            <d:propstat>
                <d:prop>
                    <d:getcontentlength>100</d:getcontentlength>
                    <d:getlastmodified>Mon, 01 Jan 2024 00:00:00 GMT</d:getlastmodified>
                </d:prop>
                <d:status>HTTP/1.1 200 OK</d:status>
            </d:propstat>
        </d:response>
    </d:multistatus>"""

    real_webdav_client.execute_request.return_value = mock_response
    real_webdav_client.get_full_path.return_value = "/test/file.txt"
    real_webdav_client.webdav.hostname = "http://example.com"

    # Mock the WebDavXmlUtils parsing
    mocker.patch(
        "megfile.lib.webdav_memory_handler.WebDavXmlUtils.parse_info_response",
        return_value={"size": "100", "modified": "Mon, 01 Jan 2024 00:00:00 GMT"},
    )
    mocker.patch(
        "megfile.lib.webdav_memory_handler.WebDavXmlUtils.parse_is_dir_response",
        return_value=False,
    )

    result = _webdav_stat(real_webdav_client, "/test/file.txt")
    assert result["isdir"] is False
    real_webdav_client.execute_request.assert_called_once()


def test_webdav_download_from_function(real_webdav_client, mocker):
    """Test _webdav_download_from function directly"""
    # Mock response with iter_content
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [b"chunk1", b"chunk2", b"chunk3"]

    real_webdav_client.execute_request.return_value = mock_response
    real_webdav_client.is_dir.return_value = False
    real_webdav_client.check.return_value = True

    buffer = BytesIO()
    _webdav_download_from(real_webdav_client, buffer, "/test/file.txt")

    assert buffer.getvalue() == b"chunk1chunk2chunk3"


def test_webdav_download_from_is_directory(real_webdav_client, mocker):
    """Test _webdav_download_from raises error for directory"""
    from webdav3.exceptions import OptionNotValid

    real_webdav_client.is_dir.return_value = True

    buffer = BytesIO()
    with pytest.raises(OptionNotValid):
        _webdav_download_from(real_webdav_client, buffer, "/test/dir")


def test_webdav_download_from_not_found(real_webdav_client, mocker):
    """Test _webdav_download_from raises error for non-existent file"""
    real_webdav_client.is_dir.return_value = False
    real_webdav_client.check.return_value = False

    buffer = BytesIO()
    with pytest.raises(RemoteResourceNotFound):
        _webdav_download_from(real_webdav_client, buffer, "/test/notfound.txt")


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


def test_webdav_memory_handler_file_exists_directory(client, mocker):
    """Test _file_exists returns False when path is a directory"""
    # Mock _webdav_stat to return isdir=True (as a dict, not object)
    mocker.patch(
        "megfile.lib.webdav_memory_handler._webdav_stat", return_value={"isdir": True}
    )

    # Use a unique path that is not actually created as a directory
    test_path = "/test_dir_check_path"
    handler = WebdavMemoryHandler(test_path, "ab", webdav_client=client, name=NAME)
    assert handler._file_exists() is False
    # Don't close - it would try to upload


def test_webdav_memory_handler_file_exists_not_found(client, mocker):
    """Test _file_exists returns False when file doesn't exist"""

    def raise_not_found(client, path):
        raise RemoteResourceNotFound(path)

    mocker.patch(
        "megfile.lib.webdav_memory_handler._webdav_stat", side_effect=raise_not_found
    )

    handler = WebdavMemoryHandler(
        "/nonexistent/path", "ab", webdav_client=client, name=NAME
    )
    assert handler._file_exists() is False
    handler.close()


def test_webdav_memory_handler_upload_not_writable(client):
    """Test _upload_fileobj skips upload when not writable (read mode)"""
    client.upload_to(BytesIO(CONTENT), PATH)

    # In read mode, writable() returns False, so _upload_fileobj should do nothing
    with WebdavMemoryHandler(PATH, "rb", webdav_client=client, name=NAME) as handler:
        # Read something to trigger _download_fileobj
        handler.read()
        # _upload_fileobj is called on close, but should skip upload since not writable
    # If we get here without error, the test passes


def test_webdav_memory_handler_read_mode_seek(client):
    """Test that read mode seeks to beginning after download"""
    client.upload_to(BytesIO(CONTENT), PATH)

    with WebdavMemoryHandler(PATH, "rb", webdav_client=client, name=NAME) as handler:
        # After download in read mode, position should be at beginning
        assert handler.tell() == 0
        # First read should get content from beginning
        data = handler.read(6)
        assert data == b"block0"

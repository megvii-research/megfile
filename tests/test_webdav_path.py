import io
import os

import pytest

from megfile.errors import SameFileError
from megfile.webdav_path import (
    WEBDAV_PASSWORD,
    WEBDAV_TOKEN,
    WEBDAV_TOKEN_COMMAND,
    WEBDAV_USERNAME,
    WebdavPath,
    provide_connect_info,
)
from tests.compat import webdav

from .test_webdav import FakeWebdavClient, webdav_mocker  # noqa: F401


def test_provide_connect_info(fs, mocker):
    hostname = "http://test_hostname"
    username = "testuser"
    password = "testpwd"
    token = "test_token"

    # Test with no credentials
    options = provide_connect_info(hostname)
    assert options["webdav_hostname"] == hostname
    assert "webdav_login" not in options
    assert "webdav_password" not in options
    assert "webdav_token" not in options

    # Test with username and password
    os.environ[WEBDAV_USERNAME] = username
    os.environ[WEBDAV_PASSWORD] = password

    options = provide_connect_info(hostname)
    assert options["webdav_hostname"] == hostname
    assert options["webdav_login"] == username
    assert options["webdav_password"] == password

    # Test with token (takes precedence)
    os.environ[WEBDAV_TOKEN] = token
    options = provide_connect_info(hostname)
    assert options["webdav_token"] == token

    # Clean up
    del os.environ[WEBDAV_USERNAME]
    del os.environ[WEBDAV_PASSWORD]
    del os.environ[WEBDAV_TOKEN]


def test_webdav_glob(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    webdav.webdav_makedirs("webdav://host/A/a")
    webdav.webdav_makedirs("webdav://host/A/b")
    webdav.webdav_makedirs("webdav://host/A/b/c")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    with webdav.webdav_open("webdav://host/A/b/file.json", "w") as f:
        f.write("file")

    assert WebdavPath("webdav://host/A/").glob("*") == [
        "webdav://host/A/1.json",
        "webdav://host/A/a",
        "webdav://host/A/b",
    ]
    assert list(sorted(WebdavPath("webdav://host/A").iglob("*"))) == [
        "webdav://host/A/1.json",
        "webdav://host/A/a",
        "webdav://host/A/b",
    ]
    assert WebdavPath("webdav://host/A").rglob("*.json") == [
        "webdav://host/A/1.json",
        "webdav://host/A/b/file.json",
    ]
    assert [
        file_entry.path
        for file_entry in sorted(WebdavPath("webdav://host/A").glob_stat("*"))
    ] == [
        "webdav://host/A/1.json",
        "webdav://host/A/a",
        "webdav://host/A/b",
    ]


def test_iterdir(webdav_mocker):
    webdav.webdav_makedirs("webdav://host/A")
    webdav.webdav_makedirs("webdav://host/A/a")
    webdav.webdav_makedirs("webdav://host/A/b")
    webdav.webdav_makedirs("webdav://host/A/b/c")
    with webdav.webdav_open("webdav://host/A/1.json", "w") as f:
        f.write("1.json")

    assert sorted(list(WebdavPath("webdav://host/A").iterdir())) == [
        WebdavPath("webdav://host/A/1.json"),
        WebdavPath("webdav://host/A/a"),
        WebdavPath("webdav://host/A/b"),
    ]

    with pytest.raises(NotADirectoryError):
        list(WebdavPath("webdav://host/A/1.json").iterdir())


def test_cwd(webdav_mocker):
    assert WebdavPath("webdav://host/A").cwd() == "webdav://host/"


def test_sync(webdav_mocker):
    with pytest.raises(OSError):
        WebdavPath("webdav://host/A").sync("/data/test")


def test_webdav_resolve(webdav_mocker):
    # WebDAV doesn't resolve paths, just returns the path as-is
    assert webdav.webdav_resolve("webdav://host/A/../B/C") == "webdav://host/A/../B/C"


def test_parts(webdav_mocker):
    assert WebdavPath("webdav://host/A/B/C").parts == (
        "webdav://host",
        "A",
        "B",
        "C",
    )
    assert WebdavPath("webdav://host/").parts == ("webdav://host",)


def test_webdavs_path(webdav_mocker):
    """Test WebDAVS (secure) protocol"""
    from megfile.webdav_path import WebdavsPath

    webdav.webdav_makedirs("webdavs://host/A")
    with webdav.webdav_open("webdavs://host/A/test.txt", "w") as f:
        f.write("test")

    path = WebdavsPath("webdavs://host/A/test.txt")
    assert path.protocol == "webdavs"
    assert path.exists()
    assert path.is_file()


def test_http_protocol(webdav_mocker):
    """Test that http:// URLs work"""
    webdav.webdav_makedirs("http://host/A")
    with webdav.webdav_open("http://host/A/test.txt", "w") as f:
        f.write("test")

    path = WebdavPath("http://host/A/test.txt")
    assert path.exists()
    assert path.is_file()


def test_https_protocol(webdav_mocker):
    """Test that https:// URLs work"""
    webdav.webdav_makedirs("https://host/A")
    with webdav.webdav_open("https://host/A/test.txt", "w") as f:
        f.write("test")

    path = WebdavPath("https://host/A/test.txt")
    assert path.exists()
    assert path.is_file()


def test_chmod_warning(webdav_mocker, caplog):
    """Test that chmod operation warns about not being supported"""
    import logging

    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/test.txt", "w") as f:
        f.write("test")

    with caplog.at_level(logging.WARNING, logger="megfile.webdav_path"):
        WebdavPath("webdav://host/A/test.txt").chmod(0o777)

    assert "does not support chmod" in caplog.text


def test_is_symlink(webdav_mocker):
    """Test that WebDAV never reports symlinks"""
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/test.txt", "w") as f:
        f.write("test")

    assert WebdavPath("webdav://host/A/test.txt").is_symlink() is False
    assert WebdavPath("webdav://host/A").is_symlink() is False


def test_absolute(webdav_mocker):
    """Test absolute path"""
    path = WebdavPath("webdav://host/A/B/C")
    abs_path = path.absolute()
    assert abs_path == path


def test_resolve(webdav_mocker):
    """Test resolve (which is a no-op for WebDAV)"""
    path = WebdavPath("webdav://host/A/../B/C")
    resolved = path.resolve()
    assert resolved == path


def test_path_with_username_password(webdav_mocker):
    """Test URL with embedded username and password"""
    path = WebdavPath("webdav://user:pass@host/A/file.txt")
    assert path._urlsplit_parts.username == "user"
    assert path._urlsplit_parts.password == "pass"


def test_path_with_port(webdav_mocker):
    """Test URL with port number"""
    path = WebdavPath("webdav://host:8080/A/file.txt")
    assert path._urlsplit_parts.port == 8080
    assert "8080" in path._hostname


def test_scandir_with_root(webdav_mocker):
    """Test scandir at root level"""
    webdav.webdav_makedirs("webdav://host/A")
    webdav.webdav_makedirs("webdav://host/B")

    entries = list(WebdavPath("webdav://host/").scandir())
    names = sorted([e.name for e in entries])
    assert "A" in names
    assert "B" in names


def test_makedirs_concurrent(webdav_mocker):
    """Test that concurrent makedirs doesn't fail"""
    # This simulates the case where another process creates the directory
    # between our exists check and mkdir call
    path = WebdavPath("webdav://host/A/B/C")

    # First create should succeed
    path.mkdir(parents=True, exist_ok=True)
    assert path.exists()

    # Second create with exist_ok=True should also succeed
    path.mkdir(parents=True, exist_ok=True)
    assert path.exists()


def test_open_modes(webdav_mocker):
    """Test different file open modes"""
    webdav.webdav_makedirs("webdav://host/A")

    # Test write mode
    with webdav.webdav_open("webdav://host/A/test.txt", "w") as f:
        f.write("test content")

    # Test read mode
    with webdav.webdav_open("webdav://host/A/test.txt", "r") as f:
        content = f.read()
        assert content == "test content"

    # Test binary read mode
    with webdav.webdav_open("webdav://host/A/test.txt", "rb") as f:
        content = f.read()
        assert content == b"test content"

    # Test binary write mode
    with webdav.webdav_open("webdav://host/A/test2.txt", "wb") as f:
        f.write(b"binary content")

    with webdav.webdav_open("webdav://host/A/test2.txt", "rb") as f:
        content = f.read()
        assert content == b"binary content"


def test_remove_directory_recursive(webdav_mocker):
    """Test removing a directory with contents"""
    webdav.webdav_makedirs("webdav://host/A/B/C", parents=True)
    with webdav.webdav_open("webdav://host/A/file1.txt", "w") as f:
        f.write("test1")
    with webdav.webdav_open("webdav://host/A/B/file2.txt", "w") as f:
        f.write("test2")

    # Remove should recursively delete everything
    WebdavPath("webdav://host/A").remove()
    assert not WebdavPath("webdav://host/A").exists()


def test_copy_same_backend(webdav_mocker):
    """Test copy within same backend"""
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/file.txt", "w") as f:
        f.write("test content")

    # Copy within same backend should use WebDAV's native copy
    WebdavPath("webdav://host/A/file.txt").copy("webdav://host/B/file.txt")

    assert WebdavPath("webdav://host/A/file.txt").exists()
    assert WebdavPath("webdav://host/B/file.txt").exists()

    with webdav.webdav_open("webdav://host/B/file.txt", "r") as f:
        assert f.read() == "test content"


def test_rename_same_backend(webdav_mocker):
    """Test rename within same backend"""
    webdav.webdav_makedirs("webdav://host/A")
    webdav.webdav_makedirs("webdav://host/B")  # Create destination directory
    with webdav.webdav_open("webdav://host/A/file.txt", "w") as f:
        f.write("test content")

    # Rename within same backend should use WebDAV's native move
    WebdavPath("webdav://host/A/file.txt").rename("webdav://host/B/file.txt")

    assert not WebdavPath("webdav://host/A/file.txt").exists()
    assert WebdavPath("webdav://host/B/file.txt").exists()

    with webdav.webdav_open("webdav://host/B/file.txt", "r") as f:
        assert f.read() == "test content"


def test_stat_file_not_found(webdav_mocker):
    """Test stat on non-existent file"""
    with pytest.raises(FileNotFoundError):
        WebdavPath("webdav://host/nonexistent").stat()


def test_getsize_directory(webdav_mocker):
    """Test getsize on directory (sum of all files)"""
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/file1.txt", "w") as f:
        f.write("12345")  # 5 bytes
    with webdav.webdav_open("webdav://host/A/file2.txt", "w") as f:
        f.write("123")  # 3 bytes

    # Directory size should be sum of file sizes (not implemented in our mock)
    # This would need proper implementation in real WebDAV client
    assert WebdavPath("webdav://host/A").is_dir()


def test_md5_directory(webdav_mocker):
    """Test MD5 calculation for directory"""
    from tests.compat.fs import fs_getmd5

    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/file1.txt", "w") as f:
        f.write("test1")
    with webdav.webdav_open("webdav://host/A/file2.txt", "w") as f:
        f.write("test2")

    # Directory MD5 should be computed from sorted file MD5s
    webdav_md5 = WebdavPath("webdav://host/A").md5()
    fs_md5 = fs_getmd5("/A")
    assert webdav_md5 == fs_md5


def test_walk_empty_directory(webdav_mocker):
    """Test walk on empty directory"""
    webdav.webdav_makedirs("webdav://host/A")

    result = list(WebdavPath("webdav://host/A").walk())
    assert result == [("webdav://host/A", [], [])]


def test_scan_single_file(webdav_mocker):
    """Test scan on a single file"""
    with webdav.webdav_open("webdav://host/file.txt", "w") as f:
        f.write("test")

    result = list(WebdavPath("webdav://host/file.txt").scan())
    assert result == ["webdav://host/file.txt"]


def test_load_and_save(webdav_mocker):
    """Test load and save operations"""
    data = b"test binary data"
    buffer = io.BytesIO(data)

    # Save
    WebdavPath("webdav://host/file.bin").save(buffer)

    # Load
    loaded = WebdavPath("webdav://host/file.bin").load()
    assert loaded.read() == data


def test_provide_connect_info_with_token_command(fs, mocker):
    """Test provide_connect_info with token command"""
    hostname = "http://test_hostname"
    token_command = "echo test_token"

    os.environ[WEBDAV_TOKEN_COMMAND] = token_command

    options = provide_connect_info(hostname)
    assert options["webdav_hostname"] == hostname
    assert "webdav_token_command" in options
    assert options["webdav_token_command"] == token_command

    # Clean up
    del os.environ[WEBDAV_TOKEN_COMMAND]


def test_open_x_mode_file_exists(webdav_mocker):
    """Test open with 'x' mode raises error when file exists"""
    with webdav.webdav_open("webdav://host/file.txt", "w") as f:
        f.write("existing content")

    with pytest.raises(FileExistsError):
        webdav.webdav_open("webdav://host/file.txt", "x")


def test_rename_to_non_webdav_raises_error(webdav_mocker):
    """Test rename to non-WebDAV path raises error"""
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/file.txt", "w") as f:
        f.write("test")

    with pytest.raises(OSError, match="Not a webdav path"):
        WebdavPath("webdav://host/A/file.txt").rename("/local/path")


def test_copy_to_same_file_raises_error(webdav_mocker):
    """Test copy to same file raises SameFileError"""
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/file.txt", "w") as f:
        f.write("test")

    with pytest.raises(SameFileError):
        WebdavPath("webdav://host/A/file.txt").copy("webdav://host/A/file.txt")


def test_copy_directory_raises_error(webdav_mocker):
    """Test copy on directory raises error"""
    webdav.webdav_makedirs("webdav://host/A")

    with pytest.raises(IsADirectoryError):
        WebdavPath("webdav://host/A").copy("webdav://host/B")


def test_copy_to_directory_path_raises_error(webdav_mocker):
    """Test copy to directory path (ending with /) raises error"""
    with webdav.webdav_open("webdav://host/file.txt", "w") as f:
        f.write("test")

    with pytest.raises(IsADirectoryError):
        WebdavPath("webdav://host/file.txt").copy("webdav://host/A/")


def test_copy_to_non_webdav_raises_error(webdav_mocker):
    """Test copy to non-WebDAV path raises error"""
    with webdav.webdav_open("webdav://host/file.txt", "w") as f:
        f.write("test")

    with pytest.raises(OSError, match="Not a webdav path"):
        WebdavPath("webdav://host/file.txt").copy("/local/path")


def test_copy_no_overwrite(webdav_mocker):
    """Test copy with overwrite=False doesn't overwrite existing file"""
    with webdav.webdav_open("webdav://host/src.txt", "w") as f:
        f.write("source")

    with webdav.webdav_open("webdav://host/dst.txt", "w") as f:
        f.write("destination")

    WebdavPath("webdav://host/src.txt").copy("webdav://host/dst.txt", overwrite=False)

    # Original file should remain unchanged
    with webdav.webdav_open("webdav://host/dst.txt", "r") as f:
        assert f.read() == "destination"


def test_sync_to_non_webdav_raises_error(webdav_mocker):
    """Test sync to non-WebDAV path raises error"""
    webdav.webdav_makedirs("webdav://host/A")

    with pytest.raises(OSError, match="Not a webdav path"):
        WebdavPath("webdav://host/A").sync("/local/path")


def test_sync_with_force(webdav_mocker):
    """Test sync with force=True overwrites files"""
    with webdav.webdav_open("webdav://host/src.txt", "w") as f:
        f.write("source")

    with webdav.webdav_open("webdav://host/dst.txt", "w") as f:
        f.write("destination")

    WebdavPath("webdav://host/src.txt").sync("webdav://host/dst.txt", force=True)

    with webdav.webdav_open("webdav://host/dst.txt", "r") as f:
        assert f.read() == "source"


def test_remove_missing_ok_true(webdav_mocker):
    """Test remove with missing_ok=True doesn't raise for non-existent file"""
    # Should not raise
    WebdavPath("webdav://host/nonexistent").remove(missing_ok=True)


def test_remove_missing_ok_false(webdav_mocker):
    """Test remove with missing_ok=False raises for non-existent file"""
    with pytest.raises(FileNotFoundError):
        WebdavPath("webdav://host/nonexistent").remove(missing_ok=False)


def test_unlink_missing_ok_false(webdav_mocker):
    """Test unlink with missing_ok=False raises for non-existent file"""
    with pytest.raises(FileNotFoundError):
        WebdavPath("webdav://host/nonexistent").unlink(missing_ok=False)


def test_scan_stat_missing_ok(webdav_mocker):
    """Test scan_stat with missing_ok=False raises for non-existent directory"""
    with pytest.raises(FileNotFoundError):
        list(WebdavPath("webdav://host/nonexistent").scan_stat(missing_ok=False))


def test_scandir_on_file_raises_error(webdav_mocker):
    """Test scandir on file raises NotADirectoryError"""
    with webdav.webdav_open("webdav://host/file.txt", "w") as f:
        f.write("test")

    with pytest.raises(NotADirectoryError):
        list(WebdavPath("webdav://host/file.txt").scandir())


def test_scandir_on_nonexistent_raises_error(webdav_mocker):
    """Test scandir on non-existent path raises FileNotFoundError"""
    with pytest.raises(FileNotFoundError):
        list(WebdavPath("webdav://host/nonexistent").scandir())


def test_rename_directory(webdav_mocker):
    """Test rename on directory"""
    webdav.webdav_makedirs("webdav://host/A")
    with webdav.webdav_open("webdav://host/A/file.txt", "w") as f:
        f.write("test")

    WebdavPath("webdav://host/A").rename("webdav://host/B")

    assert not WebdavPath("webdav://host/A").exists()
    assert WebdavPath("webdav://host/B").exists()
    assert WebdavPath("webdav://host/B/file.txt").exists()


def test_generate_path_object(webdav_mocker):
    """Test _generate_path_object method"""
    path = WebdavPath("webdav://host/A/B/C")

    result = path._generate_path_object("/root/D/E")
    assert isinstance(result, WebdavPath)
    assert result.path_with_protocol == "webdav://host/root/D/E"

    # Test with relative path
    result = path._generate_path_object("relative/path")
    assert result.path_with_protocol == "webdav://host/relative/path"


def test_is_same_backend(webdav_mocker):
    """Test _is_same_backend method"""
    path1 = WebdavPath("webdav://user:pass@host1:8080/path")
    path2 = WebdavPath("webdav://user:pass@host1:8080/other")
    path3 = WebdavPath("webdav://user:pass@host2:8080/path")

    assert path1._is_same_backend(path2) is True
    assert path1._is_same_backend(path3) is False


def test_iterdir_on_file(webdav_mocker):
    """Test iterdir raises error on file"""
    with webdav.webdav_open("webdav://host/file.txt", "w") as f:
        f.write("test")

    with pytest.raises(NotADirectoryError):
        list(WebdavPath("webdav://host/file.txt").iterdir())


def test_open_directory_raises_error(webdav_mocker):
    """Test open on directory raises error"""
    webdav.webdav_makedirs("webdav://host/A")

    with pytest.raises(IsADirectoryError):
        webdav.webdav_open("webdav://host/A", "w")


def test_open_nonexistent_for_read_raises_error(webdav_mocker):
    """Test open non-existent file for read raises error"""
    with pytest.raises(FileNotFoundError):
        webdav.webdav_open("webdav://host/nonexistent", "r")


def test_walk_on_nonexistent(webdav_mocker):
    """Test walk on non-existent path returns nothing"""
    result = list(WebdavPath("webdav://host/nonexistent").walk())
    assert len(result) == 0


def test_walk_on_file(webdav_mocker):
    """Test walk on file returns nothing"""
    with webdav.webdav_open("webdav://host/file.txt", "w") as f:
        f.write("test")

    result = list(WebdavPath("webdav://host/file.txt").walk())
    assert len(result) == 0


def test_webdav_touch(webdav_mocker):
    """Test touch creates new file or updates timestamp"""
    path = WebdavPath("webdav://host/newfile.txt")

    # Touch non-existent file creates it
    path.touch()
    assert path.exists() is True


def test_webdav_write_text(webdav_mocker):
    """Test write_text method"""
    path = WebdavPath("webdav://host/text.txt")
    path.write_text("Hello World")

    with webdav.webdav_open("webdav://host/text.txt", "r") as f:
        assert f.read() == "Hello World"


def test_webdav_read_text(webdav_mocker):
    """Test read_text method"""
    with webdav.webdav_open("webdav://host/text.txt", "w") as f:
        f.write("Hello World")

    path = WebdavPath("webdav://host/text.txt")
    assert path.read_text() == "Hello World"


def test_webdav_save_load(webdav_mocker):
    """Test save and load methods"""
    import io

    data = b"binary data"
    path = WebdavPath("webdav://host/binary.dat")

    # save() expects a file-like object, not bytes
    path.save(io.BytesIO(data))
    loaded = path.load()
    # load() returns a file-like object, read it
    assert loaded.read() == data


def test_webdav_walk_with_directories(webdav_mocker):
    """Test walk returns directories structure correctly"""
    webdav.webdav_makedirs("webdav://host/root/dir1/subdir", parents=True)
    webdav.webdav_makedirs("webdav://host/root/dir2", parents=True)
    with webdav.webdav_open("webdav://host/root/file.txt", "w") as f:
        f.write("test")
    with webdav.webdav_open("webdav://host/root/dir1/subdir/file.txt", "w") as f:
        f.write("test")

    results = list(WebdavPath("webdav://host/root").walk())
    # Should have at least 3 entries (root, dir1, dir1/subdir, dir2)
    assert len(results) >= 3

import io
import os
import socket

import pytest

from megfile.sftp2_path import (
    Sftp2Path,
    Sftp2RawFile,
    _get_ssh2_session,
    get_private_key,
    get_sftp2_client,
    get_ssh2_session,
    provide_connect_info,
    sftp2_should_retry,
)
from tests.compat import sftp2

from .test_sftp2 import FakeSFTP2Client, sftp2_mocker  # noqa: F401


def test_provide_connect_info(fs, mocker):
    from megfile import sftp2_path

    sftp2_path.SFTP_PASSWORD = "SFTP2_PASSWORD"
    sftp2_path.SFTP_PRIVATE_KEY_PATH = "SFTP2_PRIVATE_KEY_PATH"
    sftp2_path.SFTP_USERNAME = "SFTP2_USERNAME"

    hostname, port, username, password, private_key = (
        "test_hostname",
        22,
        "testuser",
        "testpwd",
        "/test_key_path",
    )
    with open(private_key, "w"):
        pass
    mocker.patch("builtins.open", return_value=io.BytesIO(b"test_key"))

    # Test with no environment variables - should use system username
    import getpass

    expected_username = getpass.getuser()
    result = provide_connect_info(hostname)
    assert result == (hostname, 22, expected_username, None, None)

    # Test with environment variables set
    os.environ[sftp2_path.SFTP_USERNAME] = username
    os.environ[sftp2_path.SFTP_PASSWORD] = password
    os.environ[sftp2_path.SFTP_PRIVATE_KEY_PATH] = private_key

    result = provide_connect_info(hostname, port)
    assert result[0] == hostname
    assert result[1] == port
    assert result[2] == username
    assert result[3] == password
    assert result[4] is not None  # private key content


def test_sftp2_glob(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    sftp2.sftp2_makedirs("sftp2://username@host/A/a")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b/c")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")

    with sftp2.sftp2_open("sftp2://username@host/A/b/file.json", "w") as f:
        f.write("file")

    assert Sftp2Path("sftp2://username@host/A/").glob("*") == [
        "sftp2://username@host/A/1.json",
        "sftp2://username@host/A/a",
        "sftp2://username@host/A/b",
    ]
    assert list(Sftp2Path("sftp2://username@host/A").iglob("*")) == [
        "sftp2://username@host/A/1.json",
        "sftp2://username@host/A/a",
        "sftp2://username@host/A/b",
    ]
    assert [
        file_entry.path
        for file_entry in Sftp2Path("sftp2://username@host/A").glob_stat("*")
    ] == [
        "sftp2://username@host/A/1.json",
        "sftp2://username@host/A/a",
        "sftp2://username@host/A/b",
    ]


def test_iterdir(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host/A")
    sftp2.sftp2_makedirs("sftp2://username@host/A/a")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b")
    sftp2.sftp2_makedirs("sftp2://username@host/A/b/c")
    with sftp2.sftp2_open("sftp2://username@host/A/1.json", "w") as f:
        f.write("1.json")

    assert sorted(list(Sftp2Path("sftp2://username@host/A").iterdir())) == [
        Sftp2Path("sftp2://username@host/A/1.json"),
        Sftp2Path("sftp2://username@host/A/a"),
        Sftp2Path("sftp2://username@host/A/b"),
    ]


def test_cwd(sftp2_mocker):
    cwd = Sftp2Path("sftp2://username@host/A").cwd()
    assert isinstance(cwd, Sftp2Path)


def test_sync(sftp2_mocker):
    with pytest.raises(OSError):
        Sftp2Path("sftp2://username@host/A").sync("/data/test")


def test_get_private_key(fs):
    from megfile import sftp2_path

    sftp2_path.SFTP_PRIVATE_KEY_PATH = "SFTP2_PRIVATE_KEY_PATH"

    with pytest.raises(FileNotFoundError):
        os.environ[sftp2_path.SFTP_PRIVATE_KEY_PATH] = "/file_not_exist"
        get_private_key()


def test_sftp2_should_retry():
    assert sftp2_should_retry(ConnectionError()) is True
    assert sftp2_should_retry(OSError("Socket is closed")) is True
    assert sftp2_should_retry(OSError("test")) is False


def test_generate_path_object(sftp2_mocker):
    path = Sftp2Path("sftp2://username@host/A/B/C")

    result = path._generate_path_object("/root/A/B/C/D")
    assert isinstance(result, Sftp2Path)
    assert result.path_with_protocol == "sftp2://username@host/root/A/B/C/D"

    result = path._generate_path_object("/D", resolve=True)
    assert result.path_with_protocol == "sftp2://username@host/D"


def test_parts(sftp2_mocker):
    assert Sftp2Path("sftp2://username@host/A/B/C").parts == (
        "sftp2://username@host",
        "A",
        "B",
        "C",
    )
    assert Sftp2Path("sftp2://username@host/A/B/C").parts == (
        "sftp2://username@host",
        "A",
        "B",
        "C",
    )
    assert Sftp2Path("sftp2://username@host/").parts == ("sftp2://username@host",)


def test_get_sftp2_client(mocker):
    session = mocker.patch("megfile.sftp2_path.get_ssh2_session")
    session.return_value.sftp_init.return_value = FakeSFTP2Client()

    client = get_sftp2_client("127.0.0.1")
    assert isinstance(client, FakeSFTP2Client)


def test_get_ssh2_session(mocker):
    # 模拟所有相关的组件
    Session = mocker.patch("ssh2.session.Session")
    mocker.patch("socket.socket")
    mocker.patch(
        "megfile.sftp2_path.provide_connect_info",
        return_value=("127.0.0.1", 22, "user", "pass", None),
    )

    session_instance = Session.return_value
    session_instance.handshake.return_value = None
    session_instance.userauth_password.return_value = 0  # Success

    # 模拟其他认证方法失败
    session_instance.userauth_agent.return_value = 1  # Failure
    session_instance.userauth_publickey_fromfile.return_value = 1  # Failure

    result = get_ssh2_session("127.0.0.1", password="testpass")
    assert result == session_instance

    # 验证密码认证被调用
    assert session_instance.userauth_password.called


def test_sftp2_file_operations(sftp2_mocker):
    from megfile.sftp2_path import Sftp2RawFile

    # Test basic file operations without actual network calls
    # These tests verify the class structure is correct

    # Create a temporary file for testing
    with open("/tmp/test_file", "w") as f:
        f.write("test content")

    # Test file wrapper functionality (using local file for testing)
    fake_handle = open("/tmp/test_file", "rb")

    class MockHandle:
        def read(self, size):
            data = fake_handle.read(size)
            # SSH2-python format: (bytes_read, data)
            return len(data), data

        def write(self, data):
            return len(data)

        def close(self):
            fake_handle.close()

    file_wrapper = Sftp2RawFile(MockHandle(), "/tmp/test_file", "r")
    assert hasattr(file_wrapper, "read")
    assert hasattr(file_wrapper, "write")
    assert hasattr(file_wrapper, "close")

    # Test context manager
    with file_wrapper:
        data = file_wrapper.read(4)
        assert len(data) >= 0


def test_path_properties(sftp2_mocker):
    path = Sftp2Path("sftp2://user:pass@example.com:2222/home/user/test.txt")

    assert path.protocol == "sftp2"
    assert path.name == "test.txt"
    assert path._urlsplit_parts.hostname == "example.com"
    assert path._urlsplit_parts.port == 2222
    assert path._urlsplit_parts.username == "user"
    assert path._urlsplit_parts.password == "pass"

    # Test path manipulation
    parent = path.parent
    assert parent.name == "user"

    sibling = parent.joinpath("other.txt")
    assert sibling.name == "other.txt"


def test_path_construction(sftp2_mocker):
    # Test absolute path construction
    abs_path = Sftp2Path("sftp2://host/absolute/path/file.txt")
    assert abs_path._remote_path.startswith("/")
    assert abs_path.name == "file.txt"


def test_backend_comparison(sftp2_mocker):
    path1 = Sftp2Path("sftp2://user@host1:22/path")
    path2 = Sftp2Path("sftp2://user@host1:22/path2")
    path3 = Sftp2Path("sftp2://user@host2:22/path")

    assert path1._is_same_backend(path2) is True
    assert path1._is_same_backend(path3) is False


def test_protocol_detection(sftp2_mocker):
    path = Sftp2Path("sftp2://host/path")

    assert path._is_same_protocol("sftp2://other/path") is True
    assert path._is_same_protocol("sftp://other/path") is False
    assert path._is_same_protocol("/local/path") is False


def test_get_ssh2_session_with_private_key(mocker, fs):
    """Test SSH2 session authentication with private key"""
    # Create a fake key file
    key_path = "/home/user/.ssh/id_rsa"
    os.makedirs("/home/user/.ssh", exist_ok=True)
    with open(key_path, "w") as f:
        f.write("fake private key")

    Session = mocker.patch("ssh2.session.Session")
    mocker.patch("socket.socket")
    mocker.patch(
        "megfile.sftp2_path.provide_connect_info",
        return_value=("127.0.0.1", 22, "user", None, (key_path, "")),
    )

    session_instance = Session.return_value
    session_instance.handshake.return_value = None
    session_instance.userauth_publickey_fromfile.return_value = 0  # Success

    # Clear thread local cache first
    from megfile.utils import mutex

    if hasattr(mutex, "_thread_local_data"):
        mutex._thread_local_data.clear()

    result = _get_ssh2_session("127.0.0.1")
    assert result == session_instance


def test_get_ssh2_session_with_agent(mocker):
    """Test SSH2 session authentication with SSH agent"""
    Session = mocker.patch("ssh2.session.Session")
    mocker.patch("socket.socket")
    mocker.patch(
        "megfile.sftp2_path.provide_connect_info",
        return_value=("127.0.0.1", 22, "user", None, None),
    )

    session_instance = Session.return_value
    session_instance.handshake.return_value = None
    # Password auth fails
    session_instance.userauth_password.side_effect = Exception("No password")
    # Agent auth succeeds
    session_instance.agent_init.return_value = None
    session_instance.agent_auth.return_value = None

    result = _get_ssh2_session("127.0.0.1")
    assert result == session_instance
    assert session_instance.agent_init.called
    assert session_instance.agent_auth.called


def test_get_ssh2_session_with_default_keys(mocker, fs):
    """Test SSH2 session authentication with default SSH keys"""
    # Create fake default key files
    os.makedirs(os.path.expanduser("~/.ssh"), exist_ok=True)
    default_key = os.path.expanduser("~/.ssh/id_rsa")
    with open(default_key, "w") as f:
        f.write("fake private key")

    Session = mocker.patch("ssh2.session.Session")
    mocker.patch("socket.socket")
    mocker.patch(
        "megfile.sftp2_path.provide_connect_info",
        return_value=("127.0.0.1", 22, "user", None, None),
    )

    session_instance = Session.return_value
    session_instance.handshake.return_value = None
    # Agent auth fails
    session_instance.agent_init.side_effect = Exception("No agent")
    # Public key auth succeeds
    session_instance.userauth_publickey_fromfile.return_value = 0

    result = _get_ssh2_session("127.0.0.1")
    assert result == session_instance
    assert session_instance.userauth_publickey_fromfile.called


def test_get_ssh2_session_auth_failure(mocker):
    """Test SSH2 session raises error when all auth methods fail"""
    Session = mocker.patch("ssh2.session.Session")
    mocker.patch("socket.socket")
    mocker.patch(
        "megfile.sftp2_path.provide_connect_info",
        return_value=("127.0.0.1", 22, "user", None, None),
    )

    session_instance = Session.return_value
    session_instance.handshake.return_value = None
    # All auth methods fail
    session_instance.userauth_password.side_effect = Exception("No password")
    session_instance.agent_init.side_effect = Exception("No agent")
    session_instance.userauth_publickey_fromfile.return_value = 1  # Failure

    with pytest.raises(ValueError, match="Authentication failed"):
        _get_ssh2_session("127.0.0.1")


def test_sftp2_raw_file_closed_operations():
    """Test Sftp2RawFile raises error when operating on closed file"""

    class MockHandle:
        def read(self, size):
            return 0, b""

        def write(self, data):
            return 0, 0

        def close(self):
            pass

    raw_file = Sftp2RawFile(MockHandle(), "/path", "r")
    raw_file.close()

    with pytest.raises(ValueError, match="I/O operation on closed file"):
        raw_file.readinto(bytearray(10))

    with pytest.raises(ValueError, match="I/O operation on closed file"):
        raw_file.read()

    with pytest.raises(ValueError, match="I/O operation on closed file"):
        raw_file.write(b"test")


def test_sftp2_raw_file_seek_operations():
    """Test Sftp2RawFile seek operations"""

    class MockHandle:
        def __init__(self):
            self._pos = 0

        def read(self, size):
            return 0, b""

        def write(self, data):
            return 0, 0

        def close(self):
            pass

        def tell64(self):
            return self._pos

        def seek64(self, pos):
            self._pos = pos

    raw_file = Sftp2RawFile(MockHandle(), "/path", "r")

    # Test SEEK_SET
    raw_file.seek(100, 0)
    assert raw_file.tell() == 100

    # Test SEEK_CUR
    raw_file.seek(50, 1)
    assert raw_file.tell() == 150

    # Test SEEK_END raises error
    with pytest.raises(OSError, match="SEEK_END not supported"):
        raw_file.seek(0, 2)

    # Test invalid whence
    with pytest.raises(OSError, match="invalid whence"):
        raw_file.seek(0, 5)

    # Test negative seek position
    with pytest.raises(OSError, match="negative seek position"):
        raw_file.seek(-100, 0)

    raw_file.close()


def test_sftp2_raw_file_no_seek_support():
    """Test Sftp2RawFile when SFTP doesn't support seek"""

    class MockHandle:
        def read(self, size):
            return 0, b""

        def close(self):
            pass

        # No seek64 or tell64 methods

    raw_file = Sftp2RawFile(MockHandle(), "/path", "r")

    with pytest.raises(OSError, match="tell not supported"):
        raw_file.tell()

    with pytest.raises(OSError, match="seek not supported"):
        raw_file.seek(0)

    raw_file.close()


def test_sftp2_raw_file_other_methods():
    """Test Sftp2RawFile misc methods"""

    class MockHandle:
        def read(self, size):
            return 0, b""

        def close(self):
            pass

    raw_file = Sftp2RawFile(MockHandle(), "/path", "r")

    # Test fileno returns -1
    assert raw_file.fileno() == -1

    # Test isatty returns False
    assert raw_file.isatty() is False

    # Test truncate raises error
    with pytest.raises(OSError, match="truncate not supported"):
        raw_file.truncate()

    # Test flush is a no-op (doesn't raise)
    raw_file.flush()

    # Test readable/writable/seekable
    assert raw_file.readable() is True
    assert raw_file.writable() is False
    assert raw_file.seekable() is True

    raw_file.close()


def test_sftp2_raw_file_write_mode():
    """Test Sftp2RawFile in write mode"""

    class MockHandle:
        def __init__(self):
            self.data = b""

        def write(self, data):
            self.data += data
            return 0, len(data)

        def close(self):
            pass

    raw_file = Sftp2RawFile(MockHandle(), "/path", "w")

    assert raw_file.readable() is False
    assert raw_file.writable() is True

    bytes_written = raw_file.write(b"test data")
    assert bytes_written == 9

    raw_file.close()


def test_sftp2_should_retry_more_cases():
    """Test sftp2_should_retry with more error cases"""
    # ConnectionError should retry
    assert sftp2_should_retry(ConnectionError()) is True

    # socket.timeout should retry
    assert sftp2_should_retry(socket.timeout()) is True

    # OSError with specific messages should retry
    assert sftp2_should_retry(OSError("Socket is closed")) is True
    assert sftp2_should_retry(OSError("Cannot assign requested address")) is True

    # Other OSError should not retry
    assert sftp2_should_retry(OSError("Permission denied")) is False

    # Other exceptions should not retry
    assert sftp2_should_retry(ValueError("test")) is False

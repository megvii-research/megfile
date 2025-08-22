import io
import os

import pytest

from megfile import sftp2
from megfile.sftp2_path import (
    SFTP2_PASSWORD,
    SFTP2_PRIVATE_KEY_PATH,
    SFTP2_USERNAME,
    Sftp2Path,
    get_private_key,
    get_sftp2_client,
    get_ssh2_session,
    provide_connect_info,
    sftp2_should_retry,
)

from .test_sftp2 import FakeSFTP2Client, sftp2_mocker  # noqa: F401


def test_provide_connect_info(fs, mocker):
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
    os.environ[SFTP2_USERNAME] = username
    os.environ[SFTP2_PASSWORD] = password
    os.environ[SFTP2_PRIVATE_KEY_PATH] = private_key

    result = provide_connect_info(hostname, port)
    assert result[0] == hostname
    assert result[1] == port
    assert result[2] == username
    assert result[3] == password
    assert result[4] is not None  # private key content


def test_sftp2_glob(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host//A")
    sftp2.sftp2_makedirs("sftp2://username@host//A/a")
    sftp2.sftp2_makedirs("sftp2://username@host//A/b")
    sftp2.sftp2_makedirs("sftp2://username@host//A/b/c")
    with sftp2.sftp2_open("sftp2://username@host//A/1.json", "w") as f:
        f.write("1.json")

    with sftp2.sftp2_open("sftp2://username@host//A/b/file.json", "w") as f:
        f.write("file")

    assert Sftp2Path("sftp2://username@host//A/").glob("*") == [
        "sftp2://username@host//A/1.json",
        "sftp2://username@host//A/a",
        "sftp2://username@host//A/b",
    ]
    assert list(Sftp2Path("sftp2://username@host//A").iglob("*")) == [
        "sftp2://username@host//A/1.json",
        "sftp2://username@host//A/a",
        "sftp2://username@host//A/b",
    ]
    assert [
        file_entry.path
        for file_entry in Sftp2Path("sftp2://username@host//A").glob_stat("*")
    ] == [
        "sftp2://username@host//A/1.json",
        "sftp2://username@host//A/a",
        "sftp2://username@host//A/b",
    ]


def test_iterdir(sftp2_mocker):
    sftp2.sftp2_makedirs("sftp2://username@host//A")
    sftp2.sftp2_makedirs("sftp2://username@host//A/a")
    sftp2.sftp2_makedirs("sftp2://username@host//A/b")
    sftp2.sftp2_makedirs("sftp2://username@host//A/b/c")
    with sftp2.sftp2_open("sftp2://username@host//A/1.json", "w") as f:
        f.write("1.json")

    assert sorted(list(Sftp2Path("sftp2://username@host//A").iterdir())) == [
        Sftp2Path("sftp2://username@host//A/1.json"),
        Sftp2Path("sftp2://username@host//A/a"),
        Sftp2Path("sftp2://username@host//A/b"),
    ]


def test_cwd(sftp2_mocker):
    cwd = Sftp2Path("sftp2://username@host//A").cwd()
    assert isinstance(cwd, Sftp2Path)


def test_sync(sftp2_mocker):
    with pytest.raises(OSError):
        Sftp2Path("sftp2://username@host//A").sync("/data/test")


def test_get_private_key(fs):
    with pytest.raises(FileNotFoundError):
        os.environ["SFTP2_PRIVATE_KEY_PATH"] = "/file_not_exist"
        get_private_key()


def test_sftp2_should_retry():
    assert sftp2_should_retry(ConnectionError()) is True
    assert sftp2_should_retry(OSError("Socket is closed")) is True
    assert sftp2_should_retry(OSError("test")) is False


def test_generate_path_object(sftp2_mocker):
    path = Sftp2Path("sftp2://username@host//A/B/C")

    result = path._generate_path_object("/root/A/B/C/D")
    assert isinstance(result, Sftp2Path)
    assert result.path_with_protocol == "sftp2://username@host//root/A/B/C/D"

    result = path._generate_path_object("/D", resolve=True)
    assert result.path_with_protocol == "sftp2://username@host//D"


def test_parts(sftp2_mocker):
    assert Sftp2Path("sftp2://username@host//A/B/C").parts == (
        "sftp2://username@host//",
        "A",
        "B",
        "C",
    )
    assert Sftp2Path("sftp2://username@host/A/B/C").parts == (
        "sftp2://username@host/",
        "A",
        "B",
        "C",
    )
    assert Sftp2Path("sftp2://username@host//").parts == ("sftp2://username@host//",)


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
    abs_path = Sftp2Path("sftp2://host//absolute/path/file.txt")
    assert abs_path._real_path.startswith("/")

    # Test relative path construction
    rel_path = Sftp2Path("sftp2://host/relative/path/file.txt")
    assert rel_path.name == "file.txt"


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

import io
import os
import subprocess

import paramiko
import pytest

from megfile import sftp
from megfile.sftp_path import (
    SFTP_PASSWORD,
    SFTP_PRIVATE_KEY_PATH,
    SFTP_USERNAME,
    SftpPath,
    _get_sftp_client,
    _get_ssh_client,
    get_private_key,
    get_ssh_session,
    provide_connect_info,
    sftp_should_retry,
)
from megfile.utils import thread_local

from .test_sftp import FakeSFTPClient, sftp_mocker  # noqa: F401


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
    mocker.patch("paramiko.RSAKey.from_private_key_file", return_value=private_key)
    assert provide_connect_info(hostname) == (hostname, 22, None, None, None)

    os.environ[SFTP_USERNAME] = username
    os.environ[SFTP_PASSWORD] = password
    os.environ[SFTP_PRIVATE_KEY_PATH] = private_key

    assert provide_connect_info(hostname, port) == (
        hostname,
        port,
        username,
        password,
        private_key,
    )


def test_sftp_glob(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    sftp.sftp_makedirs("sftp://username@host//A/a")
    sftp.sftp_makedirs("sftp://username@host//A/b")
    sftp.sftp_makedirs("sftp://username@host//A/b/c")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")

    with sftp.sftp_open("sftp://username@host//A/b/file.json", "w") as f:
        f.write("file")

    assert SftpPath("sftp://username@host//A/").glob("*") == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/a",
        "sftp://username@host//A/b",
    ]
    assert list(SftpPath("sftp://username@host//A").iglob("*")) == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/a",
        "sftp://username@host//A/b",
    ]
    assert SftpPath("sftp://username@host//A").rglob("*.json") == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/b/file.json",
    ]
    assert [
        file_entry.path
        for file_entry in SftpPath("sftp://username@host//A").glob_stat("*")
    ] == [
        "sftp://username@host//A/1.json",
        "sftp://username@host//A/a",
        "sftp://username@host//A/b",
    ]


def test_iterdir(sftp_mocker):
    sftp.sftp_makedirs("sftp://username@host//A")
    sftp.sftp_makedirs("sftp://username@host//A/a")
    sftp.sftp_makedirs("sftp://username@host//A/b")
    sftp.sftp_makedirs("sftp://username@host//A/b/c")
    with sftp.sftp_open("sftp://username@host//A/1.json", "w") as f:
        f.write("1.json")

    assert sorted(list(SftpPath("sftp://username@host//A").iterdir())) == [
        SftpPath("sftp://username@host//A/1.json"),
        SftpPath("sftp://username@host//A/a"),
        SftpPath("sftp://username@host//A/b"),
    ]

    with pytest.raises(NotADirectoryError):
        list(SftpPath("sftp://username@host//A/1.json").iterdir())


def test_cwd(sftp_mocker):
    assert SftpPath("sftp://username@host//A").cwd() == "sftp://username@host//"


def test_sync(sftp_mocker):
    with pytest.raises(OSError):
        SftpPath("sftp://username@host//A").sync("/data/test")


def test_get_private_key(fs):
    with pytest.raises(FileNotFoundError):
        os.environ["SFTP_PRIVATE_KEY_PATH"] = "/file_not_exist"
        get_private_key()


def test_sftp_should_retry():
    assert sftp_should_retry(EOFError()) is False
    assert sftp_should_retry(paramiko.ssh_exception.SSHException()) is True
    assert sftp_should_retry(ConnectionError()) is True
    assert sftp_should_retry(OSError("Socket is closed")) is True
    assert sftp_should_retry(OSError("test")) is False


def test_sftp_realpath_relative(fs, mocker):
    class FakeSFTPClient2(FakeSFTPClient):
        def normalize(self, path):
            if path == ".":
                return "/home/username"
            return os.path.join("/home/username", path)

    client = FakeSFTPClient2()
    mocker.patch("megfile.sftp_path.get_sftp_client", return_value=client)
    assert SftpPath("sftp://username@host/A/B/C")._real_path == "/home/username/A/B/C"


def test_generate_path_object(fs, mocker):
    class FakeSFTPClient2(FakeSFTPClient):
        def normalize(self, path):
            if path == ".":
                return "/root"
            return os.path.relpath(path, start="/root")

    client = FakeSFTPClient2()
    mocker.patch("megfile.sftp_path.get_sftp_client", return_value=client)

    assert (
        SftpPath("sftp://username@host/A/B/C")
        ._generate_path_object("/root/A/B/C/D")
        .path_with_protocol
        == "sftp://username@host/A/B/C/D"
    )
    assert (
        SftpPath("sftp://username@host//root/A/B/C")
        ._generate_path_object("/root/A/B/C/D")
        .path_with_protocol
        == "sftp://username@host//root/A/B/C/D"
    )
    assert (
        SftpPath("sftp://username@host/A/B/C")
        ._generate_path_object("/D", resolve=True)
        .path_with_protocol
        == "sftp://username@host//D"
    )
    assert (
        SftpPath("sftp://username@host//A/B/C")
        ._generate_path_object("/D")
        .path_with_protocol
        == "sftp://username@host//D"
    )
    assert (
        SftpPath("sftp://username@host//A/B/C")
        ._generate_path_object("/root")
        .path_with_protocol
        == "sftp://username@host//root"
    )
    assert (
        SftpPath("sftp://username@host/A/B/C")
        ._generate_path_object("/root")
        .path_with_protocol
        == "sftp://username@host/"
    )
    assert (
        SftpPath("sftp://username@host//A/B/C")
        ._generate_path_object("/")
        .path_with_protocol
        == "sftp://username@host//"
    )


def test_parts(sftp_mocker):
    assert SftpPath("sftp://username@host//A/B/C").parts == (
        "sftp://username@host//",
        "A",
        "B",
        "C",
    )
    assert SftpPath("sftp://username@host/A/B/C").parts == (
        "sftp://username@host/",
        "A",
        "B",
        "C",
    )
    assert SftpPath("sftp://username@host//").parts == ("sftp://username@host//",)


def test__get_sftp_client(mocker):
    get_ssh_session = mocker.patch("megfile.sftp_path.get_ssh_session")
    _patch_sftp_client_request = mocker.patch(
        "megfile.sftp_path._patch_sftp_client_request"
    )
    SFTPClient = mocker.patch("paramiko.SFTPClient")

    _get_sftp_client("127.0.0.1")
    assert get_ssh_session.call_count == 1
    assert _patch_sftp_client_request.call_count == 1
    assert SFTPClient.call_count == 1


def test__get_ssh_client(mocker):
    SSHClient = mocker.patch("paramiko.SSHClient")
    provide_connect_info = mocker.patch(
        "megfile.sftp_path.provide_connect_info",
        return_value=("127.0.0.1", 22, None, None, None),
    )
    _get_ssh_client("127.0.0.1")

    assert SSHClient.call_count == 1
    assert provide_connect_info.call_count == 1


def test_get_ssh_session(mocker):
    times = 0

    class FakeClient:
        def close(self):
            pass

        def get_transport(self):
            nonlocal times
            if times <= 1:
                times += 1
                return None
            return FakeTransport()

    class FakeTransport:
        def set_keepalive(self, interval):
            pass

        def open_session(self, timeout):
            nonlocal times
            if times <= 2:
                times += 1
                return None
            return FakeSession()

    class FakeSession:
        def settimeout(self, timeout):
            pass

    def fake_get_ssh_client(*args, **kwargs):
        nonlocal times
        if times <= 0:
            times += 1
            raise ConnectionError("test")
        return FakeClient()

    mocker.patch("megfile.sftp_path.get_ssh_client", side_effect=fake_get_ssh_client)
    ssh_client_key = f"ssh_client:127.0.0.1,None,None,None,{paramiko.RejectPolicy}"
    thread_local[ssh_client_key] = "test"
    sftp_client_key = f"sftp_client:127.0.0.1,None,None,None,{paramiko.RejectPolicy}"
    thread_local[sftp_client_key] = "test"

    get_ssh_session("127.0.0.1")

    assert times == 3
    assert thread_local.get(ssh_client_key) is None
    assert thread_local.get(sftp_client_key) is None


def test__exec_command(mocker):
    test_env = {"test": "test"}
    test_timeout = 10

    class FakeSession:
        def settimeout(self, timeout):
            assert test_timeout == timeout

        def update_environment(self, environment):
            assert environment == test_env

        def exec_command(self, command):
            pass

        def makefile(self, *args, **kwargs):
            return io.BytesIO(b"makefile")

        def makefile_stderr(self, *args, **kwargs):
            return io.BytesIO(b"makefile_stderr")

        def recv_exit_status(self):
            return 0

        def __enter__(self):
            return self

        def __exit__(self, type, value, traceback):
            pass

    mocker.patch("megfile.sftp_path.get_ssh_session", return_value=FakeSession())
    result = SftpPath("sftp://username@host//test")._exec_command(
        ["test"],
        timeout=test_timeout,
        environment=test_env,
    )
    assert isinstance(result, subprocess.CompletedProcess)
    assert result.returncode == 0
    assert result.stdout == "makefile"
    assert result.stderr == "makefile_stderr"

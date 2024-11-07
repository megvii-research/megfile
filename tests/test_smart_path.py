import os
import stat

import boto3
import pytest
from mock import PropertyMock, patch
from moto import mock_aws

from megfile.errors import (
    ProtocolExistsError,
    ProtocolNotFoundError,
)
from megfile.fs_path import FSPath
from megfile.hdfs_path import HdfsPath
from megfile.http_path import HttpPath, HttpsPath
from megfile.interfaces import Access
from megfile.s3_path import S3Path
from megfile.sftp_path import SftpPath
from megfile.smart_path import PurePath, SmartPath, _load_aliases_config, aliases_config
from megfile.stdio_path import StdioPath

FS_PROTOCOL_PREFIX = FSPath.protocol + "://"
FS_TEST_ABSOLUTE_PATH = "/test/dir/file"
FS_TEST_ABSOLUTE_PATH_WITH_PROTOCOL = FS_PROTOCOL_PREFIX + FS_TEST_ABSOLUTE_PATH
FS_TEST_RELATIVE_PATH = "test/dir/file"
FS_TEST_RELATIVE_PATH_WITH_PROTOCOL = FS_PROTOCOL_PREFIX + FS_TEST_RELATIVE_PATH
FS_TEST_SRC_PATH = "/test/dir/src_file"
FS_TEST_DST_PATH = "/test/dir/dst_file"

S3_PROTOCOL_PREFIX = S3Path.protocol + "://"
S3_TEST_PATH_WITHOUT_PROTOCOL = "bucket/dir/file"
S3_SRC_PATH_WITHOUT_PROTOCOL = "bucket/dir/src"
S3_TEST_PATH = S3_PROTOCOL_PREFIX + S3_TEST_PATH_WITHOUT_PROTOCOL
S3_SRC_PATH = S3_PROTOCOL_PREFIX + S3_SRC_PATH_WITHOUT_PROTOCOL

HTTP_PROTOCOL_PRFIX = HttpPath.protocol + "://"
HTTP_TEST_PATH_WITHOUT_PROTOCOL = "www.test.com"
HTTP_TEST_PATH = HTTP_PROTOCOL_PRFIX + HTTP_TEST_PATH_WITHOUT_PROTOCOL

HTTPS_PROTOCOL_PRFIX = HttpsPath.protocol + "://"
HTTPS_TEST_PATH_WITHOUT_PROTOCOL = "www.test.com"
HTTPS_TEST_PATH = HTTPS_PROTOCOL_PRFIX + HTTPS_TEST_PATH_WITHOUT_PROTOCOL

STDIO_PROTOCOL_PRFIX = StdioPath.protocol + "://"
STDIO_TEST_PATH_WITHOUT_PROTOCOL = "-"
STDIO_TEST_PATH = STDIO_PROTOCOL_PRFIX + STDIO_TEST_PATH_WITHOUT_PROTOCOL

BUCKET = "bucket"


@pytest.fixture
def s3_empty_client(mocker):
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket=BUCKET)
        mocker.patch("megfile.s3_path.get_s3_client", return_value=client)
        yield client


def test_register_result():
    assert len(SmartPath._registered_protocols) == 7
    assert S3Path.protocol in SmartPath._registered_protocols
    assert FSPath.protocol in SmartPath._registered_protocols
    assert HttpPath.protocol in SmartPath._registered_protocols
    assert HttpsPath.protocol in SmartPath._registered_protocols
    assert StdioPath.protocol in SmartPath._registered_protocols
    assert SftpPath.protocol in SmartPath._registered_protocols
    assert HdfsPath.protocol in SmartPath._registered_protocols

    assert SmartPath._registered_protocols[S3Path.protocol] == S3Path
    assert SmartPath._registered_protocols[FSPath.protocol] == FSPath
    assert SmartPath._registered_protocols[HttpPath.protocol] == HttpPath
    assert SmartPath._registered_protocols[HttpsPath.protocol] == HttpsPath
    assert SmartPath._registered_protocols[StdioPath.protocol] == StdioPath
    assert SmartPath._registered_protocols[HdfsPath.protocol] == HdfsPath
    assert SmartPath.from_uri(FS_TEST_ABSOLUTE_PATH) == SmartPath(FS_TEST_ABSOLUTE_PATH)


def test_aliases(fs):
    config_path = os.path.expanduser(aliases_config)
    fs.create_file(
        config_path,
        contents="[oss2]\nprotocol = s3+oss2\n[tos]\nprotocol = s3+tos",
    )
    aliases = {"oss2": {"protocol": "s3+oss2"}, "tos": {"protocol": "s3+tos"}}
    assert _load_aliases_config(config_path) == aliases

    with patch.object(SmartPath, "_aliases", new_callable=PropertyMock) as mock_aliases:
        mock_aliases.return_value = aliases
        assert (
            SmartPath.from_uri("oss2://bucket/dir/file").pathlike
            == SmartPath("s3+oss2://bucket/dir/file").pathlike
        )


@patch.object(SmartPath, "_create_pathlike")
def test_init(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH)
    funcA.assert_called_once_with(FS_TEST_ABSOLUTE_PATH)
    """
    assert isinstance(SmartPath(FS_TEST_ABSOLUTE_PATH).path, FSPath)
    assert isinstance(SmartPath(FS_TEST_ABSOLUTE_PATH_WITH_PROTOCOL).path, FSPath)
    assert isinstance(SmartPath(FS_TEST_RELATIVE_PATH).path, FSPath)
    assert isinstance(SmartPath(FS_TEST_RELATIVE_PATH_WITH_PROTOCOL).path, FSPath)
    assert isinstance(SmartPath(S3_TEST_PATH).path, S3Path)
    """


def test_extract_protocol():
    assert SmartPath._extract_protocol(FS_TEST_ABSOLUTE_PATH) == (
        FSPath.protocol,
        FS_TEST_ABSOLUTE_PATH,
    )
    assert SmartPath._extract_protocol(FS_TEST_ABSOLUTE_PATH_WITH_PROTOCOL) == (
        FSPath.protocol,
        FS_TEST_ABSOLUTE_PATH_WITH_PROTOCOL,
    )
    assert SmartPath._extract_protocol(FS_TEST_RELATIVE_PATH) == (
        FSPath.protocol,
        FS_TEST_RELATIVE_PATH,
    )
    assert SmartPath._extract_protocol(FS_TEST_RELATIVE_PATH_WITH_PROTOCOL) == (
        FSPath.protocol,
        FS_TEST_RELATIVE_PATH_WITH_PROTOCOL,
    )
    assert SmartPath._extract_protocol(S3_TEST_PATH) == (
        S3Path.protocol,
        S3_TEST_PATH,
    )
    assert SmartPath._extract_protocol(HTTP_TEST_PATH) == (
        HttpPath.protocol,
        HTTP_TEST_PATH,
    )
    assert SmartPath._extract_protocol(HTTPS_TEST_PATH) == (
        HttpsPath.protocol,
        HTTPS_TEST_PATH,
    )
    assert SmartPath._extract_protocol(STDIO_TEST_PATH) == (
        StdioPath.protocol,
        STDIO_TEST_PATH,
    )

    fs_path = FSPath(FS_TEST_ABSOLUTE_PATH)
    assert SmartPath._extract_protocol(fs_path) == (
        FSPath.protocol,
        FS_TEST_ABSOLUTE_PATH,
    )
    fs_path = FSPath(FS_TEST_RELATIVE_PATH)
    assert SmartPath._extract_protocol(fs_path) == (
        FSPath.protocol,
        FS_TEST_RELATIVE_PATH,
    )
    s3_path = S3Path(S3_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(s3_path) == (
        S3Path.protocol,
        S3_TEST_PATH_WITHOUT_PROTOCOL,
    )
    http_path = HttpPath(HTTP_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(http_path) == (
        HttpPath.protocol,
        HTTP_TEST_PATH_WITHOUT_PROTOCOL,
    )
    https_path = HttpsPath(HTTPS_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(https_path) == (
        HttpsPath.protocol,
        HTTPS_TEST_PATH_WITHOUT_PROTOCOL,
    )
    stdio_path = StdioPath(STDIO_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(stdio_path) == (
        StdioPath.protocol,
        STDIO_TEST_PATH_WITHOUT_PROTOCOL,
    )
    int_path = 1
    assert SmartPath._extract_protocol(int_path) == ("file", int_path)
    pure_path = PurePath(FS_TEST_ABSOLUTE_PATH)
    assert SmartPath._extract_protocol(pure_path) == ("file", FS_TEST_ABSOLUTE_PATH)

    with pytest.raises(ProtocolNotFoundError):
        SmartPath._extract_protocol(None)


@patch.object(SmartPath, "_extract_protocol")
def _create_pathlike(funcA):
    funcA.return_value = (S3Path.protocol, S3_TEST_PATH_WITHOUT_PROTOCOL)
    assert isinstance(SmartPath._create_pathlike("S3 Case"), S3Path)

    funcA.return_value = (FSPath.protocol, FS_TEST_ABSOLUTE_PATH)
    assert isinstance(SmartPath._create_pathlike("FS Case"), FSPath)

    funcA.return_value = ("NotExistProtocol", "")
    with pytest.raises(ProtocolNotFoundError):
        SmartPath._create_pathlike("tcp://Not Exist Case")


@patch.object(SmartPath, "_extract_protocol")
def test_create_pathlike(funcA):
    funcA.return_value = ("NotExistProtocol", "")
    with pytest.raises(ProtocolNotFoundError):
        SmartPath._create_pathlike("tcp://Not Exist Case")


def test_register():
    pre_cnt = len(SmartPath._registered_protocols)
    with pytest.raises(ProtocolExistsError):

        @SmartPath.register
        class OverridingPath:
            protocol = S3Path.protocol

    @SmartPath.register
    class FakePath:
        protocol = "fake"

    assert len(SmartPath._registered_protocols) == pre_cnt + 1
    assert "fake" in SmartPath._registered_protocols
    assert SmartPath._registered_protocols["fake"] == FakePath
    del SmartPath._registered_protocols["fake"]


@patch.object(FSPath, "unlink")
def test_unlink(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).unlink(missing_ok=True)
    funcA.assert_called_once_with(missing_ok=True)


@patch.object(FSPath, "remove")
def test_remove(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).remove(missing_ok=True, followlinks=True)
    funcA.assert_called_once_with(missing_ok=True, followlinks=True)


@patch.object(FSPath, "replace")
def test_replace(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).replace(missing_ok=True, followlinks=True)
    funcA.assert_called_once_with(missing_ok=True, followlinks=True)


@patch.object(FSPath, "rename")
def test_rename(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).rename(missing_ok=True, followlinks=True)
    funcA.assert_called_once_with(missing_ok=True, followlinks=True)


@patch.object(FSPath, "stat")
def test_stat(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).stat()
    funcA.assert_called_once()


@patch.object(FSPath, "is_dir")
def test_is_dir(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).is_dir()
    SmartPath(FS_TEST_ABSOLUTE_PATH).is_dir(followlinks=True)
    funcA.call_count == 2


@patch.object(FSPath, "is_file")
def test_is_file(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).is_file()
    SmartPath(FS_TEST_ABSOLUTE_PATH).is_file(followlinks=True)
    funcA.call_count == 2


def test_is_symlink(mocker):
    funcA = mocker.patch("os.path.islink")
    path = SmartPath(FS_TEST_ABSOLUTE_PATH)
    path.is_symlink()
    funcA.assert_called_once_with(FS_TEST_ABSOLUTE_PATH)


@patch.object(FSPath, "access")
def test_access(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).access(mode=Access.READ)
    SmartPath(FS_TEST_ABSOLUTE_PATH).access(mode=Access.WRITE)
    funcA.call_count == 2


@patch.object(FSPath, "exists")
def test_exists(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).exists()
    SmartPath(FS_TEST_ABSOLUTE_PATH).exists(followlinks=True)
    funcA.call_count == 2


@patch.object(FSPath, "getmtime")
def test_getmtime(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).getmtime()
    funcA.assert_called_once()


@patch.object(FSPath, "getsize")
def test_getsize(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).getsize()
    funcA.assert_called_once()


@patch.object(S3Path, "joinpath")
def test_path_join(funcA):
    SmartPath(S3_TEST_PATH).joinpath(S3_TEST_PATH)
    funcA.assert_called_once_with(S3_TEST_PATH)


@patch.object(FSPath, "mkdir")
def test_makedirs(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).makedirs(exist_ok=True)
    funcA.assert_called_once_with(parents=True, exist_ok=True)


@patch.object(FSPath, "glob_stat")
def test_glob_stat(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).glob_stat(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(recursive=False, missing_ok=False)


@patch.object(FSPath, "glob")
def test_glob(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).glob(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(recursive=False, missing_ok=False)


@patch.object(FSPath, "iglob")
def test_iglob(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).iglob(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(recursive=False, missing_ok=False)


@patch.object(FSPath, "scan")
def test_scan(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).scan(missing_ok=False, followlinks=True)
    funcA.assert_called_once_with(missing_ok=False, followlinks=True)


@patch.object(FSPath, "scan_stat")
def test_scan_stat(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).scan_stat(missing_ok=False, followlinks=True)
    funcA.assert_called_once_with(missing_ok=False, followlinks=True)


@patch.object(FSPath, "scandir")
def test_scandir(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).scandir()
    funcA.assert_called_once_with()


def test_scandir(fs):
    os.makedirs("/test")
    SmartPath("/test/1").write_bytes(b"test")
    for file_entry in SmartPath("/test").scandir():
        file_entry.inode() == os.stat("/test/1").st_ino


@patch.object(S3Path, "listdir")
def test_listdir(funcA):
    SmartPath(S3_TEST_PATH).listdir()
    funcA.assert_called_once()


@patch.object(FSPath, "walk")
def test_walk(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).walk(followlinks=True)
    funcA.assert_called_once_with(followlinks=True)


@patch.object(FSPath, "load")
def test_load_from(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).load()
    funcA.assert_called_once()


@patch.object(FSPath, "md5")
def test_md5(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).md5()
    funcA.assert_called_once()


def test_md5_un_support():
    with pytest.raises(NotImplementedError):
        SmartPath("http://test/test").md5()


@patch.object(FSPath, "symlink")
def test_symlink_to(funcA):
    SmartPath(FS_TEST_DST_PATH).symlink(FS_TEST_SRC_PATH)
    funcA.assert_called_once()


@patch.object(FSPath, "readlink")
def test_readlink(funcA):
    SmartPath(FS_TEST_DST_PATH).readlink()
    funcA.assert_called_once()


@patch.object(S3Path, "symlink")
def test_symlink_to_s3(funcA):
    SmartPath(S3_TEST_PATH).symlink(S3_SRC_PATH)
    funcA.assert_called_once()


@patch.object(S3Path, "readlink")
def test_readlink_s3(funcA):
    SmartPath(S3_TEST_PATH).readlink()
    funcA.assert_called_once()


def test_stat(s3_empty_client, fs):
    path = SmartPath("/test")
    path.write_text("test")
    path_stat = path.stat()
    origin_stat = os.stat("/test")
    assert path_stat.st_mode == origin_stat.st_mode
    assert path_stat.st_ino == origin_stat.st_ino
    assert path_stat.st_dev == origin_stat.st_dev
    assert path_stat.st_nlink == origin_stat.st_nlink
    assert path_stat.st_uid == origin_stat.st_uid
    assert path_stat.st_gid == origin_stat.st_gid
    assert path_stat.st_size == origin_stat.st_size
    assert path_stat.st_atime == origin_stat.st_atime
    assert path_stat.st_mtime == origin_stat.st_mtime
    assert path_stat.st_ctime == origin_stat.st_ctime
    assert path_stat.st_atime_ns == origin_stat.st_atime_ns
    assert path_stat.st_mtime_ns == origin_stat.st_mtime_ns
    assert path_stat.st_ctime_ns == origin_stat.st_ctime_ns

    path = SmartPath(f"s3://{BUCKET}/testA")
    path.write_bytes(b"test")
    path_stat = path.stat()
    assert path_stat.st_mode == stat.S_IFREG

    content = s3_empty_client.head_object(Bucket=BUCKET, Key="testA")
    etag_int = int(content["ETag"][1:-1], 16)

    assert path_stat.st_ino == etag_int
    assert path_stat.st_dev == 0
    assert path_stat.st_nlink == 0
    assert path_stat.st_uid == 0
    assert path_stat.st_gid == 0
    assert path_stat.st_size == content["ContentLength"]
    assert path_stat.st_atime == 0.0
    assert path_stat.st_mtime == content["LastModified"].timestamp()
    assert path_stat.st_ctime == 0.0
    assert path_stat.st_atime_ns == 0
    assert path_stat.st_mtime_ns == 0
    assert path_stat.st_ctime_ns == 0

    path = SmartPath(f"s3://{BUCKET}/dir/testA")
    path.write_bytes(b"test")
    path_stat = SmartPath(f"s3://{BUCKET}/dir").stat()
    assert path_stat.st_mode == stat.S_IFDIR

    lnk_path = SmartPath(f"s3://{BUCKET}/dir/testA.lnk")
    path.symlink(lnk_path)
    lnk_stat = lnk_path.lstat()
    assert lnk_stat.st_mode == stat.S_IFLNK

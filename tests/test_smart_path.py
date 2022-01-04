import pytest
from mock import patch

from megfile.errors import ProtocolExistsError, ProtocolNotFoundError
from megfile.fs_path import FSPath
from megfile.http_path import HttpPath, HttpsPath
from megfile.interfaces import Access
from megfile.s3_path import S3Path
from megfile.smart_path import SmartPath
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
S3_TEST_PATH = S3_PROTOCOL_PREFIX + S3_TEST_PATH_WITHOUT_PROTOCOL

HTTP_PROTOCOL_PRFIX = HttpPath.protocol + "://"
HTTP_TEST_PATH_WITHOUT_PROTOCOL = "www.test.com"
HTTP_TEST_PATH = HTTP_PROTOCOL_PRFIX + HTTP_TEST_PATH_WITHOUT_PROTOCOL

HTTPS_PROTOCOL_PRFIX = HttpsPath.protocol + "://"
HTTPS_TEST_PATH_WITHOUT_PROTOCOL = "www.test.com"
HTTPS_TEST_PATH = HTTPS_PROTOCOL_PRFIX + HTTPS_TEST_PATH_WITHOUT_PROTOCOL

STDIO_PROTOCOL_PRFIX = StdioPath.protocol + "://"
STDIO_TEST_PATH_WITHOUT_PROTOCOL = "-"
STDIO_TEST_PATH = STDIO_PROTOCOL_PRFIX + STDIO_TEST_PATH_WITHOUT_PROTOCOL


def test_register_result():
    assert len(SmartPath._registered_protocols) == 5
    assert S3Path.protocol in SmartPath._registered_protocols
    assert FSPath.protocol in SmartPath._registered_protocols
    assert HttpPath.protocol in SmartPath._registered_protocols
    assert HttpsPath.protocol in SmartPath._registered_protocols
    assert StdioPath.protocol in SmartPath._registered_protocols

    assert SmartPath._registered_protocols[S3Path.protocol] == S3Path
    assert SmartPath._registered_protocols[FSPath.protocol] == FSPath
    assert SmartPath._registered_protocols[HttpPath.protocol] == HttpPath
    assert SmartPath._registered_protocols[HttpsPath.protocol] == HttpsPath
    assert SmartPath._registered_protocols[StdioPath.protocol] == StdioPath


@patch.object(SmartPath, '_create_pathlike')
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
        FSPath.protocol, FS_TEST_ABSOLUTE_PATH)
    assert SmartPath._extract_protocol(FS_TEST_ABSOLUTE_PATH_WITH_PROTOCOL) == (
        FSPath.protocol, FS_TEST_ABSOLUTE_PATH)
    assert SmartPath._extract_protocol(FS_TEST_RELATIVE_PATH) == (
        FSPath.protocol, FS_TEST_RELATIVE_PATH)
    assert SmartPath._extract_protocol(FS_TEST_RELATIVE_PATH_WITH_PROTOCOL) == (
        FSPath.protocol, FS_TEST_RELATIVE_PATH)
    assert SmartPath._extract_protocol(S3_TEST_PATH) == (
        S3Path.protocol, S3_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(HTTP_TEST_PATH) == (
        HttpPath.protocol, HTTP_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(HTTPS_TEST_PATH) == (
        HttpsPath.protocol, HTTPS_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(STDIO_TEST_PATH) == (
        StdioPath.protocol, STDIO_TEST_PATH_WITHOUT_PROTOCOL)

    fs_path = FSPath(FS_TEST_ABSOLUTE_PATH)
    assert SmartPath._extract_protocol(fs_path) == (
        FSPath.protocol, FS_TEST_ABSOLUTE_PATH)
    fs_path = FSPath(FS_TEST_RELATIVE_PATH)
    assert SmartPath._extract_protocol(fs_path) == (
        FSPath.protocol, FS_TEST_RELATIVE_PATH)
    s3_path = S3Path(S3_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(s3_path) == (
        S3Path.protocol, S3_TEST_PATH_WITHOUT_PROTOCOL)
    http_path = HttpPath(HTTP_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(http_path) == (
        HttpPath.protocol, HTTP_TEST_PATH_WITHOUT_PROTOCOL)
    https_path = HttpsPath(HTTPS_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(https_path) == (
        HttpsPath.protocol, HTTPS_TEST_PATH_WITHOUT_PROTOCOL)
    stdio_path = StdioPath(STDIO_TEST_PATH_WITHOUT_PROTOCOL)
    assert SmartPath._extract_protocol(stdio_path) == (
        StdioPath.protocol, STDIO_TEST_PATH_WITHOUT_PROTOCOL)


@patch.object(SmartPath, '_extract_protocol')
def _create_pathlike(funcA):
    funcA.return_value = (S3Path.protocol, S3_TEST_PATH_WITHOUT_PROTOCOL)
    assert isinstance(SmartPath._create_pathlike("S3 Case"), S3Path)

    funcA.return_value = (FSPath.protocol, FS_TEST_ABSOLUTE_PATH)
    assert isinstance(SmartPath._create_pathlike("FS Case"), FSPath)

    funcA.return_value = ("NotExistProtocol", "")
    with pytest.raises(ProtocolNotFoundError):
        SmartPath._create_pathlike("Not Exist Case")


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


@patch.object(FSPath, 'unlink')
def test_unlink(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).unlink(missing_ok=True)
    funcA.assert_called_once_with(missing_ok=True)


@patch.object(FSPath, 'remove')
def test_remove(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).remove(missing_ok=True)
    funcA.assert_called_once_with(missing_ok=True)


@patch.object(FSPath, 'replace')
def test_replace(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).replace(missing_ok=True)
    funcA.assert_called_once_with(missing_ok=True)


@patch.object(FSPath, 'rename')
def test_rename(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).rename(missing_ok=True)
    funcA.assert_called_once_with(missing_ok=True)


@patch.object(FSPath, 'stat')
def test_stat(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).stat()
    funcA.assert_called_once()


@patch.object(FSPath, 'is_dir')
def test_is_dir(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).is_dir()
    funcA.assert_called_once()


@patch.object(FSPath, 'is_file')
def test_is_file(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).is_file()
    funcA.assert_called_once()


def test_is_symlink(mocker):
    funcA = mocker.patch("os.path.islink")
    path = SmartPath(FS_TEST_ABSOLUTE_PATH)
    path.is_symlink()
    funcA.assert_called_once_with(FS_TEST_ABSOLUTE_PATH)


@patch.object(FSPath, 'access')
def test_access(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).access(mode=Access.READ)
    SmartPath(FS_TEST_ABSOLUTE_PATH).access(mode=Access.WRITE)
    funcA.call_count == 2


@patch.object(FSPath, 'exists')
def test_exists(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).exists()
    funcA.assert_called_once()


@patch.object(FSPath, 'getmtime')
def test_getmtime(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).getmtime()
    funcA.assert_called_once()


@patch.object(FSPath, 'getsize')
def test_getsize(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).getsize()
    funcA.assert_called_once()


@patch.object(S3Path, 'joinpath')
def test_path_join(funcA):
    SmartPath(S3_TEST_PATH).joinpath(S3_TEST_PATH)
    funcA.assert_called_once_with(S3_TEST_PATH)


@patch.object(FSPath, 'makedirs')
def test_makedirs(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).makedirs(exist_ok=True)
    funcA.assert_called_once_with(exist_ok=True)


@patch.object(FSPath, 'glob_stat')
def test_glob_stat(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).glob_stat(
        recursive=False, missing_ok=False)
    funcA.assert_called_once_with(recursive=False, missing_ok=False)


@patch.object(FSPath, 'glob')
def test_glob(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).glob(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(recursive=False, missing_ok=False)


@patch.object(FSPath, 'iglob')
def test_iglob(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).iglob(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(recursive=False, missing_ok=False)


@patch.object(FSPath, 'scan')
def test_scan(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).scan(missing_ok=False)
    funcA.assert_called_once_with(missing_ok=False)


@patch.object(FSPath, 'scan_stat')
def test_scan_stat(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).scan_stat(missing_ok=False)
    funcA.assert_called_once_with(missing_ok=False)


@patch.object(FSPath, 'scandir')
def test_scandir(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).scandir()
    funcA.assert_called_once_with()


@patch.object(S3Path, 'listdir')
def test_listdir(funcA):
    SmartPath(S3_TEST_PATH).listdir()
    funcA.assert_called_once()


@patch.object(FSPath, 'walk')
def test_walk(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).walk()
    funcA.assert_called_once()


@patch.object(FSPath, 'load')
def test_load_from(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).load()
    funcA.assert_called_once()


@patch.object(FSPath, 'md5')
def test_md5(funcA):
    SmartPath(FS_TEST_ABSOLUTE_PATH).md5()
    funcA.assert_called_once()


@patch.object(FSPath, 'symlink')
def test_symlink(funcA):
    SmartPath(FS_TEST_DST_PATH).symlink(FS_TEST_SRC_PATH)
    funcA.assert_called_once()


@patch.object(FSPath, 'readlink')
def test_readlink(funcA):
    SmartPath(FS_TEST_DST_PATH).readlink()
    funcA.assert_called_once()

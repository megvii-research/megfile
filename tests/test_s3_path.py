import pytest

from megfile.interfaces import Access
from megfile.s3_path import S3Path

S3_PROTOCOL_PREFIX = S3Path.protocol + "://"
TEST_PATH = "bucket/dir/file"
TEST_PATH_WITH_PROTOCOL = S3_PROTOCOL_PREFIX + TEST_PATH
path = S3Path(TEST_PATH)


def test_as_uri():
    assert path.as_uri() == TEST_PATH_WITH_PROTOCOL


def test_magic_fspath():
    assert path.__fspath__() == TEST_PATH_WITH_PROTOCOL


def test_from_uri():
    assert S3Path.from_uri(TEST_PATH_WITH_PROTOCOL).path == TEST_PATH


def test_remove(mocker):
    funcA = mocker.patch('megfile.s3.s3_remove')
    path.remove(missing_ok=True)
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, missing_ok=True)


def test_move(mocker):
    funcA = mocker.patch('megfile.s3.s3_move')
    path.move(missing_ok=True)
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, missing_ok=True)


def test_rename(mocker):
    funcA = mocker.patch('megfile.s3.s3_rename')
    path.rename(missing_ok=True)
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, missing_ok=True)


def test_unlink(mocker):
    funcA = mocker.patch('megfile.s3.s3_unlink')
    path.unlink(missing_ok=True)
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, missing_ok=True)


def test_copy(mocker):
    dst = 's3://bucket/result'
    funcA = mocker.patch('megfile.s3.s3_copy')
    path.copy(dst)
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, dst)


def test_sync(mocker):
    dst = 's3://bucket/result'
    funcA = mocker.patch('megfile.s3.s3_sync')
    path.sync(dst)
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, dst)


def test_stat(mocker):
    funcA = mocker.patch('megfile.s3.s3_stat')
    path.stat()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)


def test_is_dir(mocker):
    funcA = mocker.patch('megfile.s3.s3_isdir')
    path.is_dir()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)


def test_is_file(mocker):
    funcA = mocker.patch('megfile.s3.s3_isfile')
    path.is_file()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)


def test_is_symlink(mocker):
    assert path.is_symlink() == False


def test_access(mocker):
    funcA = mocker.patch('megfile.s3.s3_access')
    path.access(mode=Access.READ)
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, mode=Access.READ)


def test_exists(mocker):
    funcA = mocker.patch('megfile.s3.s3_exists')
    path.exists()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)


def test_getmtime(mocker):
    funcA = mocker.patch('megfile.s3.s3_getmtime')
    path.getmtime()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)


def test_getsize(mocker):
    funcA = mocker.patch('megfile.s3.s3_getsize')
    path.getsize()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)


# TODO: 修改了实现，不再调用 s3_path_join
# def test_path_join(mocker):
#     funcA = mocker.patch('megfile.s3.s3_path_join')

#     TEST_PATH1 = TEST_PATH_WITH_PROTOCOL + "1"
#     TEST_PATH2 = TEST_PATH_WITH_PROTOCOL + "2"
#     path.joinpath(TEST_PATH1, TEST_PATH2)
#     funcA.assert_called_once_with(
#         TEST_PATH_WITH_PROTOCOL, TEST_PATH1, TEST_PATH2)


def test_makedirs(mocker):
    funcA = mocker.patch('megfile.s3.s3_makedirs')
    path.makedirs(exist_ok=True)
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, exist_ok=True)


def test_glob_stat(mocker):
    funcA = mocker.patch('megfile.s3.s3_glob_stat')
    path.glob_stat(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(
        TEST_PATH_WITH_PROTOCOL, recursive=False, missing_ok=False)


def test_glob(mocker):
    funcA = mocker.patch('megfile.s3.s3_glob')
    path.glob(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(
        TEST_PATH_WITH_PROTOCOL, recursive=False, missing_ok=False)


def test_iglob(mocker):
    funcA = mocker.patch('megfile.s3.s3_iglob')
    path.iglob(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(
        TEST_PATH_WITH_PROTOCOL, recursive=False, missing_ok=False)


def test_scan(mocker):
    funcA = mocker.patch('megfile.s3.s3_scan')
    path.scan(missing_ok=False)
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, missing_ok=False)


def test_scan_stat(mocker):
    funcA = mocker.patch('megfile.s3.s3_scan_stat')
    path.scan_stat(missing_ok=False)
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, missing_ok=False)


def test_scandir(mocker):
    funcA = mocker.patch('megfile.s3.s3_scandir')
    path.scandir()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)


def test_listdir(mocker):
    funcA = mocker.patch('megfile.s3.s3_listdir')
    path.listdir()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)


def test_walk(mocker):
    funcA = mocker.patch('megfile.s3.s3_walk')
    path.walk()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)


def test_load_from(mocker):
    funcA = mocker.patch('megfile.s3.s3_load_from')
    path.load()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)


def test_md5(mocker):
    funcA = mocker.patch('megfile.s3.s3_getmd5')
    path.md5()
    funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)

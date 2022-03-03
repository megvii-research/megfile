import pytest

from megfile.fs_path import FSPath
from megfile.interfaces import Access

FS_PROTOCOL_PREFIX = FSPath.protocol + "://"
TEST_PATH = "/test/dir/file"
TEST_PATH_WITH_PROTOCOL = FS_PROTOCOL_PREFIX + TEST_PATH
path = FSPath(TEST_PATH)


def test_as_uri():
    assert path.as_uri() == TEST_PATH_WITH_PROTOCOL


def test_magic_fspath():
    assert path.__fspath__() == TEST_PATH


def test_from_uri():
    assert FSPath.from_uri(TEST_PATH).path == TEST_PATH
    assert FSPath.from_uri(
        TEST_PATH_WITH_PROTOCOL).path == TEST_PATH_WITH_PROTOCOL


def test_remove(mocker):
    funcA = mocker.patch('megfile.fs.fs_remove')
    path.remove(missing_ok=True, followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, missing_ok=True, followlinks=True)


def test_unlink(mocker):
    funcA = mocker.patch('megfile.fs.fs_unlink')
    path.unlink(missing_ok=True)
    funcA.assert_called_once_with(TEST_PATH, missing_ok=True)


def test_stat(mocker):
    funcA = mocker.patch('megfile.fs.fs_stat')
    path.stat()
    funcA.assert_called_once_with(TEST_PATH)


def test_is_dir(mocker):
    funcA = mocker.patch('megfile.fs.fs_isdir')
    path.isdir(followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, followlinks=True)


def test_is_file(mocker):
    funcA = mocker.patch('megfile.fs.fs_isfile')
    path.isfile(followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, followlinks=True)


def test_is_symlink(mocker):
    funcA = mocker.patch('os.path.islink')
    path.is_symlink()
    funcA.assert_called_once_with(TEST_PATH)


def test_access(mocker):
    funcA = mocker.patch('megfile.fs.fs_access')
    path.access(mode=Access.READ)
    funcA.assert_called_once_with(TEST_PATH, mode=Access.READ)


def test_exists(mocker):
    funcA = mocker.patch('megfile.fs.fs_exists')
    path.exists(followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, followlinks=True)


def test_getmtime(mocker):
    funcA = mocker.patch('megfile.fs.fs_getmtime')
    path.getmtime()
    funcA.assert_called_once_with(TEST_PATH)


def test_getsize(mocker):
    funcA = mocker.patch('megfile.fs.fs_getsize')
    path.getsize()
    funcA.assert_called_once_with(TEST_PATH)


# TODO: 修改了实现，不再调用 fs_path_join
# def test_path_join(mocker):
#     funcA = mocker.patch('megfile.fs.fs_path_join')

#     TEST_PATH1 = TEST_PATH + "1"
#     TEST_PATH2 = TEST_PATH + "2"
#     path.joinpath(TEST_PATH1, TEST_PATH2)
#     funcA.assert_called_once_with(TEST_PATH, TEST_PATH1, TEST_PATH2)


def test_makedirs(mocker):
    funcA = mocker.patch('megfile.fs.fs_mkdir')
    path.makedirs(exist_ok=True)
    funcA.assert_called_once_with(TEST_PATH, exist_ok=True)


def test_glob_stat(mocker):
    funcA = mocker.patch('megfile.fs.fs_glob_stat')
    path.glob_stat(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(TEST_PATH, recursive=False, missing_ok=False)


def test_glob(mocker):
    funcA = mocker.patch('megfile.fs.fs_glob')
    path.glob(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(TEST_PATH, recursive=False, missing_ok=False)


def test_iglob(mocker):
    funcA = mocker.patch('megfile.fs.fs_iglob')
    path.iglob(recursive=False, missing_ok=False)
    funcA.assert_called_once_with(TEST_PATH, recursive=False, missing_ok=False)


def test_scan(mocker):
    funcA = mocker.patch('megfile.fs.fs_scan')
    path.scan(missing_ok=False, followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, missing_ok=False, followlinks=True)


def test_scan_stat(mocker):
    funcA = mocker.patch('megfile.fs.fs_scan_stat')
    path.scan_stat(missing_ok=False, followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, missing_ok=False, followlinks=True)


def test_scandir(mocker):
    funcA = mocker.patch('megfile.fs.fs_scandir')
    path.scandir()
    funcA.assert_called_once_with(TEST_PATH)


def test_listdir(mocker):
    funcA = mocker.patch('megfile.fs.os.listdir')
    path.listdir()
    funcA.assert_called_once_with(TEST_PATH)


def test_walk(mocker):
    funcA = mocker.patch('megfile.fs.fs_walk')
    path.walk(followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, followlinks=True)


def test_load_from(mocker):
    funcA = mocker.patch('megfile.fs.fs_load')
    path.load()
    funcA.assert_called_once_with(TEST_PATH)


def test_mkdir(mocker):
    funcA = mocker.patch('megfile.fs.fs_mkdir')
    path.mkdir()
    funcA.assert_called_once_with(TEST_PATH)


def test_rmdir(mocker):
    funcA = mocker.patch('megfile.fs.fs_remove')
    path.rmdir()
    funcA.assert_called_once_with(TEST_PATH)


def test_rename(mocker):
    funcA = mocker.patch('megfile.fs.fs_move')
    path.rename(followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, followlinks=True)


def test_replace(mocker):
    funcA = mocker.patch('megfile.fs.fs_move')
    path.replace(followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, followlinks=True)


def test_copy(mocker):
    dst = '/tmp/refiletest/dst'
    funcA = mocker.patch('megfile.fs.fs_copy')
    path.copy(dst, followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, dst, followlinks=True)


def test_sync(mocker):
    dst = '/tmp/refiletest/dst'
    funcA = mocker.patch('megfile.fs.fs_sync')
    path.sync(dst, followlinks=True)
    funcA.assert_called_once_with(TEST_PATH, dst, followlinks=True)


def test_cwd(mocker):
    funcA = mocker.patch('os.getcwd')
    FSPath.cwd()
    funcA.assert_called_once_with()


def test_home(mocker):
    funcA = mocker.patch('os.path.expanduser')
    FSPath.home()
    funcA.assert_called_once_with('~')


def test_expanduser(mocker):
    funcA = mocker.patch('megfile.fs.fs_expanduser')
    path.expanduser()
    funcA.assert_called_once_with(TEST_PATH)


def test_resolve(mocker):
    funcA = mocker.patch('megfile.fs.fs_resolve')
    path.resolve()
    funcA.assert_called_once_with(TEST_PATH)


def test_md5(mocker):
    funcA = mocker.patch('megfile.fs.fs_getmd5')
    path.md5()
    funcA.assert_called_once_with(TEST_PATH)


def test_symlink_to(mocker):
    funcA = mocker.patch('megfile.fs.fs_symlink')
    path.symlink(src_path='/test/dir/src_file')
    funcA.assert_called_once_with(TEST_PATH, src_path='/test/dir/src_file')


def test_readlink(mocker):
    funcA = mocker.patch('megfile.fs.fs_readlink')
    path.readlink()
    funcA.assert_called_once_with(TEST_PATH)


def test_path_with_protocol():
    path = FSPath(TEST_PATH)
    assert path.path_with_protocol == TEST_PATH_WITH_PROTOCOL

    int_path = FSPath(1)
    assert int_path.path_with_protocol == 1

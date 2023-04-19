import io
import os
import shutil
import stat
import subprocess
import time
from typing import List, Optional

import paramiko
import pytest

from megfile import sftp


class FakeSFTPClient:

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        pass

    def listdir(self, path="."):
        return os.listdir(path)

    def listdir_attr(self, path="."):
        return list(self.listdir_iter(path=path))

    def listdir_iter(self, path=".", read_aheads=50):
        for filename in os.listdir(path):
            yield paramiko.SFTPAttributes.from_stat(
                os.stat(os.path.join(path, filename)))

    def open(self, filename, mode="r", bufsize=-1):
        if "r" in mode and "b" not in mode:
            mode = mode + "b"
        return io.open(file=filename, mode=mode, buffering=bufsize)

    # Python continues to vacillate about "open" vs "file"...
    file = open

    def remove(self, path):
        os.unlink(path)

    unlink = remove

    def rename(self, oldpath, newpath):
        os.rename(oldpath, newpath)

    def posix_rename(self, oldpath, newpath):
        os.rename(oldpath, newpath)

    def mkdir(self, path, mode=0o777):
        os.mkdir(path=path, mode=mode)

    def rmdir(self, path):
        os.rmdir(path)

    def stat(self, path):
        return paramiko.SFTPAttributes.from_stat(os.stat(path))

    def lstat(self, path):
        return paramiko.SFTPAttributes.from_stat(os.lstat(path))

    def symlink(self, source, dest):
        os.symlink(source, dest)

    def chmod(self, path, mode):
        os.chmod(path, mode)

    def chown(self, path, uid, gid):
        os.chown(path, uid, gid)

    def utime(self, path, times):
        if times is None:
            times = (time.time(), time.time())
        os.utime(path, times)

    def truncate(self, path, size):
        os.truncate(path, size)

    def readlink(self, path):
        return os.readlink(path)

    def normalize(self, path):
        return os.path.realpath(path)

    def chdir(self, path=None):
        os.chdir(path)

    def getcwd(self):
        return os.getcwd()

    def putfo(self, fl, remotepath, file_size=0, callback=None, confirm=True):
        with io.open(remotepath, 'wb') as fdst:
            if file_size:
                while True:
                    buf = fl.read(file_size)
                    if not buf:
                        break
                    fdst.write(buf)
                    if callback:
                        callback(len(buf))
            else:
                buf = fl.read()
                fdst.write(buf)

    def put(self, localpath, remotepath, callback=None, confirm=True):
        file_size = os.stat(localpath).st_size
        with io.open(localpath, "rb") as fl:
            return self.putfo(fl, remotepath, file_size, callback, confirm)

    def getfo(self, remotepath, fl, callback=None, prefetch=True):
        with io.open(remotepath, "rb") as fr:
            buf = fr.read()
            fl.write(buf)
            if callback:
                callback(len(buf))

    def get(self, remotepath, localpath, callback=None, prefetch=True):
        with io.open(localpath, "wb") as fl:
            self.getfo(remotepath, fl, callback, prefetch)


def _fake_exec_command(
        command: List[str],
        bufsize: int = -1,
        timeout: Optional[int] = None,
        environment: Optional[int] = None,
) -> subprocess.CompletedProcess:
    if command[0] == 'cp':
        shutil.copy(command[1], command[2])
    elif command[0] == 'cat':
        with open(command[-1], 'wb') as f:
            for file_name in command[1:-2]:
                with open(file_name, 'rb') as f_src:
                    f.write(f_src.read())
            return subprocess.CompletedProcess(
                args=command,
                returncode=0,
                stdout=b'',
                stderr=b'',
            )
    else:
        raise OSError('Nonsupport command')
    return subprocess.CompletedProcess(
        args=command,
        returncode=0,
        stdout=b'',
        stderr=b'',
    )


@pytest.fixture
def sftp_mocker(fs, mocker):
    client = FakeSFTPClient()
    mocker.patch('megfile.sftp_path.get_sftp_client', return_value=client)
    mocker.patch('megfile.sftp_path.get_ssh_client', return_value=client)
    mocker.patch(
        'megfile.sftp_path.SftpPath._exec_command',
        side_effect=_fake_exec_command)
    yield client


def test_is_sftp():
    assert sftp.is_sftp('sftp://username@host/data') is True
    assert sftp.is_sftp('ftp://username@host/data') is False


def test_sftp_readlink(sftp_mocker):
    path = 'sftp://username@host/file'
    link_path = 'sftp://username@host/file.lnk'

    with sftp.sftp_open(path, 'w') as f:
        f.write('test')

    sftp.sftp_symlink(path, link_path)
    assert sftp.sftp_readlink(link_path) == path

    with pytest.raises(FileNotFoundError):
        sftp.sftp_readlink('sftp://username@host/notFound')

    with pytest.raises(OSError):
        sftp.sftp_readlink('sftp://username@host/file')


def test_sftp_absolute(sftp_mocker):
    assert sftp.sftp_absolute(
        'sftp://username@host/dir/../file') == 'sftp://username@host/file'


def test_sftp_resolve(sftp_mocker):
    assert sftp.sftp_resolve(
        'sftp://username@host/dir/../file') == 'sftp://username@host/file'


def test_sftp_glob(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    sftp.sftp_makedirs('sftp://username@host/A/a')
    sftp.sftp_makedirs('sftp://username@host/A/b')
    sftp.sftp_makedirs('sftp://username@host/A/b/c')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')

    with sftp.sftp_open('sftp://username@host/A/b/file.json', 'w') as f:
        f.write('file')

    assert sftp.sftp_glob('sftp://username@host/A/*') == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/a',
        'sftp://username@host/A/b',
    ]
    assert list(sftp.sftp_iglob('sftp://username@host/A/*')) == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/a',
        'sftp://username@host/A/b',
    ]
    assert sftp.sftp_glob('sftp://username@host/A/**/*.json') == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/b/file.json',
    ]
    assert [
        file_entry.path
        for file_entry in sftp.sftp_glob_stat('sftp://username@host/A/*')
    ] == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/a',
        'sftp://username@host/A/b',
    ]
    assert sftp.sftp_glob('sftp://username@host/A/**/*') == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/a',
        'sftp://username@host/A/b',
        'sftp://username@host/A/b/c',
        'sftp://username@host/A/b/file.json',
    ]
    assert sftp.sftp_glob('sftp://username@host/A/') == [
        'sftp://username@host/A/',
    ]


def test_sftp_isdir_sftp_isfile(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A/B', parents=True)

    with sftp.sftp_open('sftp://username@host/A/B/file', 'w') as f:
        f.write('test')

    assert sftp.sftp_isdir('sftp://username@host/A/B') is True
    assert sftp.sftp_isdir('sftp://username@host/A/B/file') is False
    assert sftp.sftp_isfile('sftp://username@host/A/B/file') is True
    assert sftp.sftp_isfile('sftp://username@host/A/B') is False


def test_sftp_exists(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A/B', parents=True)

    with sftp.sftp_open('sftp://username@host/A/B/file', 'w') as f:
        f.write('test')

    assert sftp.sftp_exists('sftp://username@host/A/B/file') is True

    sftp.sftp_symlink(
        'sftp://username@host/A/B/file', 'sftp://username@host/A/B/file.lnk')
    sftp.sftp_unlink('sftp://username@host/A/B/file')
    assert sftp.sftp_exists('sftp://username@host/A/B/file.lnk') is True
    assert sftp.sftp_exists(
        'sftp://username@host/A/B/file.lnk', followlinks=True) is False


def test_sftp_scandir(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    sftp.sftp_makedirs('sftp://username@host/A/a')
    sftp.sftp_makedirs('sftp://username@host/A/b')
    sftp.sftp_makedirs('sftp://username@host/A/b/c')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')

    with sftp.sftp_open('sftp://username@host/A/b/file.json', 'w') as f:
        f.write('file')

    assert [
        file_entry.path
        for file_entry in sftp.sftp_scandir('sftp://username@host/A')
    ] == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/a',
        'sftp://username@host/A/b',
    ]

    with pytest.raises(FileNotFoundError):
        list(sftp.sftp_scandir('sftp://username@host/A/not_found'))

    with pytest.raises(NotADirectoryError):
        list(sftp.sftp_scandir('sftp://username@host/A/1.json'))


def test_sftp_stat(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/test', 'w') as f:
        f.write('test')
    sftp.sftp_symlink(
        'sftp://username@host/A/test', 'sftp://username@host/A/test.lnk')

    stat = sftp.sftp_stat('sftp://username@host/A/test', follow_symlinks=True)
    os_stat = os.stat('/A/test')
    assert stat.size == os_stat.st_size
    assert stat.isdir is False
    assert stat.islnk is False
    assert stat.mtime == os_stat.st_mtime
    assert stat.ctime == 0

    stat = sftp.sftp_stat(
        'sftp://username@host/A/test.lnk', follow_symlinks=True)
    os_stat = os.stat('/A/test')
    assert stat.size == os_stat.st_size
    assert stat.isdir is False
    assert stat.islnk is False
    assert stat.mtime == os_stat.st_mtime
    assert stat.ctime == 0

    stat = sftp.sftp_lstat('sftp://username@host/A/test.lnk')
    os_stat = os.lstat('/A/test.lnk')
    assert stat.size == os_stat.st_size
    assert stat.isdir is False
    assert stat.islnk is True
    assert stat.mtime == os_stat.st_mtime
    assert stat.ctime == 0

    os_stat = os.stat('/A/test')
    assert sftp.sftp_getmtime('sftp://username@host/A/test') == os_stat.st_mtime
    assert sftp.sftp_getsize('sftp://username@host/A/test') == os_stat.st_size


def test_sftp_listdir(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    sftp.sftp_makedirs('sftp://username@host/A/a')
    sftp.sftp_makedirs('sftp://username@host/A/b')
    sftp.sftp_makedirs('sftp://username@host/A/b/c')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')

    assert sftp.sftp_listdir('sftp://username@host/A') == [
        '1.json',
        'a',
        'b',
    ]


def test_sftp_load_from(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/test', 'w') as f:
        f.write('test')
    assert sftp.sftp_load_from('sftp://username@host/A/test').read() == b'test'


def test_sftp_makedirs(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A/B/C', parents=True)
    assert sftp.sftp_exists('sftp://username@host/A/B/C') is True

    with pytest.raises(FileExistsError):
        sftp.sftp_makedirs('sftp://username@host/A/B/C')

    with pytest.raises(FileNotFoundError):
        sftp.sftp_makedirs('sftp://username@host/D/B/C')


def test_sftp_realpath(sftp_mocker):
    assert sftp.sftp_realpath(
        'sftp://username@host/A/../B/C') == 'sftp://username@host/B/C'


def test_sftp_rename(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/test', 'w') as f:
        f.write('test')

    sftp.sftp_rename(
        'sftp://username@host/A/test', 'sftp://username@host/A/test2')
    assert sftp.sftp_exists('sftp://username@host/A/test') is False
    assert sftp.sftp_exists('sftp://username@host/A/test2') is True

    sftp.sftp_rename('sftp://username@host/A', 'sftp://username@host/A2')
    assert sftp.sftp_exists('sftp://username@host/A/test2') is False
    assert sftp.sftp_exists('sftp://username@host/A2/test2') is True

    sftp.sftp_rename(
        'sftp://username@host/A2/test2', 'sftp://username2@host2/A2/test')
    assert sftp.sftp_exists('sftp://username@host/A2/test2') is False
    assert sftp.sftp_exists('sftp://username2@host2/A2/test') is True

    sftp.sftp_rename('sftp://username@host/A2', 'sftp://username2@host2/A')
    assert sftp.sftp_exists('sftp://username@host/A2/test') is False
    assert sftp.sftp_exists('sftp://username2@host2/A/test') is True


def test_sftp_move(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/test', 'w') as f:
        f.write('test')

    sftp.sftp_move(
        'sftp://username@host/A/test', 'sftp://username@host/A/test2')
    assert sftp.sftp_exists('sftp://username@host/A/test') is False
    assert sftp.sftp_exists('sftp://username@host/A/test2') is True

    sftp.sftp_move('sftp://username@host/A', 'sftp://username@host/A2')
    assert sftp.sftp_exists('sftp://username@host/A/test2') is False
    assert sftp.sftp_exists('sftp://username@host/A2/test2') is True


def test_sftp_open(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')

    with sftp.sftp_open('sftp://username@host/A/test', 'w') as f:
        f.write('test')

    with sftp.sftp_open('sftp://username@host/A/test', 'r') as f:
        assert f.read() == 'test'

    with sftp.sftp_open('sftp://username@host/A/test', 'rb') as f:
        assert f.read() == b'test'

    with pytest.raises(FileNotFoundError):
        with sftp.sftp_open('sftp://username@host/A/notFound', 'r') as f:
            f.read()

    with pytest.raises(IsADirectoryError):
        with sftp.sftp_open('sftp://username@host/A', 'r') as f:
            f.read()

    with pytest.raises(IsADirectoryError):
        with sftp.sftp_open('sftp://username@host/A', 'w') as f:
            f.read()


def test_sftp_remove(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/test', 'w') as f:
        f.write('test')

    sftp.sftp_remove('sftp://username@host/A/test')
    sftp.sftp_remove('sftp://username@host/A/test', missing_ok=True)

    assert sftp.sftp_exists('sftp://username@host/A/test') is False
    assert sftp.sftp_exists('sftp://username@host/A') is True

    with sftp.sftp_open('sftp://username@host/A/test', 'w') as f:
        f.write('test')
    sftp.sftp_remove('sftp://username@host/A')
    assert sftp.sftp_exists('sftp://username@host/A') is False


def test_sftp_scan(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    sftp.sftp_makedirs('sftp://username@host/A/a')
    sftp.sftp_makedirs('sftp://username@host/A/b')
    sftp.sftp_makedirs('sftp://username@host/A/b/c')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')
    sftp.sftp_symlink(
        'sftp://username@host/A/1.json', 'sftp://username@host/A/1.json.lnk')

    with sftp.sftp_open('sftp://username@host/A/b/file.json', 'w') as f:
        f.write('file')

    assert list(sftp.sftp_scan('sftp://username@host/A')) == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/1.json.lnk',
        'sftp://username@host/A/b/file.json',
    ]

    assert list(sftp.sftp_scan('sftp://username@host/A', followlinks=True)) == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/1.json.lnk',
        'sftp://username@host/A/b/file.json',
    ]

    assert [
        file_entry.path
        for file_entry in sftp.sftp_scan_stat('sftp://username@host/A')
    ] == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/1.json.lnk',
        'sftp://username@host/A/b/file.json',
    ]

    assert [
        file_entry.stat.size for file_entry in sftp.sftp_scan_stat(
            'sftp://username@host/A', followlinks=True)
    ] == [
        os.stat('/A/1.json').st_size,
        os.stat('/A/1.json').st_size,
        os.stat('/A/b/file.json').st_size,
    ]


def test_sftp_unlink(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/test', 'w') as f:
        f.write('test')

    sftp.sftp_unlink('sftp://username@host/A/test')
    sftp.sftp_unlink('sftp://username@host/A/test', missing_ok=True)

    assert sftp.sftp_exists('sftp://username@host/A/test') is False
    assert sftp.sftp_exists('sftp://username@host/A') is True

    with pytest.raises(IsADirectoryError):
        sftp.sftp_unlink('sftp://username@host/A')


def test_sftp_walk(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    sftp.sftp_makedirs('sftp://username@host/A/a')
    sftp.sftp_makedirs('sftp://username@host/A/a/b')
    sftp.sftp_makedirs('sftp://username@host/A/b')
    sftp.sftp_makedirs('sftp://username@host/A/b/c')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')
    with sftp.sftp_open('sftp://username@host/A/a/2.json', 'w') as f:
        f.write('2.json')
    with sftp.sftp_open('sftp://username@host/A/b/3.json', 'w') as f:
        f.write('3.json')

    assert list(sftp.sftp_walk('sftp://username@host/A')) == [
        ('sftp://username@host/A', ['a', 'b'], ['1.json']),
        ('sftp://username@host/A/a', ['b'], ['2.json']),
        ('sftp://username@host/A/a/b', [], []),
        ('sftp://username@host/A/b', ['c'], ['3.json']),
        ('sftp://username@host/A/b/c', [], []),
    ]

    assert list(sftp.sftp_walk('sftp://username@host/A/not_found')) == []
    assert list(sftp.sftp_walk('sftp://username@host/A/1.json')) == []


def test_sftp_getmd5(sftp_mocker):
    from megfile.fs import fs_getmd5

    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')
    assert sftp.sftp_getmd5('sftp://username@host/A/1.json') == fs_getmd5(
        '/A/1.json')
    assert sftp.sftp_getmd5('sftp://username@host/A') == fs_getmd5('/A')


def test_sftp_symlink(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')
    sftp.sftp_symlink(
        'sftp://username@host/A/1.json', 'sftp://username@host/A/1.json.lnk')
    assert sftp.sftp_islink('sftp://username@host/A/1.json.lnk') is True
    assert sftp.sftp_islink('sftp://username@host/A/1.json') is False

    with pytest.raises(FileExistsError):
        sftp.sftp_symlink(
            'sftp://username@host/A/1.json',
            'sftp://username@host/A/1.json.lnk')


def test_sftp_save_as(sftp_mocker):
    sftp.sftp_save_as(io.BytesIO(b'test'), 'sftp://username@host/test')
    assert sftp.sftp_load_from('sftp://username@host/test').read() == b'test'


def test_sftp_chmod(sftp_mocker):
    path = 'sftp://username@host/test'
    sftp.sftp_save_as(io.BytesIO(b'test'), path)

    sftp.sftp_chmod(path, mode=0o777)
    assert stat.S_IMODE(os.stat('/test').st_mode) == 0o777


def test_sftp_rmdir(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')

    with pytest.raises(OSError):
        sftp.sftp_rmdir('sftp://username@host/A')

    with pytest.raises(NotADirectoryError):
        sftp.sftp_rmdir('sftp://username@host/A/1.json')

    sftp.sftp_unlink('sftp://username@host/A/1.json')
    sftp.sftp_rmdir('sftp://username@host/A')
    assert sftp.sftp_exists('sftp://username@host/A') is False


def test_sftp_copy(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')
    sftp.sftp_symlink(
        'sftp://username@host/A/1.json', 'sftp://username@host/A/1.json.lnk')

    with pytest.raises(IsADirectoryError):
        sftp.sftp_copy('sftp://username@host/A', 'sftp://username@host/A2')

    with pytest.raises(OSError):
        sftp.sftp_copy('sftp://username@host/A', '/A2')

    def callback(length):
        assert length == len('1.json')

    sftp.sftp_copy(
        'sftp://username@host/A/1.json.lnk',
        'sftp://username@host/A/1.json.bak',
        followlinks=True,
        callback=callback)

    assert sftp.sftp_stat(
        'sftp://username@host/A/1.json').size == sftp.sftp_stat(
            'sftp://username@host/A/1.json.bak').size


def test_sftp_copy_with_different_host(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')

    def callback(length):
        assert length == len('1.json')

    sftp.sftp_copy(
        'sftp://username@host/A/1.json',
        'sftp://username@host2/A/2.json',
        callback=callback,
    )

    assert sftp.sftp_stat(
        'sftp://username@host/A/1.json').size == sftp.sftp_stat(
            'sftp://username@host2/A/2.json').size


def test_sftp_sync(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')

    sftp.sftp_sync('sftp://username@host/A', 'sftp://username@host/A2')
    assert sftp.sftp_stat(
        'sftp://username@host/A/1.json').size == sftp.sftp_stat(
            'sftp://username@host/A2/1.json').size

    sftp.sftp_sync(
        'sftp://username@host/A/1.json', 'sftp://username@host/A/1.json.bak')
    assert sftp.sftp_stat(
        'sftp://username@host/A/1.json').size == sftp.sftp_stat(
            'sftp://username@host/A/1.json.bak').size


def test_sftp_download(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')
    sftp.sftp_symlink(
        'sftp://username@host/A/1.json', 'sftp://username@host/A/1.json.lnk')

    sftp.sftp_download(
        'sftp://username@host/A/1.json.lnk', '/1.json', followlinks=True)
    assert sftp.sftp_stat('sftp://username@host/A/1.json').size == os.stat(
        '/1.json').st_size

    with pytest.raises(OSError):
        sftp.sftp_download(
            'sftp://username@host/A/1.json', 'sftp://username@host/1.json')

    with pytest.raises(OSError):
        sftp.sftp_download('/1.json', '/1.json')

    with pytest.raises(IsADirectoryError):
        sftp.sftp_download('sftp://username@host/A', '/1.json')


def test_sftp_upload(sftp_mocker):
    with sftp.sftp_open('/1.json', 'w') as f:
        f.write('1.json')
    os.symlink('/1.json', '/1.json.lnk')

    sftp.sftp_makedirs('sftp://username@host/A')
    sftp.sftp_upload(
        '/1.json.lnk', 'sftp://username@host/A/1.json', followlinks=True)
    assert sftp.sftp_stat('sftp://username@host/A/1.json').size == os.stat(
        '/1.json').st_size

    with pytest.raises(OSError):
        sftp.sftp_upload(
            'sftp://username@host/A/1.json', 'sftp://username@host/1.json')

    with pytest.raises(OSError):
        sftp.sftp_upload('/1.json', '/1.json')

    with pytest.raises(IsADirectoryError):
        sftp.sftp_upload('/', 'sftp://username@host/A')


def test_sftp_path_join():
    assert sftp.sftp_path_join(
        'sftp://username@host/A/', 'a', 'b') == 'sftp://username@host/A/a/b'


def test_sftp_concat(sftp_mocker, mocker):
    with sftp.sftp_open('sftp://username@host/1', 'w') as f:
        f.write('1')
    with sftp.sftp_open('sftp://username@host/2', 'w') as f:
        f.write('2')
    with sftp.sftp_open('sftp://username@host/3', 'w') as f:
        f.write('3')

    sftp.sftp_concat(
        [
            'sftp://username@host/1', 'sftp://username@host/2',
            'sftp://username@host/3'
        ], 'sftp://username@host/4')
    with sftp.sftp_open('sftp://username@host/4', 'r') as f:
        assert f.read() == '123'

    def _error_exec_command(
            command: List[str],
            bufsize: int = -1,
            timeout: Optional[int] = None,
            environment: Optional[int] = None,
    ):
        return subprocess.CompletedProcess(args=command, returncode=1)

    mocker.patch(
        'megfile.sftp_path.SftpPath._exec_command',
        side_effect=_error_exec_command)
    with pytest.raises(OSError):
        sftp.sftp_concat(
            [
                'sftp://username@host/1', 'sftp://username@host/2',
                'sftp://username@host/3'
            ], 'sftp://username@host/4')

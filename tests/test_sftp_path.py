import os

import pytest

from megfile import sftp
from megfile.sftp_path import SFTP_PASSWORD, SFTP_PRIVATE_KEY_PATH, SFTP_USERNAME, SftpPath, get_private_key, provide_connect_info


def test_provide_connect_info(fs, mocker):
    hostname, port, username, password, private_key = 'test_hostname', 22, 'testuser', 'testpwd', '/test_key_path'
    with open(private_key, 'w'):
        pass
    mocker.patch(
        'paramiko.RSAKey.from_private_key_file', return_value=private_key)
    assert provide_connect_info(hostname) == (hostname, 22, None, None, None)

    os.environ[SFTP_USERNAME] = username
    os.environ[SFTP_PASSWORD] = password
    os.environ[SFTP_PRIVATE_KEY_PATH] = private_key

    assert provide_connect_info(
        hostname, port) == (hostname, port, username, password, private_key)


def test_sftp_glob(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    sftp.sftp_makedirs('sftp://username@host/A/a')
    sftp.sftp_makedirs('sftp://username@host/A/b')
    sftp.sftp_makedirs('sftp://username@host/A/b/c')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')

    with sftp.sftp_open('sftp://username@host/A/b/file.json', 'w') as f:
        f.write('file')

    assert SftpPath('sftp://username@host/A/').glob('*') == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/a',
        'sftp://username@host/A/b',
    ]
    assert list(SftpPath('sftp://username@host/A').iglob('*')) == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/a',
        'sftp://username@host/A/b',
    ]
    assert SftpPath('sftp://username@host/A').rglob('*.json') == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/b/file.json',
    ]
    assert [
        file_entry.path
        for file_entry in SftpPath('sftp://username@host/A').glob_stat('*')
    ] == [
        'sftp://username@host/A/1.json',
        'sftp://username@host/A/a',
        'sftp://username@host/A/b',
    ]


def test_iterdir(sftp_mocker):
    sftp.sftp_makedirs('sftp://username@host/A')
    sftp.sftp_makedirs('sftp://username@host/A/a')
    sftp.sftp_makedirs('sftp://username@host/A/b')
    sftp.sftp_makedirs('sftp://username@host/A/b/c')
    with sftp.sftp_open('sftp://username@host/A/1.json', 'w') as f:
        f.write('1.json')

    assert list(SftpPath('sftp://username@host/A').iterdir()) == [
        SftpPath('sftp://username@host/A/1.json'),
        SftpPath('sftp://username@host/A/a'),
        SftpPath('sftp://username@host/A/b'),
    ]

    with pytest.raises(NotADirectoryError):
        list(SftpPath('sftp://username@host/A/1.json').iterdir())


def test_cwd(sftp_mocker):
    assert SftpPath('sftp://username@host/A').cwd() == 'sftp://username@host'


def test_get_private_key(fs):
    with pytest.raises(FileNotFoundError):
        os.environ['SFTP_PRIVATE_KEY_PATH'] = '/file_not_exist'
        get_private_key()

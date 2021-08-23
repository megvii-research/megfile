import os
from io import BytesIO

import pytest

from megfile import fs, smart
from megfile.interfaces import Access, StatResult

from . import FakeStatResult, Now


@pytest.fixture
def filesystem(fs):
    return fs


def make_stat(size=0, time=Now(), isdir=False, islnk=False):
    return StatResult(
        size=size, ctime=time, mtime=time, isdir=isdir, islnk=islnk)


def test_is_fs():
    assert fs.is_fs('/abs/path') is True
    assert fs.is_fs('rel/path') is True
    assert fs.is_fs('file:///abs/path') is True
    assert fs.is_fs('file://rel/path') is True
    assert fs.is_fs('s3://rel/path') is False


def test_fs_getsize(filesystem):
    with pytest.raises(FileNotFoundError):
        fs.fs_getsize('NotExist')
    with pytest.raises(FileNotFoundError):
        fs.fs_getsize('')

    # nothing in the root
    assert fs.fs_getsize('/') == 0
    assert fs.fs_getsize('.') == 0

    # /
    #   - empty-folder
    os.mkdir('empty-folder')
    assert fs.fs_getsize('/') == 0
    assert fs.fs_getsize('.') == 0

    # /
    #   - empty-folder
    #   - folderA/fileA, content: 'fileA'
    if not os.path.exists('folderA'):
        os.mkdir('folderA')
    with open('/folderA/fileA', 'wb') as fileA:
        fileA.write(b'fileA')
    assert fs.fs_getsize('/') == 5
    assert fs.fs_getsize('.') == 5

    # /
    #   - empty-folder
    #   - folderA/fileA, content: 'fileA'
    #   - fileB, content: 'fileB'
    with open('/fileB', 'wb') as fileB:
        fileB.write(b'fileB')
    assert fs.fs_getsize('/') == 10
    assert fs.fs_getsize('.') == 10

    # /
    #   - empty-folder
    #   - folderA/
    #               - fileA, content: 'fileA'
    #               - symbol-link -> /fileB
    #   - fileB, content: 'fileB'
    os.symlink('fileB', '/folderA/symbol-link')
    # folder size = fileA + fileB + sizeof(symbol-link)
    symlink_size = os.lstat('/folderA/symbol-link').st_size
    expected_size = 10 + symlink_size
    assert fs.fs_getsize('/') == expected_size
    assert fs.fs_getsize('.') == expected_size

    # /
    #   - empty-folder
    #   - folderA/
    #               - fileA, content: 'fileA'
    #               - symbol-link -> /fileB
    #               - hard-link -> /fileB
    #   - fileB, content: 'fileB'
    os.link('fileB', '/folderA/hard-link')
    # folder size = fileA + fileB + sizeof(symbol-link) + sizeof(hard-link)
    hardlink_size = os.lstat('/folderA/hard-link').st_size
    expected_size = 10 + symlink_size + hardlink_size
    assert fs.fs_getsize('/') == expected_size
    assert fs.fs_getsize('.') == expected_size

    os.mkdir('folder')
    assert fs.fs_getsize('folder') == 0
    with open('file', 'wb') as f:
        f.write(b"This is a file.")
    assert fs.fs_getsize('file') == os.path.getsize("file")
    os.link('file', 'hard_link')
    assert fs.fs_getsize('hard_link') == os.path.getsize("file")
    os.symlink('file', 'soft_link_file')
    assert fs.fs_getsize('soft_link_file') == os.lstat("soft_link_file").st_size
    os.symlink('folder', 'soft_link_folder')
    assert fs.fs_getsize('soft_link_folder') == os.lstat(
        "soft_link_folder").st_size


def test_fs_getmtime(filesystem):
    with pytest.raises(FileNotFoundError):
        fs.fs_getmtime('NotExist')
    with pytest.raises(FileNotFoundError):
        fs.fs_getmtime('')

    # nothing in the root
    assert fs.fs_getmtime('/') == Now()
    assert fs.fs_getmtime('.') == Now()

    # /
    #   - empty-folder
    os.mkdir('empty-folder')
    assert fs.fs_getmtime('/') == Now()
    assert fs.fs_getmtime('.') == Now()

    # /
    #   - empty-folder
    #   - folderA/fileA, content: 'fileA'
    if not os.path.exists('folderA'):
        os.mkdir('folderA')
    with open('/folderA/fileA', 'wb') as fileA:
        fileA.write(b'fileA')
    assert fs.fs_getmtime('/') == Now()
    assert fs.fs_getmtime('.') == Now()

    # /
    #   - empty-folder
    #   - folderA/fileA, content: 'fileA'
    #   - fileB, content: 'fileB'
    with open('/fileB', 'wb') as fileB:
        fileB.write(b'fileB')
    assert fs.fs_getmtime('/') == Now()
    assert fs.fs_getmtime('.') == Now()

    # /
    #   - empty-folder
    #   - folderA/
    #               - fileA, content: 'fileA'
    #               - symbol-link -> /fileB
    #   - fileB, content: 'fileB'
    os.symlink('fileB', '/folderA/symbol-link')
    # folder size = fileA + fileB + sizeof(symbol-link)
    assert fs.fs_getmtime('/') == Now()
    assert fs.fs_getmtime('.') == Now()

    # /
    #   - empty-folder
    #   - folderA/
    #               - fileA, content: 'fileA'
    #               - symbol-link -> /fileB
    #               - hard-link -> /fileB
    #   - fileB, content: 'fileB'
    os.link('fileB', '/folderA/hard-link')
    # folder size = fileA + fileB + sizeof(symbol-link) + sizeof(hard-link)
    assert fs.fs_getmtime('/') == Now()
    assert fs.fs_getmtime('.') == Now()

    os.mkdir('folder')
    assert fs.fs_getmtime('folder') == Now()
    with open('file', 'wb') as f:
        f.write(b"This is a file.")
    assert fs.fs_getmtime('file') == Now()
    os.link('file', 'hard_link')
    assert fs.fs_getmtime('hard_link') == Now()
    os.symlink('file', 'soft_link_file')
    assert fs.fs_getmtime('soft_link_file') == Now()
    os.symlink('folder', 'soft_link_folder')
    assert fs.fs_getmtime('soft_link_folder') == Now()


def test_fs_stat(filesystem, mocker):
    mocker.patch('megfile.fs.StatResult', side_effect=FakeStatResult)

    with pytest.raises(FileNotFoundError):
        fs.fs_stat('NotExist')
    with pytest.raises(FileNotFoundError):
        fs.fs_stat('')

    # nothing in the root
    assert fs.fs_stat('/') == make_stat(isdir=True)
    assert fs.fs_stat('.') == make_stat(isdir=True)

    # /
    #   - empty-folder
    os.mkdir('empty-folder')
    assert fs.fs_stat('/') == make_stat(isdir=True)
    assert fs.fs_stat('.') == make_stat(isdir=True)

    # /
    #   - empty-folder
    #   - folderA/fileA, content: 'fileA'
    if not os.path.exists('folderA'):
        os.mkdir('folderA')
    with open('/folderA/fileA', 'wb') as fileA:
        fileA.write(b'fileA')
    assert fs.fs_stat('/') == make_stat(size=5, isdir=True)
    assert fs.fs_stat('.') == make_stat(size=5, isdir=True)

    # /
    #   - empty-folder
    #   - folderA/fileA, content: 'fileA'
    #   - fileB, content: 'fileB'
    with open('/fileB', 'wb') as fileB:
        fileB.write(b'fileB')
    assert fs.fs_stat('/') == make_stat(size=10, isdir=True)
    assert fs.fs_stat('.') == make_stat(size=10, isdir=True)

    # /
    #   - empty-folder
    #   - folderA/
    #               - fileA, content: 'fileA'
    #               - symbol-link -> /fileB
    #   - fileB, content: 'fileB'
    os.symlink('fileB', '/folderA/symbol-link')
    # folder size = fileA + fileB + sizeof(symbol-link)
    symlink_size = os.lstat('/folderA/symbol-link').st_size
    expected_size = 10 + symlink_size
    assert fs.fs_stat('/') == make_stat(size=expected_size, isdir=True)
    assert fs.fs_stat('.') == make_stat(size=expected_size, isdir=True)

    # /
    #   - empty-folder
    #   - folderA/
    #               - fileA, content: 'fileA'
    #               - symbol-link -> /fileB
    #               - hard-link -> /fileB
    #   - fileB, content: 'fileB'
    os.link('fileB', '/folderA/hard-link')
    # folder size = fileA + fileB + sizeof(symbol-link) + sizeof(hard-link)
    hardlink_size = os.lstat('/folderA/hard-link').st_size
    expected_size = 10 + symlink_size + hardlink_size
    assert fs.fs_stat('/') == make_stat(size=expected_size, isdir=True)
    assert fs.fs_stat('.') == make_stat(size=expected_size, isdir=True)

    os.mkdir('folder')
    assert fs.fs_stat('folder') == make_stat(isdir=True)
    with open('file', 'wb') as f:
        f.write(b"This is a file.")
    assert fs.fs_stat('file') == make_stat(size=os.path.getsize("file"))
    os.link('file', 'hard_link')
    assert fs.fs_stat('hard_link') == make_stat(size=os.path.getsize("file"))
    os.symlink('file', 'soft_link_file')
    assert fs.fs_stat('soft_link_file') == make_stat(
        size=os.lstat("soft_link_file").st_size, islnk=True)
    os.symlink('folder', 'soft_link_folder')
    assert fs.fs_stat('soft_link_folder') == make_stat(
        size=os.lstat("soft_link_folder").st_size, islnk=True)


def test_fs_isdir(filesystem):
    os.mkdir('folder')
    assert fs.fs_isdir('folder') is True
    assert fs.fs_isdir('NotExist') is False
    with open('file', 'w') as f:
        f.write('file')
    assert fs.fs_isdir('file') is False
    os.symlink('folder', 'soft_link_folder')
    assert fs.fs_isdir('soft_link_folder') is False


def test_fs_isfile(filesystem):
    os.mkdir('folder')
    assert fs.fs_isfile('folder') is False
    assert fs.fs_isfile('NotExist') is False
    with open('file', 'w') as f:
        f.write('file')
    assert fs.fs_isfile('file') is True
    os.symlink('folder', 'soft_link_folder')
    assert fs.fs_isfile('soft_link_folder') is True


def test_fs_access(filesystem):
    os.mkdir('folder')
    with open('file', 'w') as f:
        f.write('file')
    with pytest.raises(TypeError) as error:
        fs.fs_access('folder', 'r')
    assert fs.fs_access('folder', Access.READ) == True
    assert fs.fs_access('folder', Access.WRITE) == True
    assert fs.fs_access('file', Access.READ) == True
    assert fs.fs_access('file', Access.WRITE) == True
    os.chmod('./file', 0o000)
    assert fs.fs_access('file', Access.READ) == False
    assert fs.fs_access('file', Access.WRITE) == False


def test_fs_exists(filesystem):
    os.mkdir('folder')
    assert fs.fs_exists('folder') is True
    assert fs.fs_exists('NotExist') is False
    with open('file', 'w') as f:
        f.write('file')
    assert fs.fs_exists('file') is True
    os.symlink('folder', 'soft_link_folder')
    os.removedirs('folder')
    assert fs.fs_exists('soft_link_folder') is True
    assert fs.fs_exists('folder') is False


def test_fs_remove(filesystem, mocker):
    remove = mocker.patch('megfile.fs.os.remove')
    rmtree = mocker.patch('megfile.fs.shutil.rmtree')
    if_func = mocker.patch('megfile.fs.fs_isdir')
    exists_func = mocker.patch('megfile.fs.fs_exists')

    def isdir(path: str) -> bool:
        return path == 'folder'

    def exists(path: str) -> bool:
        return path != 'notExist'

    if_func.side_effect = isdir
    exists_func.side_effect = exists

    fs.fs_remove('folder')
    rmtree.assert_called_once_with('folder')
    rmtree.reset_mock()
    fs.fs_remove('file')
    remove.assert_called_once_with('file')
    remove.reset_mock()
    fs.fs_remove('link')
    remove.assert_called_once_with('link')
    remove.reset_mock()

    fs.fs_remove('notExist')
    remove.assert_called_once_with('notExist')
    remove.reset_mock()
    fs.fs_remove('notExist', missing_ok=True)
    # remove.assert_not_called() in Python 3.6+
    assert remove.call_count == 0


def test_fs_unlink(filesystem, mocker):
    os.makedirs('folder')
    with pytest.raises(IsADirectoryError):
        fs.fs_unlink('folder')

    with pytest.raises(FileNotFoundError):
        fs.fs_unlink('notExist')

    fs.fs_unlink('notExist', missing_ok=True)


def test_fs_makedirs(filesystem):
    fs.fs_makedirs('folder/folder')
    assert os.path.isdir('folder/folder') is True

    with pytest.raises(FileExistsError) as error:
        fs.fs_makedirs('folder/blah/../folder')
    assert 'folder/folder' in str(error.value)

    fs.fs_makedirs('folder/folder', exist_ok=True)

    with open('file', 'w') as f:
        f.write('')

    with pytest.raises(FileExistsError) as error:
        fs.fs_makedirs('file', exist_ok=True)
    assert 'file' in str(error.value)


def test_fs_path_join():
    assert fs.fs_path_join('/') == '/'
    assert fs.fs_path_join('/', 'bucket/key') == '/bucket/key'
    assert fs.fs_path_join('/', 'bucket//key') == '/bucket//key'
    assert fs.fs_path_join('/', 'bucket', 'key') == '/bucket/key'
    assert fs.fs_path_join('/', 'bucket/', 'key') == '/bucket/key'
    assert fs.fs_path_join('/', 'bucket', '/key') == '/key'
    assert fs.fs_path_join('/', 'bucket', 'key/') == '/bucket/key/'


def test_smart_open_read_not_found(filesystem):
    with pytest.raises(FileNotFoundError) as error:
        smart.smart_open('notexists', 'r')
    assert 'notexists' in str(error.value)


def test_fs_walk_not_a_dir(filesystem):
    '''
    /
        - file
    '''
    not_exist = 'notExists'
    assert list(fs.fs_walk(not_exist)) == []

    with open('file', 'w') as f:
        f.write('')
    assert list(fs.fs_walk('file')) == []


def test_fs_walk_empty(filesystem):
    '''
    /A/
        <nothing>
    '''
    os.mkdir('A')
    assert list(fs.fs_walk("/A")) == [('/A', [], [])]
    assert list(fs.fs_walk("./A")) == [('A', [], [])]
    assert list(fs.fs_walk("A")) == [('A', [], [])]


def test_fs_walk_skip_link(filesystem):
    '''
    /A/
        - link --> A
    '''
    os.mkdir('A')
    os.symlink('A', '/A/link')
    assert list(fs.fs_walk('A')) == [('A', [], ['link'])]
    assert list(fs.fs_walk('./A')) == [('A', [], ['link'])]
    assert list(fs.fs_walk('/A')) == [('/A', [], ['link'])]


def test_fs_walk_with_lexicographical_order(filesystem):
    '''
    /A/
        - file1
        - file2
        - folder1/
        - folder2/
    '''
    os.mkdir('A')
    os.mkdir('A/folder1')
    os.mkdir('A/folder2')
    with open('A/file1', 'w') as f1, open('A/file2', 'w') as f2:
        f1.write('')
        f2.write('')
    assert list(fs.fs_walk("/A")) == [
        ('/A', ['folder1', 'folder2'], ['file1', 'file2']),
        ('/A/folder1', [], []),
        ('/A/folder2', [], []),
    ]
    assert list(fs.fs_walk("./A")) == [
        ('A', ['folder1', 'folder2'], ['file1', 'file2']),
        ('A/folder1', [], []),
        ('A/folder2', [], []),
    ]
    assert list(fs.fs_walk("A")) == [
        ('A', ['folder1', 'folder2'], ['file1', 'file2']),
        ('A/folder1', [], []),
        ('A/folder2', [], []),
    ]


def test_fs_walk_with_nested_subdirs(filesystem):
    '''
    /A/
        - folder1/
            - sub1/file1
            - sub2
        - folder2/
            - link --> /A/folder1
    '''
    os.mkdir('A')
    os.mkdir('/A/folder1')
    os.mkdir('/A/folder2')
    os.mkdir('/A/folder1/sub1')
    os.mkdir('/A/folder1/sub2')
    with open('A/folder1/sub1/file1', 'w') as f:
        f.write('')
    os.symlink('/A/folder1', '/A/folder2/link')
    assert list(fs.fs_walk('A')) == [
        ('A', ['folder1', 'folder2'], []), ('A/folder1', ['sub1', 'sub2'], []),
        ('A/folder1/sub1', [], ['file1']), ('A/folder1/sub2', [], []),
        ('A/folder2', [], ['link'])
    ]


def test_fs_scan(filesystem):
    '''
    /A/
        - folder1/
            - sub1/file1
            - sub2
        - folder2/
            - link --> /A/folder1
    '''
    os.mkdir('A')
    os.mkdir('/A/folder1')
    os.mkdir('/A/folder2')
    os.mkdir('/A/folder3')
    os.mkdir('/A/folder1/sub1')
    os.mkdir('/A/folder1/sub2')
    with open('A/folder1/sub1/file1', 'w') as f:
        f.write('')
    os.symlink('/A/folder1', '/A/folder2/link')
    assert list(fs.fs_scan('A')) == ['A/folder1/sub1/file1', 'A/folder2/link']
    assert list(fs.fs_scan('A/folder1/sub1/file1')) == ['A/folder1/sub1/file1']
    with pytest.raises(FileNotFoundError):
        list(fs.fs_scan('/B', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(fs.fs_scan('/A/folder3', missing_ok=False))


def test_fs_scan_stat(filesystem, mocker):
    '''
    /A/
        - folder1/
            - sub1/file1
            - sub2
        - folder2/
            - link --> /A/folder1
    '''
    mocker.patch('megfile.fs.StatResult', side_effect=FakeStatResult)

    os.mkdir('A')
    os.mkdir('/A/folder1')
    os.mkdir('/A/folder2')
    os.mkdir('/A/folder3')
    os.mkdir('/A/folder1/sub1')
    os.mkdir('/A/folder1/sub2')
    with open('A/folder1/sub1/file1', 'w') as f:
        f.write('file1')
    os.symlink('/A/folder1', '/A/folder2/link')
    assert list(fs.fs_scan_stat('A')) == [
        ('A/folder1/sub1/file1', make_stat(size=5)),
        ('A/folder2/link', make_stat(size=10, islnk=True)),  # symlink size
    ]

    assert list(fs.fs_scan_stat('A/folder1/sub1/file1')) == [
        ('A/folder1/sub1/file1', make_stat(size=5))
    ]

    with pytest.raises(FileNotFoundError):
        list(fs.fs_scan('/B', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(fs.fs_scan('/A/folder3', missing_ok=False))


def test_fs_scandir(filesystem):
    '''
    /A/
        - folder1/
            - sub1/file1
            - sub2
        - folder2/
            - link --> /A/folder1
    '''
    os.mkdir('A')
    os.mkdir('/A/folder1')
    os.mkdir('/A/folder2')
    os.mkdir('/A/folder1/sub1')
    os.mkdir('/A/folder1/sub2')
    with open('A/folder1/sub1/file1', 'w') as f:
        f.write('')
    os.symlink('/A/folder1', '/A/folder2/link')
    assert len(list(fs.fs_scandir('A'))) == 2
    assert len(list(fs.fs_scandir('A/folder1'))) == 2


@pytest.fixture
def create_glob_fake_dirtree(filesystem):
    '''
    /A/
        - a/
            -b/
                - c/
                    - 1.json
                    - 2.json
            - .hidden
        - b/
            - b <symbol-link to /A/a/b>
            - file
    '''
    os.mkdir('A')
    os.mkdir('A/a')
    os.mkdir('A/a/b')
    os.mkdir('A/a/b/c')
    os.mkdir('A/b')
    with open('A/a/b/c/1.json', 'w') as f:
        f.write('1.json')
    with open('A/a/.hidden', 'w') as f:
        f.write('.hidden')
    with open('A/a/b/c/2.json', 'w') as f:
        f.write('2.json')

    os.symlink('A/a/b', '/A/b/b')
    with open('A/b/file', 'w') as f:
        f.write('file')


def test_fs_glob_returns_lexicographical_result(create_glob_fake_dirtree):
    res = list(fs.fs_glob('A/b/file'))
    assert res == ['A/b/file']
    # lexifographical
    # without hidden file
    res = list(fs.fs_glob('A/a/**', recursive=True))
    expected = [
        'A/a/',  # 奇怪！
        'A/a/b',
        'A/a/b/c',
        'A/a/b/c/1.json',
        'A/a/b/c/2.json',
    ]
    assert sorted(res) == expected

    with pytest.raises(FileNotFoundError):
        list(fs.fs_glob('B', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(fs.fs_glob('B/**', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(fs.fs_glob('A/a/**.notExists', missing_ok=False))


def test_fs_glob_with_symbol_link(create_glob_fake_dirtree):
    # treat symbol link as file
    res = list(fs.fs_glob('A/b/*'))
    expected = ['A/b/b', 'A/b/file']
    assert sorted(res) == expected


def test_fs_glob_with_double_star(create_glob_fake_dirtree):
    res = list(fs.fs_glob('A/b/**', recursive=False))
    expected = ['A/b/b', 'A/b/file']
    assert sorted(res) == expected

    res = list(fs.fs_glob('A/b/**', recursive=True))
    expected = ['A/b/', 'A/b/b', 'A/b/file']
    assert sorted(res) == expected

    res = list(fs.fs_glob('A/b/file/**', recursive=True))
    expected = ['A/b/file/']  # 奇怪！
    assert sorted(res) == expected

    res = list(fs.fs_glob('A/b/b/**', recursive=True))
    expected = ['A/b/b/']  # 奇怪！
    assert sorted(res) == expected


def test_fs_glob_with_not_exists_directory(filesystem):

    # not exists
    res = list(fs.fs_glob('notExistsDir/**', recursive=True))
    expected = []
    assert sorted(res) == expected

    res = list(fs.fs_glob('notExistsDir/*'))
    expected = []
    assert sorted(res) == expected

    res = list(fs.fs_glob('notExistsDir/**', recursive=False))
    expected = []
    assert sorted(res) == expected


def test_fs_save_as(filesystem):
    content = b'value'
    fs.fs_save_as(BytesIO(content), '/path/to/file')
    with open('/path/to/file', 'rb') as result:
        assert content == result.read()


def test_fs_glob_stat(create_glob_fake_dirtree):
    res = list(fs.fs_glob_stat('A/b/file'))
    assert res[0][0] == 'A/b/file'
    assert res[0][1].size == 4
    res = list(fs.fs_glob_stat('A/a/**', recursive=True))
    expected_names = [
        'A/a/',
        'A/a/b',
        'A/a/b/c',
        'A/a/b/c/1.json',
        'A/a/b/c/2.json',
    ]
    expected_sizes = [
        0,
        0,
        0,
        6,
        6,
    ]
    expected_isdirs = [
        True,
        True,
        True,
        False,
        False,
    ]
    res = sorted(res)
    assert [r[0] for r in res] == expected_names
    assert [r[1].size for r in res] == expected_sizes
    assert [r[1].isdir for r in res] == expected_isdirs

    with pytest.raises(FileNotFoundError):
        list(fs.fs_glob('B', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(fs.fs_glob('B/**', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(fs.fs_glob('A/a/**.notExists', missing_ok=False))


def test_fs_load_from(filesystem):
    with open('file', 'wb') as f:
        f.write(b'value')
    content = fs.fs_load_from('/file')
    assert content.read() == b'value'


def test_fs_copy(filesystem):
    with open('file', 'wb') as f:
        f.write(b'0' * (16 * 1024 + 1))

    class bar:

        def __init__(self):
            self._num = 0

        def __call__(self, x):
            if self._num == 0:
                assert x == 16 * 1024
            else:
                x == 1

            self._num += x

    fs.fs_copy('/file', '/file1')


def test_fs_rename(filesystem):
    src = 'file'
    dst = 'file1'
    with open(src, 'w') as f:
        f.write("test")
    assert os.path.exists(src)
    fs.fs_rename(src, dst)
    assert os.path.exists(dst)
    assert not os.path.exists(src)


def test_fs_move(filesystem):
    src = '/tmp/refiletest/src'
    dst = '/tmp/refiletest/dst'
    os.makedirs(src)
    fs.fs_move(src, dst)
    assert os.path.exists(dst)
    assert not os.path.exists(src)


def test_fs_sync(filesystem):
    src = '/tmp/refiletest/src'
    dst = '/tmp/refiletest/dst'
    os.makedirs(src)
    fs.fs_sync(src, dst)
    assert os.path.exists(dst)
    assert os.path.exists(src)


def test_fs_cwd(filesystem):
    src = '/tmp/refiletest/src'
    os.makedirs(src)
    os.chdir(src)
    assert fs.fs_cwd() == src


def test_fs_home(mocker):
    funcA = mocker.patch('os.path.expanduser')
    fs.fs_home()
    funcA.assert_called_once_with('~')


def test_fs_expanduser(mocker):
    path = '"~/file.txt"'
    funcA = mocker.patch('os.path.expanduser')
    fs.fs_expanduser(path)
    funcA.assert_called_once_with(path)


def test_fs_resolve(mocker):
    path = '/tmp/refiletest/src'
    funcA = mocker.patch('os.path.realpath')
    fs.fs_resolve(path)
    funcA.assert_called_once_with(path)


def test_fs_getmd5(filesystem):
    path = '/tmp/md5.txt'
    with open(path, 'wb') as f:
        f.write(b'00000')
    assert fs.fs_getmd5(path) == 'dcddb75469b4b4875094e14561e573d8'

import os
from io import BytesIO, StringIO
from pathlib import Path, PurePath

import boto3
import botocore
import pytest
from mock import patch
from moto import mock_s3

import megfile
from megfile import smart
from megfile.errors import ProtocolNotFoundError
from megfile.interfaces import Access, StatResult
from megfile.s3 import _s3_binary_mode, s3_load_content


@pytest.fixture
def filesystem(fs):
    return fs


BUCKET = "bucket"


@pytest.fixture
def s3_empty_client(mocker):
    with mock_s3():
        client = boto3.client('s3')
        client.create_bucket(Bucket=BUCKET)
        mocker.patch('megfile.s3.get_s3_client', return_value=client)
        yield client


def test_smart_copy(mocker):

    def is_symlink(path: str) -> bool:
        return path == 'link'

    s3_copy = mocker.patch('megfile.smart.s3_copy')
    s3_download = mocker.patch('megfile.smart.s3_download')
    s3_upload = mocker.patch('megfile.smart.s3_upload')
    fs_copy = mocker.patch('megfile.smart.fs_copy')
    default_copy_func = mocker.patch('megfile.smart._default_copy_func')
    copyfile = mocker.patch('megfile.fs._copyfile')

    smart_islink = mocker.patch(
        'megfile.smart.smart_islink', side_effect=is_symlink)

    patch_dict = {
        's3': {
            's3': s3_copy,
            'fs': s3_download
        },
        'fs': {
            's3': s3_upload,
            'fs': fs_copy,
        }
    }

    with patch('megfile.smart._copy_funcs', patch_dict) as _:

        smart.smart_copy('link', 's3://a/b')
        assert s3_copy.called is False
        assert s3_download.called is False
        assert s3_upload.called is False
        assert fs_copy.called is False

        src_path = '/tmp/src_file'
        smart.smart_copy('link', src_path, followlinks=True)
        assert s3_copy.called is False
        assert s3_download.called is False
        assert s3_upload.called is False
        assert fs_copy.called is True
        fs_copy.assert_called_once_with(
            'link', src_path, callback=None, followlinks=True)
        fs_copy.reset_mock()

        smart.smart_copy('s3://a/b', 's3://a/b')
        s3_copy.assert_called_once_with('s3://a/b', 's3://a/b', callback=None)

        smart.smart_copy('http://a/b', 'fs')
        default_copy_func.assert_called_once_with(
            'http://a/b', 'fs', callback=None)

        smart.smart_copy('s3://a/b', 'fs')
        s3_download.assert_called_once_with('s3://a/b', 'fs', callback=None)

        smart.smart_copy('fs', 's3://a/b')
        s3_upload.assert_called_once_with('fs', 's3://a/b', callback=None)

        fs_stat = mocker.patch(
            'megfile.fs.fs_stat', return_value=StatResult(islnk=False, size=10))
        smart.smart_copy('fs', 'fs', followlinks=False)
        fs_copy.assert_called_once_with(
            'fs', 'fs', callback=None, followlinks=False)
        fs_copy.reset_mock()
        fs_stat.stop()


def test_smart_copy_fs2fs(mocker):
    fs_makedirs = mocker.patch(
        'megfile.fs.fs_makedirs', side_effect=lambda *args, **kwargs:...)

    class fake_copy:
        flag = False

        def __call__(self, *args, **kwargs):
            if self.flag:
                return
            else:
                error = FileNotFoundError()
                error.filename = 'fs/a/b/c'
                self.flag = True
                raise error

    copyfile = mocker.patch('megfile.fs._copyfile')
    copyfile.side_effect = fake_copy()
    smart.smart_copy('fs', 'fs/a/b/c')
    fs_makedirs.call_count == 1
    fs_makedirs.assert_called_once_with('fs/a/b', exist_ok=True)
    fs_makedirs.reset_mock()


def test_smart_copy_UP2UP(filesystem):

    patch_dict = {}
    with patch('megfile.smart._copy_funcs', patch_dict) as _:
        data = b'test'
        with smart.smart_open('a', 'wb') as writer:
            writer.write(data)
        smart.smart_copy('a', 'b')
        assert data == smart.smart_open('b', 'rb').read()


def test_smart_sync(mocker):

    def isdir(path: str) -> bool:
        return os.path.basename(path).startswith('folder')

    def isfile(path: str) -> bool:
        return os.path.basename(path).startswith('file')

    smart_copy = mocker.patch('megfile.smart.smart_copy')
    smart_isdir = mocker.patch('megfile.smart.smart_isdir', side_effect=isdir)
    smart_isfile = mocker.patch(
        'megfile.smart.smart_isfile', side_effect=isfile)
    smart_scan = mocker.patch('megfile.smart.smart_scan')
    '''
      folder/
        - folderA/
          -fileB
        - fileA
      - a/
        - b/
          - c
        - d
      - a
    '''

    def listdir(path: str, followlinks: bool):
        if path == 'folder':
            return ["folder/folderA/fileB", "folder/fileA"]
        if path == 'folder/fileA':
            return ["folder/fileA"]
        if path == 'a':
            return ['a', 'a/b/c', 'a/d']

    smart_scan.side_effect = listdir

    smart.smart_sync('folder', 'dst', followlinks=True)
    assert smart_copy.call_count == 2
    smart_copy.assert_any_call(
        'folder/fileA', 'dst/fileA', callback=None, followlinks=True)
    smart_copy.assert_any_call(
        'folder/folderA/fileB',
        'dst/folderA/fileB',
        callback=None,
        followlinks=True)
    smart_copy.reset_mock()

    smart.smart_sync('folder/fileA', 'dst/file', followlinks=True)
    assert smart_copy.call_count == 1
    smart_copy.assert_any_call(
        'folder/fileA', 'dst/file', callback=None, followlinks=True)
    smart_copy.reset_mock()

    smart.smart_sync('a', 'dst', followlinks=True)
    assert smart_copy.call_count == 3
    smart_copy.assert_any_call('a', 'dst', callback=None, followlinks=True)
    smart_copy.assert_any_call(
        'a/b/c', 'dst/b/c', callback=None, followlinks=True)
    smart_copy.assert_any_call('a/d', 'dst/d', callback=None, followlinks=True)


def test_smart_move(mocker):
    funcA = mocker.patch('megfile.smart.s3_rename')
    funcA.return_value = None
    res = smart.smart_move('s3://bucket/a', 's3://bucket/b')
    assert res is None
    funcA.assert_called_once_with('s3://bucket/a', 's3://bucket/b')

    funcB = mocker.patch('megfile.smart.fs_rename')
    res = smart.smart_move('/bucket/a', '/bucket/b')
    assert res is None
    funcB.assert_called_once_with('/bucket/a', '/bucket/b')

    func_smart_sync = mocker.patch('megfile.smart.smart_sync')
    func_smart_remove = mocker.patch('megfile.smart.smart_remove')
    smart.smart_move('/bucket/a', 's3://bucket/b')
    func_smart_sync.assert_called_once_with(
        '/bucket/a', 's3://bucket/b', followlinks=False)
    func_smart_remove.assert_called_once_with('/bucket/a', followlinks=False)


def test_smart_rename(mocker):
    funcA = mocker.patch('megfile.smart.s3_rename', return_value=None)
    res = smart.smart_move('s3://bucket/a', 's3://bucket/b')
    assert res is None
    funcA.assert_called_once_with('s3://bucket/a', 's3://bucket/b')


def test_smart_rename_fs(s3_empty_client, filesystem):
    '''
    /tmp_rename/
        /src/
            -src_file
        /dst/
            -link --> tmp_rename/src/src_file
    '''
    os.mkdir('tmp_rename')
    os.mkdir('tmp_rename/src')
    os.mkdir('tmp_rename/dst')
    with open('tmp_rename/src/src_file', 'w') as f:
        f.write('')
    os.symlink('tmp_rename/src/src_file', 'tmp_rename/dst/link')
    os.path.exists('tmp_rename/dst/link') is True
    assert os.path.exists('tmp_rename/src')
    smart.smart_rename('tmp_rename/src/src_file', 'tmp_rename/src_copy')
    assert os.path.exists('tmp_rename/src_copy')
    assert not os.path.exists('tmp_rename/src/src_file')
    assert not os.path.exists('tmp_rename/dst/link')

    with pytest.raises(IsADirectoryError):
        smart.smart_rename('tmp_rename/src', 'tmp_rename/src_copy')

    smart.smart_rename('tmp_rename/src_copy', 's3://bucket/src_copy')
    assert smart.smart_exists('s3://bucket/src_copy')
    assert not smart.smart_exists('tmp_rename/src_copy')

    smart.smart_rename('s3://bucket/src_copy', 's3://bucket/dst_copy')
    assert smart.smart_exists('s3://bucket/dst_copy')
    assert not smart.smart_exists('s3://bucket/src_copy')


def test_smart_unlink(mocker):
    funcA = mocker.patch('megfile.smart.fs_unlink', return_value=None)

    res = smart.smart_unlink("/False Case", False)
    assert res is None
    funcA.assert_called_once_with(path="/False Case", missing_ok=False)

    res = smart.smart_unlink("/True Case", True)
    assert res is None
    funcA.assert_called_with(path="/True Case", missing_ok=True)


def test_smart_makedirs(mocker):
    funcA = mocker.patch('megfile.smart.fs_makedirs', return_value=None)
    res = smart.smart_makedirs("/Test Case", exist_ok=True)
    assert res is None
    funcA.assert_called_once_with(path="/Test Case", exist_ok=True)


def test_smart_open(mocker, fs):
    '''
    This test is pretty naïve. Feel free to improve it
    in order to ensure smart_open works as we expected.

    Even ourselves do not know what we expect up to now.
    '''
    # s3_writer = mocker.patch('megfile.smart.S3BufferedWriter')
    # s3_reader = mocker.patch('megfile.smart.S3PrefetchReader')
    # fs_open = mocker.patch('io.open', side_effect=open)
    # text_wrapper = mocker.patch('io.TextIOWrapper')
    # is_s3_func = mocker.patch('megfile.smart.is_s3')
    # fs_isdir_func = mocker.patch('megfile.smart.fs_isdir')
    # s3_isdir_func = mocker.patch('megfile.smart.s3_isdir')
    # s3_isfile_func = mocker.patch('megfile.smart.s3_isfile')
    # parse_s3_url = mocker.patch('megfile.smart.parse_s3_url')
    # mocker.patch('megfile.smart.get_s3_client')

    # is_s3_func.return_value = False
    # fs_isdir_func.return_value = True

    # with pytest.raises(IsADirectoryError):
    #     smart.smart_open('folder')
    # is_s3_func.return_value = False
    # fs_isdir_func.return_value = False
    # with pytest.raises(FileNotFoundError):
    #     smart.smart_open('non-exist.file')
    # fs_open.side_effect = None
    # fs_open.reset_mock()

    # is_s3_func.return_value = False
    # fs_isdir_func.return_value = False
    # smart.smart_open('file', 'wb+')
    # fs_open.assert_called_once_with('file', 'wb+', encoding=None, errors=None)
    # fs_open.reset_mock()

    # is_s3_func.return_value = False
    # fs_isdir_func.return_value = False
    # smart.smart_open('non-exist/file', 'wb')
    # fs_open.assert_called_once_with(
    #     'non-exist/file', 'wb', encoding=None, errors=None)
    # fs_open.reset_mock()

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = True
    # s3_isfile_func.return_value = True
    # parse_s3_url.return_value = ('bucket', 'key')
    # smart.smart_open('s3://bucket/key')
    # s3_reader.side_effect = None
    # s3_reader.reset_mock()
    # text_wrapper.reset_mock()

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = True
    # s3_isfile_func.return_value = False
    # parse_s3_url.return_value = ('bucket', 'key')
    # with pytest.raises(IsADirectoryError) as e:
    #     smart.smart_open('s3://bucket/key')

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = False
    # s3_isfile_func.return_value = False
    # parse_s3_url.return_value = ('bucket', 'key')
    # with pytest.raises(FileNotFoundError) as e:
    #     smart.smart_open('s3://bucket/key')

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = True
    # s3_isfile_func.return_value = True
    # parse_s3_url.return_value = ('bucket', 'key')
    # with pytest.raises(FileExistsError) as e:
    #     smart.smart_open('s3://bucket/key', 'x')

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = False
    # s3_isfile_func.return_value = False
    # parse_s3_url.return_value = ('bucket', 'key')
    # with pytest.raises(ValueError) as e:
    #     smart.smart_open('s3://bucket/key', 'wb+')
    # assert 'wb+' in str(e.value)

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = False
    # s3_isfile_func.return_value = False
    # parse_s3_url.return_value = ('bucket', 'key')
    # smart.smart_open('s3://bucket/key', 'w')
    # # s3_writer.assert_called_once() in Python 3.6+
    # assert s3_writer.call_count == 1
    # # text_wrapper.assert_called_once() in Python 3.6+
    # assert text_wrapper.call_count == 1
    # s3_writer.reset_mock()
    # text_wrapper.reset_mock()

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = False
    # s3_isfile_func.return_value = False
    # parse_s3_url.return_value = ('bucket', 'key')
    # smart.smart_open('s3://bucket/key', 'xb')
    # # s3_writer.assert_called_once() in Python 3.6+
    # assert s3_writer.call_count == 1
    # # text_wrapper.assert_not_called() in Python 3.6+
    # assert text_wrapper.call_count == 0
    # s3_writer.reset_mock()
    # text_wrapper.reset_mock()

    # is_s3_func.return_value = True
    # s3_isdir_func.return_value = False
    # s3_isfile_func.return_value = True
    # parse_s3_url.return_value = ('bucket', 'key')
    # smart.smart_open('s3://bucket/key', 'r')
    # # s3_reader.assert_called_once() in Python 3.6+
    # assert s3_reader.call_count == 1
    # # text_wrapper.assert_called_once() in Python 3.6+
    # assert text_wrapper.call_count == 1
    # s3_reader.reset_mock()
    # text_wrapper.reset_mock()
    funcA = mocker.patch('megfile.smart.fs_open')
    smart.smart_open('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_open')
    smart.smart_open('s3://test/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.http_open')
    smart.smart_open('http://test/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.stdio_open')
    smart.smart_open('stdio://test/test')
    funcA.assert_called_once()


def test_smart_open_custom_s3_open_func(mocker, fs):
    s3_open = mocker.Mock()
    s3_binary_open = _s3_binary_mode(s3_open)
    text_wrapper = mocker.patch('io.TextIOWrapper')
    s3_hasbucket_func = mocker.patch('megfile.s3.s3_hasbucket')
    s3_hasbucket_func.return_value = True
    s3_isfile_func = mocker.patch('megfile.s3.s3_isfile')
    s3_isfile_func.return_value = False
    parse_s3_url = mocker.patch('megfile.s3.parse_s3_url')
    parse_s3_url.return_value = ('bucket', 'key')

    smart.smart_open('s3://bucket/key', 'r', s3_open_func=s3_binary_open)
    s3_open.assert_called_once_with('s3://bucket/key', 'rb')
    # text_wrapper.assert_called_once() in Python 3.6+
    assert text_wrapper.call_count == 1
    s3_open.reset_mock()
    text_wrapper.reset_mock()

    smart.smart_open('s3://bucket/key', 'wt', s3_open_func=s3_binary_open)
    s3_open.assert_called_once_with('s3://bucket/key', 'wb')
    # text_wrapper.assert_called_once() in Python 3.6+
    assert text_wrapper.call_count == 1
    s3_open.reset_mock()
    text_wrapper.reset_mock()

    smart.smart_open('s3://bucket/key', 'wb', s3_open_func=s3_binary_open)
    s3_open.assert_called_once_with('s3://bucket/key', 'wb')
    # text_wrapper.assert_not_called() in Python 3.6+
    assert text_wrapper.call_count == 0
    s3_open.reset_mock()
    text_wrapper.reset_mock()

    smart.smart_open('s3://bucket/key', 'ab+', s3_open_func=s3_binary_open)
    s3_open.assert_called_once_with('s3://bucket/key', 'ab+')
    # text_wrapper.assert_not_called() in Python 3.6+
    assert text_wrapper.call_count == 0
    s3_open.reset_mock()
    text_wrapper.reset_mock()

    smart.smart_open('s3://bucket/key', 'x', s3_open_func=s3_binary_open)
    s3_open.assert_called_once_with('s3://bucket/key', 'wb')
    # text_wrapper.assert_not_called() in Python 3.6+
    assert text_wrapper.call_count == 1
    s3_open.reset_mock()
    text_wrapper.reset_mock()

    with pytest.raises(FileNotFoundError):
        s3_hasbucket_func.return_value = False
        smart.smart_open('s3://bucket/key', 'wt', s3_open_func=s3_binary_open)

    with pytest.raises(FileExistsError):
        s3_isfile_func.return_value = True
        smart.smart_open('s3://bucket/key', 'x', s3_open_func=s3_binary_open)


def test_smart_path_join(mocker):
    funcA = mocker.patch('megfile.smart.s3_path_join', return_value=Path())
    res = smart.smart_path_join(
        "s3://Test Case1", "s3://Test Case2", "s3://Test Case3")
    funcA.assert_called_once_with(
        "s3://Test Case1", "s3://Test Case2", "s3://Test Case3")


def test_smart_path_join_result():
    assert smart.smart_path_join('path') == 'path'
    assert smart.smart_path_join('path', 'to/file') == 'path/to/file'
    assert smart.smart_path_join('path', 'to//file') == 'path/to/file'
    assert smart.smart_path_join('path', 'to', 'file') == 'path/to/file'
    assert smart.smart_path_join('path', 'to/', 'file') == 'path/to/file'
    assert smart.smart_path_join('path', 'to', '/file') == '/file'
    assert smart.smart_path_join('path', 'to', 'file/') == 'path/to/file'
    assert smart.smart_path_join('s3://') == 's3://'
    assert smart.smart_path_join('s3://', 'bucket/key') == 's3://bucket/key'
    assert smart.smart_path_join('s3://', 'bucket//key') == 's3://bucket//key'
    assert smart.smart_path_join('s3://', 'bucket', 'key') == 's3://bucket/key'
    assert smart.smart_path_join('s3://', 'bucket/', 'key') == 's3://bucket/key'
    assert smart.smart_path_join('s3://', 'bucket', '/key') == 's3://bucket/key'
    assert smart.smart_path_join(
        's3://', 'bucket', 'key/') == 's3://bucket/key/'


def test_smart_walk(mocker):
    funcA = mocker.patch('megfile.smart.s3_walk', return_value=None)
    res = smart.smart_walk("s3://test", followlinks=True)
    assert res is None
    funcA.assert_called_once()

    res = smart.smart_walk("s3://test", followlinks=True)
    assert res is None
    funcA.call_count == 2


def test_smart_scan(mocker):
    funcA = mocker.patch('megfile.smart.s3_scan', return_value=None)

    smart.smart_scan("s3://test", followlinks=True)
    funcA.assert_called_once()

    smart.smart_scan("s3://test", followlinks=True)
    funcA.call_count == 2


def test_smart_scan_stat(mocker):
    funcA = mocker.patch('megfile.smart.s3_scan_stat', return_value=None)
    smart.smart_scan_stat("s3://test", followlinks=True)
    funcA.assert_called_once()

    smart.smart_scan_stat("s3://test", followlinks=True)
    funcA.call_count == 2


def test_smart_glob(mocker):
    funcA = mocker.patch('megfile.smart.s3_glob', return_value=[])
    smart.smart_glob('s3://bucket/*')
    funcA.assert_called_once()


def test_smart_glob_cross_backend(mocker):
    funcA = mocker.patch('megfile.smart.s3_glob')
    funcB = mocker.patch('megfile.smart.fs_glob')
    smart.smart_glob(r'{/a,s3://bucket/key}/filename')
    assert funcA.call_count == 1
    assert funcB.call_count == 1


def test_smart_iglob(mocker):
    funcA = mocker.patch('megfile.smart.s3_iglob')
    smart.smart_iglob('s3://bucket/*')
    funcA.assert_called_once()


def test_smart_iglob_cross_backend(mocker):
    funcA = mocker.patch('megfile.smart.s3_iglob')
    funcB = mocker.patch('megfile.smart.fs_iglob')
    smart.smart_iglob(r'{/a,s3://bucket/key,s3://bucket2/key}/filename')
    assert funcA.call_count == 1
    assert funcB.call_count == 1


def test_smart_glob_stat(mocker):
    funcA = mocker.patch('megfile.smart.s3_glob_stat')
    smart.smart_glob_stat('s3://bucket/*')
    funcA.assert_called_once()


def test_smart_glob_stat_cross_backend(mocker):
    funcA = mocker.patch('megfile.smart.s3_glob_stat')
    funcB = mocker.patch('megfile.smart.fs_glob_stat')
    smart.smart_glob_stat(r'{/a,s3://bucket/key,s3://bucket2/key}/filename')
    assert funcA.call_count == 1
    assert funcB.call_count == 1


def test_smart_save_as(mocker):
    funcA = mocker.patch('megfile.smart.s3_save_as', return_value=None)
    funcB = mocker.patch('megfile.smart.fs_save_as', return_value=None)
    stream = BytesIO()
    smart.smart_save_as(stream, 's3://test/ture_case')
    funcA.assert_called_once()
    smart.smart_save_as(stream, '/test/false_case')
    funcB.assert_called_once()


@patch.object(smart, "fs_load_from")
def test_smart_load_from(funcA):
    smart.smart_load_from('/Test Case')
    funcA.assert_called_once()


@pytest.fixture
def s3_path():
    yield 's3://bucket/test'


@pytest.fixture
def abs_path(fs):
    fs.create_file(os.path.join(os.path.dirname(__file__), 'test'))
    yield os.path.join(os.path.dirname(__file__), 'test')


@pytest.fixture
def rel_path(fs):
    fs.create_file('./test')
    yield 'test'


@pytest.fixture
def link_path(fs, abs_path):
    fs.create_symlink('link', abs_path)
    yield 'link'


@pytest.fixture
def mount_point(fs):
    fs.add_mount_point(os.path.dirname(__file__))
    yield os.path.dirname(__file__)


def test_smart_open_stdin(mocker):
    stdin_buffer_read = mocker.patch('sys.stdin.buffer.read')
    stdin_buffer_read.return_value = b'test\ntest1\n'

    reader = megfile.smart_open('stdio://-', 'r')
    assert reader.read() == 'test\ntest1\n'


def test_smart_open_stdout(mocker):
    # TODO: 这里 pytest 会把 sys.stdout mocker 掉，导致无法测试，之后想办法解决
    return
    data = BytesIO()

    def fake_write(w_data):
        data.write(w_data)
        return len(w_data)

    mocker.patch('_io.FileIO', side_effect=fake_write)
    writer = megfile.smart_open('stdio://-', 'w')
    writer.write('test')
    assert data.getvalue() == b'test'


@patch.object(smart, 's3_load_content')
def test_smart_load_content(funcA):
    path = 'tests/test_smart.py'
    content = open(path, 'rb').read()

    assert smart.smart_load_content(path) == content
    assert smart.smart_load_content(path, 1) == content[1:]
    assert smart.smart_load_content(path, stop=-1) == content[:-1]
    assert smart.smart_load_content(path, 4, 7) == content[4:7]

    with pytest.raises(Exception) as error:
        smart.smart_load_content(path, 5, 2)

    smart.smart_load_content('s3://bucket/test.txt')
    funcA.assert_called_once()


def test_smart_save_content(mocker):
    content = 'test data for smart_save_content'
    smart_open = mocker.patch('megfile.smart.smart_open')


def test_smart_load_text(mocker):
    test = 'test data for smart_load_text'
    smart_open = mocker.patch('megfile.smart.smart_open')


def test_smart_save_text(mocker):
    content = 'test data for smart_save_text'

    smart_open = mocker.patch('megfile.smart.smart_open')

    def _fake_smart_open(*args, **kwargs):
        return StringIO(content)

    smart_open.side_effect = _fake_smart_open

    assert smart.smart_load_text('s3://bucket/key') == content

    with pytest.raises(Exception) as error:
        smart.smart_load_text('s3://bucket/key', 5, 2)


def test_register_copy_func():
    test = lambda *args, **kwargs:...
    smart.register_copy_func('a', 'b', test)
    assert smart._copy_funcs['a']['b'] == test

    with pytest.raises(ValueError) as error:
        smart.register_copy_func('a', 'b', test)

    assert error.value.args[0] == 'Copy Function has already existed: a->b'


def test_smart_cache(mocker):
    s3_download = mocker.patch('megfile.s3.s3_download')
    s3_download.return_value = None

    from megfile.interfaces import NullCacher
    from megfile.s3 import S3Cacher

    cacher = megfile.smart_cache('/path/to/file')
    assert isinstance(cacher, NullCacher)
    with cacher as path:
        assert path == '/path/to/file'

    cacher = megfile.smart_cache('s3://path/to/file')
    assert isinstance(cacher, S3Cacher)
    assert s3_download.called is True


def test_smart_symlink(mocker, s3_empty_client, filesystem):
    src_path = '/tmp/src_file'
    dst_path = '/tmp/dst_file'
    smart.smart_symlink(dst_path, src_path)

    res = os.readlink(dst_path)
    assert res == src_path

    src_url = 's3://bucket/src'
    dst_url = 's3://bucket/dst'
    dst_dst_url = 's3://bucket/dst_dst'
    content = b'bytes'
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='src', Body=content)
    smart.smart_symlink(dst_url, src_url)
    smart.smart_symlink(dst_dst_url, dst_url)

    res = smart.smart_readlink(dst_dst_url)
    assert res == src_url


def test_smart_readlink(filesystem):
    src_path = '/tmp/src_file'
    dst_path = '/tmp/dst_file'
    os.symlink(src_path, dst_path)

    res = smart.smart_readlink(dst_path)
    assert res == src_path


def test_default_copy_func(filesystem):
    content = '1234'

    def callback_func(count):
        assert count == len(content)

    with open('a.txt', 'w') as f:
        f.write(content)

    smart._default_copy_func('a.txt', 'b.txt', callback=callback_func)

    with open('b.txt', 'r') as f:
        assert f.read() == content


@patch.object(smart, 'combine')
def test_smart_combine_open(funcA, mocker):
    mocker.patch('megfile.smart.smart_glob', return_value=[])
    smart.smart_combine_open('test path')
    funcA.assert_called_once()


@patch.object(smart, 'smart_open')
def test_smart_save_content(funcA):
    smart.smart_save_content('test path', b'test')
    funcA.assert_called_once()


@patch.object(smart, 'smart_open')
def test_smart_save_text(funcA):
    smart.smart_save_text('test path', 'test')
    funcA.assert_called_once()


@patch.object(smart, 'smart_open')
def test_smart_load_text(funcA):
    smart.smart_load_text('test path')
    funcA.assert_called_once()


def test_smart_access(mocker):

    funcA = mocker.patch('megfile.smart.fs_access')
    smart.smart_access('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_access')
    smart.smart_access('s3://test/test')
    funcA.assert_called_once()


def test_smart_getmd5(mocker):

    funcA = mocker.patch('megfile.smart.fs_getmd5')
    smart.smart_getmd5('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_getmd5')
    smart.smart_getmd5('s3://test/test')
    funcA.assert_called_once()


def test_smart_getmtime(mocker):

    funcA = mocker.patch('megfile.smart.fs_getmtime')
    smart.smart_getmtime('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_getmtime')
    smart.smart_getmtime('s3://test/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.http_getmtime')
    smart.smart_getmtime('http://test/test')
    funcA.assert_called_once()


def test_smart_getsize(mocker):

    funcA = mocker.patch('megfile.smart.fs_getsize')
    smart.smart_getsize('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_getsize')
    smart.smart_getsize('s3://test/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.http_getsize')
    smart.smart_getsize('http://test/test')
    funcA.assert_called_once()


def test_smart_isdir(mocker):

    funcA = mocker.patch('megfile.smart.fs_isdir')
    smart.smart_isdir('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_isdir')
    smart.smart_isdir('s3://test/test')
    funcA.assert_called_once()


def test_smart_isfile(mocker):

    funcA = mocker.patch('megfile.smart.fs_isfile')
    smart.smart_isfile('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_isfile')
    smart.smart_isfile('s3://test/test')
    funcA.assert_called_once()


def test_smart_islink(mocker):

    funcA = mocker.patch('megfile.smart.fs_islink')
    smart.smart_islink('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_islink')
    smart.smart_islink('s3://test/test')
    funcA.assert_called_once()


def test_smart_listdir(mocker):

    funcA = mocker.patch('megfile.smart.fs_listdir')
    smart.smart_listdir('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_listdir')
    smart.smart_listdir('s3://test/test')
    funcA.assert_called_once()


def test_smart_load_from(mocker):

    funcA = mocker.patch('megfile.smart.fs_load_from')
    smart.smart_load_from('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_load_from')
    smart.smart_load_from('s3://test/test')
    funcA.assert_called_once()


def test_smart_makedirs(mocker):

    funcA = mocker.patch('megfile.smart.fs_makedirs')
    smart.smart_makedirs('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_makedirs')
    smart.smart_makedirs('s3://test/test')
    funcA.assert_called_once()


def test_smart_remove(mocker):

    funcA = mocker.patch('megfile.smart.fs_remove')
    smart.smart_remove('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_remove')
    smart.smart_remove('s3://test/test')
    funcA.assert_called_once()


def test_smart_scandir(mocker):

    funcA = mocker.patch('megfile.smart.fs_scandir')
    smart.smart_scandir('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_scandir')
    smart.smart_scandir('s3://test/test')
    funcA.assert_called_once()


def test_smart_stat(mocker):

    funcA = mocker.patch('megfile.smart.fs_stat')
    smart.smart_stat('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_stat')
    smart.smart_stat('s3://test/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.http_stat')
    smart.smart_stat('http://test/test')
    funcA.assert_called_once()


def test_smart_scan_stat(mocker):

    funcA = mocker.patch('megfile.smart.fs_scan_stat')
    smart.smart_scan_stat('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_scan_stat')
    smart.smart_scan_stat('s3://test/test')
    funcA.assert_called_once()


def test_smart_unlink(mocker):

    funcA = mocker.patch('megfile.smart.fs_unlink')
    smart.smart_unlink('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_unlink')
    smart.smart_unlink('s3://test/test')
    funcA.assert_called_once()


def test_smart_walk(mocker):

    funcA = mocker.patch('megfile.smart.fs_walk')
    smart.smart_walk('/test')
    funcA.assert_called_once()

    funcA = mocker.patch('megfile.smart.s3_walk')
    smart.smart_walk('s3://test/test')
    funcA.assert_called_once()


def test_extract_protocol():
    assert smart._extract_protocol('http://a/b') == 'http'
    assert smart._extract_protocol('https://a/b') == 'http'
    assert smart._extract_protocol('/a/b') == 'fs'
    assert smart._extract_protocol(PurePath()) == 'fs'
    assert smart._extract_protocol('stdio://a/b') == 'stdio'
    assert smart._extract_protocol('s3://a/b') == 's3'
    assert smart._extract_protocol(0) == 'fs'

    with pytest.raises(ProtocolNotFoundError):
        smart._extract_protocol({})

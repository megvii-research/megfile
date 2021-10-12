import os
import sys
import threading
import time
from collections import namedtuple
from functools import partial
from io import BytesIO
from pathlib import Path
from typing import Iterable, List, Tuple

import boto3
import botocore
import pytest
from moto import mock_s3

from megfile import s3, smart
from megfile.errors import UnknownError, UnsupportedError, translate_s3_error
from megfile.interfaces import Access, FileEntry, StatResult
from megfile.s3 import _group_s3path_by_bucket, _group_s3path_by_prefix, _s3_split_magic, content_md5_header

from . import Any, FakeStatResult, Now

File = namedtuple('File', ['bucket', 'key', 'body'])
"""
bucketA/
|-folderAA/
  |-folderAAA/
    |-fileAAAA
|-folderAB-C/
  |-fileAB-C
|-folderAB/
  |-fileAB
  |-fileAC
|-fileAA
|-fileAB
bucketB/
bucketC/
|-folder    （目录）
    |-file
|-folder    （与目录同名的文件）
|-folderAA
    |-fileAA

bucketForGlobTest/ （用于 s3_glob 的测试，结构较复杂）
|-1
    |-a （目录）
        |-b
            |-c
                |-1.json
                |-A.msg
            |-1.json
    |-a （与目录同名的文件）
|-2
    |-a
        |-d
            |-c
                |-1.json
            |-2.json
        |-b
            |-c
                |-1.json
                |-2.json
            |-a
                |-1.json
emptyBucketForGlobTest/
bucketForGlobTest2/ （用于 s3_glob 多个bucket和带wildcard的测试）
|-1
    |-a （目录）
        |-b
            |-c
                |-1.json
                |-2.json
                |-A.msg
            |-1.json
        |-c
            |-1.json
            |-A.msg
        |-1.json
    |-a （与目录同名的文件）
bucketForGlobTest3/ （用于 s3_glob 多个bucket和带wildcard的测试）
|-1
    |-a （目录）
        |-b
            |-c
                |-1.json
                |-A.msg
            |-1.json
    |-a （与目录同名的文件）
"""
FILE_LIST = [
    File('bucketA', 'folderAA/folderAAA/fileAAAA', 'fileAAAA'),
    File('bucketA', 'folderAB-C/fileAB-C', 'fileAB-C'),
    File('bucketA', 'folderAB/fileAB', 'fileAB'),
    File('bucketA', 'folderAB/fileAC', 'fileAC'),
    File('bucketA', 'fileAA', 'fileAA'),
    File('bucketA', 'fileAB', 'fileAB'),
    File('bucketB', None, None),  # 空 bucket
    File('bucketC', 'folder/file', 'file'),
    File('bucketC', 'folder', 'file'),  # 与同级 folder 同名的 file
    File('bucketC', 'folderAA/fileAA', 'fileAA'),
    File('bucketForGlobTest', '1/a/b/c/1.json', '1.json'),
    File('bucketForGlobTest', '1/a/b/1.json',
         '1.json'),  # for glob(*/a/*/*.json)
    File('bucketForGlobTest', '1/a/b/c/A.msg', 'A.msg'),
    File('bucketForGlobTest', '1/a', 'file, same name with folder'),
    File('bucketForGlobTest', '2/a/d/c/1.json', '1.json'),
    File('bucketForGlobTest', '2/a/d/2.json', '2.json'),
    File('bucketForGlobTest', '2/a/b/c/1.json', '1.json'),
    File('bucketForGlobTest', '2/a/b/c/2.json', '2.json'),
    File('bucketForGlobTest', '2/a/b/a/1.json', '1.json'),
    File('emptyBucketForGlobTest', None, None),
    File('bucketForGlobTest2', '1/a/b/c/1.json', '1.json'),
    File('bucketForGlobTest2', '1/a/b/c/2.json', '2.json'),
    File('bucketForGlobTest2', '1/a/b/1.json',
         '1.json'),  # for glob(*/a/*/*.json)
    File('bucketForGlobTest2', '1/a/b/c/A.msg', 'A.msg'),
    File('bucketForGlobTest2', '1/a', 'file, same name with folder'),
    File('bucketForGlobTest2', '1/a/c/1.json', '1.json'),
    File('bucketForGlobTest2', '1/a/1.json',
         '1.json'),  # for glob(*/a/*/*.json)
    File('bucketForGlobTest2', '1/a/c/A.msg', 'A.msg'),
    File('bucketForGlobTest3', '1/a/b/c/1.json', '1.json'),
    File('bucketForGlobTest3', '1/a/b/1.json',
         '1.json'),  # for glob(*/a/*/*.json)
    File('bucketForGlobTest3', '1/a/b/c/A.msg', 'A.msg'),
    File('bucketForGlobTest3', '1/a', 'file, same name with folder'),
]


@pytest.fixture
def s3_empty_client(mocker):
    with mock_s3():
        client = boto3.client('s3')
        mocker.patch('megfile.s3.get_s3_client', return_value=client)
        yield client


@pytest.fixture
def s3_setup(mocker, s3_empty_client, file_list=FILE_LIST):
    s3_client = s3_empty_client
    buckets = []
    for file in file_list:
        if file.bucket not in buckets:
            buckets.append(file.bucket)
            s3_client.create_bucket(Bucket=file.bucket)
        if file.key is not None:
            s3_client.put_object(
                Bucket=file.bucket, Key=file.key, Body=file.body)
    return s3_client


@pytest.fixture
def truncating_client(mocker, s3_setup):
    '''将 list_objects_v2 的 MaxKeys 限定为 1，在结果超过 1 个 key 时总是截断'''
    truncating_client = mocker.patch.object(
        s3_setup,
        'list_objects_v2',
        side_effect=partial(s3_setup.list_objects_v2, MaxKeys=1))
    return truncating_client


def make_stat(size=0, mtime=None, isdir=False):
    if mtime is None:
        mtime = 0.0 if isdir else Now()
    return StatResult(size=size, mtime=mtime, isdir=isdir)


def test_retry(s3_empty_client, mocker):
    read_error = botocore.exceptions.IncompleteReadError(
        actual_bytes=0, expected_bytes=1)
    client = s3.get_s3_client()
    s3._patch_make_request(client)
    mocker.patch.object(
        client._endpoint, 'make_request', side_effect=read_error)
    sleep = mocker.patch.object(time, 'sleep')
    with pytest.raises(UnknownError) as error:
        s3.s3_exists('s3://bucket')
    assert error.value.__cause__ is read_error
    assert sleep.call_count == s3.max_retries - 1


def test_get_endpoint_url(mocker):
    mocker.patch('megfile.s3.get_scoped_config', return_value={})
    assert s3.get_endpoint_url() == 'https://s3.amazonaws.com'


def test_get_endpoint_url_from_env(mocker):
    mocker.patch('megfile.s3.get_scoped_config', return_value={})
    mocker.patch.dict(os.environ, {'OSS_ENDPOINT': 'oss-endpoint'})

    assert s3.get_endpoint_url() == 'oss-endpoint'


def test_get_s3_client(mocker):
    mock_session = mocker.Mock(spec=boto3.session.Session)
    mocker.patch('megfile.s3.get_scoped_config', return_value={})
    mocker.patch('megfile.s3.get_s3_session', return_value=mock_session)

    s3.get_s3_client()

    mock_session.client.assert_called_with(
        's3', endpoint_url='https://s3.amazonaws.com', config=Any())


def test_get_s3_client_from_env(mocker):
    mock_session = mocker.Mock(spec=boto3.session.Session)
    mocker.patch('megfile.s3.get_scoped_config', return_value={})
    mocker.patch('megfile.s3.get_s3_session', return_value=mock_session)
    mocker.patch.dict(os.environ, {'OSS_ENDPOINT': 'oss-endpoint'})

    s3.get_s3_client()

    mock_session.client.assert_called_with(
        's3', endpoint_url='oss-endpoint', config=Any())


def test_get_s3_client_with_config(mocker):
    mock_session = mocker.Mock(spec=boto3.session.Session)
    mocker.patch('megfile.s3.get_scoped_config', return_value={})
    mocker.patch('megfile.s3.get_s3_session', return_value=mock_session)

    config = botocore.config.Config(max_pool_connections=20)

    s3.get_s3_client(config)

    mock_session.client.assert_called_with(
        's3', endpoint_url='https://s3.amazonaws.com', config=config)


def test_get_s3_session_threading(mocker):
    session_call = mocker.patch('boto3.session.Session')
    for i in range(2):
        thread = threading.Thread(target=s3.get_s3_session)
        thread.start()
        thread.join()

    assert session_call.call_count == 2


def test_get_s3_session_threading_reuse(mocker):
    session_call = mocker.patch('boto3.session.Session')

    def session_twice():
        s3.get_s3_session()
        s3.get_s3_session()

    thread = threading.Thread(target=session_twice)
    thread.start()
    thread.join()

    assert session_call.call_count == 1


def test_is_s3():
    # 不以 s3:// 开头
    assert s3.is_s3('') == False
    assert s3.is_s3('s') == False
    assert s3.is_s3('s3') == False
    assert s3.is_s3('s3:') == False
    assert s3.is_s3('s3:bucket') == False
    assert s3.is_s3('S3:bucket') == False
    assert s3.is_s3('s3:/') == False
    assert s3.is_s3('s3:/xxx') == False
    assert s3.is_s3('s3:/foo/bar') == False
    assert s3.is_s3('s3://') == True
    assert s3.is_s3('s4://') == False
    assert s3.is_s3('s3:://') == False
    assert s3.is_s3('s3:/path/to/file') == False
    assert s3.is_s3('s3:base') == False
    assert s3.is_s3('/xxx') == False
    assert s3.is_s3('/path/to/file') == False

    # 以非小写字母开头的 bucket
    assert s3.is_s3('s3://Bucket') == True
    assert s3.is_s3('s3:// ucket') == True
    assert s3.is_s3('s3://.ucket') == True
    assert s3.is_s3('s3://?ucket') == True
    assert s3.is_s3('s3://\rucket') == True
    assert s3.is_s3('s3://\ncket') == True
    assert s3.is_s3('s3://\bcket') == True
    assert s3.is_s3('s3://\tcket') == True
    assert s3.is_s3('s3://-bucket') == True

    # 以非小写字母结尾的 bucket
    assert s3.is_s3('s3://buckeT') == True
    assert s3.is_s3('s3://bucke ') == True
    assert s3.is_s3('s3://bucke.') == True
    assert s3.is_s3('s3://bucke?') == True
    assert s3.is_s3('s3://bucke\r') == True
    assert s3.is_s3('s3://bucke\n') == True
    assert s3.is_s3('s3://bucke\t') == True
    assert s3.is_s3('s3://bucke\b') == True
    assert s3.is_s3('s3://bucket0') == True

    # 中间含有非字母、数字且非 '-' 字符的 bucket
    assert s3.is_s3('s3://buc.ket') == True
    assert s3.is_s3('s3://buc?ket') == True
    assert s3.is_s3('s3://buc ket') == True
    assert s3.is_s3('s3://buc\tket') == True
    assert s3.is_s3('s3://buc\rket') == True
    assert s3.is_s3('s3://buc\bket') == True
    assert s3.is_s3('s3://buc\vket') == True
    assert s3.is_s3('s3://buc\aket') == True
    assert s3.is_s3('s3://buc\nket') == True

    # bucket 长度不位于闭区间 [3, 63]
    assert s3.is_s3('s3://bu') == True
    assert s3.is_s3('s3://%s' % ('b' * 64)) == True

    # prefix，可以为 ''，或包含连续的 '/'
    assert s3.is_s3('s3://bucket') == True
    assert s3.is_s3('s3://bucket/') == True
    assert s3.is_s3('s3://bucket//') == True
    assert s3.is_s3('s3://bucket//prefix') == True
    assert s3.is_s3('s3://bucket/key/') == True
    assert s3.is_s3('s3://bucket/key//') == True
    assert s3.is_s3('s3://bucket/prefix/key/') == True
    assert s3.is_s3('s3://bucket/prefix//key/') == True
    assert s3.is_s3('s3://bucket//prefix//key/') == True
    assert s3.is_s3('s3://bucket//prefix//key') == True
    assert s3.is_s3('s3://bucket//////') == True

    # path 以不可见字符结尾
    assert s3.is_s3('s3://bucket/ ') == True
    assert s3.is_s3('s3://bucket/\r') == True
    assert s3.is_s3('s3://bucket/\n') == True
    assert s3.is_s3('s3://bucket/\a') == True
    assert s3.is_s3('s3://bucket/\b') == True
    assert s3.is_s3('s3://bucket/\t') == True
    assert s3.is_s3('s3://bucket/\v') == True
    assert s3.is_s3('s3://bucket/key ') == True
    assert s3.is_s3('s3://bucket/key\n') == True
    assert s3.is_s3('s3://bucket/key\r') == True
    assert s3.is_s3('s3://bucket/key\a') == True
    assert s3.is_s3('s3://bucket/key\b') == True
    assert s3.is_s3('s3://bucket/key\t') == True
    assert s3.is_s3('s3://bucket/key\v') == True

    # PathLike
    assert s3.is_s3(Path('/bucket/key')) == False


def test_parse_s3_url():
    assert s3.parse_s3_url('s3://bucket/prefix/key') == ('bucket', 'prefix/key')
    assert s3.parse_s3_url('s3://bucket') == ('bucket', '')
    assert s3.parse_s3_url('s3://') == ('', '')
    assert s3.parse_s3_url('s3:///') == ('', '')
    assert s3.parse_s3_url('s3:////') == ('', '/')
    assert s3.parse_s3_url('s3:///prefix/') == ('', 'prefix/')
    assert s3.parse_s3_url('s3:///prefix/key') == ('', 'prefix/key')

    assert s3.parse_s3_url('s3://bucket/prefix?') == ('bucket', 'prefix?')
    assert s3.parse_s3_url('s3://bucket/prefix#') == ('bucket', 'prefix#')
    assert s3.parse_s3_url('s3://bucket/?#') == ('bucket', '?#')
    assert s3.parse_s3_url('s3://bucket/prefix/#key') == (
        'bucket', 'prefix/#key')
    assert s3.parse_s3_url('s3://bucket/prefix/?key') == (
        'bucket', 'prefix/?key')
    assert s3.parse_s3_url('s3://bucket/prefix/key?key#key') == (
        'bucket', 'prefix/key?key#key')
    assert s3.parse_s3_url('s3://bucket/prefix/key#key?key') == (
        'bucket', 'prefix/key#key?key')


def test_s3_scandir_internal(truncating_client):

    def dir_entrys_to_tuples(entries: Iterable[FileEntry]
                            ) -> List[Tuple[str, bool]]:
        return sorted([(entry.name, entry.is_dir()) for entry in entries])

    assert dir_entrys_to_tuples(s3.s3_scandir('s3://')) == [
        ('bucketA', True),
        ('bucketB', True),
        ('bucketC', True),
        ('bucketForGlobTest', True),
        ('bucketForGlobTest2', True),
        ('bucketForGlobTest3', True),
        ('emptyBucketForGlobTest', True),
    ]
    assert dir_entrys_to_tuples(s3.s3_scandir('s3://bucketB')) == []
    assert dir_entrys_to_tuples(s3.s3_scandir('s3://bucketA')) == [
        ('fileAA', False),
        ('fileAB', False),
        ('folderAA', True),
        ('folderAB', True),
        ('folderAB-C', True),
    ]
    assert dir_entrys_to_tuples(s3.s3_scandir('s3://bucketA/folderAA')) == [
        ('folderAAA', True),
    ]
    assert dir_entrys_to_tuples(s3.s3_scandir('s3://bucketA/folderAB')) == [
        ('fileAB', False),
        ('fileAC', False),
    ]
    assert dir_entrys_to_tuples(s3.s3_scandir('s3://bucketA/folderAB/')) == [
        ('fileAB', False),
        ('fileAC', False),
    ]

    with pytest.raises(NotADirectoryError) as error:
        s3.s3_scandir('s3://bucketA/fileAA')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_scandir('s3://notExistBucket')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_scandir('s3://bucketA/notExistFolder')


def test_s3_scandir(truncating_client):

    assert sorted(list(map(lambda x: x.name, s3.s3_scandir('s3://')))) == [
        'bucketA',
        'bucketB',
        'bucketC',
        'bucketForGlobTest',
        'bucketForGlobTest2',
        'bucketForGlobTest3',
        'emptyBucketForGlobTest',
    ]
    assert sorted(list(map(lambda x: x.name,
                           s3.s3_scandir('s3://bucketB')))) == []
    assert sorted(list(map(lambda x: x.name,
                           s3.s3_scandir('s3://bucketA')))) == [
                               'fileAA',
                               'fileAB',
                               'folderAA',
                               'folderAB',
                               'folderAB-C',
                           ]
    assert sorted(
        list(map(lambda x: x.name,
                 s3.s3_scandir('s3://bucketA/folderAA')))) == ['folderAAA']
    assert sorted(
        list(map(lambda x: x.name,
                 s3.s3_scandir('s3://bucketA/folderAB')))) == [
                     'fileAB', 'fileAC'
                 ]
    assert sorted(
        list(map(lambda x: x.name,
                 s3.s3_scandir('s3://bucketA/folderAB/')))) == [
                     'fileAB', 'fileAC'
                 ]

    with pytest.raises(NotADirectoryError) as error:
        s3.s3_scandir('s3://bucketA/fileAA')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_scandir('s3://notExistBucket')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_scandir('s3://bucketA/notExistFolder')


def test_s3_listdir(truncating_client):
    assert s3.s3_listdir('s3://') == [
        'bucketA', 'bucketB', 'bucketC', 'bucketForGlobTest',
        'bucketForGlobTest2', 'bucketForGlobTest3', 'emptyBucketForGlobTest'
    ]
    assert s3.s3_listdir('s3://bucketB') == []
    assert s3.s3_listdir('s3://bucketA') == [
        'fileAA', 'fileAB', 'folderAA', 'folderAB', 'folderAB-C'
    ]
    assert s3.s3_listdir('s3://bucketA/folderAA') == ['folderAAA']
    assert s3.s3_listdir('s3://bucketA/folderAB') == ['fileAB', 'fileAC']
    assert s3.s3_listdir('s3://bucketA/folderAB/') == ['fileAB', 'fileAC']
    with pytest.raises(NotADirectoryError) as error:
        s3.s3_listdir('s3://bucketA/fileAA')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_listdir('s3://notExistBucket')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_listdir('s3://bucketA/notExistFolder')


def test_s3_isfile(s3_setup):
    assert s3.s3_isfile('s3://') is False  # root
    assert s3.s3_isfile('s3://bucketB') is False  # empty bucket
    assert s3.s3_isfile('s3://bucketA/folderAA') is False
    assert s3.s3_isfile('s3://bucketA/notExistFile') is False
    assert s3.s3_isfile('s3://notExistBucket/folderAA') is False
    assert s3.s3_isfile('s3://+InvalidBucketName/folderAA') is False
    assert s3.s3_isfile('s3://bucketA/fileAA/') is False
    assert s3.s3_isfile('s3://bucketA/fileAA') is True


def test_s3_isdir(s3_setup):
    assert s3.s3_isdir('s3://') is True  # root
    assert s3.s3_isdir('s3://bucketB') is True  # empty bucket
    assert s3.s3_isdir('s3://bucketA/folderAA') is True  # commonperfixes
    assert s3.s3_isdir('s3://bucketA/folderAA/folderAAA') is True  # context
    assert s3.s3_isdir('s3://bucketA/fileAA') is False  # file
    assert s3.s3_isdir('s3://bucketA/notExistFolder') is False
    assert s3.s3_isdir('s3://notExistBucket') is False
    assert s3.s3_isdir('s3://+InvalidBucketName') is False


def test_s3_access(s3_setup):
    assert s3.s3_access('s3://bucketA/fileAA', Access.READ) is True
    assert s3.s3_access('s3://bucketA/fileAA', Access.WRITE) is True
    with pytest.raises(TypeError) as error:
        s3.s3_access('s3://bucketA/fileAA', 'w')
    assert s3.s3_access('s3://thisdoesnotexists', Access.READ) is False
    assert s3.s3_access('s3://thisdoesnotexists', Access.WRITE) is False


def test_s3_exists(s3_setup):
    assert s3.s3_exists('s3://') is True
    assert s3.s3_exists('s3://bucketB') is True
    assert s3.s3_exists('s3://bucketA/folderAB-C') is True
    assert s3.s3_exists('s3://bucketA/folderAB') is True
    assert s3.s3_exists('s3://bucketA/folderAA') is True
    assert s3.s3_exists('s3://bucketA/folderAA/folderAAA') is True
    assert s3.s3_exists('s3://bucketA/fileAA') is True
    assert s3.s3_exists('s3://bucketA/notExistFolder') is False
    assert s3.s3_exists('s3://notExistBucket') is False
    assert s3.s3_exists('s3://notExistBucket/notExistFile') is False
    assert s3.s3_exists('s3://+InvalidBucketName') is False
    assert s3.s3_exists('s3://bucketA/file') is False  # file prefix
    assert s3.s3_exists('s3://bucketA/folder') is False  # folder prefix
    assert s3.s3_exists('s3://bucketA/fileAA/') is False  # filename as dir


def test_s3_copy(s3_empty_client):
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key', Body='value')

    s3.s3_copy('s3://bucket/key', 's3://bucket/result')

    body = s3_empty_client.get_object(
        Bucket='bucket', Key='result')['Body'].read().decode("utf-8")

    assert body == 'value'


@pytest.mark.skipif(sys.version_info < (3, 6), reason="Python3.6+")
def test_s3_copy_invalid(s3_empty_client):
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key', Body='value')

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_copy('s3://bucket/key', 's3://bucket/')
    assert 's3://bucket/' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_copy('s3://bucket/key', 's3://bucket/prefix/')
    assert 's3://bucket/prefix/' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_copy('s3://bucket/key', 's3:///key')
    assert 's3:///key' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_copy('s3://bucket/key', 's3://notExistBucket/key')
    assert 's3://notExistBucket/key' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_copy('s3://bucket/prefix/', 's3://bucket/key')
    assert 's3://bucket/prefix/' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_copy('s3:///key', 's3://bucket/key')
    assert 's3:///key' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_copy('s3://notExistBucket/key', 's3://bucket/key')
    assert 's3://notExistBucket' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_copy('s3://bucket/notExistFile', 's3://bucket/key')
    assert 's3://bucket/notExistFile' in str(error.value)


def test_s3_getsize(truncating_client):
    bucket_A_size = s3.s3_getsize('s3://bucketA')
    assert bucket_A_size == 8 + 6 + 6 + 6 + 6 + 8  # folderAA/folderAAA/fileAAAA + folderAB/fileAB + folderAB/fileAC + folderAB-C/fileAB-C + fileAA + fileAB
    assert s3.s3_getsize('s3://bucketA/fileAA') == 6
    assert s3.s3_getsize('s3://bucketA/folderAB') == 6 + 6

    with pytest.raises(UnsupportedError) as error:
        assert s3.s3_getsize('s3://')
    assert 's3://' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getsize('s3://notExistBucket')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getsize('s3://bucketA/notExistFile')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getsize('s3:///notExistFile')


def test_s3_getmtime(truncating_client):
    bucket_A_mtime = s3.s3_getmtime('s3://bucketA')
    assert bucket_A_mtime == Now()
    assert s3.s3_getmtime('s3://bucketA/fileAA') == Now()
    assert s3.s3_getmtime('s3://bucketA/folderAB') == Now()

    with pytest.raises(UnsupportedError) as error:
        assert s3.s3_getmtime('s3://')
    assert 's3://' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getmtime('s3://notExistBucket')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getmtime('s3://bucketA/notExistFile')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getmtime('s3:///notExistFile')


def test_s3_stat(truncating_client, mocker):
    mocker.patch('megfile.s3.StatResult', side_effect=FakeStatResult)

    bucket_A_stat = s3.s3_stat('s3://bucketA')
    assert bucket_A_stat == make_stat(
        size=8 + 6 + 6 + 6 + 6 +
        8,  # folderAA/folderAAA/fileAAAA + folderAB/fileAB + folderAB/fileAC + folderAB-C/fileAB-C + fileAA + fileAB
        mtime=Now(),
        isdir=True)
    assert s3.s3_stat('s3://bucketA/fileAA') == make_stat(size=6)
    assert s3.s3_stat('s3://bucketA/folderAB') == make_stat(
        size=6 + 6, mtime=Now(), isdir=True)

    # 有同名目录时，优先返回文件的状态
    assert s3.s3_stat('s3://bucketC/folder') == StatResult(size=4, mtime=Now())

    with pytest.raises(UnsupportedError) as error:
        assert s3.s3_stat('s3://')
    assert 's3://' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_stat('s3://notExistBucket')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_stat('s3://bucketA/notExistFile')
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_stat('s3:///notExistFile')


def test_s3_upload(fs, s3_empty_client):
    src_url = '/path/to/file'

    fs.create_file(src_url, contents='value')
    s3_empty_client.create_bucket(Bucket='bucket')

    s3.s3_upload(src_url, 's3://bucket/result')

    body = s3_empty_client.get_object(
        Bucket='bucket', Key='result')['Body'].read().decode('utf-8')
    md5 = s3_empty_client.head_object(
        Bucket='bucket', Key='result')['Metadata'][content_md5_header]

    assert body == 'value'
    assert md5 == '2063c1608d6e0baf80249c42e2be5804'  # md5('value').hexdigest()


def test_s3_upload_invalid(fs, s3_empty_client):
    s3_empty_client.create_bucket(Bucket='bucket')

    src_url = '/path/to/file'
    fs.create_file(src_url, contents='value')

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_upload(src_url, 's3://bucket/prefix/')
    assert 's3://bucket/prefix/' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_upload(src_url, 's3:///key')
    assert 's3:///key' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_upload('/notExistFile', 's3://bucket/key')
    assert '/notExistFile' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_upload(src_url, 's3://notExistBucket/key')
    assert 's3://notExistBucket/key' in str(error.value)


def test_s3_upload_is_directory(fs, s3_empty_client):
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_upload('/path/to/file', 's3://bucket/prefix/')
    assert 's3://bucket/prefix/' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_upload('/path/to/file', 's3://bucket')
    assert 's3://bucket' in str(error.value)

    src_url = '/path/to/'
    fs.create_dir(src_url)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_upload(src_url, 's3://bucket/key')
    assert src_url in str(error.value)


def test_s3_download(fs, s3_setup):
    dst_url = '/path/to/file'

    s3.s3_download('s3://bucketA/fileAA', dst_url)

    with open(dst_url, 'rb') as result:
        body = result.read().decode("utf-8")
        assert body == 'fileAA'

    dst_url = '/path/to/another/file'
    os.makedirs(os.path.dirname(dst_url))

    s3.s3_download('s3://bucketA/fileAA', dst_url)

    with open(dst_url, 'rb') as result:
        body = result.read().decode("utf-8")
        assert body == 'fileAA'

    dst_url = '/path/to/samename/file'

    s3.s3_download('s3://bucketC/folder', dst_url)

    with open(dst_url, 'rb') as result:
        body = result.read().decode('utf-8')
        assert body == 'file'


def test_s3_download_makedirs(mocker, fs, s3_setup):
    dst_url = '/path/to/another/file'
    dst_dir = os.path.dirname(dst_url)
    os.makedirs(dst_dir)

    mocker.patch('os.makedirs')
    s3.s3_download('s3://bucketA/fileAA', dst_url)
    os.makedirs.assert_called_once_with(dst_dir, exist_ok=True)
    os.makedirs.reset_mock()

    s3.s3_download('s3://bucketA/fileAA', 'file')
    # os.makedirs.assert_not_called() in Python 3.6+
    assert os.makedirs.call_count == 0


def test_s3_download_is_directory(fs, s3_setup):
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_download('s3://bucketA/fileAA', '')

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_download('s3://bucketA/fileAA', '/path/')
    assert '/path/' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_download('s3://bucketA', '/path/to/file')
    assert 's3://bucketA' in str(error.value)


def test_s3_download_invalid(fs, s3_setup):
    dst_url = '/path/to/file'

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_download('s3://bucket/prefix/', dst_url)
    assert 's3://bucket/prefix/' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_download('s3:///key', dst_url)
    assert 's3:///key' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_download('s3://notExistBucket/fileAA', dst_url)
    assert 's3://notExistBucket' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_download('s3://bucket/notExistFile', dst_url)
    assert 's3://bucket/notExistFile' in str(error.value)


def test_s3_remove(s3_setup):
    with pytest.raises(UnsupportedError) as error:
        s3.s3_remove('s3://')
    assert 's3://' in str(error.value)
    with pytest.raises(UnsupportedError) as error:
        s3.s3_remove('s3://bucketA/')
    assert 's3://bucketA/' in str(error.value)
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_remove('s3://bucketA/notExistFile')
    assert 's3://bucketA/notExistFile' in str(error.value)
    s3.s3_remove('s3://bucketA/notExistFile', missing_ok=True)
    s3.s3_remove('s3://bucketA/folderAA/')
    assert s3.s3_exists('s3://bucketA/folderAA/') is False
    s3.s3_remove('s3://bucketA/folderAB')
    assert s3.s3_exists('s3://bucketA/folderAB/') is False
    s3.s3_remove('s3://bucketA/fileAA')
    assert s3.s3_exists('s3://bucketA/fileAA') is False


def test_s3_remove_multi_page(truncating_client):
    s3.s3_remove('s3://bucketA/folderAA/')
    assert s3.s3_exists('s3://bucketA/folderAA/') is False
    s3.s3_remove('s3://bucketA/folderAB')
    assert s3.s3_exists('s3://bucketA/folderAB/') is False
    s3.s3_remove('s3://bucketA/fileAA')
    assert s3.s3_exists('s3://bucketA/fileAA') is False


@pytest.mark.skip('moto issue https://github.com/spulec/moto/issues/2759')
def test_s3_remove_slashes(s3_empty_client):
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='///')
    s3.s3_remove('s3://bucket//')
    assert s3.s3_exists('s3://bucket////') is False


def test_s3_move(truncating_client):
    smart.smart_touch('s3://bucketA/folderAA/folderAAA/fileAAAA')
    s3.s3_move(
        's3://bucketA/folderAA/folderAAA', 's3://bucketA/folderAA/folderAAA1')
    assert s3.s3_exists('s3://bucketA/folderAA/folderAAA') is False
    assert s3.s3_exists('s3://bucketA/folderAA/folderAAA1/fileAAAA')


def test_s3_sync(truncating_client):
    smart.smart_touch('s3://bucketA/folderAA/folderAAA/fileAAAA')
    s3.s3_sync(
        's3://bucketA/folderAA/folderAAA', 's3://bucketA/folderAA/folderAAA1')
    assert s3.s3_exists('s3://bucketA/folderAA/folderAAA')
    assert s3.s3_exists('s3://bucketA/folderAA/folderAAA1/fileAAAA')


def test_s3_rename(truncating_client):
    s3.s3_rename(
        's3://bucketA/folderAA/folderAAA/fileAAAA',
        's3://bucketA/folderAA/folderAAA/fileAAAA1')
    assert s3.s3_exists('s3://bucketA/folderAA/folderAAA/fileAAAA') is False
    assert s3.s3_exists('s3://bucketA/folderAA/folderAAA/fileAAAA1')


def test_s3_unlink(s3_setup):
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_unlink('s3://')
    assert 's3://' in str(error.value)
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_unlink('s3://bucketA/')
    assert 's3://bucketA/' in str(error.value)
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_unlink('s3://bucketA/notExistFile')
    assert 's3://bucketA/notExistFile' in str(error.value)
    s3.s3_unlink('s3://bucketA/notExistFile', missing_ok=True)
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_unlink('s3://bucketA/folderAA/')
    assert 's3://bucketA/folderAA/' in str(error.value)
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_unlink('s3://bucketA/folderAB')
    assert 's3://bucketA/folderAB' in str(error.value)
    s3.s3_unlink('s3://bucketA/fileAA')
    assert s3.s3_exists('s3://bucketA/fileAA') is False


def test_s3_makedirs(mocker, s3_setup):
    with pytest.raises(FileExistsError) as error:
        s3.s3_makedirs('s3://bucketA/folderAB')
    assert 's3://bucketA/folderAB' in str(error.value)

    s3.s3_makedirs('s3://bucketA/folderAB', exist_ok=True)

    with pytest.raises(FileExistsError) as error:
        s3.s3_makedirs('s3://bucketA/fileAA', exist_ok=True)
    assert 's3://bucketA/fileAA' in str(error.value)


def test_s3_makedirs_no_bucket(mocker, s3_empty_client):
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_makedirs('s3://bucket/key')
    assert 's3://bucket' in str(error.value)


def test_s3_makedirs_exists_folder(mocker, s3_setup):
    with pytest.raises(FileExistsError) as error:
        s3.s3_makedirs('s3://bucketA/folderAB')
    assert 's3://bucketA/folderAB' in str(error.value)


def test_s3_makedirs_root(s3_empty_client):

    with pytest.raises(PermissionError) as error:
        s3.s3_makedirs('s3://')
    assert 's3://' in str(error.value)


def test_smart_open_read_s3_file_not_found(mocker, s3_empty_client):
    mocker.patch('megfile.s3.get_endpoint_url', return_value=None)

    with pytest.raises(FileNotFoundError) as error:
        smart.smart_open('s3://non-exist-bucket/key', 'r')
    assert 's3://non-exist-bucket/key' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        smart.smart_open('s3://non-exist-bucket/key', 'w')
    assert 's3://non-exist-bucket/key' in str(error.value)

    s3_empty_client.create_bucket(Bucket='bucket')
    with pytest.raises(FileNotFoundError) as error:
        smart.smart_open('s3://bucket/non-exist-key', 'r')
    assert 's3://bucket/non-exist-key' in str(error.value)


def test_smart_open_url_is_of_credentials_format(mocker, s3_empty_client):
    '''
    测试 s3_url 中包含 ':' 和 '@' 字符的 url，该 url 将被 smart_open 误认为是包含 credential info 的 url
    详情见：https://github.com/RaRe-Technologies/smart_open/issues/378
    '''
    bucket = 'bucket'
    key = 'username:password@key_part'
    s3_empty_client.create_bucket(Bucket=bucket)
    s3_empty_client.put_object(Bucket=bucket, Key=key)

    mocker.patch('megfile.s3.get_endpoint_url', return_value=None)

    # 希望，正常打开，而不是报错
    # smart_open 将 '@' 之后的部分认为是 key
    smart.smart_open('s3://bucket/username:password@key_part')


def test_s3_walk(truncating_client):
    # walk the whole s3
    # expect: raise UnsupportedError exception
    with pytest.raises(UnsupportedError) as error:
        list(s3.s3_walk('s3://'))
    assert 's3://' in str(error.value)

    # walk the dir that is not exist
    # expect: empty generator
    assert list(s3.s3_walk('s3://notExistBucket')) == []
    assert list(s3.s3_walk('s3://bucketA/notExistFile')) == []

    # walk on file
    # expect: empty generator
    assert list(s3.s3_walk("s3://bucketA/fileAA")) == []

    # walk empty bucket
    # expect: 1 tuple only contains the folder(bucket) path
    result = list(s3.s3_walk('s3://bucketB'))
    assert len(result) == 1
    assert result[0] == ('s3://bucketB', [], [])

    result = list(s3.s3_walk('s3://bucketA'))
    assert len(result) == 5
    assert result[0] == (
        's3://bucketA', ['folderAA', 'folderAB',
                         'folderAB-C'], ['fileAA', 'fileAB'])
    assert result[1] == ('s3://bucketA/folderAA', ['folderAAA'], [])
    assert result[2] == ('s3://bucketA/folderAA/folderAAA', [], ['fileAAAA'])
    assert result[3] == ('s3://bucketA/folderAB', [], ['fileAB', 'fileAC'])
    assert result[4] == ('s3://bucketA/folderAB-C', [], ['fileAB-C'])

    result = list(s3.s3_walk('s3://bucketA/'))
    assert len(result) == 5
    assert result[0] == (
        's3://bucketA', ['folderAA', 'folderAB',
                         'folderAB-C'], ['fileAA', 'fileAB'])
    assert result[1] == ('s3://bucketA/folderAA', ['folderAAA'], [])
    assert result[2] == ('s3://bucketA/folderAA/folderAAA', [], ['fileAAAA'])
    assert result[3] == ('s3://bucketA/folderAB', [], ['fileAB', 'fileAC'])
    assert result[4] == ('s3://bucketA/folderAB-C', [], ['fileAB-C'])

    # same name of file and folder in the same folder
    result = list(s3.s3_walk('s3://bucketC/folder'))
    assert len(result) == 1
    assert result[0] == ('s3://bucketC/folder', [], ['file'])

    result = list(s3.s3_walk('s3://bucketC/folder/'))
    assert len(result) == 1
    assert result[0] == ('s3://bucketC/folder', [], ['file'])


def test_s3_scan(truncating_client):
    # walk the whole s3
    # expect: raise UnsupportedError exception
    with pytest.raises(UnsupportedError) as error:
        list(s3.s3_scan('s3://'))
    assert 's3://' in str(error.value)

    # walk the dir that is not exist
    # expect: empty generator
    assert list(s3.s3_scan('s3://notExistBucket')) == []
    assert list(s3.s3_scan('s3://bucketA/notExistFile')) == []

    # walk on file
    # expect: empty generator
    assert list(s3.s3_scan('s3://bucketA/fileAA')) == ['s3://bucketA/fileAA']

    # walk empty bucket
    # expect: 1 tuple only contains the folder(bucket) path
    result = list(s3.s3_scan('s3://bucketB'))
    assert len(result) == 0

    result = list(s3.s3_scan('s3://bucketA'))
    assert len(result) == 6
    assert result == [
        's3://bucketA/fileAA',
        's3://bucketA/fileAB',
        's3://bucketA/folderAA/folderAAA/fileAAAA',
        's3://bucketA/folderAB-C/fileAB-C',
        's3://bucketA/folderAB/fileAB',
        's3://bucketA/folderAB/fileAC',
    ]

    result = list(s3.s3_scan('s3://bucketA/'))
    assert len(result) == 6
    assert result == [
        's3://bucketA/fileAA',
        's3://bucketA/fileAB',
        's3://bucketA/folderAA/folderAAA/fileAAAA',
        's3://bucketA/folderAB-C/fileAB-C',
        's3://bucketA/folderAB/fileAB',
        's3://bucketA/folderAB/fileAC',
    ]

    # same name of file and folder in the same folder
    result = list(s3.s3_scan('s3://bucketC/folder'))
    assert len(result) == 2
    assert result == [
        's3://bucketC/folder',
        's3://bucketC/folder/file',
    ]

    result = list(s3.s3_scan('s3://bucketC/folder/'))
    assert len(result) == 1
    assert result == ['s3://bucketC/folder/file']

    with pytest.raises(UnsupportedError) as error:
        s3.s3_scan('s3://')


def test_s3_scan_stat(truncating_client, mocker):
    mocker.patch('megfile.s3.StatResult', side_effect=FakeStatResult)

    # walk the whole s3
    # expect: raise UnsupportedError exception
    with pytest.raises(UnsupportedError) as error:
        list(s3.s3_scan_stat('s3://'))
    assert 's3://' in str(error.value)

    # walk the dir that is not exist
    # expect: empty generator
    assert list(s3.s3_scan_stat('s3://notExistBucket')) == []
    assert list(s3.s3_scan_stat('s3://bucketA/notExistFile')) == []

    # walk on file
    # expect: empty generator
    assert list(s3.s3_scan_stat('s3://bucketA/fileAA')) == [
        ('s3://bucketA/fileAA', make_stat(size=6))
    ]

    # walk empty bucket
    # expect: 1 tuple only contains the folder(bucket) path
    result = list(s3.s3_scan_stat('s3://bucketB'))
    assert len(result) == 0

    result = list(s3.s3_scan_stat('s3://bucketA'))
    assert len(result) == 6
    assert result == [
        ('s3://bucketA/fileAA', make_stat(size=6)),
        ('s3://bucketA/fileAB', make_stat(size=6)),
        ('s3://bucketA/folderAA/folderAAA/fileAAAA', make_stat(size=8)),
        ('s3://bucketA/folderAB-C/fileAB-C', make_stat(size=8)),
        ('s3://bucketA/folderAB/fileAB', make_stat(size=6)),
        ('s3://bucketA/folderAB/fileAC', make_stat(size=6)),
    ]

    result = list(s3.s3_scan_stat('s3://bucketA/'))
    assert len(result) == 6
    assert result == [
        ('s3://bucketA/fileAA', make_stat(size=6)),
        ('s3://bucketA/fileAB', make_stat(size=6)),
        ('s3://bucketA/folderAA/folderAAA/fileAAAA', make_stat(size=8)),
        ('s3://bucketA/folderAB-C/fileAB-C', make_stat(size=8)),
        ('s3://bucketA/folderAB/fileAB', make_stat(size=6)),
        ('s3://bucketA/folderAB/fileAC', make_stat(size=6)),
    ]

    # same name of file and folder in the same folder
    result = list(s3.s3_scan_stat('s3://bucketC/folder'))
    assert len(result) == 2
    assert result == [
        ('s3://bucketC/folder', make_stat(size=4)),
        ('s3://bucketC/folder/file', make_stat(size=4)),
    ]

    result = list(s3.s3_scan_stat('s3://bucketC/folder/'))
    assert len(result) == 1
    assert result == [('s3://bucketC/folder/file', make_stat(size=4))]

    with pytest.raises(UnsupportedError) as error:
        s3.s3_scan_stat('s3://')


def test_s3_path_join():
    assert s3.s3_path_join('s3://') == 's3://'
    assert s3.s3_path_join('s3://', 'bucket/key') == 's3://bucket/key'
    assert s3.s3_path_join('s3://', 'bucket//key') == 's3://bucket//key'
    assert s3.s3_path_join('s3://', 'bucket', 'key') == 's3://bucket/key'
    assert s3.s3_path_join('s3://', 'bucket/', 'key') == 's3://bucket/key'
    assert s3.s3_path_join('s3://', 'bucket', '/key') == 's3://bucket/key'
    assert s3.s3_path_join('s3://', 'bucket', 'key/') == 's3://bucket/key/'


def _s3_glob_with_bucket_match():
    '''
    scenario: match s3 bucket by including wildcard in 'bucket' part.
    return: all dirs, files and buckets fully matched the pattern
    '''
    assert_glob(
        r's3://*', [
            's3://bucketA',
            's3://bucketB',
            's3://bucketC',
            's3://bucketForGlobTest',
            's3://bucketForGlobTest2',
            's3://bucketForGlobTest3',
            's3://emptyBucketForGlobTest',
        ])

    # without any wildcards
    assert_glob(
        r's3://{bucketForGlobTest}[12]/1', ['s3://bucketForGlobTest2/1'])
    assert_glob(
        r's3://bucketForGlobTest*/1', [
            's3://bucketForGlobTest/1', 's3://bucketForGlobTest2/1',
            's3://bucketForGlobTest3/1'
        ])
    assert_glob(
        r's3://*GlobTest*/1/a', [
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest3/1/a',
            's3://bucketForGlobTest3/1/a',
        ])
    assert_glob(
        r's3://**GlobTest***/1/a', [
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest3/1/a',
            's3://bucketForGlobTest3/1/a',
        ])

    assert_glob(
        r's3://bucketForGlobTest?/1/a/b/1.json', [
            's3://bucketForGlobTest2/1/a/b/1.json',
            's3://bucketForGlobTest3/1/a/b/1.json'
        ])
    assert_glob(
        r's3://bucketForGlobTest*/1/a{/b/c,/b}/1.json', [
            's3://bucketForGlobTest/1/a/b/c/1.json',
            's3://bucketForGlobTest/1/a/b/1.json',
            's3://bucketForGlobTest2/1/a/b/c/1.json',
            's3://bucketForGlobTest2/1/a/b/1.json',
            's3://bucketForGlobTest3/1/a/b/c/1.json',
            's3://bucketForGlobTest3/1/a/b/1.json'
        ])

    # all files under all direct subfolders
    assert_glob(
        r's3://bucketForGlobTest[23]/*/*', [
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest3/1/a',
            's3://bucketForGlobTest3/1/a',
        ])

    # combination of '?' and []
    assert_glob(r's3://*BucketForGlobTest/[2-3]/**/*?msg', [])
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/[13]/**/*?msg', [
            's3://bucketForGlobTest/1/a/b/c/A.msg',
            's3://bucketForGlobTest2/1/a/b/c/A.msg',
            's3://bucketForGlobTest2/1/a/c/A.msg'
        ])

    assert_glob(r's3://{a,b,c}*/notExist', [])

    with pytest.raises(FileNotFoundError) as err:
        s3.s3_glob(r's3://{a,b,c}*/notExist', missing_ok=False)
    assert r's3://{a,b,c}*/notExist' in str(err.value)


def assert_glob(pattern, expected, recursive=True, missing_ok=True):
    assert sorted(
        s3.s3_glob(pattern, recursive=recursive,
                   missing_ok=missing_ok)) == sorted(expected)


def assert_glob_stat(pattern, expected, recursive=True, missing_ok=True):
    assert sorted(
        s3.s3_glob_stat(pattern, recursive=recursive,
                        missing_ok=missing_ok)) == sorted(expected)


def _s3_glob_with_common_wildcard():
    '''
    scenario: common shell wildcard, '*', '**', '[]', '?'
    expectation: return matched pathnames in lexicographical order
    '''
    # without any wildcards
    assert_glob('s3://emptyBucketForGlobTest', ['s3://emptyBucketForGlobTest'])
    assert_glob(
        's3://emptyBucketForGlobTest/', ['s3://emptyBucketForGlobTest/'])
    assert_glob('s3://bucketForGlobTest/1', ['s3://bucketForGlobTest/1'])
    assert_glob('s3://bucketForGlobTest/1/', ['s3://bucketForGlobTest/1/'])
    assert_glob(
        's3://bucketForGlobTest/1/a',
        ['s3://bucketForGlobTest/1/a', 's3://bucketForGlobTest/1/a'])  # 同名文件
    assert_glob(
        's3://bucketForGlobTest/2/a/d/2.json',
        ['s3://bucketForGlobTest/2/a/d/2.json'])
    assert_glob(
        r's3://bucketForGlobTest/2/a/d/{c/1,2}.json', [
            's3://bucketForGlobTest/2/a/d/c/1.json',
            's3://bucketForGlobTest/2/a/d/2.json'
        ])

    # '*', all files and folders
    assert_glob('s3://emptyBucketForGlobTest/*', [])
    assert_glob(
        's3://bucketForGlobTest/*', [
            's3://bucketForGlobTest/1',
            's3://bucketForGlobTest/2',
        ])

    # all files under all direct subfolders
    assert_glob(
        's3://bucketForGlobTest/*/*', [
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/2/a',
        ])

    # combination of '?' and []
    assert_glob('s3://bucketForGlobTest/[2-3]/**/*?msg', [])
    assert_glob(
        's3://bucketForGlobTest/[13]/**/*?msg',
        ['s3://bucketForGlobTest/1/a/b/c/A.msg'])
    assert_glob(
        's3://bucketForGlobTest/1/a/b/*/A.msg',
        ['s3://bucketForGlobTest/1/a/b/c/A.msg'])

    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob('s3://notExistsBucketForGlobTest/*', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob('s3://emptyBucketForGlobTest/*', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob('s3://bucketForGlobTest/3/**', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob(
                's3://bucketForGlobTest/1/**.notExists', missing_ok=False))


def _s3_glob_with_recursive_pathname():
    '''
    scenario: recursively search target folder
    expectation: returns all subdirectory and files, without check of lexicographical order
    '''
    # recursive all files and folders
    assert_glob(
        's3://bucketForGlobTest/**', [
            's3://bucketForGlobTest/1',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a/b',
            's3://bucketForGlobTest/1/a/b/1.json',
            's3://bucketForGlobTest/1/a/b/c',
            's3://bucketForGlobTest/1/a/b/c/1.json',
            's3://bucketForGlobTest/1/a/b/c/A.msg',
            's3://bucketForGlobTest/2',
            's3://bucketForGlobTest/2/a',
            's3://bucketForGlobTest/2/a/b',
            's3://bucketForGlobTest/2/a/b/a',
            's3://bucketForGlobTest/2/a/b/a/1.json',
            's3://bucketForGlobTest/2/a/b/c',
            's3://bucketForGlobTest/2/a/b/c/1.json',
            's3://bucketForGlobTest/2/a/b/c/2.json',
            's3://bucketForGlobTest/2/a/d',
            's3://bucketForGlobTest/2/a/d/2.json',
            's3://bucketForGlobTest/2/a/d/c/1.json',
            's3://bucketForGlobTest/2/a/d/c',
        ])

    assert_glob(
        's3://bucketForGlobTest/**/*', [
            's3://bucketForGlobTest/1',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a/b',
            's3://bucketForGlobTest/1/a/b/1.json',
            's3://bucketForGlobTest/1/a/b/c',
            's3://bucketForGlobTest/1/a/b/c/1.json',
            's3://bucketForGlobTest/1/a/b/c/A.msg',
            's3://bucketForGlobTest/2',
            's3://bucketForGlobTest/2/a',
            's3://bucketForGlobTest/2/a/b',
            's3://bucketForGlobTest/2/a/b/a',
            's3://bucketForGlobTest/2/a/b/a/1.json',
            's3://bucketForGlobTest/2/a/b/c',
            's3://bucketForGlobTest/2/a/b/c/1.json',
            's3://bucketForGlobTest/2/a/b/c/2.json',
            's3://bucketForGlobTest/2/a/d',
            's3://bucketForGlobTest/2/a/d/2.json',
            's3://bucketForGlobTest/2/a/d/c/1.json',
            's3://bucketForGlobTest/2/a/d/c',
        ])


def _s3_glob_with_same_file_and_folder():
    '''
    scenario: existing same-named file and directory in a  directory
    expectation: the file and directory is returned 1 time respectively
    '''
    # same name and folder
    assert_glob(
        's3://bucketForGlobTest/1/*',
        [
            # 1 file name 'a' and 1 actual folder
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a',
        ])


def _s3_glob_with_nested_pathname():
    '''
    scenario: pathname including nested '**'
    expectation: work correctly as standard glob module
    '''
    # nested
    # non-recursive, actually: s3://bucketForGlobTest/*/a/*/*.jso?
    assert_glob(
        's3://bucketForGlobTest/**/a/**/*.jso?', [
            's3://bucketForGlobTest/2/a/d/2.json',
            's3://bucketForGlobTest/1/a/b/1.json'
        ],
        recursive=False)

    # recursive
    # s3://bucketForGlobTest/2/a/b/a/1.json is returned 2 times
    # without set, otherwise, 's3://bucketForGlobTest/2/a/b/a/1.json' would be duplicated
    assert_glob(
        's3://bucketForGlobTest/**/a/**/*.jso?', [
            's3://bucketForGlobTest/1/a/b/1.json',
            's3://bucketForGlobTest/1/a/b/c/1.json',
            's3://bucketForGlobTest/2/a/b/a/1.json',
            's3://bucketForGlobTest/2/a/b/c/1.json',
            's3://bucketForGlobTest/2/a/b/c/2.json',
            's3://bucketForGlobTest/2/a/d/2.json',
            's3://bucketForGlobTest/2/a/d/c/1.json',
        ])


def _s3_glob_with_not_exists_dir():
    '''
    scenario: glob on a directory that is not exists
    expectation: if recursive is True, return the directory with postfix of slash('/'), otherwise, an empty list.
    keep identical result with standard glob module
    '''

    assert_glob('s3://bucketForGlobTest/notExistFolder/notExistFile', [])
    assert_glob('s3://bucketForGlobTest/notExistFolder', [])

    # not exists path
    assert_glob('s3://notExistBucket/**', [])

    assert_glob('s3://bucketA/notExistFolder/**', [])

    assert_glob('s3://notExistBucket/**', [])

    assert_glob('s3://bucketForGlobTest/notExistFolder/**', [])


def _s3_glob_with_dironly():
    '''
    scenario: pathname with the postfix of slash('/')
    expectation: returns only contains pathname of directory, each of them is end with '/'
    '''
    assert_glob(
        's3://bucketForGlobTest/*/', [
            's3://bucketForGlobTest/1/',
            's3://bucketForGlobTest/2/',
        ])

    assert_glob(
        's3://bucketForGlobTest/[2-9]/', [
            's3://bucketForGlobTest/2/',
        ])

    # all sub-directories of 2, recursively
    assert_glob(
        's3://bucketForGlobTest/2/**/*/', [
            's3://bucketForGlobTest/2/a/',
            's3://bucketForGlobTest/2/a/b/',
            's3://bucketForGlobTest/2/a/b/a/',
            's3://bucketForGlobTest/2/a/b/c/',
            's3://bucketForGlobTest/2/a/d/',
            's3://bucketForGlobTest/2/a/d/c/',
        ])


def _s3_glob_with_common_wildcard_cross_bucket():
    '''
    scenario: common shell wildcard, '*', '**', '[]', '?'
    expectation: return matched pathnames in lexicographical order
    '''
    # without any wildcards
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/1',
        ['s3://bucketForGlobTest/1', 's3://bucketForGlobTest2/1'])
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/1/', [
            's3://bucketForGlobTest/1/', 's3://bucketForGlobTest2/1/',
            's3://bucketForGlobTest3/1/'
        ])
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/1/a', [
            's3://bucketForGlobTest/1/a', 's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest2/1/a', 's3://bucketForGlobTest2/1/a'
        ])  # 同名文件
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest3}/1/a/b/1.json', [
            's3://bucketForGlobTest/1/a/b/1.json',
            's3://bucketForGlobTest3/1/a/b/1.json'
        ])
    assert_glob(
        r's3://{bucketForGlobTest/2,bucketForGlobTest2/1}/a/b/c/2.json', [
            's3://bucketForGlobTest/2/a/b/c/2.json',
            's3://bucketForGlobTest2/1/a/b/c/2.json'
        ])
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/1/a{/b/c,/b}/1.json',
        [
            's3://bucketForGlobTest/1/a/b/c/1.json',
            's3://bucketForGlobTest/1/a/b/1.json',
            's3://bucketForGlobTest2/1/a/b/c/1.json',
            's3://bucketForGlobTest2/1/a/b/1.json',
            's3://bucketForGlobTest3/1/a/b/c/1.json',
            's3://bucketForGlobTest3/1/a/b/1.json'
        ])

    # '*', all files and folders
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest3}/*', [
            's3://bucketForGlobTest/1',
            's3://bucketForGlobTest/2',
            's3://bucketForGlobTest3/1',
        ])

    # all files under all direct subfolders
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/*/*', [
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/2/a',
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest2/1/a',
        ])

    # combination of '?' and []
    assert_glob(
        r's3://{bucketForGlobTest,emptyBucketForGlobTest}/[2-3]/**/*?msg', [])
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/[13]/**/*?msg', [
            's3://bucketForGlobTest/1/a/b/c/A.msg',
            's3://bucketForGlobTest2/1/a/b/c/A.msg',
            's3://bucketForGlobTest2/1/a/c/A.msg'
        ])
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/1/a/b/*/A.msg', [
            's3://bucketForGlobTest/1/a/b/c/A.msg',
            's3://bucketForGlobTest2/1/a/b/c/A.msg'
        ])

    assert_glob(
        r's3://{notExistsBucketForGlobTest,bucketForGlobTest}/*',
        ['s3://bucketForGlobTest/1', 's3://bucketForGlobTest/2'],
        missing_ok=False)

    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob(
                r's3://{notExistsBucketForGlobTest,bucketForGlobTest}/3/*',
                missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob(
                r's3://{bucketForGlobTest,bucketForGlobTest2}/3/**',
                missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob(
                r's3://{bucketForGlobTest,bucketForGlobTest2}/1/**.notExists',
                missing_ok=False))


def _s3_glob_with_recursive_pathname_cross_bucket():
    '''
    scenario: recursively search target folder
    expectation: returns all subdirectory and files, without check of lexicographical order
    '''
    # recursive all files and folders
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/**', [
            's3://bucketForGlobTest/1',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a/b',
            's3://bucketForGlobTest/1/a/b/1.json',
            's3://bucketForGlobTest/1/a/b/c',
            's3://bucketForGlobTest/1/a/b/c/1.json',
            's3://bucketForGlobTest/1/a/b/c/A.msg',
            's3://bucketForGlobTest/2',
            's3://bucketForGlobTest/2/a',
            's3://bucketForGlobTest/2/a/b',
            's3://bucketForGlobTest/2/a/b/a',
            's3://bucketForGlobTest/2/a/b/a/1.json',
            's3://bucketForGlobTest/2/a/b/c',
            's3://bucketForGlobTest/2/a/b/c/1.json',
            's3://bucketForGlobTest/2/a/b/c/2.json',
            's3://bucketForGlobTest/2/a/d',
            's3://bucketForGlobTest/2/a/d/2.json',
            's3://bucketForGlobTest/2/a/d/c/1.json',
            's3://bucketForGlobTest/2/a/d/c',
            's3://bucketForGlobTest2/1',
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest2/1/a/b',
            's3://bucketForGlobTest2/1/a/b/1.json',
            's3://bucketForGlobTest2/1/a/b/c',
            's3://bucketForGlobTest2/1/a/b/c/1.json',
            's3://bucketForGlobTest2/1/a/b/c/2.json',
            's3://bucketForGlobTest2/1/a/b/c/A.msg',
            's3://bucketForGlobTest2/1/a/c',
            's3://bucketForGlobTest2/1/a/c/1.json',
            's3://bucketForGlobTest2/1/a/c/A.msg',
            's3://bucketForGlobTest2/1/a/1.json',
        ])

    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest3}/**/*', [
            's3://bucketForGlobTest/1',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a/b',
            's3://bucketForGlobTest/1/a/b/1.json',
            's3://bucketForGlobTest/1/a/b/c',
            's3://bucketForGlobTest/1/a/b/c/1.json',
            's3://bucketForGlobTest/1/a/b/c/A.msg',
            's3://bucketForGlobTest/2',
            's3://bucketForGlobTest/2/a',
            's3://bucketForGlobTest/2/a/b',
            's3://bucketForGlobTest/2/a/b/a',
            's3://bucketForGlobTest/2/a/b/a/1.json',
            's3://bucketForGlobTest/2/a/b/c',
            's3://bucketForGlobTest/2/a/b/c/1.json',
            's3://bucketForGlobTest/2/a/b/c/2.json',
            's3://bucketForGlobTest/2/a/d',
            's3://bucketForGlobTest/2/a/d/2.json',
            's3://bucketForGlobTest/2/a/d/c/1.json',
            's3://bucketForGlobTest/2/a/d/c',
            's3://bucketForGlobTest3/1',
            's3://bucketForGlobTest3/1/a',
            's3://bucketForGlobTest3/1/a',
            's3://bucketForGlobTest3/1/a/b',
            's3://bucketForGlobTest3/1/a/b/1.json',
            's3://bucketForGlobTest3/1/a/b/c',
            's3://bucketForGlobTest3/1/a/b/c/1.json',
            's3://bucketForGlobTest3/1/a/b/c/A.msg',
        ])


def _s3_glob_with_same_file_and_folder_cross_bucket():
    '''
    scenario: existing same-named file and directory in a  directory
    expectation: the file and directory is returned 1 time respectively
    '''
    # same name and folder
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/1/*',
        [
            # 1 file name 'a' and 1 actual folder
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest/1/a',
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest2/1/a',
            's3://bucketForGlobTest3/1/a',
            's3://bucketForGlobTest3/1/a',
        ])


def _s3_glob_with_nested_pathname_cross_bucket():
    '''
    scenario: pathname including nested '**'
    expectation: work correctly as standard glob module
    '''
    # nested
    # non-recursive, actually: s3://bucketForGlobTest/*/a/*/*.jso?
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/**/a/**/*.jso?',
        [
            's3://bucketForGlobTest/2/a/d/2.json',
            's3://bucketForGlobTest/1/a/b/1.json',
            's3://bucketForGlobTest2/1/a/b/1.json',
            's3://bucketForGlobTest2/1/a/c/1.json',
            's3://bucketForGlobTest3/1/a/b/1.json',
        ],
        recursive=False)

    # recursive
    # s3://bucketForGlobTest/2/a/b/a/1.json is returned 2 times
    # without set, otherwise, 's3://bucketForGlobTest/2/a/b/a/1.json' would be duplicated
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/**/a/**/*.jso?',
        [
            's3://bucketForGlobTest/1/a/b/1.json',
            's3://bucketForGlobTest/1/a/b/c/1.json',
            's3://bucketForGlobTest/2/a/b/a/1.json',
            's3://bucketForGlobTest/2/a/b/c/1.json',
            's3://bucketForGlobTest/2/a/b/c/2.json',
            's3://bucketForGlobTest/2/a/d/2.json',
            's3://bucketForGlobTest/2/a/d/c/1.json',
            's3://bucketForGlobTest2/1/a/1.json',
            's3://bucketForGlobTest2/1/a/b/1.json',
            's3://bucketForGlobTest2/1/a/c/1.json',
            's3://bucketForGlobTest2/1/a/b/c/1.json',
            's3://bucketForGlobTest2/1/a/b/c/2.json',
            's3://bucketForGlobTest3/1/a/b/1.json',
            's3://bucketForGlobTest3/1/a/b/c/1.json',
        ])


def _s3_glob_with_not_exists_dir_cross_bucket():
    '''
    scenario: glob on a directory that is not exists
    expectation: if recursive is True, return the directory with postfix of slash('/'), otherwise, an empty list.
    keep identical result with standard glob module
    '''

    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/notExistFolder/notExistFile',
        [])
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/notExistFolder',
        [])

    # not exists path
    assert_glob(r's3://{notExistBucket,notExistBucket2}/**', [])

    assert_glob(r's3://{bucketA,falseBucket}/notExistFolder/**', [])

    assert_glob(r's3://{notExistBucket,notExistBucket2}/*', [])

    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/notExistFolder/**', [])


def _s3_glob_with_dironly_cross_bucket():
    '''
    scenario: pathname with the postfix of slash('/')
    expectation: returns only contains pathname of directory, each of them is end with '/'
    '''
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/*/', [
            's3://bucketForGlobTest/1/',
            's3://bucketForGlobTest/2/',
            's3://bucketForGlobTest2/1/',
        ])

    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/[2-9]/',
        [
            's3://bucketForGlobTest/2/',
        ])

    # all sub-directories of 2, recursively
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/2/**/*/',
        [
            's3://bucketForGlobTest/2/a/',
            's3://bucketForGlobTest/2/a/b/',
            's3://bucketForGlobTest/2/a/b/a/',
            's3://bucketForGlobTest/2/a/b/c/',
            's3://bucketForGlobTest/2/a/d/',
            's3://bucketForGlobTest/2/a/d/c/',
        ])

    # all sub-directories of 1, recursively
    assert_glob(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/1/**/*/',
        [
            's3://bucketForGlobTest/1/a/',
            's3://bucketForGlobTest/1/a/b/',
            's3://bucketForGlobTest/1/a/b/c/',
            's3://bucketForGlobTest2/1/a/',
            's3://bucketForGlobTest2/1/a/b/',
            's3://bucketForGlobTest2/1/a/b/c/',
            's3://bucketForGlobTest2/1/a/c/',
            's3://bucketForGlobTest3/1/a/',
            's3://bucketForGlobTest3/1/a/b/',
            's3://bucketForGlobTest3/1/a/b/c/',
        ])


def test_s3_glob(truncating_client):
    original_calls = (os.path.lexists, os.path.isdir, os.scandir)
    _s3_glob_with_bucket_match()
    _s3_glob_with_common_wildcard()
    _s3_glob_with_recursive_pathname()
    _s3_glob_with_same_file_and_folder()
    _s3_glob_with_nested_pathname()
    _s3_glob_with_not_exists_dir()
    _s3_glob_with_dironly()
    assert original_calls == (os.path.lexists, os.path.isdir, os.scandir)


def test_s3_glob_cross_bucket(truncating_client):
    original_calls = (os.path.lexists, os.path.isdir, os.scandir)
    _s3_glob_with_common_wildcard_cross_bucket()
    _s3_glob_with_recursive_pathname_cross_bucket()
    _s3_glob_with_same_file_and_folder_cross_bucket()
    _s3_glob_with_nested_pathname_cross_bucket()
    _s3_glob_with_not_exists_dir_cross_bucket()
    _s3_glob_with_dironly_cross_bucket()

    assert original_calls == (os.path.lexists, os.path.isdir, os.scandir)


def test_s3_iglob(truncating_client):
    with pytest.raises(UnsupportedError) as error:
        s3.s3_iglob('s3://')


def test_group_s3path_by_bucket(truncating_client):
    assert sorted(
        s3._group_s3path_by_bucket(
            r"s3://*ForGlobTest{1,2/a,2/1/a}/1.jso?,")) == [
                's3://bucketForGlobTest2/{1/a,a}/1.jso?,'
            ]

    assert sorted(
        s3._group_s3path_by_bucket(
            r's3://{emptybucket,bucket}ForGlob{Test,Test2,Test3}/c/a/a')) == [
                's3://bucketForGlobTest/c/a/a',
                's3://bucketForGlobTest2/c/a/a',
                's3://bucketForGlobTest3/c/a/a',
                's3://emptybucketForGlobTest/c/a/a',
                's3://emptybucketForGlobTest2/c/a/a',
                's3://emptybucketForGlobTest3/c/a/a',
            ]


def test_s3_glob_stat(truncating_client, mocker):
    mocker.patch('megfile.s3.StatResult', side_effect=FakeStatResult)

    original_calls = (os.path.lexists, os.path.isdir, os.scandir)
    # without any wildcards
    assert_glob_stat(
        's3://emptyBucketForGlobTest', [
            ('s3://emptyBucketForGlobTest', make_stat(isdir=True)),
        ])
    assert_glob_stat(
        's3://emptyBucketForGlobTest/', [
            ('s3://emptyBucketForGlobTest/', make_stat(isdir=True)),
        ])
    assert_glob_stat(
        's3://bucketForGlobTest/1', [
            ('s3://bucketForGlobTest/1', make_stat(isdir=True)),
        ])
    assert_glob_stat(
        's3://bucketForGlobTest/1/', [
            ('s3://bucketForGlobTest/1/', make_stat(isdir=True)),
        ])
    assert_glob_stat(
        's3://bucketForGlobTest/1/a',
        [
            ('s3://bucketForGlobTest/1/a', make_stat(isdir=True)),
            ('s3://bucketForGlobTest/1/a', make_stat(size=27)),  # 同名文件
        ])
    assert_glob_stat(
        's3://bucketForGlobTest/2/a/d/2.json', [
            ('s3://bucketForGlobTest/2/a/d/2.json', make_stat(size=6)),
        ])

    # '*', all files and folders
    assert_glob_stat('s3://emptyBucketForGlobTest/*', [])
    assert_glob_stat(
        's3://bucketForGlobTest/*', [
            ('s3://bucketForGlobTest/1', make_stat(isdir=True)),
            ('s3://bucketForGlobTest/2', make_stat(isdir=True)),
        ])

    # all files under all direct subfolders
    assert_glob_stat(
        's3://bucketForGlobTest/*/*',
        [
            ('s3://bucketForGlobTest/1/a', make_stat(isdir=True)),
            ('s3://bucketForGlobTest/1/a', make_stat(size=27)),  # 同名文件
            ('s3://bucketForGlobTest/2/a', make_stat(isdir=True)),
        ])

    # combination of '?' and []
    assert_glob_stat('s3://bucketForGlobTest/[2-3]/**/*?msg', [])
    assert_glob_stat(
        's3://bucketForGlobTest/[13]/**/*?msg',
        [('s3://bucketForGlobTest/1/a/b/c/A.msg', make_stat(size=5))])

    assert original_calls == (os.path.lexists, os.path.isdir, os.scandir)

    with pytest.raises(UnsupportedError) as error:
        s3.s3_glob_stat('s3://')

    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob_stat(
                's3://notExistsBucketForGlobTest/*', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob_stat('s3://emptyBucketForGlobTest/*', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob_stat('s3://bucketForGlobTest/3/**', missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob_stat(
                's3://bucketForGlobTest/1/**.notExists', missing_ok=False))


def test_s3_glob_stat_cross_bucket(truncating_client, mocker):
    mocker.patch('megfile.s3.StatResult', side_effect=FakeStatResult)

    original_calls = (os.path.lexists, os.path.isdir, os.scandir)
    # without any wildcards
    assert_glob_stat(
        r's3://{emptyBucketForGlobTest,bucketForGlobTest2}', [
            ('s3://emptyBucketForGlobTest', make_stat(isdir=True)),
            ('s3://bucketForGlobTest2', make_stat(isdir=True)),
        ])
    assert_glob_stat(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/1', [
            ('s3://bucketForGlobTest/1', make_stat(isdir=True)),
            ('s3://bucketForGlobTest2/1', make_stat(isdir=True)),
            ('s3://bucketForGlobTest3/1', make_stat(isdir=True)),
        ])
    assert_glob_stat(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/1/', [
            ('s3://bucketForGlobTest/1/', make_stat(isdir=True)),
            ('s3://bucketForGlobTest2/1/', make_stat(isdir=True)),
        ])
    assert_glob_stat(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/1/a',
        [
            ('s3://bucketForGlobTest/1/a', make_stat(isdir=True)),
            ('s3://bucketForGlobTest/1/a', make_stat(size=27)),  # 同名文件
            ('s3://bucketForGlobTest2/1/a', make_stat(isdir=True)),
            ('s3://bucketForGlobTest2/1/a', make_stat(size=27)),  # 同名文件
        ])
    assert_glob_stat(
        r's3://{bucketForGlobTest,bucketForGlobTest2}/2/a/d/2.json', [
            ('s3://bucketForGlobTest/2/a/d/2.json', make_stat(size=6)),
        ])

    # '*', all files and folders
    assert_glob_stat('s3://emptyBucketForGlobTest/*', [])
    assert_glob_stat(
        r's3://{bucketForGlobTest,emptyBucketForGlobTest,bucketForGlobTest2}/*',
        [
            ('s3://bucketForGlobTest/1', make_stat(isdir=True)),
            ('s3://bucketForGlobTest/2', make_stat(isdir=True)),
            ('s3://bucketForGlobTest2/1', make_stat(isdir=True)),
        ])

    # all files under all direct subfolders
    assert_glob_stat(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/*/*',
        [
            ('s3://bucketForGlobTest/1/a', make_stat(isdir=True)),
            ('s3://bucketForGlobTest/1/a', make_stat(size=27)),  # 同名文件
            ('s3://bucketForGlobTest/2/a', make_stat(isdir=True)),
            ('s3://bucketForGlobTest2/1/a', make_stat(isdir=True)),
            ('s3://bucketForGlobTest2/1/a', make_stat(size=27)),  # 同名文件
            ('s3://bucketForGlobTest3/1/a', make_stat(isdir=True)),
            ('s3://bucketForGlobTest3/1/a', make_stat(size=27)),  # 同名文件
        ])

    # combination of '?' and []
    assert_glob_stat(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/[2-3]/**/*?msg',
        [])
    assert_glob_stat(
        r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/[13]/**/*?msg',
        [
            ('s3://bucketForGlobTest/1/a/b/c/A.msg', make_stat(size=5)),
            ('s3://bucketForGlobTest2/1/a/b/c/A.msg', make_stat(size=5)),
            ('s3://bucketForGlobTest2/1/a/c/A.msg', make_stat(size=5)),
            ('s3://bucketForGlobTest3/1/a/b/c/A.msg', make_stat(size=5)),
        ])

    assert_glob_stat(
        r's3://{notExistsBucketForGlobTest,bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/[13]/**/*?msg',
        [
            ('s3://bucketForGlobTest/1/a/b/c/A.msg', make_stat(size=5)),
            ('s3://bucketForGlobTest2/1/a/b/c/A.msg', make_stat(size=5)),
            ('s3://bucketForGlobTest2/1/a/c/A.msg', make_stat(size=5)),
            ('s3://bucketForGlobTest3/1/a/b/c/A.msg', make_stat(size=5)),
        ],
        missing_ok=False)

    assert original_calls == (os.path.lexists, os.path.isdir, os.scandir)

    with pytest.raises(UnsupportedError) as error:
        s3.s3_glob_stat('s3://')

    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob_stat(
                r's3://{notExistsBucketForGlobTest,notExistsBucketForGlobTest2}/*',
                missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob_stat(
                r's3://{emptyBucketForGlobTest,notExistsBucketForGlobTest2/2}/*',
                missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob_stat(
                r's3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/3/**',
                missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob_stat(
                r's3://{bucketForGlobTest,bucketForGlobTest2}/1/**.notExists',
                missing_ok=False))


def test_s3_split_magic():
    assert _s3_split_magic('s3://bucketA/{a/b,c}*/d') == (
        's3://bucketA', '{a/b,c}*/d')


def test_group_s3path_by_bucket(truncating_client):
    assert sorted(
        _group_s3path_by_bucket(
            's3://{bucketA,bucketB}/{a,b}/{a,b}*/k/{c.json,d.json}')) == sorted(
                [
                    's3://bucketA/{a,b}/{a,b}*/k/{c.json,d.json}',
                    's3://bucketB/{a,b}/{a,b}*/k/{c.json,d.json}'
                ])

    assert sorted(
        _group_s3path_by_bucket(
            's3://{bucketA/a,bucketB/b}/c/{a,b}*/k/{c.json,d.json}')) == sorted(
                [
                    's3://bucketA/a/c/{a,b}*/k/{c.json,d.json}',
                    's3://bucketB/b/c/{a,b}*/k/{c.json,d.json}'
                ])

    assert sorted(
        _group_s3path_by_bucket(
            's3://bucketForGlobTest*/{a,b}/{a,b}*/k/{c.json,d.json}')
    ) == sorted(
        [
            's3://bucketForGlobTest/{a,b}/{a,b}*/k/{c.json,d.json}',
            's3://bucketForGlobTest2/{a,b}/{a,b}*/k/{c.json,d.json}',
            's3://bucketForGlobTest3/{a,b}/{a,b}*/k/{c.json,d.json}',
        ])


def test_group_s3path_by_prefix():
    assert sorted(
        _group_s3path_by_prefix("s3://bucketA/{a,b}/*/k/{c,d}.json")) == sorted(
            ['s3://bucketA/a/*/k/{c,d}.json', 's3://bucketA/b/*/k/{c,d}.json'])
    assert sorted(
        _group_s3path_by_prefix(
            "s3://bucketA/{a,b/*}/k/{c.json,d.json}")) == sorted(
                [
                    's3://bucketA/a/k/c.json', 's3://bucketA/a/k/d.json',
                    's3://bucketA/b/[*]/k/c.json', 's3://bucketA/b/[*]/k/d.json'
                ])
    assert sorted(
        _group_s3path_by_prefix(
            "s3://bucketA/{a,b}*/k/{c.json,d.json}")) == sorted(
                ['s3://bucketA/{a,b}*/k/{c.json,d.json}'])
    assert sorted(
        _group_s3path_by_prefix(
            "s3://bucketA/{a,b}/{c,d}*/k/{e.json,f.json}")) == sorted(
                [
                    's3://bucketA/a/{c,d}*/k/{e.json,f.json}',
                    's3://bucketA/b/{c,d}*/k/{e.json,f.json}'
                ])
    assert sorted(
        _group_s3path_by_prefix(
            "s3://bucketA/{a,b}/k/{c.json,d.json}")) == sorted(
                [
                    's3://bucketA/a/k/c.json', 's3://bucketA/a/k/d.json',
                    's3://bucketA/b/k/c.json', 's3://bucketA/b/k/d.json'
                ])
    assert sorted(_group_s3path_by_prefix("s3://bucketA/{a,b}/k/")) == sorted(
        [
            's3://bucketA/a/k/',
            's3://bucketA/b/k/',
        ])


def test_s3_save_as(s3_empty_client):
    content = b'value'
    s3_empty_client.create_bucket(Bucket='bucket')
    s3.s3_save_as(BytesIO(content), 's3://bucket/result')
    body = s3_empty_client.get_object(
        Bucket='bucket', Key='result')['Body'].read()
    assert body == content


def test_s3_save_as_invalid(s3_empty_client):
    content = b'value'
    s3_empty_client.create_bucket(Bucket='bucket')

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_save_as(BytesIO(content), 's3://bucket/prefix/')
    assert 's3://bucket/prefix/' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_save_as(BytesIO(content), 's3:///key')
    assert 's3:///key' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_save_as(BytesIO(content), 's3://notExistBucket/fileAA')
    assert 's3://notExistBucket' in str(error.value)


def test_s3_load_from(s3_empty_client):
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key', Body=b'value')
    content = s3.s3_load_from('s3://bucket/key')
    assert content.read() == b'value'


def test_s3_load_from_invalid(s3_empty_client):
    s3_empty_client.create_bucket(Bucket='bucket')

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_load_from('s3://bucket/prefix/')
    assert 's3://bucket/prefix/' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_load_from('s3:///key')
    assert 's3:///key' in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_load_from('s3://notExistBucket/fileAA')
    assert 's3://notExistBucket' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_load_from('s3://bucket/notExistFile')
    assert 's3://bucket/notExistFile' in str(error.value)


def test_s3_prefetch_open(s3_empty_client):
    content = b'test data for s3_prefetch_open'
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key', Body=content)

    with s3.s3_prefetch_open('s3://bucket/key') as reader:
        assert reader.name == 's3://bucket/key'
        assert reader.mode == 'rb'
        assert reader.read() == content

    with s3.s3_prefetch_open('s3://bucket/key', max_concurrency=1,
                             max_block_size=1) as reader:
        assert reader.read() == content


def test_s3_share_cache_open(s3_empty_client):
    content = b'test data for s3_share_cache_open'
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key', Body=content)

    with s3.s3_share_cache_open('s3://bucket/key') as reader:
        assert reader.name == 's3://bucket/key'
        assert reader.mode == 'rb'
        assert reader.read() == content

    with s3.s3_prefetch_open('s3://bucket/key', max_concurrency=1,
                             max_block_size=1) as reader:
        assert reader.read() == content


def test_s3_prefetch_open_raises_exceptions(s3_empty_client):
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key')

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_prefetch_open('s3://bucket')
    assert 's3://bucket' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_prefetch_open('s3://bucket/')
    assert 's3://bucket/' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_prefetch_open('s3://bucket/keyy')
    assert 's3://bucket/keyy' in str(error.value)


def test_s3_pipe_open(s3_empty_client):
    content = b'test data for s3_pipe_open'
    s3_empty_client.create_bucket(Bucket='bucket')

    with s3.s3_pipe_open('s3://bucket/key', 'wb') as writer:
        assert writer.name == 's3://bucket/key'
        assert writer.mode == 'wb'
        writer.write(content)
    body = s3_empty_client.get_object(Bucket='bucket', Key='key')['Body'].read()
    assert body == content

    with s3.s3_pipe_open('s3://bucket/key', 'rb') as reader:
        assert reader.name == 's3://bucket/key'
        assert reader.mode == 'rb'
        assert reader.read() == content


def test_s3_pipe_open_raises_exceptions(s3_empty_client):
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_pipe_open('s3://bucket', 'wb')
    assert 's3://bucket' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_pipe_open('s3://bucket/', 'wb')
    assert 's3://bucket/' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_pipe_open('s3://bucket/key', 'wb')
    assert 's3://bucket/key' in str(error.value)

    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key')

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_pipe_open('s3://bucket', 'rb')
    assert 's3://bucket' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_pipe_open('s3://bucket/', 'rb')
    assert 's3://bucket/' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_pipe_open('s3://bucket/keyy', 'rb')
    assert 's3://bucket/keyy' in str(error.value)


def test_s3_legacy_open(mocker, s3_empty_client):
    resource = boto3.resource('s3')
    mocker.patch('boto3.Session.resource', return_value=resource)
    mocker.patch('megfile.s3.get_endpoint_url', return_value=None)

    content = b'test data for s3_legacy_open'
    s3_empty_client.create_bucket(Bucket='bucket')

    with s3.s3_legacy_open('s3://bucket/key', 'wb') as writer:
        writer.write(content)
    body = s3_empty_client.get_object(Bucket='bucket', Key='key')['Body'].read()
    assert body == content

    with s3.s3_legacy_open('s3://bucket/key', 'rb') as reader:
        assert reader.read() == content


def test_s3_legacy_open_raises_exceptions(mocker, s3_empty_client):
    resource = boto3.resource('s3')
    mocker.patch('boto3.Session.resource', return_value=resource)
    mocker.patch('megfile.s3.get_endpoint_url', return_value=None)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_legacy_open('s3://bucket', 'wb')
    assert 's3://bucket' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_legacy_open('s3://bucket/', 'wb')
    assert 's3://bucket/' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_legacy_open('s3://bucket/key', 'wb')
    assert 's3://bucket/key' in str(error.value)

    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key')

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_legacy_open('s3://bucket', 'rb')
    assert 's3://bucket' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_legacy_open('s3://bucket/', 'rb')
    assert 's3://bucket/' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_legacy_open('s3://bucket/keyy', 'rb')
    assert 's3://bucket/keyy' in str(error.value)


def test_s3_cached_open(mocker, s3_empty_client, fs):
    content = b'test data for s3_cached_open'
    s3_empty_client.create_bucket(Bucket='bucket')
    cache_path = '/tmp/tempfile'

    with s3.s3_cached_open('s3://bucket/key', 'wb',
                           cache_path=cache_path) as writer:
        assert writer.name == 's3://bucket/key'
        assert writer.mode == 'wb'
        writer.write(content)
    body = s3_empty_client.get_object(Bucket='bucket', Key='key')['Body'].read()
    assert body == content

    with s3.s3_cached_open('s3://bucket/key', 'rb',
                           cache_path=cache_path) as reader:
        assert reader.name == 's3://bucket/key'
        assert reader.mode == 'rb'
        assert reader.read() == content


def test_s3_cached_open_raises_exceptions(mocker, s3_empty_client, fs):
    cache_path = '/tmp/tempfile'

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_cached_open('s3://bucket', 'wb', cache_path=cache_path)
    assert 's3://bucket' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_cached_open('s3://bucket/', 'wb', cache_path=cache_path)
    assert 's3://bucket/' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_cached_open('s3://bucket/key', 'wb', cache_path=cache_path)
    assert 's3://bucket/key' in str(error.value)

    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key')

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_cached_open('s3://bucket', 'rb', cache_path=cache_path)
    assert 's3://bucket' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_cached_open('s3://bucket/', 'rb', cache_path=cache_path)
    assert 's3://bucket/' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_cached_open('s3://bucket/keyy', 'rb', cache_path=cache_path)
    assert 's3://bucket/keyy' in str(error.value)


def test_s3_buffered_open(mocker, s3_empty_client, fs):
    content = b'test data for s3_buffered_open'
    s3_empty_client.create_bucket(Bucket='bucket')

    writer = s3.s3_buffered_open('s3://bucket/key', 'wb')
    assert isinstance(writer.raw, s3.S3BufferedWriter)

    writer = s3.s3_buffered_open('s3://bucket/key', 'wb', limited_seekable=True)
    assert isinstance(writer.raw, s3.S3LimitedSeekableWriter)

    reader = s3.s3_buffered_open('s3://bucket/key', 'rb')
    assert isinstance(reader.raw, s3.S3PrefetchReader)

    reader = s3.s3_buffered_open(
        's3://bucket/key', 'rb', share_cache_key='share')
    assert isinstance(reader.raw, s3.S3ShareCacheReader)

    with s3.s3_buffered_open('s3://bucket/key', 'wb') as writer:
        assert writer.name == 's3://bucket/key'
        assert writer.mode == 'wb'
        writer.write(content)
    body = s3_empty_client.get_object(Bucket='bucket', Key='key')['Body'].read()
    assert body == content

    with s3.s3_buffered_open('s3://bucket/key', 'rb') as reader:
        assert reader.name == 's3://bucket/key'
        assert reader.mode == 'rb'
        assert reader.read() == content


def test_s3_buffered_open_raises_exceptions(mocker, s3_empty_client, fs):
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_buffered_open('s3://bucket', 'wb')
    assert 's3://bucket' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_buffered_open('s3://bucket/', 'wb')
    assert 's3://bucket/' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_buffered_open('s3://bucket/key', 'wb')
    assert 's3://bucket/key' in str(error.value)

    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key')

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_buffered_open('s3://bucket', 'rb')
    assert 's3://bucket' in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_buffered_open('s3://bucket/', 'rb')
    assert 's3://bucket/' in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_buffered_open('s3://bucket/keyy', 'rb')
    assert 's3://bucket/keyy' in str(error.value)


def test_s3_memory_open(s3_empty_client):
    content = b'test data for s3_memory_open'
    s3_empty_client.create_bucket(Bucket='bucket')

    with s3.s3_memory_open('s3://bucket/key', 'wb') as writer:
        writer.write(content)
    body = s3_empty_client.get_object(Bucket='bucket', Key='key')['Body'].read()
    assert body == content

    with s3.s3_memory_open('s3://bucket/key', 'rb') as reader:
        assert reader.read() == content


def test_s3_open(s3_empty_client):
    content = b'test data for s3_open'
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key', Body=content)

    writer = s3.s3_open('s3://bucket/key', 'wb')
    assert isinstance(writer.raw, s3.S3BufferedWriter)

    reader = s3.s3_open('s3://bucket/key', 'rb')
    assert isinstance(reader.raw, s3.S3PrefetchReader)

    writer = s3.s3_open('s3://bucket/key', 'ab')
    assert isinstance(writer, s3.S3MemoryHandler)

    writer = s3.s3_open('s3://bucket/key', 'wb+')
    assert isinstance(writer, s3.S3MemoryHandler)


def test_s3_getmd5(s3_empty_client):
    s3_url = 's3://bucket/key'
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(
        Bucket='bucket', Key='key', Metadata={content_md5_header: 'md5'})

    assert s3.s3_getmd5(s3_url) == 'md5'


def test_s3_getmd5_None(s3_empty_client):
    s3_url = 's3://bucket/key'
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key')

    assert s3.s3_getmd5(s3_url) is None


def test_s3_load_content(s3_empty_client):
    content = b'test data for s3_load_content'
    s3_empty_client.create_bucket(Bucket='bucket')

    with s3.s3_memory_open('s3://bucket/key', 'wb') as writer:
        writer.write(content)

    assert s3.s3_load_content('s3://bucket/key') == content
    assert s3.s3_load_content('s3://bucket/key', 1) == content[1:]
    assert s3.s3_load_content('s3://bucket/key', stop=-1) == content[:-1]
    assert s3.s3_load_content('s3://bucket/key', 4, 7) == content[4:7]

    with pytest.raises(ValueError) as error:
        s3.s3_load_content('s3://bucket/key', 5, 2)


def test_s3_load_content_retry(s3_empty_client, mocker):
    content = b'test data for s3_load_content'
    s3_empty_client.create_bucket(Bucket='bucket')

    with s3.s3_memory_open('s3://bucket/key', 'wb') as writer:
        writer.write(content)

    read_error = botocore.exceptions.IncompleteReadError(
        actual_bytes=0, expected_bytes=1)
    mocker.patch.object(s3_empty_client, 'get_object', side_effect=read_error)
    sleep = mocker.patch.object(time, 'sleep')
    with pytest.raises(Exception) as error:
        s3.s3_load_content('s3://bucket/key')
    assert error.value.__str__() == translate_s3_error(
        read_error, 's3://bucket/key').__str__()
    assert sleep.call_count == s3.max_retries - 1


def test_s3_cacher(fs, s3_empty_client):
    content = b'test data for s3_load_content'
    s3_empty_client.create_bucket(Bucket='bucket')
    s3_empty_client.put_object(Bucket='bucket', Key='key', Body=content)

    with s3.S3Cacher('s3://bucket/key', '/path/to/file') as path:
        assert path == '/path/to/file'
        assert os.path.exists(path)
        with open(path, 'rb') as fp:
            assert fp.read() == content

    assert not os.path.exists(path)

    with s3.S3Cacher('s3://bucket/key', '/path/to/file', 'w') as path:
        assert path == '/path/to/file'
        assert not os.path.exists(path)
        with open(path, 'wb') as fp:
            assert fp.write(content)

    assert not os.path.exists(path)
    assert s3.s3_load_content('s3://bucket/key') == content

    with s3.S3Cacher('s3://bucket/key', '/path/to/file', 'a') as path:
        assert path == '/path/to/file'
        assert os.path.exists(path)
        with open(path, 'rb+') as fp:
            assert fp.read() == content
            assert fp.write(content)

    assert not os.path.exists(path)
    assert s3.s3_load_content('s3://bucket/key') == content * 2

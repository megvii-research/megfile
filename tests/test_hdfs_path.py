import configparser
import os

import pytest
import requests_mock
from mock import patch

from megfile.hdfs_path import HdfsPath, get_hdfs_client, get_hdfs_config
from megfile.lib.hdfs_tools import hdfs_api


@pytest.fixture
def config_mocker(mocker):
    mocker.patch(
        'megfile.hdfs_path.get_hdfs_config',
        return_value={
            'user': 'user',
            'url': 'http://127.0.0.1:8000',
            'root': '/',
            'timeout': 10,
            'token': 'token',
        })
    yield


@patch.dict(
    os.environ, {
        "HDFS_USER": "user",
        "HDFS_URL": "http://127.0.0.1:8000",
        "HDFS_ROOT": "/",
        "HDFS_TIMEOUT": "10",
        "HDFS_TOKEN": "token",
    })
def test_get_hdfs_config():
    assert get_hdfs_config() == {
        'user': 'user',
        'url': 'http://127.0.0.1:8000',
        'root': '/',
        'timeout': 10,
        'token': 'token',
    }


@patch.dict(os.environ, {
    "HDFS_CONFIG_PATH": ".config.ini",
})
def test_get_hdfs_config_from_file(fs):
    config = configparser.ConfigParser()
    config['global'] = {"default.alias": "default"}
    config['default.alias'] = {
        "user": "user",
        "url": "http://127.0.0.1:8000",
        "root": "/",
        "timeout": "10",
        "token": "token",
    }
    with open('.config.ini', 'w') as f:
        config.write(f)
    assert get_hdfs_config() == {
        'user': 'user',
        'url': 'http://127.0.0.1:8000',
        'root': '/',
        'timeout': 10,
        'token': 'token',
    }


def test_get_hdfs_config_error(fs):
    with pytest.raises(hdfs_api.HdfsError):
        get_hdfs_config()


@patch.dict(
    os.environ, {
        "HDFS_USER": "user",
        "HDFS_URL": "http://127.0.0.1:8000",
        "HDFS_ROOT": "/",
        "HDFS_TIMEOUT": "10",
        "HDFS_TOKEN": "token",
    })
def test_get_hdfs_client():
    assert isinstance(get_hdfs_client(), hdfs_api.TokenClient)


@patch.dict(
    os.environ, {
        "CLIENT__HDFS_USER": "user",
        "CLIENT__HDFS_URL": "http://127.0.0.1:8000",
        "CLIENT__HDFS_ROOT": "/",
        "CLIENT__HDFS_TIMEOUT": "10",
    })
def test_get_hdfs_client():
    assert isinstance(get_hdfs_client('client'), hdfs_api.InsecureClient)


def test_iterdir(requests_mock, config_mocker):
    requests_mock.get(
        'http://127.0.0.1:8000/webhdfs/v1/A?op=LISTSTATUS',
        json={
            "FileStatuses": {
                "FileStatus": [
                    {
                        "accessTime": 1320171722771,
                        "blockSize": 33554432,
                        "group": "supergroup",
                        "length": 24930,
                        "modificationTime": 1320171722771,
                        "owner": "webuser",
                        "pathSuffix": "a.patch",
                        "permission": "644",
                        "replication": 1,
                        "type": "FILE"
                    },
                    {
                        "accessTime": 0,
                        "blockSize": 0,
                        "group": "supergroup",
                        "length": 0,
                        "modificationTime": 1320895981256,
                        "owner": "szetszwo",
                        "pathSuffix": "bar",
                        "permission": "711",
                        "replication": 0,
                        "type": "DIRECTORY"
                    },
                ]
            }
        })

    assert list(HdfsPath('hdfs://A').iterdir()) == [
        HdfsPath('hdfs://A/a.patch'),
        HdfsPath('hdfs://A/bar'),
    ]

    # TODO: test not a dir
    # requests_mock.get(
    #     'http://127.0.0.1:8000/webhdfs/v1/A/1.json?op=LISTSTATUS',)

    # with pytest.raises(NotADirectoryError):
    #     list(HdfsPath('hdfs://A/1.json').iterdir())


def test_parts():
    assert HdfsPath('hdfs+a://A/B/C').parts == (
        'hdfs+a://',
        'A',
        'B',
        'C',
    )
    assert HdfsPath('hdfs://A/B/C').parts == (
        'hdfs://',
        'A',
        'B',
        'C',
    )
    assert HdfsPath('hdfs://').parts == ('hdfs://',)


def test_part_with_protocol(config_mocker):
    assert HdfsPath('hdfs://dir/test').path_with_protocol == 'hdfs://dir/test'
    assert HdfsPath('dir/test').path_with_protocol == 'hdfs://dir/test'


def test_part_without_protocol(config_mocker):
    assert HdfsPath('hdfs:///dir/test').path_without_protocol == '/dir/test'
    assert HdfsPath('/dir/test').path_without_protocol == '/dir/test'


@patch.dict(
    os.environ, {
        "TEST__HDFS_USER": "user",
        "TEST__HDFS_URL": "http://127.0.0.1:8000",
        "TEST__HDFS_ROOT": "/root",
        "TEST__HDFS_TIMEOUT": "10",
        "TEST__HDFS_TOKEN": "token",
    })
def test_absolute():
    assert get_hdfs_config('test')['root'] == '/root'
    assert HdfsPath('hdfs+test://test').absolute(
    ).path_with_protocol == 'hdfs+test:///root/test'

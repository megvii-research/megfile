import configparser
import os

import pytest
from mock import patch

from megfile.hdfs_path import HdfsPath, get_hdfs_client, get_hdfs_config
from megfile.lib.hdfs_tools import hdfs_api

from .test_hdfs import config_mocker, http_mocker  # noqa: F401


@patch.dict(
    os.environ,
    {
        "HDFS_USER": "user",
        "HDFS_URL": "http://127.0.0.1:8000",
        "HDFS_ROOT": "/",
        "HDFS_TIMEOUT": "10",
        "HDFS_TOKEN": "token",
    },
)
def test_get_hdfs_config():
    assert get_hdfs_config() == {
        "user": "user",
        "url": "http://127.0.0.1:8000",
        "root": "/",
        "timeout": 10,
        "token": "token",
    }


@patch.dict(os.environ, {"HDFS_CONFIG_PATH": ".config.ini"})
def test_get_hdfs_config_from_file(fs):
    config = configparser.ConfigParser()
    config["global"] = {"default.alias": "default"}
    config["default.alias"] = {
        "user": "user",
        "url": "http://127.0.0.1:8000",
        "root": "/",
        "timeout": "10",
        "token": "token",
    }
    with open(".config.ini", "w") as f:
        config.write(f)
    assert get_hdfs_config() == {
        "user": "user",
        "url": "http://127.0.0.1:8000",
        "root": "/",
        "timeout": 10,
        "token": "token",
    }


def test_get_hdfs_config_error(fs):
    with pytest.raises(hdfs_api.HdfsError):
        get_hdfs_config()


@patch.dict(
    os.environ,
    {
        "HDFS_USER": "user",
        "HDFS_URL": "http://127.0.0.1:8000",
        "HDFS_ROOT": "/",
        "HDFS_TIMEOUT": "10",
        "HDFS_TOKEN": "token",
    },
)
def test_get_hdfs_client():
    assert isinstance(get_hdfs_client(), hdfs_api.TokenClient)


@patch.dict(
    os.environ,
    {
        "CLIENT__HDFS_USER": "user",
        "CLIENT__HDFS_URL": "http://127.0.0.1:8000",
        "CLIENT__HDFS_ROOT": "/",
        "CLIENT__HDFS_TIMEOUT": "10",
    },
)
def test_get_hdfs_client():
    assert isinstance(get_hdfs_client("client"), hdfs_api.InsecureClient)


def test_iterdir(http_mocker):
    assert [path.path_with_protocol for path in HdfsPath("hdfs://root").iterdir()] == [
        "hdfs://root/1.txt",
        "hdfs://root/a",
        "hdfs://root/b",
    ]

    with pytest.raises(NotADirectoryError):
        list(HdfsPath("hdfs://root/1.txt").iterdir())


def test_parts():
    assert HdfsPath("hdfs+a://A/B/C").parts == ("hdfs+a://", "A", "B", "C")
    assert HdfsPath("hdfs://A/B/C").parts == ("hdfs://", "A", "B", "C")
    assert HdfsPath("hdfs://").parts == ("hdfs://",)


def test_part_with_protocol(config_mocker):
    assert HdfsPath("hdfs://dir/test").path_with_protocol == "hdfs://dir/test"
    assert HdfsPath("dir/test").path_with_protocol == "hdfs://dir/test"


def test_part_without_protocol(config_mocker):
    assert HdfsPath("hdfs:///dir/test").path_without_protocol == "/dir/test"
    assert HdfsPath("/dir/test").path_without_protocol == "/dir/test"


@patch.dict(
    os.environ,
    {
        "TEST__HDFS_USER": "user",
        "TEST__HDFS_URL": "http://127.0.0.1:8000",
        "TEST__HDFS_ROOT": "/root",
        "TEST__HDFS_TIMEOUT": "10",
        "TEST__HDFS_TOKEN": "token",
    },
)
def test_absolute():
    assert get_hdfs_config("test")["root"] == "/root"
    assert (
        HdfsPath("hdfs+test://test").absolute().path_with_protocol
        == "hdfs+test:///root/test"
    )


def test_hdfs_glob(http_mocker):
    assert [
        path.path_with_protocol
        for path in HdfsPath("hdfs://root").glob(pattern="**/*.txt")
    ] == ["hdfs://root/1.txt", "hdfs://root/a/2.txt", "hdfs://root/b/3.txt"]
    assert [
        path.path_with_protocol
        for path in HdfsPath("hdfs://root").glob(pattern="**/*.json")
    ] == ["hdfs://root/b/4.json"]
    assert [
        path.path_with_protocol for path in HdfsPath("hdfs://root").glob(pattern="*/")
    ] == ["hdfs://root/a/", "hdfs://root/b/"]

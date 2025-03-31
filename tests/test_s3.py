import hashlib
import logging
import os
import pickle
import sys
import threading
import time
from collections import namedtuple
from enum import Enum
from functools import partial
from io import BufferedReader, BufferedWriter, BytesIO
from pathlib import Path
from typing import Iterable, List, Tuple
from unittest.mock import patch

import boto3
import botocore
import pytest
from mock import patch
from moto import mock_aws

from megfile import s3, s3_path, smart
from megfile.config import (
    GLOBAL_MAX_WORKERS,
)
from megfile.errors import (
    S3BucketNotFoundError,
    S3FileNotFoundError,
    S3IsADirectoryError,
    S3NameTooLongError,
    S3NotALinkError,
    S3PermissionError,
    S3UnknownError,
    SameFileError,
    UnknownError,
    UnsupportedError,
    translate_s3_error,
)
from megfile.interfaces import Access, FileEntry, StatResult
from megfile.s3_path import (
    S3CachedHandler,
    S3MemoryHandler,
    _group_s3path_by_bucket,
    _group_s3path_by_prefix,
    _list_objects_recursive,
    _parse_s3_url_ignore_brace,
    _patch_make_request,
    _s3_split_magic,
    _s3_split_magic_ignore_brace,
)
from megfile.utils import process_local, thread_local

from . import Any, FakeStatResult, Now

File = namedtuple("File", ["bucket", "key", "body"])
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

bucketForGlobTest/ （用于 s3_glob 的测试, 结构较复杂）
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
    File("bucketA", "folderAA/folderAAA/fileAAAA", "fileAAAA"),
    File("bucketA", "folderAB-C/fileAB-C", "fileAB-C"),
    File("bucketA", "folderAB/fileAB", "fileAB"),
    File("bucketA", "folderAB/fileAC", "fileAC"),
    File("bucketA", "fileAA", "fileAA"),
    File("bucketA", "fileAB", "fileAB"),
    File("bucketB", None, None),  # 空 bucket
    File("bucketC", "folder/file", "file"),
    File("bucketC", "folder", "file"),  # 与同级 folder 同名的 file
    File("bucketC", "folderAA/fileAA", "fileAA"),
    File("bucketForGlobTest", "1/a/b/c/1.json", "1.json"),
    File("bucketForGlobTest", "1/a/b/1.json", "1.json"),  # for glob(*/a/*/*.json)
    File("bucketForGlobTest", "1/a/b/c/A.msg", "A.msg"),
    File("bucketForGlobTest", "1/a", "file, same name with folder"),
    File("bucketForGlobTest", "2/a/d/c/1.json", "1.json"),
    File("bucketForGlobTest", "2/a/d/2.json", "2.json"),
    File("bucketForGlobTest", "2/a/b/c/1.json", "1.json"),
    File("bucketForGlobTest", "2/a/b/c/2.json", "2.json"),
    File("bucketForGlobTest", "2/a/b/a/1.json", "1.json"),
    File("emptyBucketForGlobTest", None, None),
    File("bucketForGlobTest2", "1/a/b/c/1.json", "1.json"),
    File("bucketForGlobTest2", "1/a/b/c/2.json", "2.json"),
    File("bucketForGlobTest2", "1/a/b/1.json", "1.json"),  # for glob(*/a/*/*.json)
    File("bucketForGlobTest2", "1/a/b/c/A.msg", "A.msg"),
    File("bucketForGlobTest2", "1/a", "file, same name with folder"),
    File("bucketForGlobTest2", "1/a/c/1.json", "1.json"),
    File("bucketForGlobTest2", "1/a/1.json", "1.json"),  # for glob(*/a/*/*.json)
    File("bucketForGlobTest2", "1/a/c/A.msg", "A.msg"),
    File("bucketForGlobTest3", "1/a/b/c/1.json", "1.json"),
    File("bucketForGlobTest3", "1/a/b/1.json", "1.json"),  # for glob(*/a/*/*.json)
    File("bucketForGlobTest3", "1/a/b/c/A.msg", "A.msg"),
    File("bucketForGlobTest3", "1/a", "file, same name with folder"),
]


@pytest.fixture
def s3_empty_client(mocker):
    with mock_aws():
        client = boto3.client("s3")
        mocker.patch("megfile.s3_path.get_s3_client", return_value=client)
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
            s3_client.put_object(Bucket=file.bucket, Key=file.key, Body=file.body)
    return s3_client


@pytest.fixture
def truncating_client(mocker, s3_setup):
    """将 list_objects_v2 的 MaxKeys 限定为 1, 在结果超过 1 个 key 时总是截断"""
    truncating_client = mocker.patch.object(
        s3_setup,
        "list_objects_v2",
        side_effect=partial(s3_setup.list_objects_v2, MaxKeys=1),
    )
    return truncating_client


def make_stat(size=0, mtime=None, isdir=False, islnk=False):
    if mtime is None:
        mtime = 0.0 if isdir else Now()
    return StatResult(size=size, mtime=mtime, isdir=isdir, islnk=islnk)


class FakeOperateMode:
    def __init__(self, name) -> None:
        self.name = name
        pass


@pytest.fixture
def s3_empty_client_with_patch_make_request(mocker):
    def patch_make_request(
        operation_model, request_dict, request_context, *args, **kwargs
    ):
        if operation_model.name == "test_error":
            raise S3UnknownError(error=Exception(), path="test")
        return request_context

    with mock_aws():
        client = boto3.client("s3")
        client._make_request = patch_make_request
        _patch_make_request(client)
        mocker.patch("megfile.s3_path.get_s3_client", return_value=client)
        yield client


def test_parse_s3_url_ignore_brace():
    with pytest.raises(ValueError):
        _parse_s3_url_ignore_brace("/test")


def test_patch_make_request(s3_empty_client_with_patch_make_request, mocker):
    mocker.patch("megfile.s3_path.max_retries", 1)
    body = BytesIO(b"test")
    body.seek(4)
    assert body.tell() == 4
    with pytest.raises(S3UnknownError):
        s3_empty_client_with_patch_make_request._make_request(
            FakeOperateMode(name="test_error"), dict(body=body), "result"
        )
        assert body.tell() == 1

    s3_empty_client_with_patch_make_request._make_request(
        FakeOperateMode(name="test_result"), dict(body=body), "test_result"
    ) == "test_result"

    from botocore.awsrequest import AWSResponse

    test_result_tuple = (
        AWSResponse(url="http://test", status_code=200, headers={}, raw=b""),
        {},
    )
    s3_empty_client_with_patch_make_request._make_request(
        FakeOperateMode(name="test_result"), dict(body=body), test_result_tuple
    ) == test_result_tuple

    test_error_result_tuple = (
        AWSResponse(url="http://test", status_code=500, headers={}, raw=b""),
        {"Error": {"Code": 500}},
    )
    with pytest.raises(botocore.exceptions.ClientError):
        s3_empty_client_with_patch_make_request._make_request(
            FakeOperateMode(name="test_result"),
            dict(body=body),
            test_error_result_tuple,
        )


def test_retry(s3_empty_client, mocker):
    read_error = botocore.exceptions.IncompleteReadError(
        actual_bytes=0, expected_bytes=1
    )
    client = s3_path.get_s3_client()
    _patch_make_request(client)
    mocker.patch.object(client._endpoint, "make_request", side_effect=read_error)
    sleep = mocker.patch.object(time, "sleep")
    with pytest.raises(UnknownError) as error:
        s3.s3_exists("s3://bucket")
    assert error.value.__cause__ is read_error
    assert sleep.call_count == s3_path.max_retries - 1


def test_get_endpoint_url():
    assert s3.get_endpoint_url(profile_name="unknown") == "https://s3.amazonaws.com"


def test_get_endpoint_url_from_env(mocker):
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})
    mocker.patch.dict(os.environ, {"OSS_ENDPOINT": "oss-endpoint"})
    assert s3.get_endpoint_url() == "oss-endpoint"


def test_get_endpoint_url_from_env2(mocker):
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})
    mocker.patch.dict(os.environ, {"AWS_ENDPOINT_URL": "oss-endpoint2"})
    assert s3.get_endpoint_url() == "oss-endpoint2"


def test_get_endpoint_url_from_env3(mocker):
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})
    mocker.patch.dict(
        os.environ,
        {"OSS_ENDPOINT": "oss-endpoint", "AWS_ENDPOINT_URL": "oss-endpoint2"},
    )
    assert s3.get_endpoint_url() == "oss-endpoint"


def test_get_endpoint_url_from_env4(mocker):
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})
    mocker.patch.dict(os.environ, {"AWS_ENDPOINT_URL_S3": "oss-endpoint3"})
    assert s3.get_endpoint_url() == "oss-endpoint3"


def test_get_endpoint_url_from_scoped_config(mocker):
    mocker.patch(
        "megfile.s3_path.get_scoped_config",
        return_value={"s3": {"endpoint_url": "test_endpoint_url"}},
    )
    assert s3.get_endpoint_url() == "test_endpoint_url"

    mocker.patch(
        "megfile.s3_path.get_scoped_config",
        return_value={"endpoint_url": "test_endpoint_url2"},
    )
    assert s3.get_endpoint_url() == "test_endpoint_url2"

    mocker.patch(
        "megfile.s3_path.get_scoped_config",
        return_value={
            "s3": {"endpoint_url": "test_endpoint_url"},
            "endpoint_url": "test_endpoint_url2",
        },
    )
    assert s3.get_endpoint_url() == "test_endpoint_url"


def test_get_s3_client(mocker):
    mock_session = mocker.Mock(spec=boto3.Session)
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})

    def fake_get_s3_session(profile_name=None):
        if profile_name == "unknown":
            raise botocore.exceptions.ProfileNotFound(profile=profile_name)
        return mock_session

    mocker.patch("megfile.s3_path.get_s3_session", side_effect=fake_get_s3_session)

    client = s3.get_s3_client()
    access_key, secret_key, session_token = s3_path.get_access_token()

    mock_session.client.assert_called_with(
        "s3",
        endpoint_url="https://s3.amazonaws.com",
        config=Any(),
        verify=True,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    # assert _send is not patched
    assert "_send" not in client._endpoint.__dict__

    client = s3.get_s3_client(cache_key="test", profile_name="unknown")
    assert "test:unknown" in thread_local

    mocker.patch("megfile.s3_path.S3_CLIENT_CACHE_MODE", "process_local")
    client = s3.get_s3_client(cache_key="test", profile_name="test")
    assert "test:test" in process_local


@patch.dict(
    os.environ,
    {
        "AWS_S3_ADDRESSING_STYLE": "virtual",
        "TEST__AWS_S3_ADDRESSING_STYLE": "auto",
        "AWS_ACCESS_KEY_ID": "test1",
        "AWS_SECRET_ACCESS_KEY": "test1",
        "AWS_S3_VERIFY": "false",
        "AWS_S3_REDIRECT": "true",
    },
)
def test_get_s3_client_v2():
    assert (
        s3.get_s3_client()._client_config._user_provided_options["s3"][
            "addressing_style"
        ]
        == "virtual"
    )
    assert (
        s3.get_s3_client(profile_name="test")._client_config._user_provided_options[
            "s3"
        ]["addressing_style"]
        == "auto"
    )

    client = s3.get_s3_client(config=botocore.config.Config(max_pool_connections=1))
    assert client._client_config._user_provided_options["max_pool_connections"] == 1
    assert (
        client._client_config._user_provided_options["s3"]["addressing_style"]
        == "virtual"
    )

    # assert _send is patched
    assert "_send" in client._endpoint.__dict__


def test_get_s3_client_from_env(mocker):
    mock_session = mocker.Mock(spec=boto3.Session)
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})
    mocker.patch("megfile.s3_path.get_s3_session", return_value=mock_session)
    mocker.patch.dict(
        os.environ,
        {"OSS_ENDPOINT": "oss-endpoint", "AWS_S3_VERIFY": "0", "AWS_S3_REDIRECT": "1"},
    )

    client = s3.get_s3_client()
    access_key, secret_key, session_token = s3_path.get_access_token()

    mock_session.client.assert_called_with(
        "s3",
        endpoint_url="oss-endpoint",
        config=Any(),
        verify=False,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    # assert _send is patched
    assert "_send" in client._endpoint.__dict__


def test_get_s3_client_with_config(mocker):
    mock_session = mocker.Mock(spec=boto3.Session)
    mocker.patch(
        "megfile.s3_path.get_scoped_config",
        return_value={"s3": {"verify": "no", "redirect": "yes"}},
    )
    mocker.patch("megfile.s3_path.get_s3_session", return_value=mock_session)

    class EQConfig(botocore.config.Config):
        def __eq__(self, other):
            return self._user_provided_options == other._user_provided_options

    config = EQConfig(max_pool_connections=GLOBAL_MAX_WORKERS, connect_timeout=1)
    client = s3.get_s3_client(config)
    access_key, secret_key, session_token = s3_path.get_access_token()

    mock_session.client.assert_called_with(
        "s3",
        endpoint_url="https://s3.amazonaws.com",
        config=config,
        verify=False,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    # assert _send is patched
    assert "_send" in client._endpoint.__dict__


def test_get_s3_session_threading(mocker):
    session_call = mocker.patch("boto3.Session")
    for i in range(2):
        thread = threading.Thread(target=s3.get_s3_session)
        thread.start()
        thread.join()

    assert session_call.call_count == 2


def test_get_s3_session_threading_reuse(mocker):
    session_call = mocker.patch("boto3.Session")

    def session_twice():
        s3.get_s3_session()
        s3.get_s3_session()

    thread = threading.Thread(target=session_twice)
    thread.start()
    thread.join()

    assert session_call.call_count == 1


def test_is_s3():
    # 不以 s3:// 开头
    assert s3.is_s3("") is False
    assert s3.is_s3("s") is False
    assert s3.is_s3("s3") is False
    assert s3.is_s3("s3:") is False
    assert s3.is_s3("s3:bucket") is False
    assert s3.is_s3("S3:bucket") is False
    assert s3.is_s3("s3:/") is False
    assert s3.is_s3("s3:/xxx") is False
    assert s3.is_s3("s3:/foo/bar") is False
    assert s3.is_s3("s3://") is True
    assert s3.is_s3("s4://") is False
    assert s3.is_s3("s3:://") is False
    assert s3.is_s3("s3:/path/to/file") is False
    assert s3.is_s3("s3:base") is False
    assert s3.is_s3("/xxx") is False
    assert s3.is_s3("/path/to/file") is False
    assert s3.is_s3("s3+://") is False
    assert s3.is_s3("s3+test://") is True
    assert s3.is_s3("s3+test:/") is False
    assert s3.is_s3("s3+test:") is False

    # 以非小写字母开头的 bucket
    assert s3.is_s3("s3://Bucket") is True
    assert s3.is_s3("s3:// ucket") is True
    assert s3.is_s3("s3://.ucket") is True
    assert s3.is_s3("s3://?ucket") is True
    assert s3.is_s3("s3://\rucket") is True
    assert s3.is_s3("s3://\ncket") is True
    assert s3.is_s3("s3://\bcket") is True
    assert s3.is_s3("s3://\tcket") is True
    assert s3.is_s3("s3://-bucket") is True

    # 以非小写字母结尾的 bucket
    assert s3.is_s3("s3://buckeT") is True
    assert s3.is_s3("s3://bucke ") is True
    assert s3.is_s3("s3://bucke.") is True
    assert s3.is_s3("s3://bucke?") is True
    assert s3.is_s3("s3://bucke\r") is True
    assert s3.is_s3("s3://bucke\n") is True
    assert s3.is_s3("s3://bucke\t") is True
    assert s3.is_s3("s3://bucke\b") is True
    assert s3.is_s3("s3://bucket0") is True

    # 中间含有非字母、数字且非 '-' 字符的 bucket
    assert s3.is_s3("s3://buc.ket") is True
    assert s3.is_s3("s3://buc?ket") is True
    assert s3.is_s3("s3://buc ket") is True
    assert s3.is_s3("s3://buc\tket") is True
    assert s3.is_s3("s3://buc\rket") is True
    assert s3.is_s3("s3://buc\bket") is True
    assert s3.is_s3("s3://buc\vket") is True
    assert s3.is_s3("s3://buc\aket") is True
    assert s3.is_s3("s3://buc\nket") is True

    # bucket 长度不位于闭区间 [3, 63]
    assert s3.is_s3("s3://bu") is True
    assert s3.is_s3("s3://%s" % ("b" * 64)) is True

    # prefix, 可以为 '', 或包含连续的 '/'
    assert s3.is_s3("s3://bucket") is True
    assert s3.is_s3("s3://bucket/") is True
    assert s3.is_s3("s3://bucket//") is True
    assert s3.is_s3("s3://bucket//prefix") is True
    assert s3.is_s3("s3://bucket/key/") is True
    assert s3.is_s3("s3://bucket/key//") is True
    assert s3.is_s3("s3://bucket/prefix/key/") is True
    assert s3.is_s3("s3://bucket/prefix//key/") is True
    assert s3.is_s3("s3://bucket//prefix//key/") is True
    assert s3.is_s3("s3://bucket//prefix//key") is True
    assert s3.is_s3("s3://bucket//////") is True

    # path 以不可见字符结尾
    assert s3.is_s3("s3://bucket/ ") is True
    assert s3.is_s3("s3://bucket/\r") is True
    assert s3.is_s3("s3://bucket/\n") is True
    assert s3.is_s3("s3://bucket/\a") is True
    assert s3.is_s3("s3://bucket/\b") is True
    assert s3.is_s3("s3://bucket/\t") is True
    assert s3.is_s3("s3://bucket/\v") is True
    assert s3.is_s3("s3://bucket/key ") is True
    assert s3.is_s3("s3://bucket/key\n") is True
    assert s3.is_s3("s3://bucket/key\r") is True
    assert s3.is_s3("s3://bucket/key\a") is True
    assert s3.is_s3("s3://bucket/key\b") is True
    assert s3.is_s3("s3://bucket/key\t") is True
    assert s3.is_s3("s3://bucket/key\v") is True

    # PathLike
    assert s3.is_s3(Path("/bucket/key")) is False


def test_parse_s3_url():
    assert s3.parse_s3_url("s3://bucket/prefix/key") == ("bucket", "prefix/key")
    assert s3.parse_s3_url("s3://bucket") == ("bucket", "")
    assert s3.parse_s3_url("s3+test://bucket") == ("bucket", "")
    assert s3.parse_s3_url("s3://") == ("", "")
    assert s3.parse_s3_url("s3:///") == ("", "")
    assert s3.parse_s3_url("s3:////") == ("", "/")
    assert s3.parse_s3_url("s3:///prefix/") == ("", "prefix/")
    assert s3.parse_s3_url("s3:///prefix/key") == ("", "prefix/key")
    assert s3.parse_s3_url("s3+test:///prefix/key") == ("", "prefix/key")

    assert s3.parse_s3_url("s3://bucket/prefix?") == ("bucket", "prefix?")
    assert s3.parse_s3_url("s3://bucket/prefix#") == ("bucket", "prefix#")
    assert s3.parse_s3_url("s3://bucket/?#") == ("bucket", "?#")
    assert s3.parse_s3_url("s3://bucket/prefix/#key") == ("bucket", "prefix/#key")
    assert s3.parse_s3_url("s3://bucket/prefix/?key") == ("bucket", "prefix/?key")
    assert s3.parse_s3_url("s3://bucket/prefix/key?key#key") == (
        "bucket",
        "prefix/key?key#key",
    )
    assert s3.parse_s3_url("s3://bucket/prefix/key#key?key") == (
        "bucket",
        "prefix/key#key?key",
    )

    with pytest.raises(ValueError):
        s3.parse_s3_url("/test")

    with pytest.raises(ValueError):
        s3.parse_s3_url("s3test://test")


def test_s3_scandir_internal(truncating_client, mocker):
    mocker.patch("megfile.s3.s3_islink", return_value=False)

    # walk the dir that is not exist
    # expect: empty generator
    with pytest.raises(FileNotFoundError):
        list(s3.s3_scandir("s3://notExistBucket"))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_scandir("s3://bucketA/notExistFile"))
    assert list(s3.s3_scandir("s3://bucketB/")) == []
    with pytest.raises(FileNotFoundError):
        list(s3.s3_scandir("s3+test://notExistBucket"))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_scandir("s3+test://bucketA/notExistFile"))

    def dir_entrys_to_tuples(entries: Iterable[FileEntry]) -> List[Tuple[str, bool]]:
        return sorted([(entry.name, entry.is_dir()) for entry in entries])

    assert dir_entrys_to_tuples(s3.s3_scandir("s3://")) == [
        ("bucketA", True),
        ("bucketB", True),
        ("bucketC", True),
        ("bucketForGlobTest", True),
        ("bucketForGlobTest2", True),
        ("bucketForGlobTest3", True),
        ("emptyBucketForGlobTest", True),
    ]
    assert dir_entrys_to_tuples(s3.s3_scandir("s3://bucketB")) == []
    assert dir_entrys_to_tuples(s3.s3_scandir("s3://bucketA")) == [
        ("fileAA", False),
        ("fileAB", False),
        ("folderAA", True),
        ("folderAB", True),
        ("folderAB-C", True),
    ]
    assert dir_entrys_to_tuples(s3.s3_scandir("s3://bucketA/folderAA")) == [
        ("folderAAA", True)
    ]
    assert dir_entrys_to_tuples(s3.s3_scandir("s3://bucketA/folderAB")) == [
        ("fileAB", False),
        ("fileAC", False),
    ]
    assert dir_entrys_to_tuples(s3.s3_scandir("s3://bucketA/folderAB/")) == [
        ("fileAB", False),
        ("fileAC", False),
    ]

    with pytest.raises(NotADirectoryError):
        s3.s3_scandir("s3://bucketA/fileAA")


def test_s3_scandir(truncating_client, mocker):
    mocker.patch("megfile.s3.s3_islink", return_value=False)

    assert sorted(list(map(lambda x: x.name, s3.s3_scandir("s3://")))) == [
        "bucketA",
        "bucketB",
        "bucketC",
        "bucketForGlobTest",
        "bucketForGlobTest2",
        "bucketForGlobTest3",
        "emptyBucketForGlobTest",
    ]
    assert sorted(list(map(lambda x: x.name, s3.s3_scandir("s3://bucketB")))) == []
    assert sorted(list(map(lambda x: x.name, s3.s3_scandir("s3://bucketA")))) == [
        "fileAA",
        "fileAB",
        "folderAA",
        "folderAB",
        "folderAB-C",
    ]
    assert sorted(
        list(map(lambda x: x.name, s3.s3_scandir("s3://bucketA/folderAA")))
    ) == ["folderAAA"]
    assert sorted(
        list(map(lambda x: x.name, s3.s3_scandir("s3://bucketA/folderAB")))
    ) == ["fileAB", "fileAC"]
    assert sorted(
        list(map(lambda x: x.name, s3.s3_scandir("s3://bucketA/folderAB/")))
    ) == ["fileAB", "fileAC"]
    with s3.s3_scandir("s3://bucketA/folderAB/") as file_entries:
        assert sorted(list(map(lambda x: x.name, file_entries))) == ["fileAB", "fileAC"]

    with pytest.raises(NotADirectoryError):
        s3.s3_scandir("s3://bucketA/fileAA")

    with pytest.raises(S3BucketNotFoundError):
        s3.s3_scandir("s3:///fileAA")


def test_s3_listdir(truncating_client, mocker):
    mocker.patch("megfile.s3.s3_islink", return_value=False)
    assert s3.s3_listdir("s3://") == [
        "bucketA",
        "bucketB",
        "bucketC",
        "bucketForGlobTest",
        "bucketForGlobTest2",
        "bucketForGlobTest3",
        "emptyBucketForGlobTest",
    ]
    assert s3.s3_listdir("s3://bucketB") == []
    assert s3.s3_listdir("s3://bucketA") == [
        "fileAA",
        "fileAB",
        "folderAA",
        "folderAB",
        "folderAB-C",
    ]
    assert s3.s3_listdir("s3://bucketA/folderAA") == ["folderAAA"]
    assert s3.s3_listdir("s3://bucketA/folderAB") == ["fileAB", "fileAC"]
    assert s3.s3_listdir("s3://bucketA/folderAB/") == ["fileAB", "fileAC"]
    assert list(s3.s3_listdir("s3://bucketB/")) == []
    with pytest.raises(NotADirectoryError):
        s3.s3_listdir("s3://bucketA/fileAA")
    with pytest.raises(FileNotFoundError):
        s3.s3_listdir("s3://notExistBucket")
    with pytest.raises(FileNotFoundError):
        s3.s3_listdir("s3://bucketA/notExistFolder")


def test_s3_isfile(s3_setup):
    assert s3.s3_isfile("s3://") is False  # root
    assert s3.s3_isfile("s3://bucketB") is False  # empty bucket
    assert s3.s3_isfile("s3://bucketA/folderAA") is False
    assert s3.s3_isfile("s3://bucketA/notExistFile") is False
    assert s3.s3_isfile("s3://notExistBucket/folderAA") is False
    assert s3.s3_isfile("s3://+InvalidBucketName/folderAA") is False
    assert s3.s3_isfile("s3://bucketA/fileAA/") is False
    assert s3.s3_isfile("s3://bucketA/fileAA") is True


def test_s3_isdir(s3_setup):
    assert s3.s3_isdir("s3://") is True  # root
    assert s3.s3_isdir("s3://bucketB") is True  # empty bucket
    assert s3.s3_isdir("s3://bucketA/folderAA/") is True  # commonperfixes
    assert s3.s3_isdir("s3://bucketA/folderAA/folderAAA") is True  # context
    assert s3.s3_isdir("s3://bucketA/fileAA") is False  # file
    assert s3.s3_isdir("s3://bucketA/notExistFolder") is False
    assert s3.s3_isdir("s3://notExistBucket") is False
    assert s3.s3_isdir("s3://+InvalidBucketName") is False


def test_s3_access(s3_setup, mocker):
    assert s3.s3_access("s3://bucketA/fileAA", Access.READ) is True
    assert s3.s3_access("s3://bucketA/fileAA", Access.WRITE) is True
    assert s3.s3_access("s3://bucketA/folderAA/", Access.WRITE) is True

    with pytest.raises(TypeError):
        s3.s3_access("s3://bucketA/fileAA", "w")
    assert s3.s3_access("s3://thisdoesnotexists", Access.READ) is False
    assert s3.s3_access("s3://thisdoesnotexists", Access.WRITE) is False

    with patch.object(
        s3_setup,
        "create_multipart_upload",
        side_effect=botocore.exceptions.ClientError(
            {"Error": {"Code": "403"}}, "create_multipart_upload"
        ),
    ):
        assert s3.s3_access("s3://bucketA/", Access.READ) is True
        assert s3.s3_access("s3://bucketA/", Access.WRITE) is False

    with (
        patch.object(
            s3_setup,
            "create_multipart_upload",
            side_effect=botocore.exceptions.ClientError(
                {"Error": {"Code": "5000"}}, "test"
            ),
        ),
        pytest.raises(S3UnknownError),
    ):
        s3.s3_access("s3://bucketA/", Access.WRITE) is False

    mocker.patch("megfile.s3_path.S3Path.exists", side_effect=S3PermissionError())
    assert s3.s3_access("s3://bucketA/fileAA", Access.READ) is False
    assert s3.s3_access("s3://bucketA/fileAA", Access.WRITE) is False


def test_s3_exists(s3_setup):
    assert s3.s3_exists("s3://") is True
    assert s3.s3_exists("s3://bucketB") is True
    assert s3.s3_exists("s3://bucketA/folderAB-C") is True
    assert s3.s3_exists("s3://bucketA/folderAB") is True
    assert s3.s3_exists("s3://bucketA/folderAA") is True
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA") is True
    assert s3.s3_exists("s3://bucketA/fileAA") is True
    assert s3.s3_exists("s3://bucketA/notExistFolder") is False
    assert s3.s3_exists("s3://notExistBucket") is False
    assert s3.s3_exists("s3://notExistBucket/notExistFile") is False
    assert s3.s3_exists("s3://+InvalidBucketName") is False
    assert s3.s3_exists("s3://bucketA/file") is False  # file prefix
    assert s3.s3_exists("s3://bucketA/folder") is False  # folder prefix
    assert s3.s3_exists("s3://bucketA/fileAA/") is False  # filename as dir


def test_s3_copy(s3_empty_client):
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key", Body="value")

    s3.s3_copy("s3://bucket/key", "s3://bucket/result", followlinks=True)

    body = (
        s3_empty_client.get_object(Bucket="bucket", Key="result")["Body"]
        .read()
        .decode("utf-8")
    )

    assert body == "value"

    with pytest.raises(SameFileError):
        s3.s3_copy("s3://bucket/key", "s3://bucket/key", followlinks=True)


@pytest.mark.skipif(sys.version_info < (3, 6), reason="Python3.6+")
def test_s3_copy_invalid(s3_empty_client):
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key", Body="value")

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_copy("s3://bucket/key", "s3://bucket/")
    assert "s3://bucket/" in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_copy("s3://bucket/key", "s3://bucket/prefix/")
    assert "s3://bucket/prefix/" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_copy("s3://bucket/key", "s3:///key")
    assert "s3:///key" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_copy("s3://bucket/key", "s3://notExistBucket/key")
    assert "notExistBucket" in str(error.value)

    with pytest.raises(S3FileNotFoundError) as error:
        s3.s3_copy("s3://bucket/prefix/", "s3://bucket/key")
    assert "s3://bucket/prefix/" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_copy("s3:///key", "s3://bucket/key")
    assert "s3:///key" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_copy("s3://notExistBucket/key", "s3://bucket/key")
    assert "notExistBucket" in str(error.value)

    with pytest.raises(S3FileNotFoundError) as error:
        s3.s3_copy("s3://bucket/notExistFile", "s3://bucket/key")
    assert "s3://bucket/notExistFile" in str(error.value)

    with pytest.raises(S3IsADirectoryError) as error:
        s3.s3_copy("s3://bucket", "s3://bucket/key")


def test_s3_getsize(truncating_client):
    bucket_A_size = s3.s3_getsize("s3://bucketA")
    assert bucket_A_size == 8 + 6 + 6 + 6 + 6 + 8  # noqa: E501 # folderAA/folderAAA/fileAAAA + folderAB/fileAB + folderAB/fileAC + folderAB-C/fileAB-C + fileAA + fileAB
    assert s3.s3_getsize("s3://bucketA/fileAA") == 6
    assert s3.s3_getsize("s3://bucketA/folderAB") == 6 + 6

    with pytest.raises(S3BucketNotFoundError) as error:
        assert s3.s3_getsize("s3://")
    assert "s3://" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getsize("s3://notExistBucket")
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getsize("s3://bucketA/notExistFile")
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getsize("s3:///notExistFile")


def test_s3_getmtime(truncating_client):
    bucket_A_mtime = s3.s3_getmtime("s3://bucketA")
    assert bucket_A_mtime == Now()
    assert s3.s3_getmtime("s3://bucketA/fileAA") == Now()
    assert s3.s3_getmtime("s3://bucketA/folderAB") == Now()

    with pytest.raises(S3BucketNotFoundError) as error:
        assert s3.s3_getmtime("s3://")
    assert "s3://" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getmtime("s3://notExistBucket")
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getmtime("s3://bucketA/notExistFile")
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_getmtime("s3:///notExistFile")


def test_s3_stat(truncating_client, mocker):
    mocker.patch("megfile.s3_path.StatResult", side_effect=FakeStatResult)

    bucket_A_stat = s3.s3_stat("s3://bucketA")
    assert bucket_A_stat == make_stat(
        size=8 + 6 + 6 + 6 + 6 + 8,  # noqa: E501 # folderAA/folderAAA/fileAAAA + folderAB/fileAB + folderAB/fileAC + folderAB-C/fileAB-C + fileAA + fileAB
        mtime=Now(),
        isdir=True,
    )
    assert s3.s3_stat("s3://bucketA/fileAA") == make_stat(size=6)
    assert s3.s3_stat("s3://bucketA/folderAB") == make_stat(
        size=6 + 6, mtime=Now(), isdir=True
    )

    # 有同名目录时, 优先返回文件的状态
    assert s3.s3_stat("s3://bucketC/folder") == StatResult(size=4, mtime=Now())

    with pytest.raises(S3BucketNotFoundError) as error:
        assert s3.s3_stat("s3://")
    assert "s3://" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_stat("s3://notExistBucket")
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_stat("s3://bucketA/notExistFile")
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_stat("s3:///notExistFile")
    with pytest.raises(S3FileNotFoundError) as error:
        s3.s3_stat("s3:///bucketA/")


def test_s3_lstat(truncating_client, mocker):
    mocker.patch("megfile.s3_path.StatResult", side_effect=FakeStatResult)

    bucket_A_stat = s3.s3_lstat("s3://bucketA")
    assert bucket_A_stat == make_stat(
        size=8 + 6 + 6 + 6 + 6 + 8,  # noqa: E501 # folderAA/folderAAA/fileAAAA + folderAB/fileAB + folderAB/fileAC + folderAB-C/fileAB-C + fileAA + fileAB
        mtime=Now(),
        isdir=True,
    )
    assert s3.s3_lstat("s3://bucketA/fileAA") == make_stat(size=6)
    assert s3.s3_lstat("s3://bucketA/folderAB") == make_stat(
        size=6 + 6, mtime=Now(), isdir=True
    )

    # 有同名目录时, 优先返回文件的状态
    assert s3.s3_lstat("s3://bucketC/folder") == StatResult(size=4, mtime=Now())

    with pytest.raises(S3BucketNotFoundError) as error:
        assert s3.s3_lstat("s3://")
    assert "s3://" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_lstat("s3://notExistBucket")
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_lstat("s3://bucketA/notExistFile")
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_lstat("s3:///notExistFile")
    with pytest.raises(S3FileNotFoundError) as error:
        s3.s3_lstat("s3:///bucketA/")


def test_s3_upload1(s3_empty_client, fs):
    src_url = "/path/to/file"
    link_url = "/path/to/file.lnk"

    fs.create_file(src_url, contents="value")
    os.symlink(src_url, link_url)
    s3_empty_client.create_bucket(Bucket="bucket")

    s3.s3_upload(link_url, "s3://bucket/result", followlinks=True)

    body = (
        s3_empty_client.get_object(Bucket="bucket", Key="result")["Body"]
        .read()
        .decode("utf-8")
    )

    assert body == "value"

    src_url = "/path/to/file2"
    fs.create_file(src_url, contents="value2")
    s3.s3_upload(src_url, "s3://bucket/result", overwrite=False)
    assert (
        s3_empty_client.get_object(Bucket="bucket", Key="result")["Body"]
        .read()
        .decode("utf-8")
        == "value"
    )

    s3.s3_upload(src_url, "s3://bucket/result", overwrite=True)
    assert (
        s3_empty_client.get_object(Bucket="bucket", Key="result")["Body"]
        .read()
        .decode("utf-8")
        == "value2"
    )

    with pytest.raises(OSError):
        s3.s3_upload("s3://bucket/a", "s3://bucket/b")


def test_s3_upload2(s3_empty_client, fs):
    src_url = "file:///path/to/file"

    fs.create_file("/path/to/file", contents="value")
    s3_empty_client.create_bucket(Bucket="bucket")

    s3.s3_upload(src_url, "s3://bucket/result")

    body = (
        s3_empty_client.get_object(Bucket="bucket", Key="result")["Body"]
        .read()
        .decode("utf-8")
    )

    assert body == "value"


def test_s3_upload_invalid(s3_empty_client, fs):
    s3_empty_client.create_bucket(Bucket="bucket")

    src_url = "/path/to/file"
    fs.create_file(src_url, contents="value")

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_upload(src_url, "s3://bucket/prefix/")
    assert "s3://bucket/prefix/" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_upload(src_url, "s3:///key")
    assert "s3:///key" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_upload("/notExistFile", "s3://bucket/key")
    assert "/notExistFile" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_upload(src_url, "s3://notExistBucket/key")
    assert "notExistBucket" in str(error.value)


def test_s3_upload_is_directory(s3_empty_client, fs):
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_upload("/path/to/file", "s3://bucket/prefix/")
    assert "s3://bucket/prefix/" in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_upload("/path/to/file", "s3://bucket")
    assert "s3://bucket" in str(error.value)

    src_url = "/path/to/"
    fs.create_dir(src_url)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_upload(src_url, "s3://bucket/key")
    assert src_url in str(error.value)


def test_s3_download(s3_setup, fs):
    dst_url = "/path/to/file"

    s3.s3_download("s3://bucketA/fileAA", dst_url)

    with open(dst_url, "rb") as result:
        body = result.read().decode("utf-8")
        assert body == "fileAA"

    s3.s3_download("s3://bucketA/folderAA/folderAAA/fileAAAA", dst_url, overwrite=False)

    with open(dst_url, "rb") as result:
        body = result.read().decode("utf-8")
        assert body == "fileAA"

    s3.s3_download("s3://bucketA/folderAA/folderAAA/fileAAAA", dst_url, overwrite=True)

    with open(dst_url, "rb") as result:
        body = result.read().decode("utf-8")
        assert body == "fileAAAA"

    dst_url = "/path/to/another/file"
    os.makedirs(os.path.dirname(dst_url))

    s3.s3_download("s3://bucketA/fileAA", dst_url)

    with open(dst_url, "rb") as result:
        body = result.read().decode("utf-8")
        assert body == "fileAA"

    dst_url = "file:///path/to/samename/file"

    s3.s3_download("s3://bucketC/folder", dst_url)

    with open("/path/to/samename/file", "rb") as result:
        body = result.read().decode("utf-8")
        assert body == "file"

    dst_url = "/path/to/samename/dir"

    with pytest.raises(S3IsADirectoryError):
        s3.s3_download("s3://bucketC/folderAA", dst_url)

    with pytest.raises(OSError):
        s3.s3_download(
            "s3://bucketA/folderAA/folderAAA/fileAAAA",
            "s3://bucketA/folderAA/folderAAA/fileBBB",
        )

    with (
        patch.object(s3_setup, "download_file", side_effect=AssertionError("test")),
        pytest.raises(S3UnknownError) as err,
    ):
        s3.s3_download("s3://bucketA/folderAA/folderAAA/fileAAAA", "/folderAAA")
        assert "test" in str(err.value)


def test_s3_download_makedirs(s3_setup, mocker, fs):
    dst_url = "/path/to/another/file"
    dst_dir = os.path.dirname(dst_url)
    os.makedirs(dst_dir)

    mocker.patch("os.makedirs")
    s3.s3_download("s3://bucketA/fileAA", dst_url, followlinks=True)
    os.makedirs.assert_called_once_with(dst_dir, exist_ok=True)
    os.makedirs.reset_mock()

    s3.s3_download("s3://bucketA/fileAA", "file")
    # os.makedirs.assert_not_called() in Python 3.6+
    assert os.makedirs.call_count == 0


def test_s3_download_is_directory(s3_setup, fs):
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_download("s3://bucketA/fileAA", "")

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_download("s3://bucketA/fileAA", "/path/")
    assert "/path/" in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_download("s3://bucketA", "/path/to/file")
    assert "s3://bucketA" in str(error.value)


def test_s3_download_invalid(s3_setup, fs):
    dst_url = "/path/to/file"

    with pytest.raises(S3IsADirectoryError) as error:
        s3.s3_download("s3://bucketA/folderAB", dst_url)
    assert "s3://bucketA/folderAB" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_download("s3:///key", dst_url)
    assert "s3:///key" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_download("s3://notExistBucket/fileAA", dst_url)
    assert "s3://notExistBucket/fileAA" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_download("s3://bucketA/notExistFile", dst_url)
    assert "s3://bucketA/notExistFile" in str(error.value)


def test_s3_remove(s3_setup):
    with pytest.raises(S3BucketNotFoundError) as error:
        s3.s3_remove("s3:///key")
    assert "s3://" in str(error.value)
    with pytest.raises(UnsupportedError) as error:
        s3.s3_remove("s3://bucketA/")
    assert "s3://bucketA/" in str(error.value)
    with pytest.raises(UnsupportedError) as error:
        s3.s3_remove("s3://")
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_remove("s3://bucketA/notExistFile")
    assert "s3://bucketA/notExistFile" in str(error.value)
    s3.s3_remove("s3://bucketA/notExistFile", missing_ok=True)
    s3.s3_remove("s3://bucketA/folderAA/")
    assert s3.s3_exists("s3://bucketA/folderAA/") is False
    s3.s3_remove("s3://bucketA/folderAB")
    assert s3.s3_exists("s3://bucketA/folderAB/") is False
    s3.s3_remove("s3://bucketA/fileAA")
    assert s3.s3_exists("s3://bucketA/fileAA") is False


def test_s3_remove_multi_page(truncating_client):
    s3.s3_remove("s3://bucketA/folderAA/")
    assert s3.s3_exists("s3://bucketA/folderAA/") is False
    s3.s3_remove("s3://bucketA/folderAB")
    assert s3.s3_exists("s3://bucketA/folderAB/") is False
    s3.s3_remove("s3://bucketA/fileAA")
    assert s3.s3_exists("s3://bucketA/fileAA") is False


@pytest.mark.skip("moto issue https://github.com/spulec/moto/issues/2759")
def test_s3_remove_slashes(s3_empty_client):
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="///")
    s3.s3_remove("s3://bucket//")
    assert s3.s3_exists("s3://bucket////") is False


def test_s3_remove_with_error(s3_empty_client, caplog):
    with caplog.at_level(logging.INFO, logger="megfile"):
        response = {
            "Deleted": [
                {
                    "Key": "string",
                    "VersionId": "string",
                    "DeleteMarker": True,
                    "DeleteMarkerVersionId": "string",
                }
            ],
            "RequestCharged": "requester",
            "Errors": [
                {
                    "Key": "error1",
                    "VersionId": "test1",
                    "Code": "InternalError",
                    "Message": "test InternalError",
                },
                {
                    "Key": "error2",
                    "VersionId": "test2",
                    "Code": "TestError",
                    "Message": "test InternalError",
                },
            ],
        }
        s3_empty_client.delete_objects = lambda *args, **kwargs: response
        s3_empty_client.create_bucket(Bucket="bucket")
        s3_empty_client.put_object(Bucket="bucket", Key="1/test.txt", Body="test")
        path = "s3://bucket/1/"
        with pytest.raises(S3UnknownError) as error:
            s3.s3_remove(path)
        for error_info in response["Errors"]:
            if error_info["Code"] == "InternalError":
                for i in range(2):
                    log = "retry %s times, removing file: %s, with error %s: %s" % (
                        i + 1,
                        error_info["Key"],
                        error_info["Code"],
                        error_info["Message"],
                    )
                    assert log in caplog.text
            else:
                log = "failed remove file: %s, with error %s: %s" % (
                    error_info["Key"],
                    error_info["Code"],
                    error_info["Message"],
                )
                assert log in caplog.text
        assert (
            "failed remove path: %s, total file count: 1, failed count: 2" % path
        ) in str(error.value)


def test_s3_move(truncating_client):
    smart.smart_touch("s3://bucketA/folderAA/folderAAA/fileAAAA")
    s3.s3_move("s3://bucketA/folderAA/folderAAA", "s3://bucketA/folderAA/folderAAA1")
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA") is False
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA1/fileAAAA")

    with s3.s3_open("s3://bucketA/folderAA/folderAAA2/fileAAAA", "w") as f:
        f.write("fileAAAA")
    s3.s3_move(
        "s3://bucketA/folderAA/folderAAA1",
        "s3://bucketA/folderAA/folderAAA2",
        overwrite=False,
    )
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA1") is False
    with s3.s3_open("s3://bucketA/folderAA/folderAAA2/fileAAAA", "r") as f:
        assert f.read() == "fileAAAA"
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA1") is False

    smart.smart_touch("s3://bucketA/folderAA/folderAAA1/fileAAAA")
    s3.s3_move(
        "s3://bucketA/folderAA/folderAAA1",
        "s3://bucketA/folderAA/folderAAA2",
        overwrite=True,
    )
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA1") is False
    with s3.s3_open("s3://bucketA/folderAA/folderAAA2/fileAAAA", "r") as f:
        assert f.read() == ""
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA1") is False


def test_s3_move_file(truncating_client):
    smart.smart_touch("s3://bucketA/folderAA/folderAAA/fileAAAA")
    s3.s3_move(
        "s3://bucketA/folderAA/folderAAA/fileAAAA",
        "s3://bucketA/folderAA/folderAAA1/fileAAAA",
    )
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA/fileAAAA") is False
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA1/fileAAAA")


def test_s3_sync(truncating_client, mocker):
    smart.smart_touch("s3://bucketA/folderAA/folderAAA/fileAAAA")
    s3.s3_sync("s3://bucketA/folderAA/folderAAA", "s3://bucketA/folderAA/folderAAA1")
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA")
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA1/fileAAAA")

    smart.smart_save_text("s3://bucketA/folderAA/folderAAA1/fileAAAA", "test")
    s3.s3_sync(
        "s3://bucketA/folderAA/folderAAA",
        "s3://bucketA/folderAA/folderAAA1",
        overwrite=False,
    )
    assert smart.smart_load_text("s3://bucketA/folderAA/folderAAA1/fileAAAA") == "test"

    smart.smart_save_text("s3://bucketA/folderAA/folderAAA1/fileAAAA", "test")
    s3.s3_sync(
        "s3://bucketA/folderAA/folderAAA",
        "s3://bucketA/folderAA/folderAAA1",
        force=True,
    )
    assert smart.smart_load_text("s3://bucketA/folderAA/folderAAA1/fileAAAA") == ""

    func = mocker.patch("megfile.s3_path.S3Path.copy")
    s3.s3_sync("s3://bucketA/folderAA/folderAAA", "s3://bucketA/folderAA/folderAAA1")
    assert func.call_count == 0


def test_s3_rename(truncating_client):
    s3.s3_rename(
        "s3://bucketA/folderAA/folderAAA/fileAAAA",
        "s3://bucketA/folderAA/folderAAA/fileAAAA1",
    )
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA/fileAAAA") is False
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA/fileAAAA1")

    s3.s3_rename("s3://bucketA/folderAB", "s3://bucketA/folderAB1")
    assert s3.s3_exists("s3://bucketA/folderAB/fileAB") is False
    assert s3.s3_exists("s3://bucketA/folderAB/fileAC") is False
    assert s3.s3_exists("s3://bucketA/folderAB1/fileAB")
    assert s3.s3_exists("s3://bucketA/folderAB1/fileAC")

    with s3.s3_open("s3://bucketA/folderAA/folderAAA/fileAAAA2", "w") as f:
        f.write("fileAAAA2")

    s3.s3_rename(
        "s3://bucketA/folderAA/folderAAA/fileAAAA2",
        "s3://bucketA/folderAA/folderAAA/fileAAAA1",
        overwrite=False,
    )
    with s3.s3_open("s3://bucketA/folderAA/folderAAA/fileAAAA1", "r") as f:
        assert f.read() == "fileAAAA"
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA/fileAAAA2") is False

    with s3.s3_open("s3://bucketA/folderAA/folderAAA/fileAAAA2", "w") as f:
        f.write("fileAAAA2")

    s3.s3_rename(
        "s3://bucketA/folderAA/folderAAA/fileAAAA2",
        "s3://bucketA/folderAA/folderAAA/fileAAAA1",
        overwrite=True,
    )
    with s3.s3_open("s3://bucketA/folderAA/folderAAA/fileAAAA1", "r") as f:
        assert f.read() == "fileAAAA2"
    assert s3.s3_exists("s3://bucketA/folderAA/folderAAA/fileAAAA2") is False


def test_s3_unlink(s3_setup):
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_unlink("s3://")
    assert "s3://" in str(error.value)
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_unlink("s3://bucketA/")
    assert "s3://bucketA/" in str(error.value)
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_unlink("s3://bucketA/notExistFile")
    assert "s3://bucketA/notExistFile" in str(error.value)
    s3.s3_unlink("s3://bucketA/notExistFile", missing_ok=True)
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_unlink("s3://bucketA/folderAA/")
    assert "s3://bucketA/folderAA/" in str(error.value)
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_unlink("s3://bucketA/folderAB")
    assert "s3://bucketA/folderAB" in str(error.value)
    s3.s3_unlink("s3://bucketA/fileAA")
    assert s3.s3_exists("s3://bucketA/fileAA") is False


def test_s3_makedirs(mocker, s3_setup):
    with pytest.raises(FileExistsError) as error:
        s3.s3_makedirs("s3://bucketA/folderAB")
    assert "s3://bucketA/folderAB" in str(error.value)

    s3.s3_makedirs("s3://bucketA/folderAB", exist_ok=True)

    mocker.patch("megfile.s3_path.S3Path.hasbucket", side_effect=S3PermissionError())
    s3.s3_makedirs("s3://bucketA/folderAB", exist_ok=True)


def test_s3_makedirs_no_bucket(mocker, s3_empty_client):
    with pytest.raises(FileNotFoundError) as error:
        s3.s3_makedirs("s3://bucket/key")
    assert "s3://bucket" in str(error.value)


def test_s3_makedirs_exists_folder(mocker, s3_setup):
    with pytest.raises(FileExistsError) as error:
        s3.s3_makedirs("s3://bucketA/folderAB")
    assert "s3://bucketA/folderAB" in str(error.value)


def test_s3_makedirs_root(s3_empty_client):
    with pytest.raises(PermissionError) as error:
        s3.s3_makedirs("s3://")
    assert "s3://" in str(error.value)


def test_smart_open_read_s3_file_not_found(mocker, s3_empty_client):
    mocker.patch("megfile.s3_path.get_endpoint_url", return_value=None)

    with pytest.raises(FileNotFoundError) as error:
        smart.smart_open("s3://non-exist-bucket/key", "r")
    assert "non-exist-bucket" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        with smart.smart_open("s3://non-exist-bucket/key", "w") as f:
            f.write("test")
    assert "non-exist-bucket" in str(error.value)

    s3_empty_client.create_bucket(Bucket="bucket")
    with pytest.raises(FileNotFoundError) as error:
        smart.smart_open("s3://bucket/non-exist-key", "r")
    assert "s3://bucket/non-exist-key" in str(error.value)


def test_smart_open_url_is_of_credentials_format(mocker, s3_empty_client):
    """
    测试 s3_url 中包含 ':' 和 '@' 字符的 url,
    该 url 将被 smart_open 误认为是包含 credential info 的 url

    详情见: https://github.com/RaRe-Technologies/smart_open/issues/378
    """
    bucket = "bucket"
    key = "username:password@key_part"
    s3_empty_client.create_bucket(Bucket=bucket)
    s3_empty_client.put_object(Bucket=bucket, Key=key)

    mocker.patch("megfile.s3_path.get_endpoint_url", return_value=None)

    # 希望, 正常打开, 而不是报错
    # smart_open 将 '@' 之后的部分认为是 key
    smart.smart_open("s3://bucket/username:password@key_part")


def test_s3_walk(truncating_client):
    # walk the whole s3
    # expect: raise UnsupportedError exception
    with pytest.raises(UnsupportedError) as error:
        list(s3.s3_walk("s3://"))
    assert "s3://" in str(error.value)

    # walk the dir that is not exist
    # expect: empty generator
    assert list(s3.s3_walk("s3://notExistBucket")) == []
    assert list(s3.s3_walk("s3://bucketA/notExistFile")) == []

    # walk on file
    # expect: empty generator
    assert list(s3.s3_walk("s3://bucketA/fileAA")) == []

    # walk empty bucket
    # expect: 1 tuple only contains the folder(bucket) path
    result = list(s3.s3_walk("s3://bucketB"))
    assert len(result) == 1
    assert result[0] == ("s3://bucketB", [], [])

    result = list(s3.s3_walk("s3://bucketA"))
    assert len(result) == 5
    assert result[0] == (
        "s3://bucketA",
        ["folderAA", "folderAB", "folderAB-C"],
        ["fileAA", "fileAB"],
    )
    assert result[1] == ("s3://bucketA/folderAA", ["folderAAA"], [])
    assert result[2] == ("s3://bucketA/folderAA/folderAAA", [], ["fileAAAA"])
    assert result[3] == ("s3://bucketA/folderAB", [], ["fileAB", "fileAC"])
    assert result[4] == ("s3://bucketA/folderAB-C", [], ["fileAB-C"])

    result = list(s3.s3_walk("s3://bucketA/"))
    assert len(result) == 5
    assert result[0] == (
        "s3://bucketA",
        ["folderAA", "folderAB", "folderAB-C"],
        ["fileAA", "fileAB"],
    )
    assert result[1] == ("s3://bucketA/folderAA", ["folderAAA"], [])
    assert result[2] == ("s3://bucketA/folderAA/folderAAA", [], ["fileAAAA"])
    assert result[3] == ("s3://bucketA/folderAB", [], ["fileAB", "fileAC"])
    assert result[4] == ("s3://bucketA/folderAB-C", [], ["fileAB-C"])

    # same name of file and folder in the same folder
    result = list(s3.s3_walk("s3://bucketC/folder"))
    assert len(result) == 1
    assert result[0] == ("s3://bucketC/folder", [], ["file"])

    result = list(s3.s3_walk("s3://bucketC/folder/"))
    assert len(result) == 1
    assert result[0] == ("s3://bucketC/folder", [], ["file"])


def test_s3_scan(truncating_client):
    # walk the whole s3
    # expect: raise UnsupportedError exception
    with pytest.raises(UnsupportedError) as error:
        list(s3.s3_scan("s3://"))
    assert "s3://" in str(error.value)

    # walk the dir that is not exist
    # expect: empty generator
    assert list(s3.s3_scan("s3://notExistBucket")) == []
    assert list(s3.s3_scan("s3://bucketA/notExistFile")) == []
    assert list(s3.s3_scan("s3+test://notExistBucket")) == []
    assert list(s3.s3_scan("s3+test://bucketA/notExistFile")) == []

    # walk on file
    # expect: empty generator
    assert list(s3.s3_scan("s3+test://bucketA/fileAA")) == ["s3+test://bucketA/fileAA"]
    assert list(s3.s3_scan("s3+test://bucketA/fileAA")) == ["s3+test://bucketA/fileAA"]

    # walk empty bucket
    # expect: 1 tuple only contains the folder(bucket) path
    result = list(s3.s3_scan("s3://bucketB"))
    assert len(result) == 0

    result = list(s3.s3_scan("s3://bucketA"))
    assert len(result) == 6
    assert result == [
        "s3://bucketA/fileAA",
        "s3://bucketA/fileAB",
        "s3://bucketA/folderAA/folderAAA/fileAAAA",
        "s3://bucketA/folderAB-C/fileAB-C",
        "s3://bucketA/folderAB/fileAB",
        "s3://bucketA/folderAB/fileAC",
    ]

    result = list(s3.s3_scan("s3+test://bucketA"))
    assert len(result) == 6
    assert result == [
        "s3+test://bucketA/fileAA",
        "s3+test://bucketA/fileAB",
        "s3+test://bucketA/folderAA/folderAAA/fileAAAA",
        "s3+test://bucketA/folderAB-C/fileAB-C",
        "s3+test://bucketA/folderAB/fileAB",
        "s3+test://bucketA/folderAB/fileAC",
    ]

    result = list(s3.s3_scan("s3://bucketA/"))
    assert len(result) == 6
    assert result == [
        "s3://bucketA/fileAA",
        "s3://bucketA/fileAB",
        "s3://bucketA/folderAA/folderAAA/fileAAAA",
        "s3://bucketA/folderAB-C/fileAB-C",
        "s3://bucketA/folderAB/fileAB",
        "s3://bucketA/folderAB/fileAC",
    ]

    # same name of file and folder in the same folder
    result = list(s3.s3_scan("s3://bucketC/folder"))
    assert len(result) == 2
    assert result == ["s3://bucketC/folder", "s3://bucketC/folder/file"]

    result = list(s3.s3_scan("s3://bucketC/folder/"))
    assert len(result) == 1
    assert result == ["s3://bucketC/folder/file"]

    with pytest.raises(UnsupportedError) as error:
        s3.s3_scan("s3://")
    with pytest.raises(S3BucketNotFoundError) as error:
        s3.s3_scan("s3://notExistBucket", missing_ok=False)


def test_s3_scan_stat(truncating_client, mocker):
    mocker.patch("megfile.s3_path.StatResult", side_effect=FakeStatResult)

    # walk the whole s3
    # expect: raise UnsupportedError exception
    with pytest.raises(UnsupportedError) as error:
        list(s3.s3_scan_stat("s3://"))
    assert "s3://" in str(error.value)

    # walk the dir that is not exist
    # expect: empty generator
    assert list(s3.s3_scan_stat("s3://notExistBucket")) == []
    assert list(s3.s3_scan_stat("s3://bucketA/notExistFile")) == []

    # walk on file
    # expect: empty generator
    assert list(s3.s3_scan_stat("s3://bucketA/fileAA")) == [
        ("fileAA", "s3://bucketA/fileAA", make_stat(size=6))
    ]

    # walk empty bucket
    # expect: 1 tuple only contains the folder(bucket) path
    result = list(s3.s3_scan_stat("s3://bucketB"))
    assert len(result) == 0

    result = list(s3.s3_scan_stat("s3://bucketA"))
    assert len(result) == 6
    assert result == [
        ("fileAA", "s3://bucketA/fileAA", make_stat(size=6)),
        ("fileAB", "s3://bucketA/fileAB", make_stat(size=6)),
        ("fileAAAA", "s3://bucketA/folderAA/folderAAA/fileAAAA", make_stat(size=8)),
        ("fileAB-C", "s3://bucketA/folderAB-C/fileAB-C", make_stat(size=8)),
        ("fileAB", "s3://bucketA/folderAB/fileAB", make_stat(size=6)),
        ("fileAC", "s3://bucketA/folderAB/fileAC", make_stat(size=6)),
    ]

    result = list(s3.s3_scan_stat("s3://bucketA/"))
    assert len(result) == 6
    assert result == [
        ("fileAA", "s3://bucketA/fileAA", make_stat(size=6)),
        ("fileAB", "s3://bucketA/fileAB", make_stat(size=6)),
        ("fileAAAA", "s3://bucketA/folderAA/folderAAA/fileAAAA", make_stat(size=8)),
        ("fileAB-C", "s3://bucketA/folderAB-C/fileAB-C", make_stat(size=8)),
        ("fileAB", "s3://bucketA/folderAB/fileAB", make_stat(size=6)),
        ("fileAC", "s3://bucketA/folderAB/fileAC", make_stat(size=6)),
    ]

    # same name of file and folder in the same folder
    result = list(s3.s3_scan_stat("s3://bucketC/folder"))
    assert len(result) == 2
    assert result == [
        ("folder", "s3://bucketC/folder", make_stat(size=4)),
        ("file", "s3://bucketC/folder/file", make_stat(size=4)),
    ]

    result = list(s3.s3_scan_stat("s3://bucketC/folder/"))
    assert len(result) == 1
    assert result == [("file", "s3://bucketC/folder/file", make_stat(size=4))]

    with pytest.raises(UnsupportedError) as error:
        s3.s3_scan_stat("s3://")


def test_s3_path_join():
    assert s3.s3_path_join("s3://") == "s3://"
    assert s3.s3_path_join("s3://", "bucket/key") == "s3://bucket/key"
    assert s3.s3_path_join("s3://", "bucket//key") == "s3://bucket//key"
    assert s3.s3_path_join("s3://", "bucket", "key") == "s3://bucket/key"
    assert s3.s3_path_join("s3://", "bucket/", "key") == "s3://bucket/key"
    assert s3.s3_path_join("s3://", "bucket", "/key") == "s3://bucket/key"
    assert s3.s3_path_join("s3://", "bucket", "key/") == "s3://bucket/key/"


def _s3_glob_with_bucket_match():
    """
    scenario: match s3 bucket by including wildcard in 'bucket' part.
    return: all dirs, files and buckets fully matched the pattern
    """
    assert_glob(
        r"s3://*",
        [
            "s3://bucketA",
            "s3://bucketB",
            "s3://bucketC",
            "s3://bucketForGlobTest",
            "s3://bucketForGlobTest2",
            "s3://bucketForGlobTest3",
            "s3://emptyBucketForGlobTest",
        ],
    )

    # without any wildcards
    assert_glob(r"s3://{bucketForGlobTest}[12]/1", ["s3://bucketForGlobTest2/1"])
    assert_glob(
        r"s3://bucketForGlobTest*/1",
        [
            "s3://bucketForGlobTest/1",
            "s3://bucketForGlobTest2/1",
            "s3://bucketForGlobTest3/1",
        ],
    )
    assert_glob(
        r"s3://*GlobTest*/1/a",
        [
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest3/1/a",
            "s3://bucketForGlobTest3/1/a",
        ],
    )
    assert_glob(
        r"s3://**GlobTest***/1/a",
        [
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest3/1/a",
            "s3://bucketForGlobTest3/1/a",
        ],
    )

    assert_glob(
        r"s3://bucketForGlobTest?/1/a/b/1.json",
        [
            "s3://bucketForGlobTest2/1/a/b/1.json",
            "s3://bucketForGlobTest3/1/a/b/1.json",
        ],
    )
    assert_glob(
        r"s3://bucketForGlobTest*/1/a{/b/c,/b}/1.json",
        [
            "s3://bucketForGlobTest/1/a/b/c/1.json",
            "s3://bucketForGlobTest/1/a/b/1.json",
            "s3://bucketForGlobTest2/1/a/b/c/1.json",
            "s3://bucketForGlobTest2/1/a/b/1.json",
            "s3://bucketForGlobTest3/1/a/b/c/1.json",
            "s3://bucketForGlobTest3/1/a/b/1.json",
        ],
    )

    # all files under all direct subfolders
    assert_glob(
        r"s3://bucketForGlobTest[23]/*/*",
        [
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest3/1/a",
            "s3://bucketForGlobTest3/1/a",
        ],
    )

    # combination of '?' and []
    assert_glob(r"s3://*BucketForGlobTest/[2-3]/**/*?msg", [])
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/[13]/**/*?msg",
        [
            "s3://bucketForGlobTest/1/a/b/c/A.msg",
            "s3://bucketForGlobTest2/1/a/b/c/A.msg",
            "s3://bucketForGlobTest2/1/a/c/A.msg",
        ],
    )

    assert_glob(r"s3://{a,b,c}*/notExist", [])

    with pytest.raises(FileNotFoundError) as err:
        s3.s3_glob(r"s3://{a,b,c}*/notExist", missing_ok=False)
    assert r"s3://{a,b,c}*/notExist" in str(err.value)


def assert_glob(pattern, expected, recursive=True, missing_ok=True):
    assert sorted(
        s3.s3_glob(pattern, recursive=recursive, missing_ok=missing_ok)
    ) == sorted(expected)


def assert_glob_stat(pattern, expected, recursive=True, missing_ok=True):
    assert sorted(
        list(s3.s3_glob_stat(pattern, recursive=recursive, missing_ok=missing_ok))
    ) == sorted(list(expected))


def _s3_glob_with_common_wildcard():
    """
    scenario: common shell wildcard, '*', '**', '[]', '?'
    expectation: return matched pathnames in lexicographical order
    """
    # without any wildcards
    assert_glob("s3://emptyBucketForGlobTest", ["s3://emptyBucketForGlobTest"])
    assert_glob("s3://emptyBucketForGlobTest/", ["s3://emptyBucketForGlobTest/"])
    assert_glob("s3://bucketForGlobTest/1", ["s3://bucketForGlobTest/1"])
    assert_glob("s3://bucketForGlobTest/1/", ["s3://bucketForGlobTest/1/"])
    assert_glob(
        "s3://bucketForGlobTest/1/a",
        ["s3://bucketForGlobTest/1/a", "s3://bucketForGlobTest/1/a"],
    )  # 同名文件
    assert_glob(
        "s3://bucketForGlobTest/2/a/d/2.json", ["s3://bucketForGlobTest/2/a/d/2.json"]
    )
    assert_glob(
        r"s3://bucketForGlobTest/2/a/d/{c/1,2}.json",
        [
            "s3://bucketForGlobTest/2/a/d/c/1.json",
            "s3://bucketForGlobTest/2/a/d/2.json",
        ],
    )

    # '*', all files and folders
    assert_glob("s3://emptyBucketForGlobTest/*", [])
    assert_glob(
        "s3://bucketForGlobTest/*",
        ["s3://bucketForGlobTest/1", "s3://bucketForGlobTest/2"],
    )

    # all files under all direct subfolders
    assert_glob(
        "s3://bucketForGlobTest/*/*",
        [
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/2/a",
        ],
    )

    # combination of '?' and []
    assert_glob("s3://bucketForGlobTest/[2-3]/**/*?msg", [])
    assert_glob(
        "s3://bucketForGlobTest/[13]/**/*?msg", ["s3://bucketForGlobTest/1/a/b/c/A.msg"]
    )
    assert_glob(
        "s3://bucketForGlobTest/1/a/b/*/A.msg", ["s3://bucketForGlobTest/1/a/b/c/A.msg"]
    )
    assert_glob(
        "s3://bucketForGlobTest/1/a/b/c/*.{json,msg}",
        [
            "s3://bucketForGlobTest/1/a/b/c/1.json",
            "s3://bucketForGlobTest/1/a/b/c/A.msg",
        ],
    )

    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob("s3://notExistsBucketForGlobTest/*", missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob("s3://emptyBucketForGlobTest/*", missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob("s3://bucketForGlobTest/3/**", missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob("s3://bucketForGlobTest/1/**.notExists", missing_ok=False))


def _s3_glob_with_recursive_pathname():
    """
    scenario: recursively search target folder
    expectation: returns all subdirectory and files,
        without check of lexicographical order
    """
    # recursive all files and folders
    assert_glob(
        "s3://bucketForGlobTest/**",
        [
            "s3://bucketForGlobTest/1",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a/b",
            "s3://bucketForGlobTest/1/a/b/1.json",
            "s3://bucketForGlobTest/1/a/b/c",
            "s3://bucketForGlobTest/1/a/b/c/1.json",
            "s3://bucketForGlobTest/1/a/b/c/A.msg",
            "s3://bucketForGlobTest/2",
            "s3://bucketForGlobTest/2/a",
            "s3://bucketForGlobTest/2/a/b",
            "s3://bucketForGlobTest/2/a/b/a",
            "s3://bucketForGlobTest/2/a/b/a/1.json",
            "s3://bucketForGlobTest/2/a/b/c",
            "s3://bucketForGlobTest/2/a/b/c/1.json",
            "s3://bucketForGlobTest/2/a/b/c/2.json",
            "s3://bucketForGlobTest/2/a/d",
            "s3://bucketForGlobTest/2/a/d/2.json",
            "s3://bucketForGlobTest/2/a/d/c/1.json",
            "s3://bucketForGlobTest/2/a/d/c",
        ],
    )

    assert_glob(
        "s3://bucketForGlobTest/**/*",
        [
            "s3://bucketForGlobTest/1",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a/b",
            "s3://bucketForGlobTest/1/a/b/1.json",
            "s3://bucketForGlobTest/1/a/b/c",
            "s3://bucketForGlobTest/1/a/b/c/1.json",
            "s3://bucketForGlobTest/1/a/b/c/A.msg",
            "s3://bucketForGlobTest/2",
            "s3://bucketForGlobTest/2/a",
            "s3://bucketForGlobTest/2/a/b",
            "s3://bucketForGlobTest/2/a/b/a",
            "s3://bucketForGlobTest/2/a/b/a/1.json",
            "s3://bucketForGlobTest/2/a/b/c",
            "s3://bucketForGlobTest/2/a/b/c/1.json",
            "s3://bucketForGlobTest/2/a/b/c/2.json",
            "s3://bucketForGlobTest/2/a/d",
            "s3://bucketForGlobTest/2/a/d/2.json",
            "s3://bucketForGlobTest/2/a/d/c/1.json",
            "s3://bucketForGlobTest/2/a/d/c",
        ],
    )


def _s3_glob_with_recursive_pathname_and_custom_protocol():
    """
    scenario: recursively search target folder
    expectation: returns all subdirectory and files,
        without check of lexicographical order
    """
    # recursive all files and folders
    assert_glob(
        "s3+test://bucketForGlobTest/**",
        [
            "s3+test://bucketForGlobTest/1",
            "s3+test://bucketForGlobTest/1/a",
            "s3+test://bucketForGlobTest/1/a",
            "s3+test://bucketForGlobTest/1/a/b",
            "s3+test://bucketForGlobTest/1/a/b/1.json",
            "s3+test://bucketForGlobTest/1/a/b/c",
            "s3+test://bucketForGlobTest/1/a/b/c/1.json",
            "s3+test://bucketForGlobTest/1/a/b/c/A.msg",
            "s3+test://bucketForGlobTest/2",
            "s3+test://bucketForGlobTest/2/a",
            "s3+test://bucketForGlobTest/2/a/b",
            "s3+test://bucketForGlobTest/2/a/b/a",
            "s3+test://bucketForGlobTest/2/a/b/a/1.json",
            "s3+test://bucketForGlobTest/2/a/b/c",
            "s3+test://bucketForGlobTest/2/a/b/c/1.json",
            "s3+test://bucketForGlobTest/2/a/b/c/2.json",
            "s3+test://bucketForGlobTest/2/a/d",
            "s3+test://bucketForGlobTest/2/a/d/2.json",
            "s3+test://bucketForGlobTest/2/a/d/c/1.json",
            "s3+test://bucketForGlobTest/2/a/d/c",
        ],
    )

    assert_glob(
        "s3+test://bucketForGlobTest/**/*",
        [
            "s3+test://bucketForGlobTest/1",
            "s3+test://bucketForGlobTest/1/a",
            "s3+test://bucketForGlobTest/1/a",
            "s3+test://bucketForGlobTest/1/a/b",
            "s3+test://bucketForGlobTest/1/a/b/1.json",
            "s3+test://bucketForGlobTest/1/a/b/c",
            "s3+test://bucketForGlobTest/1/a/b/c/1.json",
            "s3+test://bucketForGlobTest/1/a/b/c/A.msg",
            "s3+test://bucketForGlobTest/2",
            "s3+test://bucketForGlobTest/2/a",
            "s3+test://bucketForGlobTest/2/a/b",
            "s3+test://bucketForGlobTest/2/a/b/a",
            "s3+test://bucketForGlobTest/2/a/b/a/1.json",
            "s3+test://bucketForGlobTest/2/a/b/c",
            "s3+test://bucketForGlobTest/2/a/b/c/1.json",
            "s3+test://bucketForGlobTest/2/a/b/c/2.json",
            "s3+test://bucketForGlobTest/2/a/d",
            "s3+test://bucketForGlobTest/2/a/d/2.json",
            "s3+test://bucketForGlobTest/2/a/d/c/1.json",
            "s3+test://bucketForGlobTest/2/a/d/c",
        ],
    )


def _s3_glob_with_same_file_and_folder():
    """
    scenario: existing same-named file and directory in a  directory
    expectation: the file and directory is returned 1 time respectively
    """
    # same name and folder
    assert_glob(
        "s3://bucketForGlobTest/1/*",
        [
            # 1 file name 'a' and 1 actual folder
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
        ],
    )


def _s3_glob_with_nested_pathname():
    """
    scenario: pathname including nested '**'
    expectation: work correctly as standard glob module
    """
    # nested
    # non-recursive, actually: s3://bucketForGlobTest/*/a/*/*.jso?
    assert_glob(
        "s3://bucketForGlobTest/**/a/**/*.jso?",
        ["s3://bucketForGlobTest/2/a/d/2.json", "s3://bucketForGlobTest/1/a/b/1.json"],
        recursive=False,
    )

    # recursive
    # s3://bucketForGlobTest/2/a/b/a/1.json is returned 2 times
    # without set, otherwise,
    # 's3://bucketForGlobTest/2/a/b/a/1.json' would be duplicated
    assert_glob(
        "s3://bucketForGlobTest/**/a/**/*.jso?",
        [
            "s3://bucketForGlobTest/1/a/b/1.json",
            "s3://bucketForGlobTest/1/a/b/c/1.json",
            "s3://bucketForGlobTest/2/a/b/a/1.json",
            "s3://bucketForGlobTest/2/a/b/c/1.json",
            "s3://bucketForGlobTest/2/a/b/c/2.json",
            "s3://bucketForGlobTest/2/a/d/2.json",
            "s3://bucketForGlobTest/2/a/d/c/1.json",
        ],
    )


def _s3_glob_with_not_exists_dir():
    """
    scenario: glob on a directory that is not exists
    expectation: if recursive is True, return the directory with postfix of slash('/'),
        otherwise, an empty list. keep identical result with standard glob module
    """

    assert_glob("s3://bucketForGlobTest/notExistFolder/notExistFile", [])
    assert_glob("s3://bucketForGlobTest/notExistFolder", [])

    # not exists path
    assert_glob("s3://notExistBucket/**", [])

    assert_glob("s3://bucketA/notExistFolder/**", [])

    assert_glob("s3://notExistBucket/**", [])

    assert_glob("s3://bucketForGlobTest/notExistFolder/**", [])


def _s3_glob_with_dironly():
    """
    scenario: pathname with the postfix of slash('/')
    expectation: returns only contains pathname of directory,
        each of them is end with '/'
    """
    assert_glob(
        "s3://bucketForGlobTest/*/",
        ["s3://bucketForGlobTest/1/", "s3://bucketForGlobTest/2/"],
    )

    assert_glob("s3://bucketForGlobTest/[2-9]/", ["s3://bucketForGlobTest/2/"])

    # all sub-directories of 2, recursively
    assert_glob(
        "s3://bucketForGlobTest/2/**/*/",
        [
            "s3://bucketForGlobTest/2/a/",
            "s3://bucketForGlobTest/2/a/b/",
            "s3://bucketForGlobTest/2/a/b/a/",
            "s3://bucketForGlobTest/2/a/b/c/",
            "s3://bucketForGlobTest/2/a/d/",
            "s3://bucketForGlobTest/2/a/d/c/",
        ],
    )


def _s3_glob_with_common_wildcard_cross_bucket():
    """
    scenario: common shell wildcard, '*', '**', '[]', '?'
    expectation: return matched pathnames in lexicographical order
    """
    # without any wildcards
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/1",
        ["s3://bucketForGlobTest/1", "s3://bucketForGlobTest2/1"],
    )
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/1/",
        [
            "s3://bucketForGlobTest/1/",
            "s3://bucketForGlobTest2/1/",
            "s3://bucketForGlobTest3/1/",
        ],
    )
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/1/a",
        [
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest2/1/a",
        ],
    )  # 同名文件
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest3}/1/a/b/1.json",
        ["s3://bucketForGlobTest/1/a/b/1.json", "s3://bucketForGlobTest3/1/a/b/1.json"],
    )
    assert_glob(
        r"s3://{bucketForGlobTest/2,bucketForGlobTest2/1}/a/b/c/2.json",
        [
            "s3://bucketForGlobTest/2/a/b/c/2.json",
            "s3://bucketForGlobTest2/1/a/b/c/2.json",
        ],
    )
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/1/a{/b/c,/b}/1.json",
        [
            "s3://bucketForGlobTest/1/a/b/c/1.json",
            "s3://bucketForGlobTest/1/a/b/1.json",
            "s3://bucketForGlobTest2/1/a/b/c/1.json",
            "s3://bucketForGlobTest2/1/a/b/1.json",
            "s3://bucketForGlobTest3/1/a/b/c/1.json",
            "s3://bucketForGlobTest3/1/a/b/1.json",
        ],
    )

    # '*', all files and folders
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest3}/*",
        [
            "s3://bucketForGlobTest/1",
            "s3://bucketForGlobTest/2",
            "s3://bucketForGlobTest3/1",
        ],
    )

    # all files under all direct subfolders
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/*/*",
        [
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/2/a",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest2/1/a",
        ],
    )

    # combination of '?' and []
    assert_glob(r"s3://{bucketForGlobTest,emptyBucketForGlobTest}/[2-3]/**/*?msg", [])
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/[13]/**/*?msg",
        [
            "s3://bucketForGlobTest/1/a/b/c/A.msg",
            "s3://bucketForGlobTest2/1/a/b/c/A.msg",
            "s3://bucketForGlobTest2/1/a/c/A.msg",
        ],
    )
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/1/a/b/*/A.msg",
        [
            "s3://bucketForGlobTest/1/a/b/c/A.msg",
            "s3://bucketForGlobTest2/1/a/b/c/A.msg",
        ],
    )

    assert_glob(
        r"s3://{notExistsBucketForGlobTest,bucketForGlobTest}/*",
        ["s3://bucketForGlobTest/1", "s3://bucketForGlobTest/2"],
        missing_ok=False,
    )

    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob(
                r"s3://{notExistsBucketForGlobTest,bucketForGlobTest}/3/*",
                missing_ok=False,
            )
        )
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob(
                r"s3://{bucketForGlobTest,bucketForGlobTest2}/3/**", missing_ok=False
            )
        )
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob(
                r"s3://{bucketForGlobTest,bucketForGlobTest2}/1/**.notExists",
                missing_ok=False,
            )
        )


def _s3_glob_with_recursive_pathname_cross_bucket():
    """
    scenario: recursively search target folder
    expectation: returns all subdirectory and files,
        without check of lexicographical order
    """
    # recursive all files and folders
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/**",
        [
            "s3://bucketForGlobTest/1",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a/b",
            "s3://bucketForGlobTest/1/a/b/1.json",
            "s3://bucketForGlobTest/1/a/b/c",
            "s3://bucketForGlobTest/1/a/b/c/1.json",
            "s3://bucketForGlobTest/1/a/b/c/A.msg",
            "s3://bucketForGlobTest/2",
            "s3://bucketForGlobTest/2/a",
            "s3://bucketForGlobTest/2/a/b",
            "s3://bucketForGlobTest/2/a/b/a",
            "s3://bucketForGlobTest/2/a/b/a/1.json",
            "s3://bucketForGlobTest/2/a/b/c",
            "s3://bucketForGlobTest/2/a/b/c/1.json",
            "s3://bucketForGlobTest/2/a/b/c/2.json",
            "s3://bucketForGlobTest/2/a/d",
            "s3://bucketForGlobTest/2/a/d/2.json",
            "s3://bucketForGlobTest/2/a/d/c/1.json",
            "s3://bucketForGlobTest/2/a/d/c",
            "s3://bucketForGlobTest2/1",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest2/1/a/b",
            "s3://bucketForGlobTest2/1/a/b/1.json",
            "s3://bucketForGlobTest2/1/a/b/c",
            "s3://bucketForGlobTest2/1/a/b/c/1.json",
            "s3://bucketForGlobTest2/1/a/b/c/2.json",
            "s3://bucketForGlobTest2/1/a/b/c/A.msg",
            "s3://bucketForGlobTest2/1/a/c",
            "s3://bucketForGlobTest2/1/a/c/1.json",
            "s3://bucketForGlobTest2/1/a/c/A.msg",
            "s3://bucketForGlobTest2/1/a/1.json",
        ],
    )

    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest3}/**/*",
        [
            "s3://bucketForGlobTest/1",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a/b",
            "s3://bucketForGlobTest/1/a/b/1.json",
            "s3://bucketForGlobTest/1/a/b/c",
            "s3://bucketForGlobTest/1/a/b/c/1.json",
            "s3://bucketForGlobTest/1/a/b/c/A.msg",
            "s3://bucketForGlobTest/2",
            "s3://bucketForGlobTest/2/a",
            "s3://bucketForGlobTest/2/a/b",
            "s3://bucketForGlobTest/2/a/b/a",
            "s3://bucketForGlobTest/2/a/b/a/1.json",
            "s3://bucketForGlobTest/2/a/b/c",
            "s3://bucketForGlobTest/2/a/b/c/1.json",
            "s3://bucketForGlobTest/2/a/b/c/2.json",
            "s3://bucketForGlobTest/2/a/d",
            "s3://bucketForGlobTest/2/a/d/2.json",
            "s3://bucketForGlobTest/2/a/d/c/1.json",
            "s3://bucketForGlobTest/2/a/d/c",
            "s3://bucketForGlobTest3/1",
            "s3://bucketForGlobTest3/1/a",
            "s3://bucketForGlobTest3/1/a",
            "s3://bucketForGlobTest3/1/a/b",
            "s3://bucketForGlobTest3/1/a/b/1.json",
            "s3://bucketForGlobTest3/1/a/b/c",
            "s3://bucketForGlobTest3/1/a/b/c/1.json",
            "s3://bucketForGlobTest3/1/a/b/c/A.msg",
        ],
    )


def _s3_glob_with_same_file_and_folder_cross_bucket():
    """
    scenario: existing same-named file and directory in a  directory
    expectation: the file and directory is returned 1 time respectively
    """
    # same name and folder
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/1/*",
        [
            # 1 file name 'a' and 1 actual folder
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest/1/a",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest2/1/a",
            "s3://bucketForGlobTest3/1/a",
            "s3://bucketForGlobTest3/1/a",
        ],
    )


def _s3_glob_with_nested_pathname_cross_bucket():
    """
    scenario: pathname including nested '**'
    expectation: work correctly as standard glob module
    """
    # nested
    # non-recursive, actually: s3://bucketForGlobTest/*/a/*/*.jso?
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/**/a/**/*.jso?",
        [
            "s3://bucketForGlobTest/2/a/d/2.json",
            "s3://bucketForGlobTest/1/a/b/1.json",
            "s3://bucketForGlobTest2/1/a/b/1.json",
            "s3://bucketForGlobTest2/1/a/c/1.json",
            "s3://bucketForGlobTest3/1/a/b/1.json",
        ],
        recursive=False,
    )

    # recursive
    # s3://bucketForGlobTest/2/a/b/a/1.json is returned 2 times
    # without set, otherwise,
    # 's3://bucketForGlobTest/2/a/b/a/1.json' would be duplicated
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/**/a/**/*.jso?",
        [
            "s3://bucketForGlobTest/1/a/b/1.json",
            "s3://bucketForGlobTest/1/a/b/c/1.json",
            "s3://bucketForGlobTest/2/a/b/a/1.json",
            "s3://bucketForGlobTest/2/a/b/c/1.json",
            "s3://bucketForGlobTest/2/a/b/c/2.json",
            "s3://bucketForGlobTest/2/a/d/2.json",
            "s3://bucketForGlobTest/2/a/d/c/1.json",
            "s3://bucketForGlobTest2/1/a/1.json",
            "s3://bucketForGlobTest2/1/a/b/1.json",
            "s3://bucketForGlobTest2/1/a/c/1.json",
            "s3://bucketForGlobTest2/1/a/b/c/1.json",
            "s3://bucketForGlobTest2/1/a/b/c/2.json",
            "s3://bucketForGlobTest3/1/a/b/1.json",
            "s3://bucketForGlobTest3/1/a/b/c/1.json",
        ],
    )


def _s3_glob_with_not_exists_dir_cross_bucket():
    """
    scenario: glob on a directory that is not exists
    expectation: if recursive is True, return the directory with postfix of slash('/'),
        otherwise, an empty list. keep identical result with standard glob module
    """

    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/notExistFolder/notExistFile",
        [],
    )
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/notExistFolder",
        [],
    )

    # not exists path
    assert_glob(r"s3://{notExistBucket,notExistBucket2}/**", [])

    assert_glob(r"s3://{bucketA,falseBucket}/notExistFolder/**", [])

    assert_glob(r"s3://{notExistBucket,notExistBucket2}/*", [])

    assert_glob(r"s3://{bucketForGlobTest,bucketForGlobTest2}/notExistFolder/**", [])


def _s3_glob_with_dironly_cross_bucket():
    """
    scenario: pathname with the postfix of slash('/')
    expectation: returns only contains pathname of directory,
        each of them is end with '/'
    """
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/*/",
        [
            "s3://bucketForGlobTest/1/",
            "s3://bucketForGlobTest/2/",
            "s3://bucketForGlobTest2/1/",
        ],
    )

    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/[2-9]/",
        ["s3://bucketForGlobTest/2/"],
    )

    # all sub-directories of 2, recursively
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/2/**/*/",
        [
            "s3://bucketForGlobTest/2/a/",
            "s3://bucketForGlobTest/2/a/b/",
            "s3://bucketForGlobTest/2/a/b/a/",
            "s3://bucketForGlobTest/2/a/b/c/",
            "s3://bucketForGlobTest/2/a/d/",
            "s3://bucketForGlobTest/2/a/d/c/",
        ],
    )

    # all sub-directories of 1, recursively
    assert_glob(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/1/**/*/",
        [
            "s3://bucketForGlobTest/1/a/",
            "s3://bucketForGlobTest/1/a/b/",
            "s3://bucketForGlobTest/1/a/b/c/",
            "s3://bucketForGlobTest2/1/a/",
            "s3://bucketForGlobTest2/1/a/b/",
            "s3://bucketForGlobTest2/1/a/b/c/",
            "s3://bucketForGlobTest2/1/a/c/",
            "s3://bucketForGlobTest3/1/a/",
            "s3://bucketForGlobTest3/1/a/b/",
            "s3://bucketForGlobTest3/1/a/b/c/",
        ],
    )


def test_s3_glob(truncating_client):
    original_calls = (os.path.lexists, os.path.isdir, os.scandir)
    _s3_glob_with_bucket_match()
    _s3_glob_with_common_wildcard()
    _s3_glob_with_recursive_pathname()
    _s3_glob_with_recursive_pathname_and_custom_protocol()
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
    with pytest.raises(UnsupportedError):
        list(s3.s3_iglob("s3://"))


def test_group_s3path_by_bucket(truncating_client):
    assert sorted(
        s3._group_s3path_by_bucket(r"s3://*ForGlobTest{1,2/a,2/1/a}/1.jso?,")
    ) == ["s3://bucketForGlobTest2/{1/a,a}/1.jso?,"]

    assert sorted(
        s3._group_s3path_by_bucket(
            r"s3://{emptybucket,bucket}ForGlob{Test,Test2,Test3}/c/a/a"
        )
    ) == [
        "s3://bucketForGlobTest/c/a/a",
        "s3://bucketForGlobTest2/c/a/a",
        "s3://bucketForGlobTest3/c/a/a",
        "s3://emptybucketForGlobTest/c/a/a",
        "s3://emptybucketForGlobTest2/c/a/a",
        "s3://emptybucketForGlobTest3/c/a/a",
    ]


def test_s3_glob_stat(truncating_client, mocker):
    mocker.patch("megfile.s3_path.StatResult", side_effect=FakeStatResult)

    original_calls = (os.path.lexists, os.path.isdir, os.scandir)
    # without any wildcards
    assert_glob_stat(
        "s3://emptyBucketForGlobTest",
        [
            (
                "emptyBucketForGlobTest",
                "s3://emptyBucketForGlobTest",
                make_stat(isdir=True),
            )
        ],
    )
    assert_glob_stat(
        "s3://emptyBucketForGlobTest/",
        [("", "s3://emptyBucketForGlobTest/", make_stat(isdir=True))],
    )
    assert_glob_stat(
        "s3://bucketForGlobTest/1",
        [("1", "s3://bucketForGlobTest/1", make_stat(isdir=True))],
    )
    assert_glob_stat(
        "s3://bucketForGlobTest/1/",
        [("", "s3://bucketForGlobTest/1/", make_stat(isdir=True))],
    )
    assert_glob_stat(
        "s3://bucketForGlobTest/1/a",
        [
            ("a", "s3://bucketForGlobTest/1/a", make_stat(isdir=True)),
            ("a", "s3://bucketForGlobTest/1/a", make_stat(size=27)),  # 同名文件
        ],
    )
    assert_glob_stat(
        "s3://bucketForGlobTest/2/a/d/2.json",
        [("2.json", "s3://bucketForGlobTest/2/a/d/2.json", make_stat(size=6))],
    )

    # '*', all files and folders
    assert_glob_stat("s3://emptyBucketForGlobTest/*", [])
    assert_glob_stat(
        "s3://bucketForGlobTest/*",
        [
            ("1", "s3://bucketForGlobTest/1", make_stat(isdir=True)),
            ("2", "s3://bucketForGlobTest/2", make_stat(isdir=True)),
        ],
    )

    # all files under all direct subfolders
    assert_glob_stat(
        "s3://bucketForGlobTest/*/*",
        [
            ("a", "s3://bucketForGlobTest/1/a", make_stat(isdir=True)),
            ("a", "s3://bucketForGlobTest/1/a", make_stat(size=27)),  # 同名文件
            ("a", "s3://bucketForGlobTest/2/a", make_stat(isdir=True)),
        ],
    )

    assert_glob_stat(
        "s3://{bucketA/folderAB/fileAB,bucketC/folder/file}",
        [
            ("fileAB", "s3://bucketA/folderAB/fileAB", make_stat(size=6)),  # 同名文件
            ("file", "s3://bucketC/folder/file", make_stat(size=4)),  # 同名文件
        ],
    )

    assert_glob_stat(
        "s3://{bucket*/fileAB,bucketC/folder/file}",
        [
            ("fileAB", "s3://bucketA/fileAB", make_stat(size=6)),
            ("file", "s3://bucketC/folder/file", make_stat(size=4)),
        ],
    )

    # combination of '?' and []
    assert_glob_stat("s3://bucketForGlobTest/[2-3]/**/*?msg", [])
    assert_glob_stat(
        "s3://bucketForGlobTest/[13]/**/*?msg",
        [("A.msg", "s3://bucketForGlobTest/1/a/b/c/A.msg", make_stat(size=5))],
    )

    assert original_calls == (os.path.lexists, os.path.isdir, os.scandir)

    with pytest.raises(UnsupportedError):
        list(s3.s3_glob_stat("s3://"))

    with pytest.raises(S3BucketNotFoundError):
        list(s3.s3_glob_stat("s3:///key"))

    with pytest.raises(UnsupportedError):
        list(s3.s3_glob_stat("/"))

    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob_stat("s3://notExistsBucketForGlobTest/*", missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob_stat("s3://emptyBucketForGlobTest/*", missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob_stat("s3://bucketForGlobTest/3/**", missing_ok=False))
    with pytest.raises(FileNotFoundError):
        list(s3.s3_glob_stat("s3://bucketForGlobTest/1/**.notExists", missing_ok=False))


def test_s3_glob_stat_cross_bucket(truncating_client, mocker):
    mocker.patch("megfile.s3_path.StatResult", side_effect=FakeStatResult)

    original_calls = (os.path.lexists, os.path.isdir, os.scandir)
    # without any wildcards
    assert_glob_stat(
        r"s3://{emptyBucketForGlobTest,bucketForGlobTest2}",
        [
            (
                "emptyBucketForGlobTest",
                "s3://emptyBucketForGlobTest",
                make_stat(isdir=True),
            ),
            ("bucketForGlobTest2", "s3://bucketForGlobTest2", make_stat(isdir=True)),
        ],
    )
    assert_glob_stat(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/1",
        [
            ("1", "s3://bucketForGlobTest/1", make_stat(isdir=True)),
            ("1", "s3://bucketForGlobTest2/1", make_stat(isdir=True)),
            ("1", "s3://bucketForGlobTest3/1", make_stat(isdir=True)),
        ],
    )
    assert_glob_stat(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/1/",
        [
            ("", "s3://bucketForGlobTest/1/", make_stat(isdir=True)),
            ("", "s3://bucketForGlobTest2/1/", make_stat(isdir=True)),
        ],
    )
    assert_glob_stat(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/1/a",
        [
            ("a", "s3://bucketForGlobTest/1/a", make_stat(isdir=True)),
            ("a", "s3://bucketForGlobTest/1/a", make_stat(size=27)),  # 同名文件
            ("a", "s3://bucketForGlobTest2/1/a", make_stat(isdir=True)),
            ("a", "s3://bucketForGlobTest2/1/a", make_stat(size=27)),  # 同名文件
        ],
    )
    assert_glob_stat(
        r"s3://{bucketForGlobTest,bucketForGlobTest2}/2/a/d/2.json",
        [("2.json", "s3://bucketForGlobTest/2/a/d/2.json", make_stat(size=6))],
    )

    # '*', all files and folders
    assert_glob_stat("s3://emptyBucketForGlobTest/*", [])
    assert_glob_stat(
        r"s3://{bucketForGlobTest,emptyBucketForGlobTest,bucketForGlobTest2}/*",
        [
            ("1", "s3://bucketForGlobTest/1", make_stat(isdir=True)),
            ("2", "s3://bucketForGlobTest/2", make_stat(isdir=True)),
            ("1", "s3://bucketForGlobTest2/1", make_stat(isdir=True)),
        ],
    )

    # all files under all direct subfolders
    assert_glob_stat(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/*/*",
        [
            ("a", "s3://bucketForGlobTest/1/a", make_stat(isdir=True)),
            ("a", "s3://bucketForGlobTest/1/a", make_stat(size=27)),  # 同名文件
            ("a", "s3://bucketForGlobTest/2/a", make_stat(isdir=True)),
            ("a", "s3://bucketForGlobTest2/1/a", make_stat(isdir=True)),
            ("a", "s3://bucketForGlobTest2/1/a", make_stat(size=27)),  # 同名文件
            ("a", "s3://bucketForGlobTest3/1/a", make_stat(isdir=True)),
            ("a", "s3://bucketForGlobTest3/1/a", make_stat(size=27)),  # 同名文件
        ],
    )

    # combination of '?' and []
    assert_glob_stat(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/[2-3]/**/*?msg",
        [],
    )
    assert_glob_stat(
        r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/[13]/**/*?msg",
        [
            ("A.msg", "s3://bucketForGlobTest/1/a/b/c/A.msg", make_stat(size=5)),
            ("A.msg", "s3://bucketForGlobTest2/1/a/b/c/A.msg", make_stat(size=5)),
            ("A.msg", "s3://bucketForGlobTest2/1/a/c/A.msg", make_stat(size=5)),
            ("A.msg", "s3://bucketForGlobTest3/1/a/b/c/A.msg", make_stat(size=5)),
        ],
    )

    assert_glob_stat(
        r"s3://{notExistsBucketForGlobTest,bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/[13]/**/*?msg",
        [
            ("A.msg", "s3://bucketForGlobTest/1/a/b/c/A.msg", make_stat(size=5)),
            ("A.msg", "s3://bucketForGlobTest2/1/a/b/c/A.msg", make_stat(size=5)),
            ("A.msg", "s3://bucketForGlobTest2/1/a/c/A.msg", make_stat(size=5)),
            ("A.msg", "s3://bucketForGlobTest3/1/a/b/c/A.msg", make_stat(size=5)),
        ],
        missing_ok=False,
    )

    assert original_calls == (os.path.lexists, os.path.isdir, os.scandir)

    with pytest.raises(UnsupportedError):
        list(s3.s3_glob_stat("s3://"))

    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob_stat(
                r"s3://{notExistsBucketForGlobTest,notExistsBucketForGlobTest2}/*",
                missing_ok=False,
            )
        )
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob_stat(
                r"s3://{emptyBucketForGlobTest,notExistsBucketForGlobTest2/2}/*",
                missing_ok=False,
            )
        )
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob_stat(
                r"s3://{bucketForGlobTest,bucketForGlobTest2,bucketForGlobTest3}/3/**",
                missing_ok=False,
            )
        )
    with pytest.raises(FileNotFoundError):
        list(
            s3.s3_glob_stat(
                r"s3://{bucketForGlobTest,bucketForGlobTest2}/1/**.notExists",
                missing_ok=False,
            )
        )


def test_s3_split_magic():
    assert _s3_split_magic("s3://bucketA/{a/b,c}*/d") == ("s3://bucketA", "{a/b,c}*/d")


def test_group_s3path_by_bucket(truncating_client):
    assert sorted(
        _group_s3path_by_bucket("s3://{bucketA,bucketB}/{a,b}/{a,b}*/k/{c.json,d.json}")
    ) == sorted(
        [
            "s3://bucketA/{a,b}/{a,b}*/k/{c.json,d.json}",
            "s3://bucketB/{a,b}/{a,b}*/k/{c.json,d.json}",
        ]
    )

    assert sorted(
        _group_s3path_by_bucket("s3://{bucketA/a,bucketB/b}/c/{a,b}*/k/{c.json,d.json}")
    ) == sorted(
        [
            "s3://bucketA/a/c/{a,b}*/k/{c.json,d.json}",
            "s3://bucketB/b/c/{a,b}*/k/{c.json,d.json}",
        ]
    )

    assert sorted(
        _group_s3path_by_bucket(
            "s3://bucketForGlobTest*/{a,b}/{a,b}*/k/{c.json,d.json}"
        )
    ) == sorted(
        [
            "s3://bucketForGlobTest/{a,b}/{a,b}*/k/{c.json,d.json}",
            "s3://bucketForGlobTest2/{a,b}/{a,b}*/k/{c.json,d.json}",
            "s3://bucketForGlobTest3/{a,b}/{a,b}*/k/{c.json,d.json}",
        ]
    )


def test_group_s3path_by_prefix():
    assert sorted(
        _group_s3path_by_prefix("s3://bucketA/{a,b}/*/k/{c,d}.json")
    ) == sorted(["s3://bucketA/a/*/k/{c,d}.json", "s3://bucketA/b/*/k/{c,d}.json"])
    assert sorted(
        _group_s3path_by_prefix("s3://bucketA/{a,b/*}/k/{c.json,d.json}")
    ) == sorted(
        [
            "s3://bucketA/a/k/c.json",
            "s3://bucketA/a/k/d.json",
            "s3://bucketA/b/[*]/k/c.json",
            "s3://bucketA/b/[*]/k/d.json",
        ]
    )
    assert sorted(
        _group_s3path_by_prefix("s3://bucketA/{a,b}*/k/{c.json,d.json}")
    ) == sorted(["s3://bucketA/{a,b}*/k/{c.json,d.json}"])
    assert sorted(
        _group_s3path_by_prefix("s3://bucketA/{a,b}/{c,d}*/k/{e.json,f.json}")
    ) == sorted(
        [
            "s3://bucketA/a/{c,d}*/k/{e.json,f.json}",
            "s3://bucketA/b/{c,d}*/k/{e.json,f.json}",
        ]
    )
    assert sorted(
        _group_s3path_by_prefix("s3://bucketA/{a,b}/k/{c.json,d.json}")
    ) == sorted(
        [
            "s3://bucketA/a/k/c.json",
            "s3://bucketA/a/k/d.json",
            "s3://bucketA/b/k/c.json",
            "s3://bucketA/b/k/d.json",
        ]
    )
    assert sorted(_group_s3path_by_prefix("s3://bucketA/{a,b}/k/")) == sorted(
        ["s3://bucketA/a/k/", "s3://bucketA/b/k/"]
    )


def test_s3_save_as(s3_empty_client):
    content = b"value"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3.s3_save_as(BytesIO(content), "s3://bucket/result")
    body = s3_empty_client.get_object(Bucket="bucket", Key="result")["Body"].read()
    assert body == content


def test_s3_save_as_invalid(s3_empty_client):
    content = b"value"
    s3_empty_client.create_bucket(Bucket="bucket")

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_save_as(BytesIO(content), "s3://bucket/prefix/")
    assert "s3://bucket/prefix/" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_save_as(BytesIO(content), "s3:///key")
    assert "s3:///key" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_save_as(BytesIO(content), "s3://notExistBucket/fileAA")
    assert "notExistBucket" in str(error.value)


def test_s3_load_from(s3_empty_client):
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key", Body=b"value")
    content = s3.s3_load_from("s3://bucket/key")
    assert content.read() == b"value"


def test_s3_load_from_invalid(s3_empty_client):
    s3_empty_client.create_bucket(Bucket="bucket")

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_load_from("s3://bucket/prefix/")
    assert "s3://bucket/prefix/" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_load_from("s3:///key")
    assert "s3:///key" in str(error.value)

    with pytest.raises(PermissionError) as error:
        s3.s3_load_from("s3://notExistBucket/fileAA")
    assert "notExistBucket" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_load_from("s3://bucket/notExistFile")
    assert "s3://bucket/notExistFile" in str(error.value)


def test_s3_prefetch_open(s3_empty_client):
    content = b"test data for s3_prefetch_open"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key", Body=content)
    s3.s3_symlink("s3://bucket/key", "s3://bucket/symlink")

    with s3.s3_prefetch_open("s3://bucket/key") as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_prefetch_open("s3://bucket/key", followlinks=True) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_prefetch_open("s3://bucket/key", max_workers=1, block_size=1) as reader:
        assert reader.read() == content

    with s3.s3_prefetch_open("s3://bucket/symlink", followlinks=True) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_prefetch_open(
        "s3://bucket/symlink", max_workers=1, block_size=1, followlinks=True
    ) as reader:
        assert reader.read() == content

    with pytest.raises(S3BucketNotFoundError):
        s3.s3_prefetch_open("s3://", max_workers=1, block_size=1)

    with pytest.raises(ValueError):
        s3.s3_prefetch_open("s3://bucket/key", mode="wb")


def test_s3_share_cache_open(s3_empty_client):
    content = b"test data for s3_share_cache_open"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key", Body=content)
    s3.s3_symlink("s3://bucket/key", "s3://bucket/symlink")

    with s3.s3_share_cache_open("s3://bucket/key") as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_share_cache_open("s3://bucket/key", followlinks=True) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_prefetch_open("s3://bucket/key", max_workers=1, block_size=1) as reader:
        assert reader.read() == content

    with s3.s3_share_cache_open("s3://bucket/symlink", followlinks=True) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_prefetch_open(
        "s3://bucket/symlink", max_workers=1, block_size=1, followlinks=True
    ) as reader:
        assert reader.read() == content

    with pytest.raises(ValueError):
        s3.s3_share_cache_open("s3://bucket/key", mode="wb")


def test_s3_prefetch_open_raises_exceptions(s3_empty_client):
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key")

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_prefetch_open("s3://bucket")
    assert "s3://bucket" in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_prefetch_open("s3://bucket/")
    assert "s3://bucket/" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_prefetch_open("s3://bucket/keyy")
    assert "s3://bucket/keyy" in str(error.value)


def test_s3_pipe_open(s3_empty_client):
    content = b"test data for s3_pipe_open"
    s3_empty_client.create_bucket(Bucket="bucket")

    with s3.s3_pipe_open("s3://bucket/key", "wb") as writer:
        assert writer.name == "s3://bucket/key"
        assert writer.mode == "wb"
        writer.write(content)
    body = s3_empty_client.get_object(Bucket="bucket", Key="key")["Body"].read()
    assert body == content
    s3.s3_symlink("s3://bucket/key", "s3://bucket/symlink")

    with s3.s3_pipe_open("s3://bucket/key", "rb") as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_pipe_open("s3://bucket/key", "rb", followlinks=True) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_pipe_open("s3://bucket/symlink", "rb", followlinks=True) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with pytest.raises(ValueError):
        s3.s3_pipe_open("s3://bucket/key", mode="ab")


def test_s3_pipe_open_raises_exceptions(s3_empty_client):
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_pipe_open("s3://bucket", "wb")
    assert "s3://bucket" in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_pipe_open("s3://bucket/", "wb")
    assert "s3://bucket/" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        with s3.s3_pipe_open("s3://bucket/key", "wb") as f:
            f.write(b"test")
    assert "bucket" in str(error.value)

    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key")

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_pipe_open("s3://bucket", "rb")
    assert "s3://bucket" in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_pipe_open("s3://bucket/", "rb")
    assert "s3://bucket/" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_pipe_open("s3://bucket/keyy", "rb")
    assert "s3://bucket/keyy" in str(error.value)


def test_s3_cached_open(mocker, s3_empty_client, fs):
    content = b"test data for s3_cached_open"
    s3_empty_client.create_bucket(Bucket="bucket")
    cache_path = "/tmp/tempfile"

    with s3.s3_cached_open("s3://bucket/key", "wb", cache_path=cache_path) as writer:
        assert writer.name == "s3://bucket/key"
        assert writer.mode == "wb"
        writer.write(content)
    body = s3_empty_client.get_object(Bucket="bucket", Key="key")["Body"].read()
    assert body == content
    s3.s3_symlink("s3://bucket/key", "s3://bucket/symlink")

    with s3.s3_cached_open("s3://bucket/key", "rb", cache_path=cache_path) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_cached_open(
        "s3://bucket/key", "rb", cache_path=cache_path, followlinks=True
    ) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_cached_open(
        "s3://bucket/symlink", "rb", cache_path=cache_path, followlinks=True
    ) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_cached_open(
        "s3://bucket/symlink", "r+b", cache_path=cache_path, followlinks=True
    ) as reader:
        pass

    with pytest.raises(ValueError):
        s3.s3_cached_open("s3://bucket/key", mode="abc")


def test_s3_cached_open_raises_exceptions(mocker, s3_empty_client, fs):
    cache_path = "/tmp/tempfile"

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_cached_open("s3://bucket", "wb", cache_path=cache_path)
    assert "s3://bucket" in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_cached_open("s3://bucket/", "wb", cache_path=cache_path)
    assert "s3://bucket/" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        with s3.s3_cached_open("s3://bucket/key", "wb", cache_path=cache_path) as f:
            f.write(b"test")
    assert "bucket" in str(error.value)

    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key")

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_cached_open("s3://bucket", "rb", cache_path=cache_path)
    assert "s3://bucket" in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_cached_open("s3://bucket/", "rb", cache_path=cache_path)
    assert "s3://bucket/" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_cached_open("s3://bucket/keyy", "rb", cache_path=cache_path)
    assert "s3://bucket/keyy" in str(error.value)


def test_s3_buffered_open(mocker, s3_empty_client, fs):
    content = b"test data for s3_buffered_open"
    s3_empty_client.create_bucket(Bucket="bucket")

    writer = s3.s3_buffered_open("s3://bucket/key", "wb")
    assert isinstance(writer, s3_path.S3BufferedWriter)

    writer = s3.s3_buffered_open("s3://bucket/key", "ab", cache_path="/test")
    assert isinstance(writer, S3CachedHandler)

    writer = s3.s3_buffered_open("s3://bucket/key", "wb", limited_seekable=True)
    assert isinstance(writer, s3_path.S3LimitedSeekableWriter)

    writer = s3.s3_buffered_open("s3://bucket/key.pkl", "wb")
    assert isinstance(writer, BufferedWriter)

    s3_empty_client.put_object(Bucket="bucket", Key="key", Body=pickle.dumps("test"))
    reader = s3.s3_buffered_open("s3://bucket/key", "rb")
    assert isinstance(reader, BufferedReader)

    s3_empty_client.put_object(Bucket="bucket", Key="key.pkl", Body=content)
    reader = s3.s3_buffered_open("s3://bucket/key.pkl", "rb")
    assert isinstance(reader, BufferedReader)

    s3_empty_client.put_object(Bucket="bucket", Key="key", Body=content)
    reader = s3.s3_buffered_open("s3://bucket/key", "rb", block_forward=1)
    assert isinstance(reader, s3_path.S3PrefetchReader)
    assert reader._block_forward == 1

    reader = s3.s3_buffered_open("s3://bucket/key", "rb", share_cache_key="share")
    assert isinstance(reader, s3_path.S3ShareCacheReader)

    with s3.s3_buffered_open("s3://bucket/key", "wb") as writer:
        assert writer.name == "s3://bucket/key"
        assert writer.mode == "wb"
        writer.write(content)
    body = s3_empty_client.get_object(Bucket="bucket", Key="key")["Body"].read()
    assert body == content

    with s3.s3_buffered_open("s3://bucket/key", "rb") as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with s3.s3_buffered_open("s3://bucket/key", "rb", followlinks=True) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.read() == content

    with pytest.raises(ValueError):
        with s3.s3_buffered_open("s3://bucket/key", "test_mode"):
            pass

    with s3.s3_buffered_open("s3://bucket/key", "r+b") as reader:
        pass


def test_s3_buffered_open_raises_exceptions(mocker, s3_empty_client, fs):
    with pytest.raises(IsADirectoryError) as error:
        s3.s3_buffered_open("s3://bucket", "wb")
    assert "s3://bucket" in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_buffered_open("s3://bucket/", "wb")
    assert "s3://bucket/" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        with s3.s3_buffered_open("s3://bucket/key", "wb") as f:
            f.write(b"test")
    assert "bucket" in str(error.value)

    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key")

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_buffered_open("s3://bucket", "rb")
    assert "s3://bucket" in str(error.value)

    with pytest.raises(IsADirectoryError) as error:
        s3.s3_buffered_open("s3://bucket/", "rb")
    assert "s3://bucket/" in str(error.value)

    with pytest.raises(FileNotFoundError) as error:
        s3.s3_buffered_open("s3://bucket/keyy", "rb")
    assert "s3://bucket/keyy" in str(error.value)


def test_s3_memory_open(s3_empty_client):
    content = b"test data for s3_memory_open"
    s3_empty_client.create_bucket(Bucket="bucket")

    with s3.s3_memory_open("s3://bucket/key", "wb") as writer:
        writer.write(content)
    body = s3_empty_client.get_object(Bucket="bucket", Key="key")["Body"].read()
    assert body == content
    s3.s3_symlink("s3://bucket/key", "s3://bucket/symlink")

    with s3.s3_memory_open("s3://bucket/key", "rb") as reader:
        assert reader.read() == content

    with s3.s3_memory_open("s3://bucket/key", "rb", followlinks=True) as reader:
        assert reader.read() == content

    with s3.s3_memory_open("s3://bucket/symlink", "rb", followlinks=True) as reader:
        assert reader.read() == content

    with s3.s3_memory_open("s3://bucket/symlink", "r+b", followlinks=True) as reader:
        pass

    with pytest.raises(ValueError):
        with s3.s3_memory_open("s3://bucket/key", "test_mode"):
            pass


def test_s3_open(s3_empty_client):
    content = b"test data for s3_open"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key", Body=content)

    writer = s3.s3_open("s3://bucket/key", "wb")
    assert isinstance(writer, s3_path.S3BufferedWriter)

    reader = s3.s3_open("s3://bucket/key", "rb")
    assert isinstance(reader, s3_path.S3PrefetchReader)

    writer = s3.s3_open("s3://bucket/key", "ab")
    assert isinstance(writer, S3MemoryHandler)

    writer = s3.s3_open("s3://bucket/key", "wb+")
    assert isinstance(writer, S3MemoryHandler)


def test_s3_getmd5(s3_empty_client):
    s3_url = "s3://bucket/key"
    content = b"bytes"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key", Body=content)

    hash_md5 = hashlib.md5()  # nosec
    hash_md5.update(content)

    assert s3.s3_getmd5(s3_url) == hash_md5.hexdigest()
    assert s3.s3_getmd5(s3_url, recalculate=True) == hash_md5.hexdigest()

    s3_dir_url = "s3://bucket"
    hash_md5_dir = hashlib.md5()  # nosec
    hash_md5_dir.update(hash_md5.hexdigest().encode())
    assert s3.s3_getmd5(s3_dir_url) == hash_md5_dir.hexdigest()

    with pytest.raises(S3BucketNotFoundError):
        s3.s3_getmd5("s3://")

    symlink_s3_url = "s3://bucket/key.lnk"
    s3.s3_symlink(s3_url, symlink_s3_url)
    assert s3.s3_getmd5(s3_url, followlinks=True) == hash_md5.hexdigest()
    assert (
        s3.s3_getmd5(s3_url, recalculate=True, followlinks=True) == hash_md5.hexdigest()
    )


def test_s3_getmd5_None(s3_empty_client):
    s3_url = "s3://bucket/key"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key")

    assert s3.s3_getmd5(s3_url) == "d41d8cd98f00b204e9800998ecf8427e"


def test_s3_load_content(s3_empty_client):
    content = b"test data for s3_load_content"
    s3_empty_client.create_bucket(Bucket="bucket")

    with s3.s3_memory_open("s3://bucket/key", "wb") as writer:
        writer.write(content)

    assert s3.s3_load_content("s3://bucket/key") == content
    assert s3.s3_load_content("s3://bucket/key", 1) == content[1:]
    assert s3.s3_load_content("s3://bucket/key", stop=-1) == content[:-1]
    assert s3.s3_load_content("s3://bucket/key", 4, 7) == content[4:7]

    with pytest.raises(S3BucketNotFoundError):
        s3.s3_load_content("s3://", 5, 2)

    with pytest.raises(S3IsADirectoryError):
        s3.s3_load_content("s3://bucket/", 5, 2)

    with pytest.raises(ValueError):
        s3.s3_load_content("s3://bucket/key", 5, 2)


def test_s3_load_content_retry(s3_empty_client, mocker):
    content = b"test data for s3_load_content"
    s3_empty_client.create_bucket(Bucket="bucket")

    with s3.s3_memory_open("s3://bucket/key", "wb") as writer:
        writer.write(content)

    read_error = botocore.exceptions.IncompleteReadError(
        actual_bytes=0, expected_bytes=1
    )
    mocker.patch.object(s3_empty_client, "get_object", side_effect=read_error)
    sleep = mocker.patch.object(time, "sleep")
    with pytest.raises(Exception) as error:
        s3.s3_load_content("s3://bucket/key")
    assert (
        error.value.__str__()
        == translate_s3_error(read_error, "s3://bucket/key").__str__()
    )
    assert sleep.call_count == s3_path.max_retries - 1


def test_s3_cacher(s3_empty_client, fs, mocker):
    content = b"test data for s3_load_content"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="key", Body=content)

    with s3_path.S3Cacher("s3://bucket/key", "/path/to/file") as path:
        assert path == "/path/to/file"
        assert os.path.exists(path)
        with open(path, "rb") as fp:
            assert fp.read() == content

    assert not os.path.exists(path)

    with s3_path.S3Cacher("s3://bucket/key", "/path/to/file", "w") as path:
        assert path == "/path/to/file"
        assert not os.path.exists(path)
        with open(path, "wb") as fp:
            assert fp.write(content)

    assert not os.path.exists(path)
    assert s3.s3_load_content("s3://bucket/key") == content

    with s3_path.S3Cacher("s3://bucket/key", "/path/to/file", "a") as path:
        assert path == "/path/to/file"
        assert os.path.exists(path)
        with open(path, "rb+") as fp:
            assert fp.read() == content
            assert fp.write(content)

    assert not os.path.exists(path)
    assert s3.s3_load_content("s3://bucket/key") == content * 2

    mocker.patch("megfile.s3_path.generate_cache_path", return_value="/test")
    with s3_path.S3Cacher("s3://bucket/key") as path:
        assert path == "/test"

    with pytest.raises(ValueError):
        with s3_path.S3Cacher("s3://bucket/key", "/path/to/file", "rb"):
            pass


@pytest.fixture
def s3_empty_client_with_patch(mocker):
    times = 0

    def list_objects_v2(*args, **kwargs):
        nonlocal times
        times += 1
        is_truncated = True
        if times > 4:
            is_truncated = False
        return {
            "IsTruncated": is_truncated,
            "NextContinuationToken": times,
            "Contents": ["test"],
        }

    def error_method(*args, **kwargs):
        raise S3UnknownError(Exception(), "")

    with mock_aws():
        client = boto3.client("s3")
        client.list_objects_v2 = list_objects_v2
        client.head_bucket = error_method
        client.head_object = error_method
        mocker.patch("megfile.s3_path.get_s3_client", return_value=client)
        yield client


def test_list_objects_recursive(s3_empty_client_with_patch):
    assert list(
        _list_objects_recursive(
            s3_empty_client_with_patch, "bucket", "prefix", "delimiter"
        )
    ) == [
        {"IsTruncated": True, "Contents": ["test"], "NextContinuationToken": 1},
        {"IsTruncated": True, "Contents": ["test"], "NextContinuationToken": 2},
        {"IsTruncated": True, "Contents": ["test"], "NextContinuationToken": 3},
        {"IsTruncated": True, "Contents": ["test"], "NextContinuationToken": 4},
        {"IsTruncated": False, "Contents": ["test"], "NextContinuationToken": 5},
    ]


def test_s3_split_magic_ignore_brace():
    with pytest.raises(ValueError):
        _s3_split_magic_ignore_brace("")

    assert _s3_split_magic_ignore_brace("s3://bucket*") == ("", "s3://bucket*")


def test_group_s3path_by_prefix():
    assert _group_s3path_by_prefix("s3://bucket*/test*") == ["s3://bucket*/test*"]


@pytest.fixture
def s3_empty_client_with_patch_for_has_bucket(mocker):
    def head_bucket_without_permission(*args, **kwargs):
        raise botocore.exceptions.ClientError({"Error": {"Code": "403"}}, "head_bucket")

    with mock_aws():
        client = boto3.client("s3")
        client.head_bucket = head_bucket_without_permission
        mocker.patch("megfile.s3_path.get_s3_client", return_value=client)
        yield client


def test_s3_hasbucket(s3_empty_client_with_patch_for_has_bucket):
    assert s3.s3_hasbucket("s3://") is False
    assert s3.s3_hasbucket("s3://bucketA") is False

    s3_empty_client_with_patch_for_has_bucket.create_bucket(Bucket="bucketA")
    s3_empty_client_with_patch_for_has_bucket.put_object(
        Bucket="bucketA", Key="test", Body=b"test"
    )
    assert s3.s3_hasbucket("s3://bucketA") is True
    assert (
        s3_empty_client_with_patch_for_has_bucket.get_object(
            Bucket="bucketA", Key="test"
        )["Body"].read()
        == b"test"
    )

    with (
        patch.object(
            s3_empty_client_with_patch_for_has_bucket,
            "head_bucket",
            side_effect=S3UnknownError(Exception(), "test"),
        ),
        pytest.raises(S3UnknownError),
    ):
        s3.s3_hasbucket("s3://bucketA")

    with (
        patch.object(
            s3_empty_client_with_patch_for_has_bucket,
            "head_bucket",
            side_effect=S3PermissionError("test"),
        ),
        patch.object(
            s3_empty_client_with_patch_for_has_bucket,
            "list_objects_v2",
            side_effect=S3PermissionError("test"),
        ),
        pytest.raises(S3PermissionError),
    ):
        s3.s3_hasbucket("s3://bucketA")


def test_error(s3_empty_client_with_patch, mocker):
    class FakeAccess(Enum):
        READ = 1
        WRITE = 2
        ERROR = 3

    mocker.patch("megfile.s3_path.Access", FakeAccess)
    mocker.patch("megfile.s3.s3_islink", return_value=False)
    with pytest.raises(Exception):
        s3.s3_access("s3://")
    with pytest.raises(TypeError):
        s3.s3_access("s3://bucketA/fileAA", FakeAccess.ERROR)
    with pytest.raises(S3UnknownError):
        s3.s3_access("s3://bucketA/fileAA", FakeAccess.READ)

    with pytest.raises(S3UnknownError):
        s3.s3_isfile("s3://bucketA/fileAA")

    assert s3.s3_isdir("s3://bucket/dir") is True


def test_exists_with_symlink(s3_empty_client):
    src_url = "s3://bucket/src"
    dst_url = "s3://bucket/dst"
    content = b"bytes"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="src", Body=content)

    s3.s3_symlink(src_url, dst_url)
    s3.s3_rename("s3://bucket/src", "s3://bucket/src_new")

    assert s3.s3_exists(dst_url, followlinks=False) is True
    assert s3.s3_exists(dst_url, followlinks=True) is False


def test_symlink(s3_empty_client):
    src_url = "s3://bucket/src"
    dst_url = "s3://bucket/dst"
    dst_dst_url = "s3://bucket/dst_dst"
    content = b"bytes"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="src", Body=content)

    assert s3.s3_exists(dst_url) is False
    s3.s3_symlink(src_url, dst_url)
    s3.s3_symlink(dst_url, dst_dst_url)

    assert s3.s3_exists(dst_url) is True
    assert s3.s3_exists(dst_dst_url) is True

    assert s3.s3_islink(dst_url) is True
    assert s3.s3_islink(dst_dst_url) is True

    assert s3.s3_readlink(dst_url) == src_url
    assert s3.s3_readlink(dst_dst_url) == src_url

    with pytest.raises(S3BucketNotFoundError):
        s3.s3_symlink("s3:///notExistFolder", dst_url)
    with pytest.raises(S3BucketNotFoundError):
        s3.s3_symlink(src_url, "s3:///notExistFolder")
    with pytest.raises(S3IsADirectoryError):
        s3.s3_symlink(src_url, "s3://bucket/dst/")
    with pytest.raises(S3NameTooLongError):
        s3.s3_symlink("s3://notExistFolder" + "/name/too/long" * 100, dst_url)


def test_islink(s3_empty_client):
    assert s3.s3_islink("s3:///") is False
    assert s3.s3_islink("s3://bucket/src/") is False
    assert s3.s3_islink("s3://bucket/not") is False


def test_read_symlink(s3_empty_client):
    src_url = "s3://bucket/src"
    dst_url = "s3://bucket/dst"
    dst_dst_url = "s3://bucket/dst_dst"
    content = b"bytes"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="src", Body=content)

    s3.s3_symlink(src_url, dst_url)
    s3.s3_symlink(dst_url, dst_dst_url)

    assert s3.s3_readlink(dst_url) == src_url
    assert s3.s3_readlink(dst_dst_url) == src_url
    assert s3.s3_islink("s3://bucket/src/") is False

    with pytest.raises(S3NotALinkError):
        s3.s3_readlink(src_url)
    with pytest.raises(S3BucketNotFoundError):
        s3.s3_readlink("s3:///notExistFolder")
    with pytest.raises(S3IsADirectoryError):
        s3.s3_readlink("s3://bucket/dst/")


def test_isfile_symlink(s3_empty_client):
    src_url = "s3://bucket/src"
    dst_url = "s3://bucket/dst"
    dst_dst_url = "s3://bucket/dst_dst"
    content = b"bytes"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.put_object(Bucket="bucket", Key="src", Body=content)

    s3.s3_symlink(src_url, dst_url)
    s3.s3_symlink(dst_url, dst_dst_url)

    assert s3.s3_isfile(dst_url, followlinks=True) is True
    assert s3.s3_isfile(dst_dst_url, followlinks=True) is True

    s3.s3_rename(src_url, "s3://bucket/src_new")
    assert s3.s3_isfile(dst_url, followlinks=True) is False
    assert s3.s3_isfile(dst_dst_url, followlinks=True) is False


def test__s3_get_metadata(mocker):
    class FakeClient:
        def head_object(self, *args, **kwargs):
            raise botocore.exceptions.ClientError(
                error_response=dict(Error=dict(Code="403")),
                operation_name="head_object",
            )

    mocker.patch("megfile.s3_path.get_s3_client", return_value=FakeClient())

    with pytest.raises(S3PermissionError):
        s3_path.S3Path("s3://bucket/key")._s3_get_metadata() == {}


def test_symlink_relevant_functions(s3_empty_client, fs):
    path = "/tmp/download_file"
    src_url = "s3://bucket/src"
    dst_url = "s3://bucket/dst"
    copy_url = "s3://bucket/copy"
    dst_dst_url = "s3://bucket/dst_dst"
    A_src_url = "s3://bucketA/pass/src"
    A_dst_url = "s3://bucketA/pass/dst"
    A_dst_dst_url = "s3://bucketA/pass/z_dst"
    A_rename_url = "s3://bucketA/pass/rename"
    sync_url = "s3://bucketA/sync/src"

    content = b"bytes"
    s3_empty_client.create_bucket(Bucket="bucket")
    s3_empty_client.create_bucket(Bucket="bucketA")
    s3_empty_client.put_object(Bucket="bucket", Key="src", Body=content)
    s3_empty_client.put_object(Bucket="bucketA", Key="pass/src", Body=content)

    s3.s3_symlink(src_url, dst_url)
    s3.s3_symlink(A_src_url, A_dst_url)
    s3.s3_symlink(A_dst_url, A_dst_dst_url)
    s3.s3_symlink(dst_url, dst_dst_url)
    s3.s3_copy(dst_url, copy_url, followlinks=True)

    assert s3_path.S3Path("s3://")._s3_get_metadata() == {}
    assert s3_path.S3Path("s3://bucket")._s3_get_metadata() == {}
    assert s3.s3_islink(dst_url) is True
    assert s3.s3_exists(A_dst_dst_url) is True
    assert s3.s3_getsize(dst_url, follow_symlinks=False) == 0
    assert s3.s3_getsize(dst_url, follow_symlinks=True) == s3.s3_getsize(
        src_url, follow_symlinks=True
    )
    assert s3.s3_getmtime(dst_url, follow_symlinks=True) == s3.s3_getmtime(
        src_url, follow_symlinks=True
    )
    assert s3.s3_getmd5(dst_url, followlinks=True) == s3.s3_getmd5(
        src_url, followlinks=True
    )

    assert s3.s3_load_from(dst_url).read() == b""
    assert s3.s3_load_content(dst_url) == b""

    assert list(s3.s3_scan_stat(A_dst_url))[0].is_symlink() is True
    s3.s3_sync(A_dst_url, sync_url)
    assert s3.s3_exists(sync_url, followlinks=False) is True
    assert s3.s3_islink(sync_url) is True
    assert list(s3.s3_scan_stat(sync_url))[0].is_symlink() is True
    assert list(s3.s3_scan_stat(sync_url, followlinks=True))[0].is_symlink() is True
    assert list(s3.s3_scan_stat(A_dst_url, followlinks=True))[0].is_symlink() is True

    s3.s3_remove(sync_url)
    s3.s3_sync(A_dst_url, sync_url, followlinks=True)
    assert s3.s3_exists(sync_url, followlinks=False) is True
    assert s3.s3_islink(sync_url) is False

    assert list(s3.s3_scan_stat(sync_url))[0].is_symlink() is False
    assert list(s3.s3_scan_stat(sync_url, followlinks=True))[0].is_symlink() is False
    assert list(s3.s3_scan_stat(src_url, followlinks=True))[0].is_symlink() is False

    for scan_entry in s3.s3_scan_stat("s3://bucketA/pass/", followlinks=True):
        if scan_entry.name == dst_url:
            scan_entry.stat.is_symlink() is True
        elif scan_entry.name == src_url:
            scan_entry.stat.is_symlink() is False
        elif scan_entry.name == A_dst_dst_url:
            scan_entry.stat.is_symlink() is True

    file_entries = list(s3.s3_scandir("s3://bucketA/pass/"))
    assert len(file_entries) == 3
    for file_entry in file_entries:
        if file_entry.name == "src":
            assert file_entry.stat.isdir is False
            assert file_entry.stat.islnk is False
            assert file_entry.stat.is_symlink() is False
            assert file_entry.is_symlink() is False
        else:
            assert file_entry.stat.isdir is False
            assert file_entry.stat.islnk is True
            assert file_entry.stat.is_file() is True
            assert file_entry.stat.is_symlink() is True
            assert file_entry.is_symlink() is True

    s3.s3_download(dst_url, path, followlinks=True)
    with open(path, "rb") as f:
        assert f.read() == content
    with s3.s3_buffered_open(dst_url, followlinks=True) as reader:
        assert reader.read() == content

    s3.s3_download(dst_dst_url, path)
    with open(path, "rb") as f:
        assert f.read() == b""
    with s3.s3_buffered_open(dst_dst_url) as reader:
        assert reader.read() == b""

    assert s3.s3_exists(A_dst_url) is True
    assert (
        list(s3_path._s3_glob_stat_single_path(dst_url, followlinks=True))[
            0
        ].is_symlink()
        is True
    )
    assert (
        list(s3_path._s3_glob_stat_single_path(src_url, followlinks=True))[
            0
        ].is_symlink()
        is False
    )
    assert (
        list(s3_path._s3_glob_stat_single_path(dst_url, followlinks=True))[0].stat.size
        == list(s3_path._s3_glob_stat_single_path(src_url, followlinks=True))[
            0
        ].stat.size
    )
    assert (
        list(s3_path._s3_glob_stat_single_path(dst_url, followlinks=True))[0].stat.mtime
        == list(s3_path._s3_glob_stat_single_path(src_url, followlinks=True))[
            0
        ].stat.mtime
    )

    assert list(s3.s3_glob_stat(dst_url))[0].is_symlink() is True
    assert list(s3.s3_glob_stat(src_url))[0].is_symlink() is False

    s3.s3_rename(A_dst_url, A_rename_url)
    s3.s3_islink(A_rename_url)
    s3.s3_exists(A_dst_url) is False
    s3.s3_exists(A_rename_url) is True

    s3.s3_remove(A_src_url)
    s3.s3_exists(A_dst_url) is True
    s3.s3_exists(A_dst_url, followlinks=True) is False

    s3.s3_remove(A_rename_url)
    s3.s3_remove(A_dst_dst_url)
    s3.s3_remove(sync_url)
    s3_empty_client.delete_bucket(Bucket="bucketA")
    assert s3.s3_access(A_dst_dst_url, Access.READ) is False


def test_s3_concat_small_file(s3_empty_client, mocker):
    s3_empty_client.create_bucket(Bucket="bucket")

    with s3.s3_open("s3://bucket/a", "w") as f:
        f.write("a")

    with s3.s3_open("s3://bucket/b", "w") as f:
        f.write("b")

    with s3.s3_open("s3://bucket/c", "w") as f:
        f.write("c")

    with s3.s3_open("s3://bucket/empty", "w") as f:
        f.write("")

    s3.s3_concat(
        [
            "s3://bucket/a",
            "s3://bucket/b",
            "s3://bucket/c",
            "s3://bucket/empty",
        ],
        "s3://bucket/d",
    )
    with s3.s3_open("s3://bucket/d", "r") as f:
        assert f.read() == "abc"

    mocker.patch("megfile.s3_path.MultiPartWriter.upload_part_by_paths")
    close = mocker.patch("megfile.s3_path.MultiPartWriter.close")

    s3.s3_concat(
        [
            "s3://bucket/a",
            "s3://bucket/b",
            "s3://bucket/c",
            "s3://bucket/empty",
        ],
        "s3://bucket/e",
        block_size=0,
    )
    assert close.call_count == 1


def test_s3_concat_case1(s3_empty_client):
    one_mb_block = b"0" * 1024 * 1024
    s3_empty_client.create_bucket(Bucket="bucket")

    with s3.s3_open("s3://bucket/a", "wb") as f:
        f.write(b"a")

    with s3.s3_open("s3://bucket/b", "wb") as f:
        for _ in range(18):
            f.write(one_mb_block)

    with s3.s3_open("s3://bucket/c", "wb") as f:
        f.write(b"c")

    assert s3_path._group_src_paths_by_block(
        ["s3://bucket/a", "s3://bucket/b", "s3://bucket/c"]
    ) == [
        [("s3://bucket/a", None), ("s3://bucket/b", f"bytes=0-{8 * 1024 * 1024 - 2}")],
        [("s3://bucket/b", f"bytes={8 * 1024 * 1024 - 1}-{18 * 1024 * 1024 - 1}")],
        [("s3://bucket/c", None)],
    ]
    s3.s3_concat(["s3://bucket/a", "s3://bucket/b", "s3://bucket/c"], "s3://bucket/d")
    with s3.s3_open("s3://bucket/d", "rb") as f:
        assert f.read(1) == b"a"
        for _ in range(18):
            assert f.read(1024 * 1024) == one_mb_block
        assert f.read() == b"c"


def test_s3_concat_case2(s3_empty_client):
    one_mb_block = b"0" * 1024 * 1024
    s3_empty_client.create_bucket(Bucket="bucket")

    for index in range(15):
        with s3.s3_open(f"s3://bucket/{index}", "wb") as f:
            f.write(one_mb_block)

    assert s3_path._group_src_paths_by_block(
        [f"s3://bucket/{index}" for index in range(15)]
    ) == [
        [(f"s3://bucket/{index}", None) for index in range(8)],
        [(f"s3://bucket/{index}", None) for index in range(8, 15)],
    ]

    s3.s3_concat([f"s3://bucket/{index}" for index in range(15)], "s3://bucket/all")
    assert s3.s3_stat("s3://bucket/all").size == 15 * 1024 * 1024


def test_s3_concat_case3(s3_empty_client):
    one_mb_block = b"0" * 1024 * 1024
    s3_empty_client.create_bucket(Bucket="bucket")

    for index in range(8):
        with s3.s3_open(f"s3://bucket/{index}", "wb") as f:
            f.write(one_mb_block)

    with s3.s3_open("s3://bucket/8", "wb") as f:
        for _ in range(10):
            f.write(one_mb_block)

    with s3.s3_open("s3://bucket/9", "wb") as f:
        f.write(b"9")

    assert s3_path._group_src_paths_by_block(
        [f"s3://bucket/{index}" for index in range(10)]
    ) == [
        [(f"s3://bucket/{index}", None) for index in range(8)],
        [("s3://bucket/8", None)],
        [("s3://bucket/9", None)],
    ]

    s3.s3_concat([f"s3://bucket/{index}" for index in range(10)], "s3://bucket/all")
    assert s3.s3_stat("s3://bucket/all").size == 18 * 1024 * 1024 + 1
    assert s3.s3_load_content("s3://bucket/all", start=18 * 1024 * 1024) == b"9"


def test_s3_concat_case4(s3_empty_client):
    one_mb_block = b"0" * 1024 * 1024
    s3_empty_client.create_bucket(Bucket="bucket")

    with s3.s3_open("s3://bucket/0", "wb") as f:
        f.write(one_mb_block)

    with s3.s3_open("s3://bucket/1", "wb") as f:
        for _ in range(9):
            f.write(one_mb_block)

    with s3.s3_open("s3://bucket/2", "wb") as f:
        f.write(b"9")

    assert s3_path._group_src_paths_by_block(
        [f"s3://bucket/{index}" for index in range(3)]
    ) == [[("s3://bucket/0", None), ("s3://bucket/1", None)], [("s3://bucket/2", None)]]

    s3.s3_concat([f"s3://bucket/{index}" for index in range(3)], "s3://bucket/all")
    assert s3.s3_stat("s3://bucket/all").size == 10 * 1024 * 1024 + 1
    assert s3.s3_load_content("s3://bucket/all", start=10 * 1024 * 1024) == b"9"

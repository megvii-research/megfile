import io
import os
import tempfile
from unittest.mock import patch

import boto3
import pytest

from megfile.s3_path import (
    S3Path,
    _patch_make_request,
    get_access_token,
    get_endpoint_url,
)


def test_absolute():
    assert S3Path("s3://foo/bar").is_absolute()
    assert S3Path("foo/bar").is_absolute()


def test_reserved():
    assert not S3Path("s3://foo/bar").is_reserved()
    assert not S3Path("foo/bar").is_reserved()


def test_joinpath():
    assert S3Path("s3://foo").joinpath("bar") == S3Path("s3://foo/bar")
    assert S3Path("s3://foo").joinpath(S3Path("bar")) == S3Path("s3://foo/bar")
    assert S3Path("s3://foo").joinpath("bar", "baz") == S3Path("s3://foo/bar/baz")

    assert S3Path("foo").joinpath("bar") == S3Path("foo/bar")
    assert S3Path("foo").joinpath(S3Path("bar")) == S3Path("foo/bar")
    assert S3Path("foo").joinpath("bar", "baz") == S3Path("foo/bar/baz")


def test_match():
    assert S3Path("a/b.py").match("*.py")
    assert S3Path("s3://a/b/c.py").match("b/*.py")
    assert not S3Path("s3://a/b/c.py").match("s3://a/*.py")
    assert S3Path("s3://a.py").match("s3://*.py")
    assert S3Path("a/b.py").match("s3://a/b.py")
    assert not S3Path("a/b.py").match("s4://*.py")
    assert not S3Path("a/b.py").match("s3://*.py")
    assert not S3Path("a/b.py").match("*.Py")


def test_relative_to():
    path = S3Path("s3://foo/bar")
    assert path.relative_to("s3://") == S3Path("foo/bar")
    assert path.relative_to("s3://foo") == S3Path("bar")
    assert path.relative_to("foo") == S3Path("bar")
    with pytest.raises(ValueError):
        path.relative_to("s3://baz")


def test_relative_to_relative():
    path = S3Path("foo/bar/baz")
    assert path.relative_to("foo/bar") == S3Path("baz")
    assert path.relative_to("foo") == S3Path("bar/baz")
    with pytest.raises(ValueError):
        path.relative_to("baz")


def test_with_name():
    path = S3Path("s3://foo/bar.tar.gz")
    assert path.with_name("baz.py") == S3Path("s3://foo/baz.py")
    path = S3Path("s3://")

    # with pytest.raises(ValueError):
    #     path.with_name('baz.py')

    path = S3Path("foo/bar.tar.gz")
    assert path.with_name("baz.py") == S3Path("foo/baz.py")


def test_with_suffix():
    path = S3Path("s3://foo/bar.tar.gz")
    assert path.with_suffix(".bz2") == S3Path("s3://foo/bar.tar.bz2")
    path = S3Path("baz")
    assert path.with_suffix(".txt") == S3Path("baz.txt")
    path = S3Path("baz.txt")
    assert path.with_suffix("") == S3Path("baz")


def test_utime():
    with pytest.raises(NotImplementedError):
        S3Path("s3://foo/bar.tar.gz").utime(0, 0)


@patch.dict(
    os.environ,
    {
        "AWS_ACCESS_KEY_ID": "default-key",
        "AWS_SECRET_ACCESS_KEY": "default-secret",
        "AWS_SESSION_TOKEN": "default-token",
        "TEST__AWS_ACCESS_KEY_ID": "test-key",
        "TEST__AWS_SECRET_ACCESS_KEY": "test-secret",
        "TEST__AWS_SESSION_TOKEN": "test-token",
    },
)
def test_get_access_token_from_env():
    assert get_access_token() == (
        "default-key",
        "default-secret",
        "default-token",
    )
    assert get_access_token("test") == ("test-key", "test-secret", "test-token")


@patch.dict(
    os.environ,
    {
        "AWS_PROFILE": "test",
        "AWS_ACCESS_KEY_ID": "default-key",
        "AWS_SECRET_ACCESS_KEY": "default-secret",
        "AWS_SESSION_TOKEN": "default-token",
        "TEST__AWS_ACCESS_KEY_ID": "test-key",
        "TEST__AWS_SECRET_ACCESS_KEY": "test-secret",
        "TEST__AWS_SESSION_TOKEN": "test-token",
    },
)
def test_get_access_token_from_env2():
    assert get_access_token() == ("test-key", "test-secret", "test-token")


@pytest.fixture
def s3_session(mocker):
    def get_s3_session_without_cache(profile_name=None) -> boto3.Session:
        return boto3.Session(profile_name=profile_name)

    mocker.patch(
        "megfile.s3_path.get_s3_session", side_effect=get_s3_session_without_cache
    )


def test_get_access_token_from_file(fs, mocker, s3_session):
    tmpdir = "/"
    config_path = os.path.join(tmpdir, "config")
    mocker.patch("os.environ", {"AWS_CONFIG_FILE": config_path})
    os.makedirs(os.path.dirname(config_path), exist_ok=True)

    with open(config_path, "w") as f:
        f.write(
            """[default]
aws_access_key_id = test_key
aws_secret_access_key = test_secret

[profile kubebrain]
aws_access_key_id = test_key_kubebrain
aws_secret_access_key = test_secret_kubebrain"""
        )

    assert get_access_token() == ("test_key", "test_secret", None)
    assert get_access_token("kubebrain") == (
        "test_key_kubebrain",
        "test_secret_kubebrain",
        None,
    )
    assert get_access_token("unknown") == (None, None, None)

    mocker.patch(
        "os.environ", {"AWS_PROFILE": "kubebrain", "AWS_CONFIG_FILE": config_path}
    )
    assert get_access_token() == (
        "test_key_kubebrain",
        "test_secret_kubebrain",
        None,
    )


def test_get_access_token_from_file2(mocker, s3_session):
    with tempfile.TemporaryDirectory() as tmpdir:
        credentials_path = os.path.join(tmpdir, "credentials")
        mocker.patch("os.environ", {"AWS_SHARED_CREDENTIALS_FILE": credentials_path})
        os.makedirs(os.path.dirname(credentials_path), exist_ok=True)

        with open(credentials_path, "w") as f:
            f.write(
                """[default]
aws_access_key_id = test_key
aws_secret_access_key = test_secret

[kubebrain]
aws_access_key_id = test_key_kubebrain
aws_secret_access_key = test_secret_kubebrain"""
            )

        assert get_access_token() == ("test_key", "test_secret", None)
        assert get_access_token("kubebrain") == (
            "test_key_kubebrain",
            "test_secret_kubebrain",
            None,
        )
        assert get_access_token("unknown") == (None, None, None)

        mocker.patch(
            "os.environ",
            {
                "AWS_PROFILE": "kubebrain",
                "AWS_SHARED_CREDENTIALS_FILE": credentials_path,
            },
        )
        assert get_access_token() == (
            "test_key_kubebrain",
            "test_secret_kubebrain",
            None,
        )


def test_get_access_token_from_file3(mocker, s3_session):
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = os.path.join(tmpdir, "config")
        credentials_path = os.path.join(tmpdir, "credentials")
        mocker.patch(
            "os.environ",
            {
                "AWS_CONFIG_FILE": config_path,
                "AWS_SHARED_CREDENTIALS_FILE": credentials_path,
            },
        )
        os.makedirs(os.path.dirname(credentials_path), exist_ok=True)

        with open(config_path, "w") as f:
            f.write(
                """[default]
aws_access_key_id = test_key
aws_secret_access_key = test_secret

[profile kubebrain]
aws_access_key_id = test_key_kubebrain
aws_secret_access_key = test_secret_kubebrain"""
            )

        with open(credentials_path, "w") as f:
            f.write(
                """[default]
aws_access_key_id = test_key_2
aws_secret_access_key = test_secret_2

[kubebrain]
aws_access_key_id = test_key_kubebrain_2
aws_secret_access_key = test_secret_kubebrain_2"""
            )

        assert get_access_token() == ("test_key_2", "test_secret_2", None)
        assert get_access_token("kubebrain") == (
            "test_key_kubebrain_2",
            "test_secret_kubebrain_2",
            None,
        )
        assert get_access_token("unknown") == (None, None, None)

        mocker.patch(
            "os.environ",
            {
                "AWS_PROFILE": "kubebrain",
                "AWS_CONFIG_FILE": config_path,
                "AWS_SHARED_CREDENTIALS_FILE": credentials_path,
            },
        )
        assert get_access_token() == (
            "test_key_kubebrain_2",
            "test_secret_kubebrain_2",
            None,
        )


def test_get_endpoint_url():
    assert get_endpoint_url(profile_name="unknown") == "https://s3.amazonaws.com"


def test_get_endpoint_url_from_env(mocker):
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})
    mocker.patch.dict(os.environ, {"OSS_ENDPOINT": "oss-endpoint"})
    assert get_endpoint_url() == "oss-endpoint"


def test_get_endpoint_url_from_env2(mocker):
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})
    mocker.patch.dict(os.environ, {"AWS_ENDPOINT_URL": "oss-endpoint2"})
    assert get_endpoint_url() == "oss-endpoint2"


def test_get_endpoint_url_from_env3(mocker):
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})
    mocker.patch.dict(
        os.environ,
        {"OSS_ENDPOINT": "oss-endpoint", "AWS_ENDPOINT_URL": "oss-endpoint2"},
    )
    assert get_endpoint_url() == "oss-endpoint"


def test_get_endpoint_url_from_env4(mocker):
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})
    mocker.patch.dict(os.environ, {"AWS_ENDPOINT_URL_S3": "oss-endpoint3"})
    assert get_endpoint_url() == "oss-endpoint3"


def test_get_endpoint_url_from_env5(mocker):
    mocker.patch("megfile.s3_path.get_scoped_config", return_value={})
    mocker.patch.dict(
        os.environ, {"AWS_PROFILE": "test", "TEST__OSS_ENDPOINT": "oss-endpoint"}
    )
    assert get_endpoint_url() == "oss-endpoint"


def test_get_endpoint_url_from_scoped_config(mocker):
    mocker.patch(
        "megfile.s3_path.get_scoped_config",
        return_value={"s3": {"endpoint_url": "test_endpoint_url"}},
    )
    assert get_endpoint_url() == "test_endpoint_url"

    mocker.patch(
        "megfile.s3_path.get_scoped_config",
        return_value={"endpoint_url": "test_endpoint_url2"},
    )
    assert get_endpoint_url() == "test_endpoint_url2"

    mocker.patch(
        "megfile.s3_path.get_scoped_config",
        return_value={
            "s3": {"endpoint_url": "test_endpoint_url"},
            "endpoint_url": "test_endpoint_url2",
        },
    )
    assert get_endpoint_url() == "test_endpoint_url"


def test__patch_make_request(mocker):
    from botocore.awsrequest import AWSPreparedRequest, AWSResponse

    mocker.patch("megfile.s3_path.max_retries", 2)
    mocker.patch("megfile.s3_path.s3_should_retry", return_value=True)

    class FakeEndpoint:
        def __init__(self):
            self.status = "start"

        def _send(self, request):
            if self.status == "start":
                self.status = "redirected"
                return AWSResponse(
                    url="http://redirect",
                    status_code=301,
                    headers={"Location": "http://real/path"},
                    raw=b"",
                )
            assert request.url == "http://real/path"
            return AWSResponse(
                url="http://real/path",
                status_code=200,
                headers={},
                raw=b"",
            )

    class FakeClient:
        def __init__(self):
            self._endpoint = FakeEndpoint()
            self.times = 0

        def _make_request(self, operation_model, request_dict, request_context):
            if self.times > 0:
                return
            self.times += 1
            raise Exception("test")

    class FakeOperationModel:
        def __init__(self):
            self.name = "test"

    body = io.BytesIO(b"test")
    body.seek(3)
    client = _patch_make_request(
        FakeClient(),
        redirect=True,
    )
    client._make_request(
        FakeOperationModel(),
        {"body": body},
        None,
    )
    assert body.tell() == 0

    client._endpoint._send(
        AWSPreparedRequest(
            method="GET",
            url="http://redirect",
            headers={},
            body=b"",
            stream_output=b"",
        )
    )
    assert client._endpoint.status == "redirected"

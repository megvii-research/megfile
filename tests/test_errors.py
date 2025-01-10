import logging
import pickle

import botocore.exceptions
import pytest
import urllib3.exceptions

from megfile.errors import (
    ClientError,
    HTTPError,
    HttpException,
    HttpFileNotFoundError,
    HttpPermissionError,
    HttpUnknownError,
    NoCredentialsError,
    ParamValidationError,
    S3BucketNotFoundError,
    S3ConfigError,
    S3Exception,
    S3FileNotFoundError,
    S3PermissionError,
    S3UnknownError,
    UnknownError,
    UnsupportedError,
    http_retry_exceptions,
    http_should_retry,
    patch_method,
    s3_endpoint_url,
    s3_retry_exceptions,
    s3_should_retry,
    translate_fs_error,
    translate_hdfs_error,
    translate_http_error,
    translate_s3_error,
)


def test_megfile_unknown_error():
    cause = Exception("cause")
    error = UnknownError(cause, "path")
    assert "Exception(" in str(error)
    assert "cause" in str(error)
    assert "path" in str(error)
    assert error.__cause__ is cause


def test_megfile_unknown_error_pickle():
    cause = Exception("cause")
    error = UnknownError(cause, "path")
    error = pickle.loads(pickle.dumps(error))
    assert "Exception(" in str(error)
    assert "cause" in str(error)
    assert "path" in str(error)
    assert str(error.__cause__) == str(cause)


def test_megfile_unsupported_error_pickle():
    error = UnsupportedError("operation", "path")
    error = pickle.loads(pickle.dumps(error))
    assert "path" in str(error)


def test_translate_s3_error():
    s3_url = "s3://test"

    s3_error = S3Exception()
    assert isinstance(translate_s3_error(s3_error, s3_url), S3Exception)

    error_response = {"Error": {"Code": "NoSuchBucket"}}
    client_error = ClientError(error_response, operation_name="test")
    assert isinstance(translate_s3_error(client_error, s3_url), S3BucketNotFoundError)

    error_response = {"Error": {"Code": "404"}}
    client_error = ClientError(error_response, operation_name="test")
    assert isinstance(translate_s3_error(client_error, s3_url), S3FileNotFoundError)

    error_response = {"Error": {"Code": "401"}}
    client_error = ClientError(error_response, operation_name="test")
    assert isinstance(translate_s3_error(client_error, s3_url), S3PermissionError)

    error_response = {"Error": {"Code": "InvalidAccessKeyId"}}
    client_error = ClientError(error_response, operation_name="test")
    assert isinstance(translate_s3_error(client_error, s3_url), S3ConfigError)

    error_response = {"Error": {"Code": "unKnow"}}
    client_error = ClientError(error_response, operation_name="test")
    assert isinstance(translate_s3_error(client_error, s3_url), S3UnknownError)

    param_validation_error = ParamValidationError(report="Invalid bucket name")
    assert isinstance(
        translate_s3_error(param_validation_error, s3_url), S3BucketNotFoundError
    )

    param_validation_error = ParamValidationError(
        report="Invalid length for parameter Key"
    )
    assert isinstance(
        translate_s3_error(param_validation_error, s3_url), S3FileNotFoundError
    )

    param_validation_error = ParamValidationError(report="unKnow")
    assert isinstance(
        translate_s3_error(param_validation_error, s3_url), S3UnknownError
    )

    no_credentials_error = NoCredentialsError()
    assert isinstance(translate_s3_error(no_credentials_error, s3_url), S3ConfigError)

    exception_error = Exception()
    assert isinstance(translate_s3_error(exception_error, s3_url), S3UnknownError)


def test_translate_http_error():
    url = "http://test.com"

    http_exception = HttpException()
    assert isinstance(translate_http_error(http_exception, http_url=url), HttpException)

    class FakeResponse:
        def __init__(self, status_code) -> None:
            self.status_code = status_code
            pass

    http_error = HTTPError(response=FakeResponse(401))
    assert isinstance(
        translate_http_error(http_error, http_url=url), HttpPermissionError
    )

    http_error = HTTPError(response=FakeResponse(404))
    assert isinstance(
        translate_http_error(http_error, http_url=url), HttpFileNotFoundError
    )

    http_error = HTTPError(response=FakeResponse(500))
    assert isinstance(translate_http_error(http_error, http_url=url), HttpUnknownError)


def test_translate_fs_error():
    error = OSError(1, "test", None, None)
    fs_path = "/test"
    translate_error = translate_fs_error(error, fs_path=fs_path)
    assert translate_error.filename == fs_path


def test_http_should_retry():
    for Error in http_retry_exceptions:
        if Error is urllib3.exceptions.IncompleteRead:
            assert http_should_retry(Error(partial="test", expected="test")) is True
        elif Error is urllib3.exceptions.ReadTimeoutError:
            assert (
                http_should_retry(Error(pool=1, url="http:test", message="test"))
                is True
            )
        else:
            assert http_should_retry(Error()) is True
    error = Exception()
    assert http_should_retry(error) is False


def test_s3_should_retry():
    for Error in s3_retry_exceptions:
        if Error is urllib3.exceptions.IncompleteRead:
            assert s3_should_retry(Error(partial="test", expected="test")) is True
        elif Error is urllib3.exceptions.ReadTimeoutError:
            assert (
                s3_should_retry(Error(pool=1, url="http:test", message="test")) is True
            )
        elif Error is botocore.exceptions.IncompleteReadError:
            assert (
                s3_should_retry(Error(actual_bytes=b"test", expected_bytes=b"test"))
                is True
            )
        elif Error is botocore.exceptions.EndpointConnectionError:
            assert s3_should_retry(Error(endpoint_url="test")) is True
        elif Error is botocore.exceptions.ReadTimeoutError:
            assert s3_should_retry(Error(endpoint_url="test")) is True
        elif Error is botocore.exceptions.ConnectTimeoutError:
            assert s3_should_retry(Error(endpoint_url="test")) is True
        elif Error is botocore.exceptions.ResponseStreamingError:
            assert s3_should_retry(Error(error="test")) is True
        elif Error is botocore.exceptions.ProxyConnectionError:
            assert s3_should_retry(Error(proxy_url="test")) is True
        elif Error is botocore.exceptions.ConnectionClosedError:
            assert s3_should_retry(Error(endpoint_url="test")) is True
        elif Error is urllib3.exceptions.HeaderParsingError:
            assert s3_should_retry(Error("", "")) is True
        else:
            assert s3_should_retry(Error()) is True

    error_response = {"Error": {"Code": "500"}}
    client_error = ClientError(error_response, operation_name="test")
    assert s3_should_retry(client_error) is True

    error = Exception()
    assert s3_should_retry(error) is False


def test_s3_endpoint_url(mocker):
    class FakeMeta:
        endpoint_url = "test"

    class Fake:
        def __init__(self) -> None:
            self.meta = FakeMeta()
            pass

    def fake_get_endpoint_url(profile_name=None) -> str:
        if profile_name:
            return profile_name
        return None

    mocker.patch("megfile.s3.get_endpoint_url", side_effect=fake_get_endpoint_url)
    mocker.patch("megfile.s3.get_s3_client", return_value=Fake())
    assert s3_endpoint_url() == "test"
    assert s3_endpoint_url("s3+test1://") == "test1"
    assert s3_endpoint_url("s3+test2://") == "test2"


def test_translate_hdfs_error():
    from megfile.hdfs_path import HdfsPath
    from megfile.lib.hdfs_tools import hdfs_api

    assert isinstance(
        translate_hdfs_error(
            hdfs_api.HdfsError(
                message="Path is not a file",
            ),
            hdfs_path=HdfsPath("hdfs://A/B/C"),
        ),
        IsADirectoryError,
    )

    assert isinstance(
        translate_hdfs_error(
            hdfs_api.HdfsError(
                message="Path is not a directory",
            ),
            hdfs_path=HdfsPath("hdfs://A/B/C"),
        ),
        NotADirectoryError,
    )


def test_patch_method(caplog):
    with caplog.at_level(logging.INFO, logger="megfile"):
        times = 0

        def test():
            nonlocal times
            if times >= 2:
                return

            times += 1
            raise ValueError("test")

        patched_test = patch_method(
            test,
            max_retries=2,
            should_retry=lambda e: True,
        )

        with pytest.raises(ValueError):
            patched_test()

        times = 1
        patched_test()
        assert "Error already fixed by retry" in caplog.text


def test_pickle_error():
    e = S3UnknownError(Exception(), "")
    d = pickle.dumps(e)
    pickle.loads(d)

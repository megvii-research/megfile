import logging
import pickle

import boto3.exceptions
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
    MaxRetriesExceededError,
    NoCredentialsError,
    ParamValidationError,
    S3BucketNotFoundError,
    S3ConfigError,
    S3Exception,
    S3FileNotFoundError,
    S3InvalidRangeError,
    S3PermissionError,
    S3UnknownError,
    UnknownError,
    UnsupportedError,
    http_retry_exceptions,
    http_should_retry,
    patch_method,
    s3_access_key_masked,
    s3_config_hint,
    s3_endpoint_url,
    s3_retry_exceptions,
    s3_should_retry,
    translate_fs_error,
    translate_hdfs_error,
    translate_http_error,
    translate_s3_error,
)


def test_megfile_max_retries_exceeded_error():
    cause = Exception("cause")
    error = MaxRetriesExceededError(cause, 16)
    assert "Exception(" in str(error)
    assert "cause" in str(error)
    assert "after 16 tries" in str(error)
    assert error.__cause__ is cause


def test_megfile_max_retries_exceeded_error_pickle():
    cause = Exception("cause")
    error = MaxRetriesExceededError(cause, 16)
    error = pickle.loads(pickle.dumps(error))
    assert "Exception(" in str(error)
    assert "cause" in str(error)
    assert "after 16 tries" in str(error)
    assert str(error.__cause__) == str(cause)


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

    exception_error = MaxRetriesExceededError(exception_error, 3)
    assert isinstance(translate_s3_error(exception_error, s3_url), S3UnknownError)
    assert "after 3 tries" in str(translate_s3_error(exception_error, s3_url))

    s3_upload_failed_error = boto3.exceptions.S3UploadFailedError("NoSuchBucket")
    assert isinstance(
        translate_s3_error(s3_upload_failed_error, s3_url), S3BucketNotFoundError
    )

    s3_transfer_failed_error = boto3.exceptions.S3TransferFailedError("NoSuchKey")
    assert isinstance(
        translate_s3_error(s3_transfer_failed_error, s3_url), S3FileNotFoundError
    )

    s3_upload_failed_error = boto3.exceptions.S3UploadFailedError("InvalidAccessKeyId")
    assert isinstance(translate_s3_error(s3_upload_failed_error, s3_url), S3ConfigError)

    s3_upload_failed_error = boto3.exceptions.S3UploadFailedError("InvalidRange")
    assert isinstance(
        translate_s3_error(s3_upload_failed_error, s3_url), S3InvalidRangeError
    )

    s3_upload_failed_error = boto3.exceptions.S3UploadFailedError("AccessDenied")
    assert isinstance(
        translate_s3_error(s3_upload_failed_error, s3_url), S3PermissionError
    )


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
        elif Error is botocore.exceptions.SSLError:
            assert s3_should_retry(Error(endpoint_url="test", error="test")) is True
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

    mocker.patch("megfile.s3_path.get_endpoint_url", side_effect=fake_get_endpoint_url)
    mocker.patch("megfile.s3_path.get_s3_client", return_value=Fake())
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

        with pytest.raises(MaxRetriesExceededError):
            patched_test()

        times = 1
        patched_test()
        assert "Error already fixed by retry" in caplog.text


def test_pickle_error():
    e = S3UnknownError(Exception(), "")
    d = pickle.dumps(e)
    pickle.loads(d)


def test_s3_access_key_masked(mocker):
    # Test with access key longer than 8 characters
    mocker.patch(
        "megfile.s3_path.get_access_token",
        return_value=("AKIAIOSFODNN7EXAMPLE", "secret", None),
    )
    result = s3_access_key_masked("s3://bucket/key")
    assert result == "AKIAIОСF****" or result == "AKIAIOSF****"
    assert result.endswith("****")
    assert len(result) == 12  # 8 chars + 4 asterisks

    # Test with access key shorter than or equal to 8 characters
    mocker.patch(
        "megfile.s3_path.get_access_token",
        return_value=("SHORTKEY", "secret", None),
    )
    result = s3_access_key_masked("s3://bucket/key")
    assert result == "SHORTKEY"

    # Test with no access key
    mocker.patch(
        "megfile.s3_path.get_access_token",
        return_value=(None, None, None),
    )
    result = s3_access_key_masked("s3://bucket/key")
    assert result is None


def test_s3_config_hint(mocker):
    mocker.patch("os.environ.get", return_value=None)

    # Test no_credentials hint
    hint = s3_config_hint("s3://bucket/key", "no_credentials")
    assert "No credentials found" in hint
    assert "AWS_ACCESS_KEY_ID" in hint
    assert "AWS_SECRET_ACCESS_KEY" in hint
    assert "~/.aws/credentials" in hint
    assert "[default]" in hint

    # Test invalid_access_key hint
    hint = s3_config_hint("s3://bucket/key", "invalid_access_key")
    assert "access_key is invalid" in hint
    assert "AWS_ACCESS_KEY_ID" in hint

    # Test signature_mismatch hint
    hint = s3_config_hint("s3://bucket/key", "signature_mismatch")
    assert "secret_key does not match" in hint
    assert "AWS_SECRET_ACCESS_KEY" in hint

    # Test access_denied hint
    hint = s3_config_hint("s3://bucket/key", "access_denied")
    assert "Access denied" in hint
    assert "permissions" in hint or "IAM" in hint

    # Test generic hint
    hint = s3_config_hint("s3://bucket/key", "generic")
    assert "invalid" in hint
    assert "AWS_ACCESS_KEY_ID" in hint


def test_s3_config_hint_with_profile(mocker):
    # Test with profile name from path
    hint = s3_config_hint("s3+myprofile://bucket/key", "no_credentials")
    assert "MYPROFILE__AWS_ACCESS_KEY_ID" in hint
    assert "MYPROFILE__AWS_SECRET_ACCESS_KEY" in hint
    assert "[myprofile]" in hint or "[profile myprofile]" in hint


def test_translate_s3_error_with_hint(mocker):
    """Test that translated errors contain access_key and hint information."""
    s3_url = "s3://test-bucket/test-key"

    # Mock access key
    mocker.patch(
        "megfile.s3_path.get_access_token",
        return_value=("AKIAIOSFODNN7EXAMPLE", "secret", None),
    )

    # Test InvalidAccessKeyId error contains access_key and hint
    error_response = {"Error": {"Code": "InvalidAccessKeyId", "Message": "Invalid key"}}
    client_error = ClientError(error_response, operation_name="test")
    result = translate_s3_error(client_error, s3_url)
    assert isinstance(result, S3ConfigError)
    error_msg = str(result)
    assert "access_key" in error_msg
    assert "AKIAIOSF****" in error_msg
    assert "Hint:" in error_msg

    # Test SignatureDoesNotMatch error contains access_key and hint
    error_response = {
        "Error": {"Code": "SignatureDoesNotMatch", "Message": "Signature mismatch"}
    }
    client_error = ClientError(error_response, operation_name="test")
    result = translate_s3_error(client_error, s3_url)
    assert isinstance(result, S3ConfigError)
    error_msg = str(result)
    assert "access_key" in error_msg
    assert "Hint:" in error_msg
    assert "secret_key" in error_msg  # hint should mention secret_key

    # Test AccessDenied error contains access_key and hint
    error_response = {"Error": {"Code": "AccessDenied", "Message": "Access denied"}}
    client_error = ClientError(error_response, operation_name="test")
    result = translate_s3_error(client_error, s3_url)
    assert isinstance(result, S3PermissionError)
    error_msg = str(result)
    assert "access_key" in error_msg
    assert "Hint:" in error_msg

    # Test NoCredentialsError contains hint
    no_creds_error = NoCredentialsError()
    result = translate_s3_error(no_creds_error, s3_url)
    assert isinstance(result, S3ConfigError)
    error_msg = str(result)
    assert "Hint:" in error_msg
    assert "No credentials found" in error_msg


def test_translate_s3_transfer_error_with_hint(mocker):
    """Test S3UploadFailedError/S3TransferFailedError with hint."""
    s3_url = "s3://test-bucket/test-key"

    mocker.patch(
        "megfile.s3_path.get_access_token",
        return_value=("AKIAIOSFODNN7EXAMPLE", "secret", None),
    )

    # Test AccessDenied in S3UploadFailedError
    s3_error = boto3.exceptions.S3UploadFailedError("AccessDenied: test")
    result = translate_s3_error(s3_error, s3_url)
    assert isinstance(result, S3PermissionError)
    error_msg = str(result)
    assert "access_key" in error_msg
    assert "Hint:" in error_msg

    # Test InvalidAccessKeyId in S3UploadFailedError
    s3_error = boto3.exceptions.S3UploadFailedError("InvalidAccessKeyId: test")
    result = translate_s3_error(s3_error, s3_url)
    assert isinstance(result, S3ConfigError)
    error_msg = str(result)
    assert "access_key" in error_msg
    assert "Hint:" in error_msg

    # Test SignatureDoesNotMatch in S3UploadFailedError
    s3_error = boto3.exceptions.S3UploadFailedError("SignatureDoesNotMatch: test")
    result = translate_s3_error(s3_error, s3_url)
    assert isinstance(result, S3ConfigError)
    error_msg = str(result)
    assert "access_key" in error_msg
    assert "Hint:" in error_msg

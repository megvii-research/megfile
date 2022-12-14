import pickle

import botocore.exceptions
import urllib3.exceptions

from megfile.errors import ClientError, HTTPError, HttpException, HttpFileNotFoundError, HttpPermissionError, HttpUnknownError, NoCredentialsError, ParamValidationError, S3BucketNotFoundError, S3ConfigError, S3Exception, S3FileNotFoundError, S3PermissionError, S3UnknownError, UnknownError, UnsupportedError, http_retry_exceptions, http_should_retry, s3_endpoint_url, s3_retry_exceptions, s3_should_retry, translate_fs_error, translate_http_error, translate_s3_error


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
    assert isinstance(
        translate_s3_error(client_error, s3_url), S3BucketNotFoundError)

    error_response = {"Error": {"Code": "404"}}
    client_error = ClientError(error_response, operation_name="test")
    assert isinstance(
        translate_s3_error(client_error, s3_url), S3FileNotFoundError)

    error_response = {"Error": {"Code": "401"}}
    client_error = ClientError(error_response, operation_name="test")
    assert isinstance(
        translate_s3_error(client_error, s3_url), S3PermissionError)

    error_response = {"Error": {"Code": "InvalidAccessKeyId"}}
    client_error = ClientError(error_response, operation_name="test")
    assert isinstance(translate_s3_error(client_error, s3_url), S3ConfigError)

    error_response = {"Error": {"Code": "unKnow"}}
    client_error = ClientError(error_response, operation_name="test")
    assert isinstance(translate_s3_error(client_error, s3_url), S3UnknownError)

    param_validation_error = ParamValidationError(report="Invalid bucket name")
    assert isinstance(
        translate_s3_error(param_validation_error, s3_url),
        S3BucketNotFoundError)

    param_validation_error = ParamValidationError(
        report="Invalid length for parameter Key")
    assert isinstance(
        translate_s3_error(param_validation_error, s3_url), S3FileNotFoundError)

    param_validation_error = ParamValidationError(report="unKnow")
    assert isinstance(
        translate_s3_error(param_validation_error, s3_url), S3UnknownError)

    no_credentials_error = NoCredentialsError()
    assert isinstance(
        translate_s3_error(no_credentials_error, s3_url), S3ConfigError)

    exception_error = Exception()
    assert isinstance(
        translate_s3_error(exception_error, s3_url), S3UnknownError)


def test_translate_http_error():
    url = "http://test.com"

    http_exception = HttpException()
    assert isinstance(
        translate_http_error(http_exception, http_url=url), HttpException)

    class FakeResponse:

        def __init__(self, status_code) -> None:
            self.status_code = status_code
            pass

    http_error = HTTPError(response=FakeResponse(401))
    assert isinstance(
        translate_http_error(http_error, http_url=url), HttpPermissionError)

    http_error = HTTPError(response=FakeResponse(404))
    assert isinstance(
        translate_http_error(http_error, http_url=url), HttpFileNotFoundError)

    http_error = HTTPError(response=FakeResponse(500))
    assert isinstance(
        translate_http_error(http_error, http_url=url), HttpUnknownError)


def test_translate_fs_error():
    error = OSError(1, "test", None, None)
    fs_path = "/test"
    translate_error = translate_fs_error(error, fs_path=fs_path)
    assert translate_error.filename == fs_path


def test_http_should_retry():
    for Error in http_retry_exceptions:
        if Error is urllib3.exceptions.IncompleteRead:
            assert http_should_retry(
                Error(partial='test', expected='test')) is True
        elif Error is urllib3.exceptions.ReadTimeoutError:
            assert http_should_retry(
                Error(pool=1, url='http:test', message="test")) is True
        else:
            assert http_should_retry(Error()) is True
    error = Exception()
    assert http_should_retry(error) is False


def test_s3_should_retry():
    for Error in s3_retry_exceptions:
        if Error is urllib3.exceptions.IncompleteRead:
            assert s3_should_retry(
                Error(partial='test', expected='test')) is True
        elif Error is urllib3.exceptions.ReadTimeoutError:
            assert s3_should_retry(
                Error(pool=1, url='http:test', message="test")) is True
        elif Error is botocore.exceptions.IncompleteReadError:
            assert s3_should_retry(
                Error(actual_bytes=b"test", expected_bytes=b'test')) is True
        elif Error is botocore.exceptions.EndpointConnectionError:
            assert s3_should_retry(Error(endpoint_url='test')) is True
        elif Error is botocore.exceptions.ReadTimeoutError:
            assert s3_should_retry(Error(endpoint_url='test')) is True
        elif Error is botocore.exceptions.ResponseStreamingError:
            assert s3_should_retry(Error(error='test')) is True
        else:
            assert s3_should_retry(Error()) is True

    error_response = {"Error": {"Code": "500"}}
    client_error = ClientError(error_response, operation_name="test")
    assert s3_should_retry(client_error) is True

    error = Exception()
    assert s3_should_retry(error) is False


def test_s3_endpoint_url(mocker):

    class FakeMeta:
        endpoint_url = 'test'

    class Fake:

        def __init__(self) -> None:
            self.meta = FakeMeta()
            pass

    mocker.patch('megfile.s3.get_endpoint_url', return_value=None)
    mocker.patch('megfile.s3.get_s3_client', return_value=Fake())
    assert s3_endpoint_url() == 'test'

# pyre-ignore-all-errors[16]
import time
from contextlib import contextmanager
from functools import wraps
from logging import getLogger
from shutil import SameFileError
from typing import Callable, Optional

import botocore.exceptions
import requests.exceptions
import urllib3.exceptions
from boto3.exceptions import (  # TODO: test different boto3 version
    S3TransferFailedError,
    S3UploadFailedError,
)
from botocore.exceptions import ClientError, NoCredentialsError, ParamValidationError
from requests.exceptions import HTTPError

from megfile.interfaces import PathLike

__all__ = [
    "S3FileNotFoundError",
    "S3BucketNotFoundError",
    "S3FileExistsError",
    "S3NotADirectoryError",
    "S3IsADirectoryError",
    "S3PermissionError",
    "S3ConfigError",
    "UnknownError",
    "UnsupportedError",
    "HttpPermissionError",
    "HttpFileNotFoundError",
    "HttpBodyIncompleteError",
    "HttpUnknownError",
    "HttpException",
    "ProtocolExistsError",
    "ProtocolNotFoundError",
    "S3UnknownError",
    "SameFileError",
    "translate_http_error",
    "translate_s3_error",
    "patch_method",
    "raise_s3_error",
    "s3_should_retry",
    "translate_fs_error",
    "http_should_retry",
]

_logger = getLogger(__name__)


def s3_endpoint_url(path: Optional[PathLike] = None):
    from megfile.s3 import get_endpoint_url, get_s3_client
    from megfile.s3_path import S3Path

    profile_name = None
    if path:
        profile_name = S3Path(path)._profile_name
    endpoint_url = get_endpoint_url(profile_name=profile_name)
    if endpoint_url is None:
        endpoint_url = get_s3_client(profile_name=profile_name).meta.endpoint_url
    return endpoint_url


def full_class_name(obj):
    # obj.__module__ + "." + obj.__class__.__qualname__ is an example in
    # this context of H.L. Mencken's "neat, plausible, and wrong."
    # Python makes no guarantees as to whether the __module__ special
    # attribute is defined, so we take a more circumspect approach.
    # Alas, the module name is explicitly excluded from __qualname__
    # in Python 3.

    module = obj.__class__.__module__
    if module is None or module == str.__class__.__module__:
        return obj.__class__.__name__  # Avoid reporting __builtin__
    else:
        return module + "." + obj.__class__.__name__


def full_error_message(error):
    return "%s(%r)" % (full_class_name(error), str(error))


def client_error_code(error: ClientError) -> str:
    error_data = error.response.get("Error", {})
    return error_data.get("Code") or error_data.get("code", "Unknown")


def client_error_message(error: ClientError) -> str:
    return error.response.get("Error", {}).get("Message", "Unknown")


def param_validation_error_report(error: ParamValidationError) -> str:
    return error.kwargs.get("report", "Unknown")


s3_retry_exceptions = [
    botocore.exceptions.IncompleteReadError,
    botocore.exceptions.EndpointConnectionError,
    botocore.exceptions.ReadTimeoutError,
    botocore.exceptions.ConnectTimeoutError,
    botocore.exceptions.ProxyConnectionError,
    botocore.exceptions.ConnectionClosedError,
    botocore.exceptions.SSLError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ConnectTimeout,
    urllib3.exceptions.IncompleteRead,
    urllib3.exceptions.ProtocolError,
    urllib3.exceptions.ReadTimeoutError,
    urllib3.exceptions.HeaderParsingError,
]
if hasattr(botocore.exceptions, "ResponseStreamingError"):  # backport botocore==1.23.24
    s3_retry_exceptions.append(
        botocore.exceptions.ResponseStreamingError  # pyre-ignore[6]
    )
s3_retry_exceptions = tuple(s3_retry_exceptions)  # pyre-ignore[9]

s3_retry_error_codes = (
    "429",  # noqa: E501 # TOS ExceedAccountQPSLimit
    "499",  # noqa: E501 # Some cloud providers may send response with http code 499 if the connection not send data in 1 min.
    "500",
    "501",
    "502",
    "503",
    "InternalError",
    "ServiceUnavailable",
    "SlowDown",
    "ContextCanceled",
    "Timeout",  # noqa: E501 # TOS Timeout
    "RequestTimeout",
    "RequestTimeTooSkewed",
    "ExceedAccountQPSLimit",
    "ExceedAccountRateLimit",
    "ExceedBucketQPSLimit",
    "ExceedBucketRateLimit",
    "DownloadTrafficRateLimitExceeded",  # noqa: E501 # OSS RateLimitExceeded
    "UploadTrafficRateLimitExceeded",
    "MetaOperationQpsLimitExceeded",
    "TotalQpsLimitExceeded",
    "ActiveRequestLimitExceeded",
    "CpuLimitExceeded",
    "QpsLimitExceeded",
)


def s3_should_retry(error: Exception) -> bool:
    if isinstance(error, s3_retry_exceptions):  # pyre-ignore[6]
        return True
    if isinstance(error, botocore.exceptions.ClientError):
        return client_error_code(error) in s3_retry_error_codes
    return False


def patch_method(
    func: Callable,
    max_retries: int,
    should_retry: Callable[[Exception], bool],
    before_callback: Optional[Callable] = None,
    after_callback: Optional[Callable] = None,
    retry_callback: Optional[Callable] = None,
):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if before_callback is not None:
            before_callback(*args, **kwargs)

        for retries in range(1, max_retries + 1):
            try:
                result = func(*args, **kwargs)
                if after_callback is not None:
                    result = after_callback(result, *args, **kwargs)
                if retries > 1:
                    _logger.info(f"Error already fixed by retry {retries - 1} times")
                return result
            except Exception as error:
                if not should_retry(error):
                    raise
                if retry_callback is not None:
                    retry_callback(error, *args, **kwargs)
                if retries == max_retries:
                    _logger.error(
                        f"Cannot handle error {full_error_message(error)} "
                        f"after {retries} tries"
                    )
                    raise
                retry_interval = min(0.1 * 2**retries, 30)
                _logger.info(
                    f"unknown error encountered: {full_error_message(error)}, "
                    f"retry in {retry_interval:.1f}s after {retries} tries"
                )
                time.sleep(retry_interval)

    return wrapper


def _create_missing_ok_generator(generator, missing_ok: bool, error: Exception):
    if missing_ok:
        return generator

    try:
        first = next(generator)
    except StopIteration:
        raise error

    def create_generator():
        yield first
        yield from generator

    return create_generator()


class UnknownError(Exception):
    def __init__(self, error: Exception, path: PathLike, extra: Optional[str] = None):
        message = "Unknown error encountered: %r, error: %s" % (
            path,
            full_error_message(error),
        )
        if extra is not None:
            message += ", " + extra
        super().__init__(message)
        self.path = path
        self.extra = extra
        self.__cause__ = error

    def __reduce__(self):
        return (self.__class__, (self.__cause__, self.path, self.extra))


class UnsupportedError(Exception):
    def __init__(self, operation: str, path: PathLike):
        super().__init__("Unsupported operation: %r, operation: %r" % (path, operation))
        self.path = path
        self.operation = operation

    def __reduce__(self):
        return (UnsupportedError, (self.operation, self.path))


class S3Exception(Exception):
    """
    Base type for all s3 errors, should NOT be constructed directly.
    When you try to do so, consider adding a new type of error.
    """


class S3FileNotFoundError(S3Exception, FileNotFoundError):
    pass


class S3BucketNotFoundError(S3FileNotFoundError, PermissionError):
    pass


class S3FileExistsError(S3Exception, FileExistsError):
    pass


class S3NotADirectoryError(S3Exception, NotADirectoryError):
    pass


class S3IsADirectoryError(S3Exception, IsADirectoryError):
    pass


class S3FileChangedError(S3Exception):
    pass


class S3PermissionError(S3Exception, PermissionError):
    pass


class S3ConfigError(S3Exception, EnvironmentError):
    """
    Error raised by wrong S3 config, including wrong config file format,
    wrong aws_secret_access_key / aws_access_key_id, and etc.
    """


class S3NotALinkError(S3FileNotFoundError, PermissionError):
    pass


class S3NameTooLongError(S3FileNotFoundError, PermissionError):
    pass


class S3InvalidRangeError(S3Exception):
    pass


class S3UnknownError(S3Exception, UnknownError):
    def __init__(self, error: Exception, path: PathLike, extra: Optional[str] = None):
        super().__init__(error, path, extra or "endpoint: %r" % s3_endpoint_url(path))


class HttpException(Exception):
    """
    Base type for all http errors, should NOT be constructed directly.
    When you try to do so, consider adding a new type of error.
    """


class HttpPermissionError(HttpException, PermissionError):
    pass


class HttpFileNotFoundError(HttpException, FileNotFoundError):
    pass


class HttpUnknownError(HttpException, UnknownError):
    pass


class HttpBodyIncompleteError(HttpException):
    pass


http_retry_exceptions = (
    requests.exceptions.ReadTimeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.HTTPError,
    requests.exceptions.ProxyError,
    urllib3.exceptions.IncompleteRead,
    urllib3.exceptions.ProtocolError,
    urllib3.exceptions.ReadTimeoutError,
    HttpBodyIncompleteError,
)


def http_should_retry(error: Exception) -> bool:
    if isinstance(error, http_retry_exceptions):
        return True
    return False


class ProtocolExistsError(Exception):
    pass


class ProtocolNotFoundError(Exception):
    pass


def translate_fs_error(fs_error: Exception, fs_path: PathLike) -> Exception:
    if isinstance(fs_error, OSError):
        if fs_error.filename is None:
            fs_error.filename = fs_path
        return fs_error
    return fs_error


def translate_s3_error(s3_error: Exception, s3_url: PathLike) -> Exception:
    """:param s3_error: error raised by boto3
    :param s3_url: s3_url
    """
    if isinstance(s3_error, S3Exception):
        return s3_error
    elif isinstance(s3_error, ClientError):
        code = client_error_code(s3_error)
        if code in ("NoSuchBucket"):
            bucket_or_url = (
                s3_error.response.get(  # pytype: disable=attribute-error
                    "Error", {}
                ).get("BucketName")
                or s3_url
            )
            return S3BucketNotFoundError(
                "No such bucket: %r, endpoint: %r"
                % (bucket_or_url, s3_endpoint_url(s3_url))
            )
        if code in ("404", "NoSuchKey"):
            return S3FileNotFoundError("No such file: %r" % s3_url)
        if code in ("401", "403", "AccessDenied"):
            message = client_error_message(s3_error)
            return S3PermissionError(
                "Permission denied: %r, code: %r, message: %r, endpoint: %r"
                % (s3_url, code, message, s3_endpoint_url(s3_url))
            )
        if code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
            message = client_error_message(s3_error)
            return S3ConfigError(
                "Invalid configuration: %r, code: %r, message: %r, endpoint: %r"
                % (s3_url, code, message, s3_endpoint_url(s3_url))
            )
        if code in ("InvalidRange", "Requested Range Not Satisfiable"):
            return S3InvalidRangeError(
                "Index out of range: %r, code: %r, message: %r, endpoint: %r"
                % (
                    s3_url,
                    code,
                    client_error_message(s3_error),
                    s3_endpoint_url(s3_url),
                )
            )
        return S3UnknownError(s3_error, s3_url)
    elif isinstance(s3_error, ParamValidationError):
        report = param_validation_error_report(s3_error)
        if "Invalid bucket name" in report:
            return S3BucketNotFoundError("Invalid bucket name: %r" % s3_url)
        if "Invalid length for parameter Key" in report:
            return S3FileNotFoundError("Invalid length for parameter Key: %r" % s3_url)
        return S3UnknownError(s3_error, s3_url)
    elif isinstance(s3_error, NoCredentialsError):
        return S3ConfigError(str(s3_error))
    elif isinstance(s3_error, (S3UploadFailedError, S3TransferFailedError)):
        if "NoSuchBucket" in str(s3_error):
            return S3BucketNotFoundError("No such bucket: %r" % s3_url)
        elif "NoSuchKey" in str(s3_error):
            return S3FileNotFoundError("No such file: %r" % s3_url)
        elif "InvalidAccessKeyId" in str(s3_error) or "SignatureDoesNotMatch" in str(
            s3_error
        ):
            return S3ConfigError("Invalid access key id: %r" % s3_url)
        elif "InvalidRange" in str(s3_error):
            return S3InvalidRangeError("Invalid range: %r" % s3_url)
        elif "AccessDenied" in str(s3_error):
            return S3PermissionError("Access denied: %r" % s3_url)
    return S3UnknownError(s3_error, s3_url)


def translate_http_error(http_error: Exception, http_url: str) -> Exception:
    """Generate exception according to http_error and status_code

    .. note ::

        This function only process the result of requests and response

    :param http_error: error raised by requests
    :param http_url: http url
    """
    if isinstance(http_error, HttpException):
        return http_error
    if isinstance(http_error, HTTPError):
        status_code = http_error.response.status_code
        if status_code == 401 or status_code == 403:
            return HttpPermissionError("Permission denied: %r" % http_url)
        elif status_code == 404:
            return HttpFileNotFoundError("No such file: %r" % http_url)
    return HttpUnknownError(http_error, http_url)


@contextmanager
def raise_s3_error(s3_url: PathLike, suppress_error_callback=None):
    try:
        yield
    except Exception as error:
        error = translate_s3_error(error, s3_url)
        if suppress_error_callback and suppress_error_callback(error):
            return
        raise error


def s3_error_code_should_retry(error: str) -> bool:
    if error in s3_retry_error_codes:
        return True
    return False


def translate_hdfs_error(hdfs_error: Exception, hdfs_path: PathLike) -> Exception:
    from megfile.lib.hdfs_tools import hdfs_api

    # pytype: disable=attribute-error
    if hdfs_api and isinstance(hdfs_error, hdfs_api.HdfsError):
        if hdfs_error.message and "Path is not a file" in hdfs_error.message:
            return IsADirectoryError("Is a directory: %r" % hdfs_path)
        elif hdfs_error.message and "Path is not a directory" in hdfs_error.message:
            return NotADirectoryError("Not a directory: %r" % hdfs_path)
        elif hdfs_error.status_code in (401, 403):
            return PermissionError("Permission denied: %r" % hdfs_path)
        elif hdfs_error.status_code == 400:
            return ValueError(f"{hdfs_error.message}, path: {hdfs_path}")
        elif hdfs_error.status_code == 404:
            return FileNotFoundError(f"No match file: {hdfs_path}")
    # pytype: enable=attribute-error
    return hdfs_error


@contextmanager
def raise_hdfs_error(hdfs_path: PathLike):
    try:
        yield
    except Exception as error:
        raise translate_hdfs_error(error, hdfs_path)

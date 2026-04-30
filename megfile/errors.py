# pyre-ignore-all-errors[16]
import os
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


def _get_s3_profile_name(path: Optional[PathLike] = None):
    """Extract profile name from S3 path."""
    from megfile.s3_path import S3Path

    profile_name = None
    if path:
        profile_name = S3Path(path)._profile_name
    if not profile_name:
        profile_name = os.environ.get("AWS_PROFILE")
    return profile_name


def _get_s3_env_var_names(profile_name: Optional[str] = None):
    """Return the environment variable names for access key and secret key."""
    if profile_name:
        env_prefix = f"{profile_name}__".upper()
        return f"{env_prefix}AWS_ACCESS_KEY_ID", f"{env_prefix}AWS_SECRET_ACCESS_KEY"
    return "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"


def _get_s3_config_sections(profile_name: Optional[str] = None):
    """Return section names for credentials/config files."""
    if profile_name:
        return f"[{profile_name}]", f"[profile {profile_name}]"
    return "[default]", "[default]"


def s3_endpoint_url(path: Optional[PathLike] = None):
    from megfile.s3_path import get_endpoint_url, get_s3_client

    profile_name = _get_s3_profile_name(path)
    endpoint_url = get_endpoint_url(profile_name=profile_name)
    if endpoint_url is None:
        endpoint_url = get_s3_client(profile_name=profile_name).meta.endpoint_url
    return endpoint_url


def s3_proxy_url(endpoint_url: Optional[str]) -> Optional[str]:
    """Return the proxy URL that botocore/urllib3 will use for ``endpoint_url``.

    megfile does not configure proxies itself; the underlying HTTP stack
    reads ``HTTPS_PROXY``/``HTTP_PROXY`` (and the lowercase variants) from
    the environment. We mirror that lookup here so the proxy can be shown
    in error messages — bad proxy configuration is a common cause of
    client/server/connection errors.
    """
    if not endpoint_url:
        return None
    if endpoint_url.startswith("https://"):
        return os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if endpoint_url.startswith("http://"):
        return os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
    return None


def s3_endpoint_extra(
    path: Optional[PathLike] = None,
    include_access_key: bool = False,
    include_proxy: bool = False,
) -> str:
    """Build the ``endpoint: ..., access_key: ..., proxy: ...`` suffix used in
    S3 error messages.

    Set ``include_access_key=True`` for credential-related errors. Set
    ``include_proxy=True`` for errors whose response body was likely not
    S3-format XML (numeric HTTP codes, connection errors), since a
    misconfigured proxy is a common cause of those.
    """
    endpoint_url = s3_endpoint_url(path)
    extra = "endpoint: %r" % endpoint_url
    if include_access_key:
        extra += ", access_key: %r" % s3_access_key_masked(path)
    if include_proxy:
        proxy_url = s3_proxy_url(endpoint_url)
        if proxy_url:
            extra += ", proxy: %r" % proxy_url
    return extra


def s3_access_key_masked(path: Optional[PathLike] = None):
    from megfile.s3_path import get_access_token

    profile_name = _get_s3_profile_name(path)
    access_key, _, _ = get_access_token(profile_name=profile_name)
    if access_key and len(access_key) > 8:
        return access_key[:8] + "****"
    return access_key


def s3_config_hint(path: Optional[PathLike] = None, error_type: str = "generic"):
    """Return a hint string for troubleshooting S3 credentials issues.

    Args:
        path: S3 path to extract profile name from
        error_type: One of 'no_credentials', 'invalid_access_key',
                   'signature_mismatch', 'access_denied', 'generic'
    """
    profile_name = _get_s3_profile_name(path)
    ak_env, sk_env = _get_s3_env_var_names(profile_name)
    credentials_section, config_section = _get_s3_config_sections(profile_name)

    config_guide = (
        f"in ~/.aws/config under section {config_section}, "
        f"or in ~/.aws/credentials under section {credentials_section}"
    )

    if error_type == "no_credentials":
        return (
            f"No credentials found. "
            f"Please set environment variables {ak_env} and {sk_env}, "
            f"or configure aws_access_key_id and aws_secret_access_key "
            f"{config_guide}"
        )
    elif error_type == "invalid_access_key":
        return (
            f"The access_key is invalid or does not exist. "
            f"Please check the value of environment variable {ak_env}, "
            f"or aws_access_key_id {config_guide}"
        )
    elif error_type == "signature_mismatch":
        return (
            f"The secret_key does not match the access_key. "
            f"Please check the value of environment variable {sk_env}, "
            f"or aws_secret_access_key {config_guide}"
        )
    elif error_type == "access_denied":
        return (
            "Access denied. Please check: "
            "1) the bucket/object permissions or IAM policy; "
            "2) whether the credentials have the required permissions; "
            "3) whether endpoint, access_key, and secret_key are correct"
        )
    else:
        return (
            f"Credentials may be invalid. "
            f"Please check environment variables {ak_env} and {sk_env}, "
            f"or aws_access_key_id and aws_secret_access_key "
            f"{config_guide}"
        )


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
    "PartitionQpsLimitted",
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
                    raise MaxRetriesExceededError(error, retries=retries)
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


class MaxRetriesExceededError(Exception):
    def __init__(self, error: Exception, retries: int = 1):
        while isinstance(error, MaxRetriesExceededError):
            retries *= error.retries
            error = error.__cause__
        message = "Max retires exceeded: %s, after %d tries" % (
            full_error_message(error),
            retries,
        )
        super().__init__(message)
        self.retries = retries
        self.__cause__ = error

    def __reduce__(self):
        return (self.__class__, (self.__cause__, self.retries))


class UnknownError(Exception):
    def __init__(self, error: Exception, path: PathLike, extra: Optional[str] = None):
        parts = [f"Unknown error encountered: {path!r}"]
        if isinstance(error, MaxRetriesExceededError):
            parts.append(f"error: {full_error_message(error.__cause__)}")
            parts.append(f"after {error.retries} tries")
        else:
            parts.append(f"error: {full_error_message(error)}")
        if extra is not None:
            parts.append(extra)
        message = ", ".join(parts)
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
        super().__init__(
            error, path, extra or s3_endpoint_extra(path, include_proxy=True)
        )


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
    if isinstance(fs_error, MaxRetriesExceededError):
        return fs_error.__cause__
    return fs_error


def translate_s3_error(s3_error: Exception, s3_url: PathLike) -> Exception:
    """:param s3_error: error raised by boto3
    :param s3_url: s3_url
    """
    if isinstance(s3_error, S3Exception):
        return s3_error
    ori_error = s3_error
    if isinstance(s3_error, MaxRetriesExceededError):
        s3_error = s3_error.__cause__
    if isinstance(s3_error, ClientError):
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
            # A numeric "404" (rather than the named "NoSuchKey") usually
            # means the response body was not S3-format XML — most often a
            # proxy returning its own error page. Surface the proxy in that
            # case so a misconfiguration is easy to spot.
            return S3FileNotFoundError(
                "No such file: %r, %s"
                % (s3_url, s3_endpoint_extra(s3_url, include_proxy=code == "404"))
            )
        if code in ("401", "403", "AccessDenied"):
            # Same reasoning as the "404" branch above: numeric 401/403 is
            # likely a proxy auth/redirect page rather than a real S3
            # AccessDenied response.
            return S3PermissionError(
                "Permission denied: %r, code: %r, message: %r, %s. Hint: %s"
                % (
                    s3_url,
                    code,
                    client_error_message(s3_error),
                    s3_endpoint_extra(
                        s3_url,
                        include_access_key=True,
                        include_proxy=code in ("401", "403"),
                    ),
                    s3_config_hint(s3_url, "access_denied"),
                )
            )
        if code == "InvalidAccessKeyId":
            return S3ConfigError(
                "Invalid access key: %r, code: %r, message: %r, %s. Hint: %s"
                % (
                    s3_url,
                    code,
                    client_error_message(s3_error),
                    s3_endpoint_extra(s3_url, include_access_key=True),
                    s3_config_hint(s3_url, "invalid_access_key"),
                )
            )
        if code == "SignatureDoesNotMatch":
            return S3ConfigError(
                "Signature mismatch: %r, code: %r, message: %r, %s. Hint: %s"
                % (
                    s3_url,
                    code,
                    client_error_message(s3_error),
                    s3_endpoint_extra(s3_url, include_access_key=True),
                    s3_config_hint(s3_url, "signature_mismatch"),
                )
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
        return S3ConfigError(
            "%s. Hint: %s" % (str(s3_error), s3_config_hint(s3_url, "no_credentials"))
        )
    elif isinstance(s3_error, (S3UploadFailedError, S3TransferFailedError)):
        if "NoSuchBucket" in str(s3_error):
            return S3BucketNotFoundError("No such bucket: %r" % s3_url)
        elif "NoSuchKey" in str(s3_error):
            return S3FileNotFoundError("No such file: %r" % s3_url)
        elif "AccessDenied" in str(s3_error):
            return S3PermissionError(
                "AccessDenied: %r, message: %r, %s. Hint: %s"
                % (
                    s3_url,
                    str(s3_error),
                    s3_endpoint_extra(s3_url, include_access_key=True),
                    s3_config_hint(s3_url, "access_denied"),
                )
            )
        elif "InvalidAccessKeyId" in str(s3_error):
            return S3ConfigError(
                "InvalidAccessKeyId: %r, message: %r, %s. Hint: %s"
                % (
                    s3_url,
                    str(s3_error),
                    s3_endpoint_extra(s3_url, include_access_key=True),
                    s3_config_hint(s3_url, "invalid_access_key"),
                )
            )
        elif "SignatureDoesNotMatch" in str(s3_error):
            return S3ConfigError(
                "SignatureDoesNotMatch: %r, message: %r, %s. Hint: %s"
                % (
                    s3_url,
                    str(s3_error),
                    s3_endpoint_extra(s3_url, include_access_key=True),
                    s3_config_hint(s3_url, "signature_mismatch"),
                )
            )
        elif "InvalidRange" in str(s3_error):
            return S3InvalidRangeError("Invalid range: %r" % s3_url)
    return S3UnknownError(ori_error, s3_url)


def translate_http_error(http_error: Exception, http_url: str) -> Exception:
    """Generate exception according to http_error and status_code

    .. note ::

        This function only process the result of requests and response

    :param http_error: error raised by requests
    :param http_url: http url
    """
    if isinstance(http_error, HttpException):
        return http_error
    ori_error = http_error
    if isinstance(http_error, MaxRetriesExceededError):
        http_error = http_error.__cause__
    if isinstance(http_error, HTTPError):
        status_code = http_error.response.status_code
        if status_code == 401 or status_code == 403:
            return HttpPermissionError("Permission denied: %r" % http_url)
        elif status_code == 404:
            return HttpFileNotFoundError("No such file: %r" % http_url)
    return HttpUnknownError(ori_error, http_url)


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
    if isinstance(hdfs_error, MaxRetriesExceededError):
        return hdfs_error.__cause__
    return hdfs_error


@contextmanager
def raise_hdfs_error(hdfs_path: PathLike):
    try:
        yield
    except Exception as error:
        raise translate_hdfs_error(error, hdfs_path)

import hashlib
import io
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from functools import cached_property, lru_cache, wraps
from logging import getLogger as get_logger
from typing import IO, Any, BinaryIO, Callable, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urlparse

import boto3
import botocore
from boto3.s3.transfer import TransferConfig
from botocore.awsrequest import AWSPreparedRequest, AWSResponse

from megfile.config import (
    GLOBAL_MAX_WORKERS,
    HTTP_AUTH_HEADERS,
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
    S3_CLIENT_CACHE_MODE,
    S3_MAX_RETRY_TIMES,
    WRITER_BLOCK_SIZE,
    WRITER_MAX_BUFFER_SIZE,
    parse_boolean,
)
from megfile.errors import (
    S3BucketNotFoundError,
    S3ConfigError,
    S3FileExistsError,
    S3FileNotFoundError,
    S3IsADirectoryError,
    S3NameTooLongError,
    S3NotADirectoryError,
    S3NotALinkError,
    S3PermissionError,
    S3UnknownError,
    SameFileError,
    UnsupportedError,
    _create_missing_ok_generator,
    patch_method,
    raise_s3_error,
    s3_error_code_should_retry,
    s3_should_retry,
    translate_fs_error,
    translate_s3_error,
)
from megfile.errors import (
    _logger as error_logger,
)
from megfile.interfaces import (
    Access,
    ContextIterator,
    FileCacher,
    FileEntry,
    PathLike,
    StatResult,
    URIPath,
)
from megfile.lib.compare import is_same_file
from megfile.lib.compat import fspath
from megfile.lib.fnmatch import translate
from megfile.lib.glob import has_magic, has_magic_ignore_brace, ungloblize
from megfile.lib.joinpath import uri_join
from megfile.lib.s3_buffered_writer import (
    S3BufferedWriter,
)
from megfile.lib.s3_cached_handler import S3CachedHandler
from megfile.lib.s3_limited_seekable_writer import S3LimitedSeekableWriter
from megfile.lib.s3_memory_handler import S3MemoryHandler
from megfile.lib.s3_pipe_handler import S3PipeHandler
from megfile.lib.s3_prefetch_reader import S3PrefetchReader
from megfile.lib.s3_share_cache_reader import S3ShareCacheReader
from megfile.lib.url import get_url_scheme
from megfile.smart_path import SmartPath
from megfile.utils import (
    _is_pickle,
    calculate_md5,
    generate_cache_path,
    get_binary_mode,
    get_content_offset,
    is_domain_or_subdomain,
    is_readable,
    necessary_params,
    process_local,
    thread_local,
)

__all__ = [
    "S3Path",
    "parse_s3_url",
    "get_endpoint_url",
    "get_s3_session",
    "get_s3_client",
    "s3_path_join",
    "is_s3",
    "s3_buffered_open",
    "s3_cached_open",
    "s3_memory_open",
    "s3_pipe_open",
    "s3_prefetch_open",
    "s3_share_cache_open",
    "s3_open",
    "S3Cacher",
    "s3_upload",
    "s3_download",
    "s3_load_content",
    "s3_concat",
]
_logger = get_logger(__name__)
content_md5_header = "megfile-content-md5"
endpoint_url = "https://s3.amazonaws.com"
max_retries = S3_MAX_RETRY_TIMES
max_keys = 1000


def _patch_make_request(client: botocore.client.BaseClient, redirect: bool = False):
    def after_callback(result: Tuple[AWSResponse, dict], *args, **kwargs):
        if (
            not isinstance(result, tuple)
            or len(result) != 2
            or not isinstance(result[0], AWSResponse)
            or not isinstance(result[1], dict)
        ):
            return result
        http, parsed_response = result
        if http.status_code >= 400:
            error_code = parsed_response.get("Error", {}).get("Code")
            operation_model = kwargs.get("operation_model") or (
                args[0] if args else None
            )
            operation_name = operation_model.name if operation_model else "ProxyMethod"
            error_class = client.exceptions.from_code(error_code)
            raise error_class(parsed_response, operation_name)
        return result

    def retry_callback(error, operation_model, request_dict, request_context):
        if is_readable(request_dict["body"]):
            request_dict["body"].seek(0)

    def before_callback(operation_model, request_dict, request_context):
        _logger.debug(
            "send s3 request: %r, with parameters: %s",
            operation_model.name,
            request_dict,
        )

    client._make_request = patch_method(
        client._make_request,
        max_retries=max_retries,
        should_retry=s3_should_retry,
        after_callback=after_callback,
        before_callback=before_callback,
        retry_callback=retry_callback,
    )

    def patch_send(send):
        def patched_send(request: AWSPreparedRequest) -> AWSResponse:
            response: AWSResponse = send(request)
            if (
                request.method == "GET"  # only support GET method for now
                and response.status_code in (301, 302, 307, 308)
                and "Location" in response.headers
            ):
                # Permit sending auth/cookie headers from "foo.com" to "sub.foo.com".
                # See also: https://go.dev/src/net/http/client.go#L980
                location = response.headers["Location"]
                ihost = urlparse(request.url).hostname
                dhost = urlparse(location).hostname
                if not is_domain_or_subdomain(dhost, ihost):
                    for name in HTTP_AUTH_HEADERS:
                        request.headers.pop(name, None)
                request.url = location
                response = send(request)
            return response

        return patched_send

    if redirect:
        client._endpoint._send = patch_send(client._endpoint._send)

    return client


def parse_s3_url(s3_url: PathLike) -> Tuple[str, str]:
    s3_url = fspath(s3_url)
    if not is_s3(s3_url):
        raise ValueError("Not a s3 url: %r" % s3_url)
    right_part = s3_url.split("://", maxsplit=1)[1]
    bucket_pattern = re.match("(.*?)/", right_part)
    if bucket_pattern is None:
        bucket = right_part
        path = ""
    else:
        bucket = bucket_pattern.group(1)
        path = right_part[len(bucket) + 1 :]
    return bucket, path


def get_scoped_config(profile_name: Optional[str] = None) -> Dict:
    try:
        return get_s3_session(profile_name=profile_name)._session.get_scoped_config()
    except botocore.exceptions.ProfileNotFound:
        return {}


@lru_cache()
def warning_endpoint_url(key: str, endpoint_url: str):
    _logger.info("using %s: %s" % (key, endpoint_url))


def get_endpoint_url(profile_name: Optional[str] = None) -> str:
    """Get the endpoint url of S3

    :returns: S3 endpoint url
    """
    if profile_name:
        environ_keys = (f"{profile_name}__OSS_ENDPOINT".upper(),)
    else:
        environ_keys = ("OSS_ENDPOINT", "AWS_ENDPOINT_URL_S3", "AWS_ENDPOINT_URL")
    for environ_key in environ_keys:
        environ_endpoint_url = os.environ.get(environ_key)
        if environ_endpoint_url:
            warning_endpoint_url(environ_key, environ_endpoint_url)
            return environ_endpoint_url
    config = get_scoped_config(profile_name=profile_name)
    config_endpoint_url = config.get("s3", {}).get("endpoint_url")
    config_endpoint_url = config_endpoint_url or config.get("endpoint_url")
    if config_endpoint_url:
        warning_endpoint_url("~/.aws/config", config_endpoint_url)
        return config_endpoint_url
    return endpoint_url


def get_s3_session(profile_name=None) -> boto3.Session:
    """Get S3 session

    :returns: S3 session
    """
    return thread_local(
        f"s3_session:{profile_name}", boto3.Session, profile_name=profile_name
    )


def get_env_var(env_name: str, profile_name=None):
    if profile_name:
        return os.getenv(f"{profile_name}__{env_name}".upper())
    return os.getenv(env_name.upper())


def get_access_token(profile_name=None):
    access_key = get_env_var("AWS_ACCESS_KEY_ID", profile_name=profile_name)
    secret_key = get_env_var("AWS_SECRET_ACCESS_KEY", profile_name=profile_name)
    session_token = get_env_var("AWS_SESSION_TOKEN", profile_name=profile_name)
    if access_key and secret_key:
        return access_key, secret_key, session_token

    try:
        credentials = get_s3_session(profile_name=profile_name).get_credentials()
    except botocore.exceptions.ProfileNotFound:
        credentials = None
    if credentials:
        if not access_key:
            access_key = credentials.access_key
        if not secret_key:
            secret_key = credentials.secret_key
        if not session_token:
            session_token = credentials.token
    return access_key, secret_key, session_token


def get_s3_client(
    config: Optional[botocore.config.Config] = None,
    cache_key: Optional[str] = None,
    profile_name: Optional[str] = None,
):
    """Get S3 client

    :returns: S3 client
    """
    if cache_key is not None:
        local_storage = thread_local
        if S3_CLIENT_CACHE_MODE == "process_local":
            local_storage = process_local
        return local_storage(
            f"{cache_key}:{profile_name}",
            get_s3_client,
            config=config,
            profile_name=profile_name,
        )

    try:
        default_config = botocore.config.Config(
            connect_timeout=5,
            max_pool_connections=GLOBAL_MAX_WORKERS,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        )
    except TypeError:  # botocore < 1.36.0
        default_config = botocore.config.Config(
            connect_timeout=5,
            max_pool_connections=GLOBAL_MAX_WORKERS,
        )

    if config:
        config = default_config.merge(config)
    else:
        config = default_config

    addressing_style = get_env_var("AWS_S3_ADDRESSING_STYLE", profile_name=profile_name)
    if addressing_style:
        config = config.merge(
            botocore.config.Config(s3={"addressing_style": addressing_style})
        )

    access_key, secret_key, session_token = get_access_token(profile_name)
    try:
        session = get_s3_session(profile_name=profile_name)
    except botocore.exceptions.ProfileNotFound:
        session = get_s3_session()

    s3_config = get_scoped_config(profile_name=profile_name).get("s3", {})
    verify = get_env_var("AWS_S3_VERIFY", profile_name=profile_name)
    verify = verify or s3_config.get("verify")
    verify = parse_boolean(verify, default=True)
    redirect = get_env_var("AWS_S3_REDIRECT", profile_name=profile_name)
    redirect = redirect or s3_config.get("redirect")
    redirect = parse_boolean(redirect, default=False)

    client = session.client(
        "s3",
        endpoint_url=get_endpoint_url(profile_name=profile_name),
        verify=verify,
        config=config,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )
    client = _patch_make_request(client, redirect=redirect)
    return client


def get_s3_client_with_cache(
    config: Optional[botocore.config.Config] = None, profile_name: Optional[str] = None
):
    return get_s3_client(
        config=config, cache_key="s3_filelike_client", profile_name=profile_name
    )


def s3_path_join(path: PathLike, *other_paths: PathLike) -> str:
    """
    Concat 2 or more path to a complete path

    :param path: Given path
    :param other_paths: Paths to be concatenated
    :returns: Concatenated complete path

    .. note ::

        The difference between this function and ``os.path.join`` is that this function
        ignores left side slash (which indicates absolute path) in ``other_paths``
        and will directly concat.

        e.g. os.path.join('/path', 'to', '/file') => '/file',
        but s3_path_join('/path', 'to', '/file') => '/path/to/file'
    """
    return uri_join(fspath(path), *map(fspath, other_paths))


def _list_all_buckets(profile_name: Optional[str] = None) -> List[str]:
    client = get_s3_client_with_cache(profile_name=profile_name)
    response = client.list_buckets()
    return [content["Name"] for content in response["Buckets"]]


def _parse_s3_url_ignore_brace(s3_pathname: str) -> Tuple[str, str]:
    left_brace = False
    right_part = s3_pathname.split("://", maxsplit=1)[1]
    for current_index, current_character in enumerate(right_part):
        if current_character == "/" and left_brace is False:
            return right_part[:current_index], right_part[current_index + 1 :]
        elif current_character == "{":
            left_brace = True
        elif current_character == "}":
            left_brace = False
    return right_part, ""


def _parse_s3_url_profile(s3_pathname: str) -> Tuple[str, Optional[str]]:
    protocol = s3_pathname.split("://", maxsplit=1)[0]
    profile_name = protocol[3:] if protocol.startswith("s3+") else None
    return protocol, profile_name


def _group_s3path_by_bucket(s3_pathname: str) -> List[str]:
    protocol, profile_name = _parse_s3_url_profile(s3_pathname)
    bucket, key = _parse_s3_url_ignore_brace(s3_pathname)
    if not bucket:
        if not key:
            raise UnsupportedError("Glob whole s3", s3_pathname)
        raise S3BucketNotFoundError("Empty bucket name: %r" % s3_pathname)

    grouped_path = []

    def generate_s3_path(bucket: str, key: str) -> str:
        if key:
            return f"{protocol}://{bucket}/{key}"
        return f"{protocol}://{bucket}{'/' if s3_pathname.endswith('/') else ''}"

    all_bucket = lru_cache(maxsize=1)(_list_all_buckets)
    for bucket_name in ungloblize(bucket):
        if has_magic(bucket_name):
            split_bucket_name = bucket_name.split("/", 1)
            path_part = None
            if len(split_bucket_name) == 2:
                bucket_name, path_part = split_bucket_name
            pattern = re.compile(translate(re.sub(r"\*{2,}", "*", bucket_name)))
            for current_bucket in all_bucket(profile_name):
                if pattern.fullmatch(current_bucket) is not None:
                    if path_part is not None:
                        current_bucket = "%s/%s" % (current_bucket, path_part)
                    grouped_path.append(generate_s3_path(current_bucket, key))
        else:
            grouped_path.append(generate_s3_path(bucket_name, key))

    return grouped_path


def _s3_split_magic_ignore_brace(s3_pathname: str) -> Tuple[str, str]:
    left_brace, left_index = False, 0
    normal_parts, magic_parts = [], []
    s3_pathname_with_suffix = s3_pathname
    s3_pathname = s3_pathname.rstrip("/")
    suffix = (len(s3_pathname_with_suffix) - len(s3_pathname)) * "/"
    for current_index, current_character in enumerate(s3_pathname):
        if current_character == "/" and left_brace is False:
            if has_magic_ignore_brace(s3_pathname[left_index:current_index]):
                magic_parts.append(s3_pathname[left_index:current_index])
                if s3_pathname[current_index + 1 :]:
                    magic_parts.append(s3_pathname[current_index + 1 :])
                    left_index = len(s3_pathname)
                break
            normal_parts.append(s3_pathname[left_index:current_index])
            left_index = current_index + 1
        elif current_character == "{":
            left_brace = True
        elif current_character == "}":
            left_brace = False
    if s3_pathname[left_index:]:
        if has_magic_ignore_brace(s3_pathname[left_index:]):
            magic_parts.append(s3_pathname[left_index:])
        else:
            normal_parts.append(s3_pathname[left_index:])
    top_dir, magic_part = "/".join(normal_parts), "/".join(magic_parts)
    if suffix:
        if magic_part:
            magic_part += suffix
        else:
            top_dir += suffix
    return top_dir, magic_part


def _group_s3path_by_prefix(s3_pathname: str) -> List[str]:
    _, key = parse_s3_url(s3_pathname)
    if not key:
        return ungloblize(s3_pathname)

    top_dir, magic_part = _s3_split_magic_ignore_brace(s3_pathname)
    if not top_dir:
        return [magic_part]
    grouped_path = []
    for pathname in ungloblize(top_dir):
        if magic_part:
            pathname = "/".join([pathname, magic_part])
        grouped_path.append(pathname)
    return grouped_path


def _become_prefix(prefix: str) -> str:
    if prefix != "" and not prefix.endswith("/"):
        prefix += "/"
    return prefix


def _s3_split_magic(s3_pathname: str) -> Tuple[str, str]:
    if not has_magic(s3_pathname):
        return s3_pathname, ""
    normal_parts = []
    magic_parts = []
    all_parts = s3_pathname.split("/")
    for i, part in enumerate(all_parts):
        if has_magic(part):
            magic_parts = all_parts[i:]
            break
        normal_parts.append(part)
    return "/".join(normal_parts), "/".join(magic_parts)


def _list_objects_recursive(s3_client, bucket: str, prefix: str, delimiter: str = ""):
    resp = s3_client.list_objects_v2(
        Bucket=bucket, Prefix=prefix, Delimiter=delimiter, MaxKeys=max_keys
    )

    while True:
        yield resp

        if not resp["IsTruncated"]:
            break

        resp = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter=delimiter,
            ContinuationToken=resp["NextContinuationToken"],
            MaxKeys=max_keys,
        )


def _make_stat(content: Dict[str, Any]):
    return StatResult(
        islnk=content.get("islnk", False),
        size=content["Size"],
        mtime=content["LastModified"].timestamp(),
        extra=content,
    )


class StatResultForIsLink:
    def __init__(self, path: "S3Path", *args, **kwargs):
        self._islnk = None
        self._path = path
        self._stat_result = StatResult(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._stat_result, name)

    @property
    def islnk(self) -> bool:
        if self._islnk is None:
            self._islnk = self._path.is_symlink()
        return self._islnk

    def is_file(self) -> bool:
        return not self._stat_result.isdir or self.islnk

    def is_dir(self) -> bool:
        return self._stat_result.isdir and not self.islnk

    def is_symlink(self) -> bool:
        return self.islnk


def _make_stat_without_metadata(content, path):
    return StatResultForIsLink(
        path=path,
        size=content["Size"],
        mtime=content["LastModified"].timestamp(),
        extra=content,
    )


def _s3_glob_stat_single_path(
    s3_pathname: PathLike,
    recursive: bool = True,
    missing_ok: bool = True,
    followlinks: bool = False,
) -> Iterator[FileEntry]:
    s3_pathname = fspath(s3_pathname)
    if not recursive:
        # If not recursive, replace ** with *
        s3_pathname = re.sub(r"\*{2,}", "*", s3_pathname)
    protocol, profile_name = _parse_s3_url_profile(s3_pathname)
    top_dir, wildcard_part = _s3_split_magic(s3_pathname)
    search_dir = wildcard_part.endswith("/")

    def should_recursive(wildcard_part: str) -> bool:
        if "**" in wildcard_part:
            return True
        for expanded_path in ungloblize(wildcard_part):
            parts_length = len(expanded_path.split("/"))
            if parts_length + search_dir >= 2:
                return True
        return False

    def create_generator(_s3_pathname) -> Iterator[FileEntry]:
        if not has_magic(_s3_pathname):
            _s3_pathname_obj = S3Path(_s3_pathname)
            if _s3_pathname_obj.is_file():
                stat = _s3_pathname_obj.stat(follow_symlinks=followlinks)
                yield FileEntry(_s3_pathname_obj.name, _s3_pathname_obj.path, stat)
            if _s3_pathname_obj.is_dir():
                yield FileEntry(
                    _s3_pathname_obj.name, _s3_pathname_obj.path, StatResult(isdir=True)
                )
            return

        delimiter = ""
        if not should_recursive(wildcard_part):
            delimiter = "/"

        dirnames = set()
        pattern = re.compile(translate(_s3_pathname))
        bucket, key = parse_s3_url(top_dir)
        prefix = _become_prefix(key)
        client = get_s3_client_with_cache(profile_name=profile_name)
        with raise_s3_error(_s3_pathname, S3BucketNotFoundError):
            for resp in _list_objects_recursive(client, bucket, prefix, delimiter):
                for content in resp.get("Contents", []):
                    path = s3_path_join(f"{protocol}://", bucket, content["Key"])
                    if not search_dir and pattern.match(path):
                        yield FileEntry(S3Path(path).name, path, _make_stat(content))
                    dirname = os.path.dirname(path)
                    while dirname not in dirnames and dirname != top_dir:
                        dirnames.add(dirname)
                        path = dirname + "/" if search_dir else dirname
                        if pattern.match(path):
                            yield FileEntry(
                                S3Path(path).name, path, StatResult(isdir=True)
                            )
                        dirname = os.path.dirname(dirname)
                for common_prefix in resp.get("CommonPrefixes", []):
                    path = s3_path_join(
                        f"{protocol}://", bucket, common_prefix["Prefix"]
                    )
                    dirname = os.path.dirname(path)
                    if dirname not in dirnames and dirname != top_dir:
                        dirnames.add(dirname)
                        path = dirname + "/" if search_dir else dirname
                        if pattern.match(path):
                            yield FileEntry(
                                S3Path(path).name, path, StatResult(isdir=True)
                            )

    return create_generator(s3_pathname)


def _s3_scan_pairs(
    src_url: PathLike, dst_url: PathLike
) -> Iterator[Tuple[PathLike, PathLike]]:
    for src_file_path in S3Path(src_url).scan():
        content_path = src_file_path[len(fspath(src_url)) :]
        if len(content_path) > 0:
            dst_file_path = s3_path_join(dst_url, content_path)
        else:
            dst_file_path = dst_url
        yield src_file_path, dst_file_path


def is_s3(path: PathLike) -> bool:
    """
    1. According to
       `aws-cli <https://docs.aws.amazon.com/cli/latest/reference/s3/index.html>`_ ,
       test if a path is s3 path.
    2. megfile also support the path like `s3[+profile_name]://bucket/key`

    :param path: Path to be tested
    :returns: True if path is s3 path, else False
    """
    path = fspath(path)
    if re.match(r"^s3(\+\w+)?:\/\/", path):
        return True
    return False


def _s3_binary_mode(s3_open_func):
    @wraps(s3_open_func)
    def wrapper(
        s3_url,
        mode: str = "rb",
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        **kwargs,
    ):
        bucket, key = parse_s3_url(s3_url)
        if not bucket:
            raise S3BucketNotFoundError("Empty bucket name: %r" % s3_url)

        if not key or key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % s3_url)

        if "x" in mode:
            if S3Path(s3_url).is_file():
                raise S3FileExistsError("File exists: %r" % s3_url)
            mode = mode.replace("x", "w")

        fileobj = s3_open_func(s3_url, get_binary_mode(mode), **kwargs)
        if "b" not in mode:
            fileobj = io.TextIOWrapper(fileobj, encoding=encoding, errors=errors)  # type: ignore
            fileobj.mode = mode  # pyre-ignore[41]
        return fileobj

    return wrapper


@_s3_binary_mode
def s3_prefetch_open(
    s3_url: PathLike,
    mode: str = "rb",
    followlinks: bool = False,
    *,
    max_workers: Optional[int] = None,
    block_size: int = READER_BLOCK_SIZE,
) -> S3PrefetchReader:
    """Open a asynchronous prefetch reader, to support fast sequential
    read and random read

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        Some parameter setting may perform well: max_workers=10 or 20,
        block_size=8 or 16 MB, default value None means using global thread pool

    :param s3_url: s3 path
    :param mode: only support "r" or "rb"
    :param encoding: encoding is the name of the encoding used to decode or encode
        the file. This should only be used in text mode.
    :param errors: errors is an optional string that specifies how encoding and
        decoding errors are to be handled—this cannot be used in binary mode.
    :param followlinks: follow symbolic link, default `False`
    :param max_workers: Max download thread number, `None` by default
    :param block_size: Max data size downloaded by each thread, in bytes,
        8MB by default
    :returns: An opened S3PrefetchReader object
    :raises: S3FileNotFoundError
    """
    if mode != "rb":
        raise ValueError("unacceptable mode: %r" % mode)
    if not isinstance(s3_url, S3Path):
        s3_url = S3Path(s3_url)
    if followlinks:
        try:
            s3_url = s3_url.readlink()
        except S3NotALinkError:
            pass

    bucket, key = parse_s3_url(s3_url.path_with_protocol)
    config = botocore.config.Config(max_pool_connections=GLOBAL_MAX_WORKERS)
    client = get_s3_client_with_cache(config=config, profile_name=s3_url._profile_name)
    return S3PrefetchReader(
        bucket,
        key,
        s3_client=client,
        max_retries=max_retries,
        max_workers=max_workers,
        block_size=block_size,
        profile_name=s3_url._profile_name,
    )


@_s3_binary_mode
def s3_share_cache_open(
    s3_url: PathLike,
    mode: str = "rb",
    followlinks: bool = False,
    *,
    cache_key: str = "lru",
    max_workers: Optional[int] = None,
    block_size: int = READER_BLOCK_SIZE,
) -> S3ShareCacheReader:
    """Open a asynchronous prefetch reader, to support fast sequential read and
    random read

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        Some parameter setting may perform well: max_workers=10 or 20,
        block_size=8 or 16 MB, default value None means using global thread pool

    :param s3_url: s3 path
    :param mode: only support "r" or "rb"
    :param encoding: encoding is the name of the encoding used to decode or encode
        the file. This should only be used in text mode.
    :param errors: errors is an optional string that specifies how encoding and
        decoding errors are to be handled—this cannot be used in binary mode.
    :param followlinks: follow symbolic link, default `False`
    :param max_workers: Max download thread number, None by default
    :param block_size: Max data size downloaded by each thread, in bytes,
        8MB by default
    :returns: An opened S3ShareCacheReader object
    :raises: S3FileNotFoundError
    """
    if mode != "rb":
        raise ValueError("unacceptable mode: %r" % mode)

    if not isinstance(s3_url, S3Path):
        s3_url = S3Path(s3_url)
    if followlinks:
        try:
            s3_url = s3_url.readlink()
        except S3NotALinkError:
            pass

    bucket, key = parse_s3_url(s3_url.path_with_protocol)
    config = botocore.config.Config(max_pool_connections=GLOBAL_MAX_WORKERS)
    client = get_s3_client_with_cache(config=config, profile_name=s3_url._profile_name)
    return S3ShareCacheReader(
        bucket,
        key,
        cache_key=cache_key,
        s3_client=client,
        max_retries=max_retries,
        max_workers=max_workers,
        block_size=block_size,
        profile_name=s3_url._profile_name,
    )


@_s3_binary_mode
def s3_pipe_open(
    s3_url: PathLike, mode: str, followlinks: bool = False, *, join_thread: bool = True
) -> S3PipeHandler:
    """Open a asynchronous read-write reader / writer, to support fast sequential
    read / write

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        When join_thread is False, while the file handle are closing,
        this function will not wait until the asynchronous writing finishes;

        False doesn't affect read-handle, but this can speed up write-handle because
        file will be written asynchronously.

        But asynchronous behavior can guarantee the file are successfully written,
        and frequent execution may cause thread and file handle exhaustion

    :param s3_url: s3 path
    :param mode: Mode to open file, either "r", "rb", "w" or "wb"
    :param encoding: encoding is the name of the encoding used to decode or encode
        the file. This should only be used in text mode.
    :param errors: errors is an optional string that specifies how encoding and
        decoding errors are to be handled—this cannot be used in binary mode.
    :param followlinks: follow symbolic link, default `False`
    :param join_thread: If wait after function execution until s3 finishes writing
    :returns: An opened BufferedReader / BufferedWriter object
    """
    if mode not in ("rb", "wb"):
        raise ValueError("unacceptable mode: %r" % mode)

    if mode[0] == "r" and not S3Path(s3_url).is_file():
        raise S3FileNotFoundError("No such file: %r" % s3_url)

    if not isinstance(s3_url, S3Path):
        s3_url = S3Path(s3_url)
    if followlinks:
        try:
            s3_url = s3_url.readlink()
        except S3NotALinkError:
            pass

    bucket, key = parse_s3_url(s3_url.path_with_protocol)
    config = botocore.config.Config(max_pool_connections=GLOBAL_MAX_WORKERS)
    client = get_s3_client_with_cache(config=config, profile_name=s3_url._profile_name)
    return S3PipeHandler(
        bucket,
        key,
        mode,
        s3_client=client,
        join_thread=join_thread,
        profile_name=s3_url._profile_name,
    )


@_s3_binary_mode
def s3_cached_open(
    s3_url: PathLike,
    mode: str,
    followlinks: bool = False,
    *,
    cache_path: Optional[str] = None,
) -> S3CachedHandler:
    """Open a local-cache file reader / writer, for frequent random read / write

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        cache_path can specify the path of cache file. Performance could be better
        if cache file path is on ssd or tmpfs

    :param s3_url: s3 path
    :param mode: Mode to open file, could be one of "rb", "wb", "ab", "rb+", "wb+"
        or "ab+"
    :param encoding: encoding is the name of the encoding used to decode or encode
        the file. This should only be used in text mode.
    :param errors: errors is an optional string that specifies how encoding and
        decoding errors are to be handled—this cannot be used in binary mode.
    :param followlinks: follow symbolic link, default `False`
    :param cache_path: cache file path
    :returns: An opened BufferedReader / BufferedWriter object
    """
    if mode not in ("rb", "wb", "ab", "rb+", "wb+", "ab+"):
        raise ValueError("unacceptable mode: %r" % mode)
    if not isinstance(s3_url, S3Path):
        s3_url = S3Path(s3_url)
    if followlinks:
        try:
            s3_url = s3_url.readlink()
        except S3NotALinkError:
            pass

    bucket, key = parse_s3_url(s3_url.path_with_protocol)
    config = botocore.config.Config(max_pool_connections=GLOBAL_MAX_WORKERS)
    client = get_s3_client_with_cache(config=config, profile_name=s3_url._profile_name)
    return S3CachedHandler(
        bucket,
        key,
        mode,
        s3_client=client,
        cache_path=cache_path,
        profile_name=s3_url._profile_name,
    )


@_s3_binary_mode
def s3_buffered_open(
    s3_url: PathLike,
    mode: str,
    followlinks: bool = False,
    *,
    max_workers: Optional[int] = None,
    max_buffer_size: Optional[int] = None,
    block_forward: Optional[int] = None,
    block_size: Optional[int] = None,
    limited_seekable: bool = False,
    buffered: bool = False,
    share_cache_key: Optional[str] = None,
    cache_path: Optional[str] = None,
) -> IO:
    """Open an asynchronous prefetch reader, to support fast sequential read

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        Some parameter setting may perform well: max_workers=10 or 20,
        default value None means using global thread pool

    :param s3_url: s3 path
    :param mode: Mode to open file, could be one of "rb", "wb", "ab", "rb+", "wb+"
        or "ab+"
    :param encoding: encoding is the name of the encoding used to decode or encode
        the file. This should only be used in text mode.
    :param errors: errors is an optional string that specifies how encoding and
        decoding errors are to be handled—this cannot be used in binary mode.
    :param followlinks: follow symbolic link, default `False`
    :param max_workers: Max download / upload thread number, `None` by default,
        will use global thread pool with 8 threads.
    :param max_buffer_size: Max cached buffer size in memory, 128MB by default.
        Set to `0` will disable cache.
    :param block_forward: How many blocks of data cached from offset position, only for
        read mode.
    :param block_size: Size of single block.
        Each block will be uploaded by single thread.
    :param limited_seekable: If write-handle supports limited seek
        (both file head part and tail part can seek block_size).
        Notes: This parameter are valid only for write-handle.
        Read-handle support arbitrary seek
    :returns: An opened File object
    :raises: S3FileNotFoundError
    """
    if mode not in ("rb", "wb", "ab", "rb+", "wb+", "ab+"):
        raise ValueError("unacceptable mode: %r" % mode)
    if not isinstance(s3_url, S3Path):
        s3_url = S3Path(s3_url)
    if followlinks:
        try:
            s3_url = s3_url.readlink()
        except S3NotALinkError:
            pass
    bucket, key = parse_s3_url(s3_url.path_with_protocol)
    config = botocore.config.Config(max_pool_connections=GLOBAL_MAX_WORKERS)
    client = get_s3_client_with_cache(config=config, profile_name=s3_url._profile_name)

    if "a" in mode or "+" in mode:
        if cache_path is None:
            return S3MemoryHandler(
                bucket, key, mode, s3_client=client, profile_name=s3_url._profile_name
            )
        return S3CachedHandler(
            bucket,
            key,
            mode,
            s3_client=client,
            cache_path=cache_path,
            profile_name=s3_url._profile_name,
        )

    if mode == "rb":
        if share_cache_key is not None:
            reader = S3ShareCacheReader(
                bucket,
                key,
                cache_key=share_cache_key,
                s3_client=client,
                max_retries=max_retries,
                max_workers=max_workers,
                block_size=block_size or READER_BLOCK_SIZE,
                block_forward=block_forward,
                profile_name=s3_url._profile_name,
            )
        else:
            if max_buffer_size is None:
                max_buffer_size = READER_MAX_BUFFER_SIZE
            reader = S3PrefetchReader(
                bucket,
                key,
                s3_client=client,
                max_retries=max_retries,
                max_workers=max_workers,
                max_buffer_size=max_buffer_size,
                block_forward=block_forward,
                block_size=block_size or READER_BLOCK_SIZE,
                profile_name=s3_url._profile_name,
            )
        if buffered or _is_pickle(reader):
            reader = io.BufferedReader(reader)  # type: ignore
        return reader

    if limited_seekable:
        if max_buffer_size is None:
            max_buffer_size = WRITER_MAX_BUFFER_SIZE
        writer = S3LimitedSeekableWriter(
            bucket,
            key,
            s3_client=client,
            max_workers=max_workers,
            block_size=block_size or WRITER_BLOCK_SIZE,
            max_buffer_size=max_buffer_size,
            profile_name=s3_url._profile_name,
        )
    else:
        if max_buffer_size is None:
            max_buffer_size = WRITER_MAX_BUFFER_SIZE
        writer = S3BufferedWriter(
            bucket,
            key,
            s3_client=client,
            max_workers=max_workers,
            block_size=block_size or WRITER_BLOCK_SIZE,
            max_buffer_size=max_buffer_size,
            profile_name=s3_url._profile_name,
        )
    if buffered or _is_pickle(writer):
        writer = io.BufferedWriter(writer)  # type: ignore
    return writer


@_s3_binary_mode
def s3_memory_open(
    s3_url: PathLike, mode: str, followlinks: bool = False
) -> S3MemoryHandler:
    """Open a memory-cache file reader / writer, for frequent random read / write

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

    :param mode: Mode to open file, could be one of "rb", "wb", "ab", "rb+",
        "wb+" or "ab+"
    :returns: An opened BufferedReader / BufferedWriter object
    """
    if mode not in ("rb", "wb", "ab", "rb+", "wb+", "ab+"):
        raise ValueError("unacceptable mode: %r" % mode)
    if not isinstance(s3_url, S3Path):
        s3_url = S3Path(s3_url)
    if followlinks:
        try:
            s3_url = s3_url.readlink()
        except S3NotALinkError:
            pass

    bucket, key = parse_s3_url(s3_url.path_with_protocol)
    config = botocore.config.Config(max_pool_connections=GLOBAL_MAX_WORKERS)
    client = get_s3_client_with_cache(config=config, profile_name=s3_url._profile_name)
    return S3MemoryHandler(
        bucket, key, mode, s3_client=client, profile_name=s3_url._profile_name
    )


s3_open = s3_buffered_open


def s3_download(
    src_url: PathLike,
    dst_url: PathLike,
    callback: Optional[Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """
    Downloads a file from s3 to local filesystem.

    :param src_url: source s3 path
    :param dst_url: target fs path
    :param callback: Called periodically during copy, and the input parameter is
        the data size (in bytes) of copy since the last call
    :param followlinks: False if regard symlink as file, else True
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    from megfile.fs import is_fs
    from megfile.fs_path import FSPath

    dst_url = fspath(dst_url)
    if not is_fs(dst_url):
        raise OSError(f"dst_url is not fs path: {dst_url}")
    if not dst_url or dst_url.endswith("/"):
        raise S3IsADirectoryError("Is a directory: %r" % dst_url)

    dst_path = FSPath(dst_url)
    if not overwrite and dst_path.exists():
        return

    if not isinstance(src_url, S3Path):
        src_url = S3Path(src_url)
    if followlinks:
        try:
            src_url = src_url.readlink()
        except S3NotALinkError:
            pass
    src_bucket, src_key = parse_s3_url(src_url.path_with_protocol)
    if not src_bucket:
        raise S3BucketNotFoundError(
            "Empty bucket name: %r" % src_url.path_with_protocol
        )

    if not src_url.is_file():
        if not src_url.is_dir():
            raise S3FileNotFoundError("File not found: %r" % src_url.path_with_protocol)
        raise S3IsADirectoryError("Is a directory: %r" % src_url.path_with_protocol)

    dst_directory = os.path.dirname(dst_path.path_without_protocol)  # pyre-ignore[6]
    if dst_directory != "":
        os.makedirs(dst_directory, exist_ok=True)

    client = get_s3_client_with_cache(profile_name=src_url._profile_name)
    download_file = patch_method(
        client.download_file, max_retries=max_retries, should_retry=s3_should_retry
    )

    transfer_config = TransferConfig(
        multipart_threshold=READER_BLOCK_SIZE,
        max_concurrency=GLOBAL_MAX_WORKERS,
        multipart_chunksize=READER_BLOCK_SIZE,
        num_download_attempts=S3_MAX_RETRY_TIMES,
        max_io_queue=max(READER_MAX_BUFFER_SIZE // READER_BLOCK_SIZE, 1),
    )
    try:
        download_file(
            src_bucket,
            src_key,
            dst_path.path_without_protocol,
            Callback=callback,
            Config=transfer_config,
        )
    except Exception as error:
        error = translate_fs_error(error, dst_url)
        error = translate_s3_error(error, src_url.path_with_protocol)
        raise error

    src_stat = src_url.stat()
    os.utime(dst_path.path_without_protocol, (src_stat.st_mtime, src_stat.st_mtime))


def s3_upload(
    src_url: PathLike,
    dst_url: PathLike,
    callback: Optional[Callable[[int], None]] = None,
    followlinks: bool = False,
    overwrite: bool = True,
) -> None:
    """
    Uploads a file from local filesystem to s3.

    :param src_url: source fs path
    :param dst_url: target s3 path
    :param callback: Called periodically during copy, and the input parameter is
        the data size (in bytes) of copy since the last call
    :param followlinks: False if regard symlink as file, else True
    :param overwrite: whether or not overwrite file when exists, default is True
    """
    from megfile.fs import is_fs
    from megfile.fs_path import FSPath

    if not is_fs(src_url):
        raise OSError(f"src_url is not fs path: {src_url}")
    src_path = FSPath(src_url)
    if followlinks and src_path.is_symlink():
        src_path = src_path.readlink()

    dst_bucket, dst_key = parse_s3_url(dst_url)
    if not dst_bucket:
        raise S3BucketNotFoundError("Empty bucket name: %r" % dst_url)
    if not dst_key or dst_key.endswith("/"):
        raise S3IsADirectoryError("Is a directory: %r" % dst_url)

    if not overwrite and S3Path(dst_url).is_file():
        return

    client = get_s3_client_with_cache(profile_name=S3Path(dst_url)._profile_name)
    upload_fileobj = patch_method(
        client.upload_fileobj, max_retries=max_retries, should_retry=s3_should_retry
    )

    transfer_config = TransferConfig(
        multipart_threshold=WRITER_BLOCK_SIZE,
        max_concurrency=GLOBAL_MAX_WORKERS,
        multipart_chunksize=WRITER_BLOCK_SIZE,
    )
    with open(src_path.path_without_protocol, "rb") as src, raise_s3_error(dst_url):
        upload_fileobj(
            src,
            Bucket=dst_bucket,
            Key=dst_key,
            Callback=callback,
            Config=transfer_config,
        )


def s3_load_content(
    s3_url,
    start: Optional[int] = None,
    stop: Optional[int] = None,
) -> bytes:
    """
    Get specified file from [start, stop) in bytes

    :param s3_url: Specified path
    :param start: start index
    :param stop: stop index
    :returns: bytes content in range [start, stop)
    """

    def _get_object(client, bucket, key, range_str):
        return client.get_object(Bucket=bucket, Key=key, Range=range_str)["Body"].read()

    s3_url = S3Path(s3_url)

    bucket, key = parse_s3_url(s3_url.path_with_protocol)
    if not bucket:
        raise S3BucketNotFoundError("Empty bucket name: %r" % s3_url)
    if not key or key.endswith("/"):
        raise S3IsADirectoryError("Is a directory: %r" % s3_url)

    start, stop = get_content_offset(start, stop, s3_url.getsize(follow_symlinks=False))
    if start == 0 and stop == 0:
        return b""
    range_str = "bytes=%d-%d" % (start, stop - 1)

    client = get_s3_client_with_cache(profile_name=s3_url._profile_name)
    with raise_s3_error(s3_url.path):
        return patch_method(
            _get_object, max_retries=max_retries, should_retry=s3_should_retry
        )(client, bucket, key, range_str)


class S3Cacher(FileCacher):
    cache_path = None

    def __init__(self, path: str, cache_path: Optional[str] = None, mode: str = "r"):
        if mode not in ("r", "w", "a"):
            raise ValueError("unacceptable mode: %r" % mode)
        if cache_path is None:
            cache_path = generate_cache_path(path)
        if mode in ("r", "a"):
            s3_download(path, cache_path)
        self.name = path
        self.mode = mode
        self.cache_path = cache_path

    def _close(self):
        if getattr(self, "cache_path", None) is not None and os.path.exists(
            self.cache_path
        ):
            if self.mode in ("w", "a"):
                s3_upload(self.cache_path, self.name)
            os.unlink(self.cache_path)


def _group_src_paths_by_block(
    src_paths: List[PathLike], block_size: int = READER_BLOCK_SIZE
) -> List[List[Tuple[PathLike, Optional[str]]]]:
    groups = []
    current_group, current_group_size = [], 0
    for src_path in src_paths:
        current_file_size = S3Path(src_path).stat().size
        if current_file_size == 0:
            continue

        if current_file_size >= block_size:
            if len(groups) == 0:
                if current_group_size + current_file_size > 2 * block_size:
                    group_lack_size = block_size - current_group_size
                    current_group.append((src_path, f"bytes=0-{group_lack_size - 1}"))
                    groups.extend(
                        [
                            current_group,
                            [
                                (
                                    src_path,
                                    f"bytes={group_lack_size}-{current_file_size - 1}",
                                )
                            ],
                        ]
                    )
                else:
                    current_group.append((src_path, None))
                    groups.append(current_group)
            else:
                groups[-1].extend(current_group)
                groups.append([(src_path, None)])
            current_group, current_group_size = [], 0
        else:
            current_group.append((src_path, None))
            current_group_size += current_file_size
            if current_group_size >= block_size:
                groups.append(current_group)
                current_group, current_group_size = [], 0
    if current_group:
        groups.append(current_group)
    return groups


def s3_concat(
    src_paths: List[PathLike],
    dst_path: PathLike,
    block_size: int = READER_BLOCK_SIZE,
    max_workers: int = GLOBAL_MAX_WORKERS,
) -> None:
    """Concatenate s3 files to one file.

    :param src_paths: Given source paths
    :param dst_path: Given destination path
    """
    client = S3Path(dst_path)._client
    with raise_s3_error(dst_path):
        if block_size == 0:
            groups = [[(src_path, None)] for src_path in src_paths]
        else:
            groups = _group_src_paths_by_block(src_paths, block_size=block_size)

        with (
            MultiPartWriter(client, dst_path) as writer,
            ThreadPoolExecutor(max_workers=max_workers) as executor,
        ):
            for index, group in enumerate(groups, start=1):
                if len(group) == 1:
                    executor.submit(
                        writer.upload_part_copy, index, group[0][0], group[0][1]
                    )
                else:
                    executor.submit(writer.upload_part_by_paths, index, group)


@SmartPath.register
class S3Path(URIPath):
    protocol = "s3"

    def __init__(self, path: "PathLike", *other_paths: "PathLike"):
        super().__init__(path, *other_paths)
        protocol = get_url_scheme(self.path)
        self._protocol_with_profile = self.protocol
        self._profile_name = None
        if protocol.startswith("s3+"):
            self._protocol_with_profile = protocol
            self._profile_name = protocol[3:]
            self._s3_path = f"s3://{self.path[len(protocol) + 3 :]}"
        elif not protocol:
            self._s3_path = f"s3://{self.path.lstrip('/')}"
        else:
            self._s3_path = self.path

    @cached_property
    def path_with_protocol(self) -> str:
        """Return path with protocol, like file:///root, s3://bucket/key"""
        path = self.path
        protocol_prefix = self._protocol_with_profile + "://"
        if path.startswith(protocol_prefix):
            return path
        return protocol_prefix + path.lstrip("/")

    @cached_property
    def path_without_protocol(self) -> str:
        """
        Return path without protocol, example: if path is s3://bucket/key,
        return bucket/key
        """
        path = self.path
        protocol_prefix = self._protocol_with_profile + "://"
        if path.startswith(protocol_prefix):
            path = path[len(protocol_prefix) :]
        return path

    @cached_property
    def parts(self) -> Tuple[str, ...]:
        """A tuple giving access to the path’s various components"""
        parts = [f"{self._protocol_with_profile}://"]
        path = self.path_without_protocol
        path = path.lstrip("/")
        if path != "":
            parts.extend(path.split("/"))
        return tuple(parts)

    @cached_property
    def _client(self):
        return get_s3_client_with_cache(profile_name=self._profile_name)

    def _s3_get_metadata(self) -> dict:
        """
        Get object metadata

        :param path: Object path
        :returns: Object metadata
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            return {}
        if not key or key.endswith("/"):
            return {}
        try:
            with raise_s3_error(self.path_with_protocol):
                resp = self._client.head_object(Bucket=bucket, Key=key)
            return dict((key.lower(), value) for key, value in resp["Metadata"].items())
        except Exception as error:
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error
            return {}

    def access(self, mode: Access = Access.READ) -> bool:
        """
        Test if path has access permission described by mode

        :param mode: access mode
        :returns: bool, if the bucket of s3_url has read/write access.
        """
        s3_url = self.path_with_protocol
        bucket, key = parse_s3_url(s3_url)  # only check bucket accessibility
        if not bucket:
            raise Exception("No available bucket")
        if not isinstance(mode, Access):
            raise TypeError(
                "Unsupported mode: {} -- Mode should use one of "
                "the enums belonging to:  {}".format(
                    mode, ", ".join([str(a) for a in Access])
                )
            )
        if mode not in (Access.READ, Access.WRITE):
            raise TypeError("Unsupported mode: {}".format(mode))
        try:
            if not self.exists():
                return False
        except Exception as error:
            error = translate_s3_error(error, s3_url)
            if isinstance(error, S3PermissionError):
                return False
            raise error

        if mode == Access.READ:
            return True
        try:
            if not key:
                key = "test"
            elif key.endswith("/"):
                key = key[:-1]
            upload_id = self._client.create_multipart_upload(Bucket=bucket, Key=key)[
                "UploadId"
            ]
            self._client.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id
            )
            return True
        except Exception as error:
            error = translate_s3_error(error, s3_url)
            if isinstance(error, S3PermissionError):
                return False
            raise error

    def exists(self, followlinks: bool = False) -> bool:
        """
        Test if s3_url exists

        If the bucket of s3_url are not permitted to read, return False

        :returns: True if s3_url exists, else False
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:  # s3:// => True, s3:///key => False
            return not key

        return self.is_file(followlinks) or self.is_dir()

    def getmtime(self, follow_symlinks: bool = False) -> float:
        """
        Get last-modified time of the file on the given s3_url path
        (in Unix timestamp format).

        If the path is an existent directory, return the latest modified time of
        all file in it. The mtime of empty directory is 1970-01-01 00:00:00

        If s3_url is not an existent path, which means s3_exist(s3_url) returns False,
        then raise S3FileNotFoundError

        :returns: Last-modified time
        :raises: S3FileNotFoundError, UnsupportedError
        """
        return self.stat(follow_symlinks=follow_symlinks).mtime

    def getsize(self, follow_symlinks: bool = False) -> int:
        """
        Get file size on the given s3_url path (in bytes).

        If the path in a directory, return the sum of all file size in it,
        including file in subdirectories (if exist).

        The result excludes the size of directory itself.
        In other words, return 0 Byte on an empty directory path.

        If s3_url is not an existent path, which means s3_exist(s3_url) returns False,
        then raise S3FileNotFoundError

        :returns: File size
        :raises: S3FileNotFoundError, UnsupportedError
        """
        return self.stat(follow_symlinks=follow_symlinks).size

    def glob(
        self,
        pattern,
        recursive: bool = True,
        missing_ok: bool = True,
    ) -> List["S3Path"]:
        """Return s3 path list in ascending alphabetical order,
        in which path matches glob pattern

        Notes: Only glob in bucket. If trying to match bucket with wildcard characters,
        raise UnsupportedError

        :param pattern: Glob the given relative pattern in the directory represented
            by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :raises: UnsupportedError, when bucket part contains wildcard characters
        :returns: A list contains paths match `s3_pathname`
        """
        return list(
            self.iglob(
                pattern=pattern,
                recursive=recursive,
                missing_ok=missing_ok,
            )
        )

    def glob_stat(
        self,
        pattern,
        recursive: bool = True,
        missing_ok: bool = True,
    ) -> Iterator[FileEntry]:
        """Return a generator contains tuples of path and file stat,
        in ascending alphabetical order, in which path matches glob pattern

        Notes: Only glob in bucket. If trying to match bucket with wildcard characters,
        raise UnsupportedError

        :param pattern: Glob the given relative pattern in the directory represented
            by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :raises: UnsupportedError, when bucket part contains wildcard characters
        :returns: A generator contains tuples of path and file stat,
            in which paths match `s3_pathname`
        """
        glob_path = self.path_with_protocol
        if pattern:
            glob_path = self.joinpath(pattern).path_with_protocol
        s3_pathname = fspath(glob_path)

        def create_generator():
            for group_s3_pathname_1 in _group_s3path_by_bucket(s3_pathname):
                for group_s3_pathname_2 in _group_s3path_by_prefix(group_s3_pathname_1):
                    for file_entry in _s3_glob_stat_single_path(
                        group_s3_pathname_2,
                        recursive,
                        missing_ok,
                    ):
                        yield file_entry

        return _create_missing_ok_generator(
            create_generator(),
            missing_ok,
            S3FileNotFoundError("No match any file: %r" % s3_pathname),
        )

    def iglob(
        self,
        pattern,
        recursive: bool = True,
        missing_ok: bool = True,
    ) -> Iterator["S3Path"]:
        """Return s3 path iterator in ascending alphabetical order,
        in which path matches glob pattern

        Notes: Only glob in bucket. If trying to match bucket with wildcard characters,
        raise UnsupportedError

        :param pattern: Glob the given relative pattern in the directory represented
            by this path
        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file,
            raise FileNotFoundError
        :raises: UnsupportedError, when bucket part contains wildcard characters
        :returns: An iterator contains paths match `s3_pathname`
        """
        for file_entry in self.glob_stat(
            pattern=pattern,
            recursive=recursive,
            missing_ok=missing_ok,
        ):
            yield self.from_path(file_entry.path)

    def is_dir(self, followlinks: bool = False) -> bool:
        """
        Test if an s3 url is directory
        Specific procedures are as follows:
        If there exists a suffix, of which ``os.path.join(s3_url, suffix)`` is a file
        If the url is empty bucket or s3://

        :param followlinks: whether followlinks is True or False, result is the same.
            Because s3 symlink not support dir.
        :returns: True if path is s3 directory, else False
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:  # s3:// => True, s3:///key => False
            return not key
        prefix = _become_prefix(key)
        try:
            resp = self._client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, Delimiter="/", MaxKeys=1
            )
        except Exception as error:
            error = translate_s3_error(error, self.path_with_protocol)
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error
            return False

        if not key:  # bucket is accessible
            return True

        if "KeyCount" in resp:
            return resp["KeyCount"] > 0

        return (
            len(resp.get("Contents", [])) > 0 or len(resp.get("CommonPrefixes", [])) > 0
        )

    def is_file(self, followlinks: bool = False) -> bool:
        """
        Test if an s3_url is file

        :returns: True if path is s3 file, else False
        """
        s3_url = self.path_with_protocol
        if followlinks:
            try:
                s3_url = self.readlink().path_with_protocol
            except S3NotALinkError:
                pass
        bucket, key = parse_s3_url(s3_url)
        if not bucket or not key or key.endswith("/"):
            # s3://, s3:///key, s3://bucket, s3://bucket/prefix/
            return False
        try:
            self._client.head_object(Bucket=bucket, Key=key)
        except Exception as error:
            error = translate_s3_error(error, s3_url)
            if isinstance(error, (S3UnknownError, S3ConfigError, S3PermissionError)):
                raise error
            return False
        return True

    def listdir(self) -> List[str]:
        """
        Get all contents of given s3_url. The result is in ascending alphabetical order.

        :param missing_ok: if True and target directory not exists return empty list,
            default is True.
        :returns: All contents have prefix of s3_url in ascending alphabetical order
        :raises: S3FileNotFoundError, S3NotADirectoryError
        """
        with self.scandir() as entries:
            return sorted([entry.name for entry in entries])

    def iterdir(self) -> Iterator["S3Path"]:
        """
        Get all contents of given s3_url. The order of result is in arbitrary order.

        :returns: All contents have prefix of s3_url
        :raises: S3FileNotFoundError, S3NotADirectoryError
        """
        with self.scandir() as entries:
            for entry in entries:
                yield self.joinpath(entry.name)

    def load(self) -> BinaryIO:
        """Read all content in binary on specified path and write into memory

        User should close the BinaryIO manually

        :returns: BinaryIO
        """
        s3_url = self.path_with_protocol
        bucket, key = parse_s3_url(s3_url)
        if not bucket:
            raise S3BucketNotFoundError("Empty bucket name: %r" % s3_url)
        if not key or key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % s3_url)

        buffer = io.BytesIO()
        with raise_s3_error(s3_url):
            self._client.download_fileobj(bucket, key, buffer)
        buffer.seek(0)
        return buffer

    def hasbucket(self) -> bool:
        """
        Test if the bucket of s3_url exists

        :returns: True if bucket of s3_url exists, else False
        """
        bucket, _ = parse_s3_url(self.path_with_protocol)
        if not bucket:
            return False

        try:
            self._client.head_bucket(Bucket=bucket)
        except Exception as error:
            error = translate_s3_error(error, self.path_with_protocol)
            if isinstance(error, S3PermissionError):
                # Aliyun OSS doesn't give bucket api permission when you only have read
                # and write permission
                try:
                    self._client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                    return True
                except Exception as error2:
                    error2 = translate_s3_error(error2, self.path_with_protocol)
                    if isinstance(
                        error2, (S3UnknownError, S3ConfigError, S3PermissionError)
                    ):
                        raise error2
                    return False
            elif isinstance(error, (S3UnknownError, S3ConfigError)):
                raise error
            elif isinstance(error, S3FileNotFoundError):
                return False

        return True

    def mkdir(self, mode=0o777, parents: bool = False, exist_ok: bool = False):
        """
        Create an s3 directory.
        Purely creating directory is invalid because it's unavailable on OSS.
        This function is to test the target bucket have WRITE access.

        :param mode: mode is ignored, only be compatible with pathlib.Path
        :param parents: parents is ignored, only be compatible with pathlib.Path
        :param exist_ok: If False and target directory exists, raise S3FileExistsError
        :raises: S3BucketNotFoundError, S3FileExistsError
        """
        bucket, _ = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )
        try:
            if not self.hasbucket():
                raise S3BucketNotFoundError(
                    "No such bucket: %r" % self.path_with_protocol
                )
        except S3PermissionError:
            pass
        if exist_ok:
            return
        if self.exists():
            raise S3FileExistsError("File exists: %r" % self.path_with_protocol)

    def move(self, dst_url: PathLike, overwrite: bool = True) -> None:
        """
        Move file/directory path from src_url to dst_url

        :param dst_url: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        for src_file_path, dst_file_path in _s3_scan_pairs(
            self.path_with_protocol, dst_url
        ):
            S3Path(src_file_path).rename(dst_file_path, overwrite)

    def remove(self, missing_ok: bool = False) -> None:
        """
        Remove the file or directory on s3, `s3://` and `s3://bucket`
        are not permitted to remove

        :param missing_ok: if False and target file/directory not exists,
            raise S3FileNotFoundError
        :raises: S3PermissionError, S3FileNotFoundError, UnsupportedError
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            if not key:
                raise UnsupportedError("Remove whole s3", self.path_with_protocol)
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )
        if not key:
            raise UnsupportedError("Remove bucket", self.path_with_protocol)
        if not self.exists():
            if missing_ok:
                return
            raise S3FileNotFoundError(
                "No such file or directory: %r" % self.path_with_protocol
            )

        client = self._client
        with raise_s3_error(self.path_with_protocol):
            if self.is_file():
                client.delete_object(Bucket=bucket, Key=key)
                return
            prefix = _become_prefix(key)
            total_count, error_count = 0, 0
            for resp in _list_objects_recursive(client, bucket, prefix):
                if "Contents" in resp:
                    keys = [{"Key": content["Key"]} for content in resp["Contents"]]
                    total_count += len(keys)
                    errors = []
                    retries = 2
                    retry_interval = min(0.1 * 2**retries, 30)
                    for i in range(retries):
                        # doc:
                        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
                        if not keys:
                            break
                        response = client.delete_objects(
                            Bucket=bucket, Delete={"Objects": keys}
                        )
                        keys = []
                        for error_info in response.get("Errors", []):
                            if s3_error_code_should_retry(error_info.get("Code")):
                                error_logger.warning(
                                    "retry %s times, removing file: %s, "
                                    "with error %s: %s"
                                    % (
                                        i + 1,
                                        error_info["Key"],
                                        error_info["Code"],
                                        error_info["Message"],
                                    )
                                )
                                keys.append({"Key": error_info["Key"]})
                            else:
                                errors.append(error_info)
                        time.sleep(retry_interval)
                    for error_info in errors:
                        error_logger.error(
                            "failed remove file: %s, with error %s: %s"
                            % (
                                error_info["Key"],
                                error_info["Code"],
                                error_info["Message"],
                            )
                        )
                    error_count += len(errors)
            if error_count > 0:
                error_msg = (
                    "failed remove path: %s, total file count: %s, failed count: %s"
                    % (self.path_with_protocol, total_count, error_count)
                )
                raise S3UnknownError(Exception(error_msg), self.path_with_protocol)

    def rename(self, dst_path: PathLike, overwrite: bool = True) -> "S3Path":
        """
        Move s3 file path from src_url to dst_url

        :param dst_path: Given destination path
        :param overwrite: whether or not overwrite file when exists
        """
        if self.is_file():
            self.copy(dst_path, overwrite=overwrite)
        else:
            self.sync(dst_path, overwrite=overwrite)
        self.remove(missing_ok=True)
        return self.from_path(dst_path)

    def scan(self, missing_ok: bool = True, followlinks: bool = False) -> Iterator[str]:
        """
        Iteratively traverse only files in given s3 directory, in alphabetical order.
        Every iteration on generator yields a path string.

        If s3_url is a file path, yields the file only

        If s3_url is a non-existent path, return an empty generator

        If s3_url is a bucket path, return all file paths in the bucket

        If s3_url is an empty bucket, return an empty generator

        If s3_url doesn't contain any bucket, which is s3_url == 's3://',
        raise UnsupportedError. walk() on complete s3 is not supported in megfile

        :param missing_ok: If False and there's no file in the directory,
            raise FileNotFoundError
        :raises: UnsupportedError
        :returns: A file path generator
        """
        scan_stat_iter = self.scan_stat(missing_ok=missing_ok, followlinks=followlinks)

        def create_generator() -> Iterator[str]:
            for file_entry in scan_stat_iter:
                yield file_entry.path

        return create_generator()

    def scan_stat(
        self, missing_ok: bool = True, followlinks: bool = False
    ) -> Iterator[FileEntry]:
        """
        Iteratively traverse only files in given directory, in alphabetical order.
        Every iteration on generator yields a tuple of path string and file stat

        :param missing_ok: If False and there's no file in the directory,
            raise FileNotFoundError
        :raises: UnsupportedError
        :returns: A file path generator
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise UnsupportedError("Scan whole s3", self.path_with_protocol)

        def create_generator() -> Iterator[FileEntry]:
            # On s3, file and directory may be of same name and level, so need
            # to test the path is file or directory
            if not key.endswith("/") and self.is_file():
                yield FileEntry(
                    self.name,
                    fspath(self.path_with_protocol),
                    self.stat(follow_symlinks=followlinks),
                )

            prefix = _become_prefix(key)
            client = self._client

            def suppress_error_callback(e):
                if missing_ok and isinstance(e, S3BucketNotFoundError):
                    return True
                return False

            with raise_s3_error(self.path_with_protocol, suppress_error_callback):
                for resp in _list_objects_recursive(client, bucket, prefix):
                    for content in resp.get("Contents", []):
                        full_path = s3_path_join(
                            f"{self._protocol_with_profile}://", bucket, content["Key"]
                        )

                        if followlinks:
                            try:
                                origin_path = self.from_path(full_path).readlink()
                                yield FileEntry(
                                    origin_path.name,
                                    origin_path.path_with_protocol,
                                    origin_path.lstat(),
                                )
                                continue
                            except S3NotALinkError:
                                pass

                        yield FileEntry(
                            S3Path(full_path).name, full_path, _make_stat(content)
                        )

        return _create_missing_ok_generator(
            create_generator(),
            missing_ok,
            S3FileNotFoundError("No match any file in: %r" % self.path_with_protocol),
        )

    def scandir(self) -> ContextIterator:
        """
        Get all contents of given s3_url, the order of result is in arbitrary order.

        :returns: All contents have prefix of s3_url
        :raises: S3BucketNotFoundError, S3FileNotFoundError, S3NotADirectoryError
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket and key:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )

        if self.is_file():
            raise S3NotADirectoryError("Not a directory: %r" % self.path_with_protocol)

        # In order to do check on creation,
        # we need to wrap the iterator in another function
        def create_generator() -> Iterator[FileEntry]:
            prefix = _become_prefix(key)
            client = self._client

            def generate_s3_path(protocol: str, bucket: str, key: str) -> str:
                return "%s://%s/%s" % (protocol, bucket, key)

            if not bucket and not key:  # list buckets
                response = client.list_buckets()
                for content in response["Buckets"]:
                    yield FileEntry(
                        content["Name"],
                        f"{self._protocol_with_profile}://{content['Name']}",
                        StatResult(
                            ctime=content["CreationDate"].timestamp(),
                            isdir=True,
                            extra=content,
                        ),
                    )
                return

            for resp in _list_objects_recursive(client, bucket, prefix, "/"):
                for common_prefix in resp.get("CommonPrefixes", []):
                    yield FileEntry(
                        common_prefix["Prefix"][len(prefix) : -1],
                        generate_s3_path(
                            self._protocol_with_profile,
                            bucket,
                            common_prefix["Prefix"],
                        ),
                        StatResult(isdir=True, extra=common_prefix),
                    )
                for content in resp.get("Contents", []):
                    src_url = generate_s3_path(
                        self._protocol_with_profile, bucket, content["Key"]
                    )
                    yield FileEntry(  # pytype: disable=wrong-arg-types
                        content["Key"][len(prefix) :],
                        src_url,
                        _make_stat_without_metadata(content, self.from_path(src_url)),
                    )

        def missing_ok_generator():
            def suppress_error_callback(e):
                if isinstance(e, S3BucketNotFoundError):
                    return False
                elif not key and isinstance(e, S3FileNotFoundError):
                    return True
                return False

            with raise_s3_error(self.path_with_protocol, suppress_error_callback):
                yield from _create_missing_ok_generator(
                    create_generator(),
                    missing_ok=False,
                    error=S3FileNotFoundError(
                        "No such directory: %r" % self.path_with_protocol
                    ),
                )

        return ContextIterator(missing_ok_generator())

    def _get_dir_stat(self) -> StatResult:
        """
        Return StatResult of given s3_url directory, including:

        1. Directory size: the sum of all file size in it,
           including file in subdirectories (if exist).
           The result excludes the size of directory itself.
           In other words, return 0 Byte on an empty directory path
        2. Last-modified time of directory: return the latest modified time
           of all file in it. The mtime of empty directory is 1970-01-01 00:00:00

        :returns: An int indicates size in Bytes
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        prefix = _become_prefix(key)
        client = self._client
        count, size, mtime = 0, 0, 0.0
        with raise_s3_error(self.path_with_protocol):
            for resp in _list_objects_recursive(client, bucket, prefix):
                for content in resp.get("Contents", []):
                    count += 1
                    size += content["Size"]
                    last_modified = content["LastModified"].timestamp()
                    if mtime < last_modified:
                        mtime = last_modified

        if count == 0:
            raise S3FileNotFoundError(
                "No such file or directory: %r" % self.path_with_protocol
            )

        return StatResult(size=size, mtime=mtime, isdir=True)

    def stat(self, follow_symlinks=True) -> StatResult:
        """
        Get StatResult of s3_url file, including file size and mtime,
        referring to s3_getsize and s3_getmtime

        If s3_url is not an existent path, which means s3_exist(s3_url) returns False,
        then raise S3FileNotFoundError

        If attempt to get StatResult of complete s3, such as s3_dir_url == 's3://',
        raise S3BucketNotFoundError

        :returns: StatResult
        :raises: S3FileNotFoundError, S3BucketNotFoundError
        """
        islnk = False
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )

        if not self.is_file():
            return self._get_dir_stat()

        client = self._client
        with raise_s3_error(self.path_with_protocol):
            content = client.head_object(Bucket=bucket, Key=key)
            if "Metadata" in content:
                metadata = dict(
                    (key.lower(), value) for key, value in content["Metadata"].items()
                )
                if metadata and "symlink_to" in metadata:
                    islnk = True
                    if islnk and follow_symlinks:
                        s3_url = metadata["symlink_to"]
                        bucket, key = parse_s3_url(s3_url)
                        content = client.head_object(Bucket=bucket, Key=key)
            stat_record = StatResult(
                islnk=islnk,
                size=content["ContentLength"],
                mtime=content["LastModified"].timestamp(),
                extra=content,
            )
        return stat_record

    def unlink(self, missing_ok: bool = False) -> None:
        """
        Remove the file on s3

        :param missing_ok: if False and target file not exists,
            raise S3FileNotFoundError
        :raises: S3PermissionError, S3FileNotFoundError, S3IsADirectoryError
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket or not key or key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % self.path_with_protocol)
        if not self.is_file():
            if missing_ok:
                return
            raise S3FileNotFoundError("No such file: %r" % self.path_with_protocol)

        with raise_s3_error(self.path_with_protocol):
            self._client.delete_object(Bucket=bucket, Key=key)

    def walk(
        self, followlinks: bool = False
    ) -> Iterator[Tuple[str, List[str], List[str]]]:
        """
        Iteratively traverse the given s3 directory, in top-bottom order.
        In other words, firstly traverse parent directory, if subdirectories exist,
        traverse the subdirectories in alphabetical order.

        Every iteration on generator yields a 3-tuple: (root, dirs, files)

        - root: Current s3 path;
        - dirs: Name list of subdirectories in current directory.
          The list is sorted by name in ascending alphabetical order;
        - files: Name list of files in current directory.
          The list is sorted by name in ascending alphabetical order;

        If s3_url is a file path, return an empty generator

        If s3_url is a non-existent path, return an empty generator

        If s3_url is a bucket path, bucket will be the top directory,
        and will be returned at first iteration of generator

        If s3_url is an empty bucket, only yield one 3-tuple
        (notes: s3 doesn't have empty directory)

        If s3_url doesn't contain any bucket, which is s3_url == 's3://',
        raise UnsupportedError. walk() on complete s3 is not supported in megfile

        :param followlinks: whether followlinks is True or False, result is the same.
            Because s3 symlink not support dir.
        :raises: UnsupportedError
        :returns: A 3-tuple generator
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise UnsupportedError("Walk whole s3", self.path_with_protocol)

        with raise_s3_error(self.path_with_protocol, S3BucketNotFoundError):
            stack = [key]
            client = self._client
            while len(stack) > 0:
                current = _become_prefix(stack.pop())
                dirs, files = [], []
                for resp in _list_objects_recursive(client, bucket, current, "/"):
                    for common_prefix in resp.get("CommonPrefixes", []):
                        dirs.append(common_prefix["Prefix"][:-1])
                    for content in resp.get("Contents", []):
                        files.append(content["Key"])

                dirs = sorted(dirs)
                stack.extend(reversed(dirs))

                root = s3_path_join(
                    f"{self._protocol_with_profile}://", bucket, current
                )[:-1]
                dirs = [path[len(current) :] for path in dirs]
                files = sorted(path[len(current) :] for path in files)
                if files or dirs or not current:
                    yield root, dirs, files

    def md5(self, recalculate: bool = False, followlinks: bool = False) -> str:
        """
        Get md5 meta info in files that uploaded/copied via megfile

        If meta info is lost or non-existent, return None

        :param recalculate: calculate md5 in real-time or return s3 etag
        :param followlinks: If is True, calculate md5 for real file
        :returns: md5 meta info
        """
        bucket, _ = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )
        stat = self.stat(follow_symlinks=False)
        if followlinks and stat.is_symlink():
            return self.readlink().md5(recalculate=recalculate, followlinks=followlinks)
        elif stat.is_dir():
            hash_md5 = hashlib.md5()  # nosec
            for file_name in self.listdir():
                chunk = (
                    self.joinpath(file_name)
                    .md5(recalculate=recalculate, followlinks=followlinks)
                    .encode()
                )
                hash_md5.update(chunk)
            return hash_md5.hexdigest()
        if recalculate:
            path_instance = self
            if followlinks:
                try:
                    path_instance = self.readlink()
                except S3NotALinkError:
                    pass
            with path_instance.open("rb") as f:
                return calculate_md5(f)
        return stat.extra.get("ETag", "")[1:-1]

    def copy(
        self,
        dst_url: PathLike,
        callback: Optional[Callable[[int], None]] = None,
        followlinks: bool = False,
        overwrite: bool = True,
    ) -> None:
        """File copy on S3
        Copy content of file on `src_path` to `dst_path`.
        It's caller's responsibility to ensure the s3_isfile(src_url) is True

        :param dst_path: Target file path
        :param callback: Called periodically during copy, and the input parameter is
            the data size (in bytes) of copy since the last call
        :param followlinks: False if regard symlink as file, else True
        :param overwrite: whether or not overwrite file when exists, default is True
        """
        if not overwrite and self.from_path(dst_url).is_file():
            return

        src_url = self.path_with_protocol
        src_bucket, src_key = parse_s3_url(src_url)
        dst_bucket, dst_key = parse_s3_url(dst_url)
        if dst_bucket == src_bucket and src_key.rstrip("/") == dst_key.rstrip("/"):
            raise SameFileError(f"'{src_url}' and '{dst_url}' are the same file")

        if not src_bucket:
            raise S3BucketNotFoundError("Empty bucket name: %r" % src_url)
        if not dst_bucket:
            raise S3BucketNotFoundError("Empty bucket name: %r" % dst_url)
        if not dst_key or dst_key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % dst_url)

        if followlinks:
            try:
                s3_url = self.readlink().path
                src_bucket, src_key = parse_s3_url(s3_url)
            except S3NotALinkError:
                pass

        try:
            with raise_s3_error(f"'{src_url}' or '{dst_url}'"):
                self._client.copy(
                    {"Bucket": src_bucket, "Key": src_key},
                    Bucket=dst_bucket,
                    Key=dst_key,
                    Callback=callback,
                )
        except S3FileNotFoundError:
            if self.is_dir():
                raise S3IsADirectoryError("Is a directory: %r" % src_url)
            raise

    def sync(
        self,
        dst_url: PathLike,
        followlinks: bool = False,
        force: bool = False,
        overwrite: bool = True,
    ) -> None:
        """
        Copy file/directory on src_url to dst_url

        :param dst_url: Given destination path
        :param followlinks: False if regard symlink as file, else True
        :param force: Sync file forcible, do not ignore same files,
            priority is higher than 'overwrite', default is False
        :param overwrite: whether or not overwrite file when exists, default is True
        """
        for src_file_path, dst_file_path in _s3_scan_pairs(
            self.path_with_protocol, dst_url
        ):
            src_file_path = self.from_path(src_file_path)
            dst_file_path = self.from_path(dst_file_path)

            if force:
                pass
            elif not overwrite and dst_file_path.exists():
                continue
            elif dst_file_path.exists() and is_same_file(
                src_file_path.stat(), dst_file_path.stat(), "copy"
            ):
                continue

            src_file_path.copy(
                dst_file_path,
                followlinks=followlinks,
                overwrite=True,
            )

    def symlink(self, dst_path: PathLike) -> None:
        """
        Create a symbolic link pointing to src_path named dst_path.

        :param dst_path: Destination path
        :raises: S3NameTooLongError, S3BucketNotFoundError, S3IsADirectoryError
        """
        if len(fspath(self._s3_path).encode()) > 1024:
            raise S3NameTooLongError("File name too long: %r" % dst_path)
        src_bucket, src_key = parse_s3_url(self.path_with_protocol)
        dst_bucket, dst_key = parse_s3_url(dst_path)

        if not src_bucket:
            raise S3BucketNotFoundError("Empty bucket name: %r" % self.path)
        if not dst_bucket:
            raise S3BucketNotFoundError("Empty bucket name: %r" % dst_path)
        if not dst_key or dst_key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % dst_path)

        src_path = self._s3_path
        try:
            src_path = self.readlink()._s3_path
        except S3NotALinkError:
            pass
        with raise_s3_error(dst_path):
            self._client.put_object(
                Bucket=dst_bucket, Key=dst_key, Metadata={"symlink_to": src_path}
            )

    def readlink(self) -> "S3Path":
        """
        Return a S3Path instance representing the path to which the symbolic link points

        :returns: Return a S3Path instance representing the path to
            which the symbolic link points.
        :raises: S3NameTooLongError, S3BucketNotFoundError, S3IsADirectoryError,
            S3NotALinkError
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )
        if not key or key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % self.path_with_protocol)
        metadata = self._s3_get_metadata()

        if "symlink_to" not in metadata:
            raise S3NotALinkError("Not a link: %r" % self.path_with_protocol)
        else:
            return self.from_path(metadata["symlink_to"])

    def is_symlink(self) -> bool:
        """
        Test whether a path is link

        :returns: True if a path is link, else False
        :raises: S3NotALinkError
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            return False
        if not key or key.endswith("/"):
            return False
        metadata = self._s3_get_metadata()
        return "symlink_to" in metadata

    def save(self, file_object: BinaryIO):
        """Write the opened binary stream to specified path,
        but the stream won't be closed

        :param file_object: Stream to be read
        """
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                "Empty bucket name: %r" % self.path_with_protocol
            )
        if not key or key.endswith("/"):
            raise S3IsADirectoryError("Is a directory: %r" % self.path_with_protocol)

        with raise_s3_error(self.path_with_protocol):
            self._client.upload_fileobj(file_object, Bucket=bucket, Key=key)

    def open(
        self,
        mode: str = "r",
        *,
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        s3_open_func: Callable = s3_open,
        **kwargs,
    ) -> IO:
        return s3_open_func(
            self,
            mode,
            encoding=encoding,
            errors=errors,
            **necessary_params(s3_open_func, **kwargs),
        )

    def absolute(self) -> "S3Path":
        """
        Make the path absolute, without normalization or resolving symlinks.
        Returns a new path object
        """
        return self

    def cwd(self) -> "S3Path":
        """Return current working directory

        returns: Current working directory
        """
        return self.from_path(self.path_with_protocol)


class MultiPartWriter:
    def __init__(self, client, path: PathLike) -> None:
        self._client = client
        self._multipart_upload_info = []

        bucket, key = parse_s3_url(path)
        self._bucket = bucket
        self._key = key
        self._upload_id = self._client.create_multipart_upload(
            Bucket=self._bucket, Key=self._key
        )["UploadId"]

    def upload_part(self, part_num: int, file_obj: io.BytesIO) -> None:
        response = self._client.upload_part(
            Body=file_obj,
            UploadId=self._upload_id,
            PartNumber=part_num,
            Bucket=self._bucket,
            Key=self._key,
        )
        self._multipart_upload_info.append(
            {"PartNumber": part_num, "ETag": response["ETag"]}
        )

    def upload_part_by_paths(
        self, part_num: int, paths: List[Tuple[PathLike, str]]
    ) -> None:
        file_obj = io.BytesIO()

        def get_object(client, bucket, key, range_str: Optional[str] = None) -> bytes:
            if range_str:
                return client.get_object(Bucket=bucket, Key=key, Range=range_str)[
                    "Body"
                ].read()
            else:
                return client.get_object(Bucket=bucket, Key=key)["Body"].read()

        get_object = patch_method(
            get_object, max_retries=max_retries, should_retry=s3_should_retry
        )
        for path, bytes_range in paths:
            bucket, key = parse_s3_url(path)
            if bytes_range:
                file_obj.write(get_object(self._client, bucket, key, bytes_range))
            else:
                file_obj.write(get_object(self._client, bucket, key))
        file_obj.seek(0, os.SEEK_SET)
        self.upload_part(part_num, file_obj)

    def upload_part_copy(
        self, part_num: int, path: PathLike, copy_source_range: Optional[str] = None
    ) -> None:
        bucket, key = parse_s3_url(path)
        params = dict(
            UploadId=self._upload_id,
            PartNumber=part_num,
            CopySource={"Bucket": bucket, "Key": key},
            Bucket=self._bucket,
            Key=self._key,
        )
        if copy_source_range:
            params["CopySourceRange"] = copy_source_range
        response = self._client.upload_part_copy(**params)
        self._multipart_upload_info.append(
            {"PartNumber": part_num, "ETag": response["CopyPartResult"]["ETag"]}
        )

    def close(self):
        self._multipart_upload_info.sort(key=lambda t: t["PartNumber"])
        self._client.complete_multipart_upload(
            UploadId=self._upload_id,
            Bucket=self._bucket,
            Key=self._key,
            MultipartUpload={"Parts": self._multipart_upload_info},
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

import hashlib
import inspect
import io
import os
import re
import time
from functools import lru_cache, wraps
from itertools import chain
from logging import getLogger as get_logger
from typing import IO, Any, AnyStr, BinaryIO, Callable, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import urlsplit

import boto3
import botocore
import smart_open.s3
from botocore.awsrequest import AWSResponse

from megfile.errors import S3BucketNotFoundError, S3ConfigError, S3FileExistsError, S3FileNotFoundError, S3IsADirectoryError, S3NameTooLongError, S3NotADirectoryError, S3NotALinkError, S3PermissionError, S3UnknownError, UnsupportedError, _create_missing_ok_generator
from megfile.errors import _logger as error_logger
from megfile.errors import patch_method, raise_s3_error, s3_error_code_should_retry, s3_should_retry, translate_fs_error, translate_s3_error
from megfile.interfaces import Access, FileCacher, FileEntry, PathLike, StatResult, URIPath
from megfile.lib.compat import fspath
from megfile.lib.fnmatch import translate
from megfile.lib.glob import has_magic, has_magic_ignore_brace, ungloblize
from megfile.lib.joinpath import uri_join
from megfile.lib.s3_buffered_writer import DEFAULT_MAX_BUFFER_SIZE, S3BufferedWriter
from megfile.lib.s3_cached_handler import S3CachedHandler
from megfile.lib.s3_limited_seekable_writer import S3LimitedSeekableWriter
from megfile.lib.s3_memory_handler import S3MemoryHandler
from megfile.lib.s3_pipe_handler import S3PipeHandler
from megfile.lib.s3_prefetch_reader import DEFAULT_BLOCK_SIZE, S3PrefetchReader
from megfile.lib.s3_share_cache_reader import S3ShareCacheReader
from megfile.smart_path import SmartPath
from megfile.utils import calculate_md5, generate_cache_path, get_binary_mode, is_readable, necessary_params, thread_local

__all__ = [
    'S3Path',
    'parse_s3_url',
    'get_endpoint_url',
    'get_s3_session',
    'get_s3_client',
    's3_path_join',
    'is_s3',
    's3_buffered_open',
    's3_cached_open',
    's3_download',
    's3_legacy_open',
    's3_memory_open',
    's3_pipe_open',
    's3_prefetch_open',
    's3_share_cache_open',
    's3_open',
    'S3Cacher',
    'S3BufferedWriter',
    'S3LimitedSeekableWriter',
    'S3PrefetchReader',
    'S3ShareCacheReader',
    's3_upload',
    's3_download',
    's3_load_content',
]
_logger = get_logger(__name__)
content_md5_header = 'megfile-content-md5'
endpoint_url = 'https://s3.amazonaws.com'
max_pool_connections = 32
max_retries = 10
max_keys = 1000

# Monkey patch for smart_open
_smart_open_parameters = inspect.signature(smart_open.s3.open).parameters
if 'resource_kwargs' in _smart_open_parameters:
    # smart_open >= 1.8.1
    def _s3_open(bucket: str, key: str, mode: str):  # pragma: no cover
        return smart_open.s3.open(
            bucket,
            key,
            mode,
            session=get_s3_session(),
            resource_kwargs={'endpoint_url': get_endpoint_url()})

elif 'client' in _smart_open_parameters:
    # smart_open >= 5.0.0
    def _s3_open(bucket: str, key: str, mode: str):
        return smart_open.s3.open(bucket, key, mode, client=get_s3_client())

else:
    # smart_open < 1.8.1, >= 1.6.0
    def _s3_open(bucket: str, key: str, mode: str):  # pragma: no cover
        return smart_open.s3.open(
            bucket,
            key,
            mode,
            s3_session=get_s3_session(),
            endpoint_url=get_endpoint_url())  # type: ignore


def _patch_make_request(client: botocore.client.BaseClient):

    def after_callback(result: Tuple[AWSResponse, dict], *args, **kwargs):
        if not isinstance(result, tuple) or len(result) != 2 \
            or not isinstance(result[0], AWSResponse) or not isinstance(result[1], dict):
            return result
        http, parsed_response = result
        if http.status_code >= 500:
            error_code = parsed_response.get("Error", {}).get("Code")
            operation_model = kwargs.get('operation_model') or (
                args[0] if args else None)
            operation_name = operation_model.name if operation_model else 'ProxyMethod'
            error_class = client.exceptions.from_code(error_code)
            raise error_class(parsed_response, operation_name)
        return result

    def retry_callback(error, operation_model, request_dict, request_context):
        if error is None:  # retry for the first time
            error_logger.debug(
                'failed to process: %r, with parameters: %s',
                operation_model.name, request_dict)
        if is_readable(request_dict['body']):
            request_dict['body'].seek(0)

    def before_callback(operation_model, request_dict, request_context):
        _logger.debug(
            'send s3 request: %r, with parameters: %s', operation_model.name,
            request_dict)

    client._make_request = patch_method(
        client._make_request,
        max_retries=max_retries,
        should_retry=s3_should_retry,
        after_callback=after_callback,
        before_callback=before_callback,
        retry_callback=retry_callback)
    return client


def parse_s3_url(s3_url: PathLike) -> Tuple[str, str]:
    s3_url = fspath(s3_url)
    s3_scheme, rightpart = s3_url[:5], s3_url[5:]
    if s3_scheme != 's3://':
        raise ValueError('Not a s3 url: %r' % s3_url)
    bucketmatch = re.match('(.*?)/', rightpart)
    if bucketmatch is None:
        bucket = rightpart
        path = ''
    else:
        bucket = bucketmatch.group(1)
        path = rightpart[len(bucket) + 1:]
    return bucket, path


def get_scoped_config() -> Dict:
    return get_s3_session()._session.get_scoped_config()


def get_endpoint_url() -> str:
    '''Get the endpoint url of S3

    :returns: S3 endpoint url
    '''
    environ_endpoint_url = os.environ.get('OSS_ENDPOINT')
    if environ_endpoint_url:
        _logger.info("using OSS_ENDPOINT: %s" % environ_endpoint_url)
        return environ_endpoint_url
    config_endpoint_url = get_scoped_config().get('s3', {}).get('endpoint_url')
    if config_endpoint_url:
        _logger.info(
            "using ~/.aws/config: endpoint_url=%s" % config_endpoint_url)
        return config_endpoint_url
    return endpoint_url


def get_s3_session():
    '''Get S3 session

    :returns: S3 session
    '''
    return thread_local('s3_session', boto3.session.Session)


def get_s3_client(
        config: Optional[botocore.config.Config] = None,
        cache_key: Optional[str] = None):
    '''Get S3 client

    :returns: S3 client
    '''
    if cache_key is not None:
        return thread_local(cache_key, get_s3_client, config)
    client = get_s3_session().client(
        's3', endpoint_url=get_endpoint_url(), config=config)
    client = _patch_make_request(client)
    return client


def s3_path_join(path: PathLike, *other_paths: PathLike) -> str:
    '''
    Concat 2 or more path to a complete path

    :param path: Given path
    :param other_paths: Paths to be concatenated
    :returns: Concatenated complete path

    .. note ::

        The difference between this function and ``os.path.join`` is that this function ignores left side slash (which indicates absolute path) in ``other_paths`` and will directly concat.
        e.g. os.path.join('/path', 'to', '/file') => '/file', but s3_path_join('/path', 'to', '/file') => '/path/to/file'
    '''
    return uri_join(fspath(path), *map(fspath, other_paths))


def _list_all_buckets() -> List[str]:
    client = get_s3_client()
    response = client.list_buckets()
    return [content['Name'] for content in response['Buckets']]


def _parse_s3_url_ignore_brace(s3_url: str) -> Tuple[str, str]:
    s3_url = fspath(s3_url)
    s3_scheme, rightpart = s3_url[:5], s3_url[5:]
    if s3_scheme != 's3://':
        raise ValueError('Not a s3 url: %r' % s3_url)
    left_brace = False
    for current_index, current_character in enumerate(rightpart):
        if current_character == "/" and left_brace is False:
            return rightpart[:current_index], rightpart[current_index + 1:]
        elif current_character == "{":
            left_brace = True
        elif current_character == "}":
            left_brace = False
    return rightpart, ""


def _group_s3path_by_bucket(s3_pathname: str) -> List[str]:
    bucket, key = _parse_s3_url_ignore_brace(s3_pathname)
    if not bucket:
        if not key:
            raise UnsupportedError('Glob whole s3', s3_pathname)
        raise S3BucketNotFoundError('Empty bucket name: %r' % s3_pathname)

    grouped_path = []

    def generate_s3_path(bucket: str, key: str) -> str:
        if key:
            return "s3://%s/%s" % (bucket, key)
        return "s3://%s%s" % (bucket, "/" if s3_pathname.endswith("/") else "")

    all_bucket = lru_cache(maxsize=1)(_list_all_buckets)
    for bucketname in ungloblize(bucket):
        if has_magic(bucketname):
            split_bucketname = bucketname.split("/", 1)
            path_part = None
            if len(split_bucketname) == 2:
                bucketname, path_part = split_bucketname
            pattern = re.compile(translate(re.sub(r'\*{2,}', '*', bucketname)))

            for bucket in all_bucket():
                if pattern.fullmatch(bucket) is not None:
                    if path_part is not None:
                        bucket = "%s/%s" % (
                            bucket, path_part)  # pragma: no cover
                    grouped_path.append(generate_s3_path(bucket, key))
        else:
            grouped_path.append(generate_s3_path(bucketname, key))

    return grouped_path


def _s3_split_magic_ignore_brace(s3_pathname: str) -> Tuple[str, str]:
    if not s3_pathname:
        raise ValueError("s3_pathname: %s", s3_pathname)

    has_protocol = False
    if s3_pathname.startswith("s3://"):
        has_protocol = True
        s3_pathname = s3_pathname[5:]

    has_delimiter = False
    if s3_pathname.endswith("/"):
        has_delimiter = True
        s3_pathname = s3_pathname[:-1]

    normal_parts = []
    magic_parts = []
    left_brace = False
    left_index = 0
    for current_index, current_character in enumerate(s3_pathname):
        if current_character == "/" and left_brace is False:
            if has_magic_ignore_brace(s3_pathname[left_index:current_index]):
                magic_parts.append(s3_pathname[left_index:current_index])
                if s3_pathname[current_index + 1:]:
                    magic_parts.append(s3_pathname[current_index + 1:])
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

    if has_protocol and normal_parts:
        normal_parts.insert(0, "s3:/")
    elif has_protocol:
        magic_parts.insert(0, "s3:/")

    if has_delimiter and magic_parts:
        magic_parts.append("")
    elif has_delimiter:
        normal_parts.append("")

    return "/".join(normal_parts), "/".join(magic_parts)


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
    if prefix != '' and not prefix.endswith('/'):
        prefix += '/'
    return prefix


def _s3_split_magic(s3_pathname: str) -> Tuple[str, str]:
    if not has_magic(s3_pathname):
        return s3_pathname, ''
    delimiter = '/'
    normal_parts = []
    magic_parts = []
    all_parts = s3_pathname.split(delimiter)
    for i, part in enumerate(all_parts):
        if not has_magic(part):
            normal_parts.append(part)
        else:
            magic_parts = all_parts[i:]
            break
    return delimiter.join(normal_parts), delimiter.join(magic_parts)


def _list_objects_recursive(
        s3_client, bucket: str, prefix: str, delimiter: str = ''):

    resp = s3_client.list_objects_v2(
        Bucket=bucket, Prefix=prefix, Delimiter=delimiter, MaxKeys=max_keys)

    while True:
        yield resp

        if not resp['IsTruncated']:
            break

        resp = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter=delimiter,
            ContinuationToken=resp['NextContinuationToken'],
            MaxKeys=max_keys)


def _make_stat(content: Dict[str, Any]):
    return StatResult(
        islnk=content.get('islnk', False),
        size=content['Size'],
        mtime=content['LastModified'].timestamp(),
        extra=content,
    )


def _s3_glob_stat_single_path(
        s3_pathname: PathLike,
        recursive: bool = True,
        missing_ok: bool = True,
        followlinks: bool = False) -> Iterator[FileEntry]:
    if not recursive:
        # If not recursive, replace ** with *
        s3_pathname = re.sub(r'\*{2,}', '*', s3_pathname)
    top_dir, wildcard_part = _s3_split_magic(s3_pathname)
    search_dir = wildcard_part.endswith('/')

    def should_recursive(wildcard_part: str) -> bool:
        if '**' in wildcard_part:
            return True
        for expanded_path in ungloblize(wildcard_part):
            parts_length = len(expanded_path.split('/'))
            if parts_length + search_dir >= 2:
                return True
        return False

    def create_generator(_s3_pathname) -> Iterator[FileEntry]:
        if not S3Path(top_dir).exists():
            return
        if not has_magic(_s3_pathname):
            if S3Path(_s3_pathname).is_file():
                yield FileEntry(
                    _s3_pathname,
                    S3Path(_s3_pathname).stat(followlinks=followlinks))
            if S3Path(_s3_pathname).is_dir():
                yield FileEntry(_s3_pathname, StatResult(isdir=True))
            return

        delimiter = ''
        if not should_recursive(wildcard_part):
            delimiter = '/'

        dirnames = set()
        pattern = re.compile(translate(_s3_pathname))
        bucket, key = parse_s3_url(top_dir)
        prefix = _become_prefix(key)
        client = get_s3_client()
        with raise_s3_error(_s3_pathname):
            for resp in _list_objects_recursive(client, bucket, prefix,
                                                delimiter):
                for content in resp.get('Contents', []):
                    path = s3_path_join('s3://', bucket, content['Key'])
                    if not search_dir and pattern.match(path):
                        yield FileEntry(path, _make_stat(content))
                    dirname = os.path.dirname(path)
                    while dirname not in dirnames and dirname != top_dir:
                        dirnames.add(dirname)
                        path = dirname + '/' if search_dir else dirname
                        if pattern.match(path):
                            yield FileEntry(path, StatResult(isdir=True))
                        dirname = os.path.dirname(dirname)
                for common_prefix in resp.get('CommonPrefixes', []):
                    path = s3_path_join(
                        's3://', bucket, common_prefix['Prefix'])
                    dirname = os.path.dirname(path)
                    if dirname not in dirnames and dirname != top_dir:
                        dirnames.add(dirname)
                        path = dirname + '/' if search_dir else dirname
                        if pattern.match(path):
                            yield FileEntry(path, StatResult(isdir=True))

    return create_generator(s3_pathname)


def _s3_scan_pairs(src_url: PathLike,
                   dst_url: PathLike) -> Iterator[Tuple[PathLike, PathLike]]:
    for src_file_path in S3Path(src_url).scan():
        content_path = src_file_path[len(src_url):]
        if len(content_path) > 0:
            dst_file_path = s3_path_join(dst_url, content_path)
        else:
            dst_file_path = dst_url
        yield src_file_path, dst_file_path


def _s3_get_metadata(src_url: PathLike) -> dict:
    '''
    Get object metadata

    :param path: Object path
    :returns: Object metadata
    '''
    bucket, key = parse_s3_url(src_url)
    if not bucket:
        return {}
    if not key or key.endswith('/'):
        return {}
    client = get_s3_client()
    try:
        with raise_s3_error(src_url):
            resp = client.head_object(Bucket=bucket, Key=key)
        return dict(
            (key.lower(), value) for key, value in resp['Metadata'].items())
    except Exception:
        return {}


def is_s3(path: PathLike) -> bool:
    '''
    According to `aws-cli <https://docs.aws.amazon.com/cli/latest/reference/s3/index.html>`_ , test if a path is s3 path

    :param path: Path to be tested
    :returns: True if path is s3 path, else False
    '''
    path = fspath(path)
    if not path.startswith('s3://'):
        return False
    parts = urlsplit(path)
    return parts.scheme == 's3'


def _s3_binary_mode(s3_open_func):

    @wraps(s3_open_func)
    def wrapper(s3_url, mode: str = 'rb', **kwargs):
        bucket, key = parse_s3_url(s3_url)
        if not bucket:
            raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)

        if not key or key.endswith('/'):
            raise S3IsADirectoryError('Is a directory: %r' % s3_url)

        if 'x' in mode:
            if S3Path(s3_url).is_file():
                raise S3FileExistsError('File exists: %r' % s3_url)
            mode = mode.replace('x', 'w')

        if 'w' in mode or 'a' in mode:
            if not S3Path(s3_url).hasbucket():
                raise S3BucketNotFoundError('No such bucket: %r' % s3_url)

        fileobj = s3_open_func(s3_url, get_binary_mode(mode), **kwargs)
        if 'b' not in mode:
            fileobj = io.TextIOWrapper(fileobj)  # pytype: disable=wrong-arg-types
            fileobj.mode = mode
        return fileobj

    return wrapper


@_s3_binary_mode
def s3_prefetch_open(
        s3_url: PathLike,
        mode: str = 'rb',
        followlinks: bool = False,
        *,
        max_concurrency: Optional[int] = None,
        max_block_size: int = DEFAULT_BLOCK_SIZE) -> S3PrefetchReader:
    '''Open a asynchronous prefetch reader, to support fast sequential read and random read

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        Some parameter setting may perform well: max_concurrency=10 or 20, max_block_size=8 or 16 MB, default value None means using global thread pool

    :param max_concurrency: Max download thread number, None by default
    :param max_block_size: Max data size downloaded by each thread, in bytes, 8MB by default
    :returns: An opened S3PrefetchReader object
    :raises: S3FileNotFoundError
    '''
    if mode != 'rb':
        raise ValueError('unacceptable mode: %r' % mode)  # pragma: no cover
    if followlinks:
        metadata = _s3_get_metadata(s3_url)
        if metadata and 'symlink_to' in metadata:
            s3_url = metadata['symlink_to']

    bucket, key = parse_s3_url(s3_url)
    config = botocore.config.Config(max_pool_connections=max_pool_connections)
    client = get_s3_client(config=config, cache_key='s3_filelike_client')
    return S3PrefetchReader(
        bucket,
        key,
        s3_client=client,
        max_retries=max_retries,
        max_workers=max_concurrency,
        block_size=max_block_size)


@_s3_binary_mode
def s3_share_cache_open(
        s3_url: PathLike,
        mode: str = 'rb',
        followlinks: bool = False,
        *,
        cache_key: str = 'lru',
        max_concurrency: Optional[int] = None,
        max_block_size: int = DEFAULT_BLOCK_SIZE) -> S3ShareCacheReader:
    '''Open a asynchronous prefetch reader, to support fast sequential read and random read

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        Some parameter setting may perform well: max_concurrency=10 or 20, max_block_size=8 or 16 MB, default value None means using global thread pool

    :param max_concurrency: Max download thread number, None by default
    :param max_block_size: Max data size downloaded by each thread, in bytes, 8MB by default
    :returns: An opened S3ShareCacheReader object
    :raises: S3FileNotFoundError
    '''
    if mode != 'rb':
        raise ValueError('unacceptable mode: %r' % mode)  # pragma: no cover
    if followlinks:
        metadata = _s3_get_metadata(s3_url)
        if metadata and 'symlink_to' in metadata:
            s3_url = metadata['symlink_to']

    bucket, key = parse_s3_url(s3_url)
    config = botocore.config.Config(max_pool_connections=max_pool_connections)
    client = get_s3_client(config=config, cache_key='s3_filelike_client')
    return S3ShareCacheReader(
        bucket,
        key,
        cache_key=cache_key,
        s3_client=client,
        max_retries=max_retries,
        max_workers=max_concurrency,
        block_size=max_block_size)


@_s3_binary_mode
def s3_pipe_open(
        s3_url: PathLike,
        mode: str,
        followlinks: bool = False,
        *,
        join_thread: bool = True) -> S3PipeHandler:
    '''Open a asynchronous read-write reader / writer, to support fast sequential read / write

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        When join_thread is False, while the file handle are closing, this function will not wait until the asynchronous writing finishes;
        False doesn't affect read-handle, but this can speed up write-handle because file will be written asynchronously.
        But asynchronous behaviour can guarantee the file are successfully written, and frequent execution may cause thread and file handle exhaustion

    :param mode: Mode to open file, either "rb" or "wb"
    :param join_thread: If wait after function execution until s3 finishes writing
    :returns: An opened BufferedReader / BufferedWriter object
    '''
    if mode not in ('rb', 'wb'):
        raise ValueError('unacceptable mode: %r' % mode)  # pragma: no cover

    if mode[0] == 'r' and not S3Path(s3_url).is_file():
        raise S3FileNotFoundError('No such file: %r' % s3_url)

    if followlinks:
        metadata = _s3_get_metadata(s3_url)
        if metadata and 'symlink_to' in metadata:
            s3_url = metadata['symlink_to']

    bucket, key = parse_s3_url(s3_url)
    config = botocore.config.Config(max_pool_connections=max_pool_connections)
    client = get_s3_client(config=config, cache_key='s3_filelike_client')
    return S3PipeHandler(
        bucket, key, mode, s3_client=client, join_thread=join_thread)


@_s3_binary_mode
def s3_cached_open(
        s3_url: PathLike,
        mode: str,
        followlinks: bool = False,
        *,
        cache_path: Optional[str] = None) -> S3CachedHandler:
    '''Open a local-cache file reader / writer, for frequent random read / write

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        cache_path can specify the path of cache file. Performance could be better if cache file path is on ssd or tmpfs

    :param mode: Mode to open file, could be one of "rb", "wb" or "ab"
    :param cache_path: cache file path
    :returns: An opened BufferedReader / BufferedWriter object
    '''
    if mode not in ('rb', 'wb', 'ab', 'rb+', 'wb+', 'ab+'):
        raise ValueError('unacceptable mode: %r' % mode)  # pragma: no cover
    if followlinks:
        metadata = _s3_get_metadata(s3_url)
        if metadata and 'symlink_to' in metadata:
            s3_url = metadata['symlink_to']

    bucket, key = parse_s3_url(s3_url)
    config = botocore.config.Config(max_pool_connections=max_pool_connections)
    client = get_s3_client(config=config, cache_key='s3_filelike_client')
    return S3CachedHandler(
        bucket, key, mode, s3_client=client, cache_path=cache_path)


@_s3_binary_mode
def s3_buffered_open(
        s3_url: PathLike,
        mode: str,
        followlinks: bool = False,
        *,
        max_concurrency: Optional[int] = None,
        max_buffer_size: int = DEFAULT_MAX_BUFFER_SIZE,
        forward_ratio: Optional[float] = None,
        block_size: int = DEFAULT_BLOCK_SIZE,
        limited_seekable: bool = False,
        buffered: bool = True,
        share_cache_key: Optional[str] = None,
        cache_path: Optional[str] = None
) -> Union[S3PrefetchReader, S3BufferedWriter, io.BufferedReader, io.
           BufferedWriter, S3MemoryHandler]:
    '''Open an asynchronous prefetch reader, to support fast sequential read

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        Some parameter setting may perform well: max_concurrency=10 or 20, max_block_size=8 or 16 MB, default value None means using global thread pool

    :param max_concurrency: Max download thread number, None by default
    :param max_buffer_size: Max cached buffer size in memory, 128MB by default
    :param block_size: Size of single block, 8MB by default. Each block will be uploaded or downloaded by single thread.
    :param limited_seekable: If write-handle supports limited seek (both file head part and tail part can seek block_size). Notes: This parameter are valid only for write-handle. Read-handle support arbitrary seek
    :returns: An opened S3PrefetchReader object
    :raises: S3FileNotFoundError
    '''
    if mode not in ('rb', 'wb', 'ab', 'rb+', 'wb+', 'ab+'):
        raise ValueError('unacceptable mode: %r' % mode)
    if followlinks:
        metadata = _s3_get_metadata(s3_url)
        if metadata and 'symlink_to' in metadata:
            s3_url = metadata['symlink_to']

    bucket, key = parse_s3_url(s3_url)
    config = botocore.config.Config(max_pool_connections=max_pool_connections)
    client = get_s3_client(config=config, cache_key='s3_filelike_client')

    if 'a' in mode or '+' in mode:
        if cache_path is None:
            return S3MemoryHandler(bucket, key, mode, s3_client=client)
        return S3CachedHandler(
            bucket, key, mode, s3_client=client, cache_path=cache_path)

    if mode == 'rb':
        # A rough conversion algorithm to align 2 types of Reader / Writer parameters
        # TODO: Optimize the conversion algorithm
        block_capacity = max_buffer_size // block_size
        if forward_ratio is None:
            block_forward = None
        else:
            block_forward = max(int(block_capacity * forward_ratio), 1)
        if share_cache_key is not None:
            reader = S3ShareCacheReader(
                bucket,
                key,
                cache_key=share_cache_key,
                s3_client=client,
                max_retries=max_retries,
                max_workers=max_concurrency,
                block_size=block_size,
                block_forward=block_forward)
        else:
            reader = S3PrefetchReader(
                bucket,
                key,
                s3_client=client,
                max_retries=max_retries,
                max_workers=max_concurrency,
                block_capacity=block_capacity,
                block_forward=block_forward,
                block_size=block_size)
        if buffered:
            reader = io.BufferedReader(reader)  # pytype: disable=wrong-arg-types
        return reader

    if limited_seekable:
        writer = S3LimitedSeekableWriter(
            bucket,
            key,
            s3_client=client,
            max_workers=max_concurrency,
            max_buffer_size=max_buffer_size,
            block_size=block_size)
    else:
        writer = S3BufferedWriter(
            bucket,
            key,
            s3_client=client,
            max_workers=max_concurrency,
            max_buffer_size=max_buffer_size,
            block_size=block_size)
    if buffered:
        writer = io.BufferedWriter(writer)  # pytype: disable=wrong-arg-types
    return writer


@_s3_binary_mode
def s3_memory_open(
        s3_url: PathLike, mode: str,
        followlinks: bool = False) -> S3MemoryHandler:
    '''Open a memory-cache file reader / writer, for frequent random read / write

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

    :param mode: Mode to open file, could be one of "rb", "wb", "ab", "rb+", "wb+" or "ab+"
    :returns: An opened BufferedReader / BufferedWriter object
    '''
    if mode not in ('rb', 'wb', 'ab', 'rb+', 'wb+', 'ab+'):
        raise ValueError('unacceptable mode: %r' % mode)
    if followlinks:
        metadata = _s3_get_metadata(s3_url)
        if metadata and 'symlink_to' in metadata:
            s3_url = metadata['symlink_to']

    bucket, key = parse_s3_url(s3_url)
    config = botocore.config.Config(max_pool_connections=max_pool_connections)
    client = get_s3_client(config=config, cache_key='s3_filelike_client')
    return S3MemoryHandler(bucket, key, mode, s3_client=client)


@_s3_binary_mode
def s3_legacy_open(s3_url: PathLike, mode: str, followlinks: bool = False):
    '''Use smart_open.s3.open open a reader / writer

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

    :param mode: Mode to open file, either "rb" or "wb"
    :returns: File-Like Object
    '''
    if mode not in ('rb', 'wb'):  # pragma: no cover
        raise ValueError('unacceptable mode: %r' % mode)
    if followlinks:
        metadata = _s3_get_metadata(s3_url)
        if metadata and 'symlink_to' in metadata:
            s3_url = metadata['symlink_to']

    bucket, key = parse_s3_url(s3_url)

    try:
        return _s3_open(bucket, key, mode)
    except Exception as error:  # pragma: no cover
        if isinstance(error, IOError):
            error_str = str(error)
            if 'NoSuchKey' in error_str:
                raise S3FileNotFoundError('No such file: %r' % s3_url)
            if 'NoSuchBucket' in error_str:
                raise S3BucketNotFoundError('No such bucket: %r' % s3_url)
            for code in ('AccessDenied', 'InvalidAccessKeyId',
                         'SignatureDoesNotMatch'):
                if code in error_str:
                    raise S3PermissionError(
                        'Permission denied: %r, code: %s' % (s3_url, code))
            raise S3UnknownError(error, s3_url)
        elif isinstance(error, ValueError):
            error_str = str(error)
            if 'does not exist' in error_str:
                # if bucket is non-existent or has no WRITE access
                raise S3BucketNotFoundError('No such bucket: %r' % s3_url)
            raise S3UnknownError(error, s3_url)
        raise translate_s3_error(error, s3_url)


s3_open = s3_buffered_open


def s3_download(
        src_url: PathLike,
        dst_url: PathLike,
        followlinks: bool = False,
        callback: Optional[Callable[[int], None]] = None) -> None:
    '''
    Downloads a file from s3 to local filesystem.
    :param src_url: source s3 path
    :param dst_url: target fs path
    :param callback: Called periodically during copy, and the input parameter is the data size (in bytes) of copy since the last call
    '''
    if followlinks:
        metadata = _s3_get_metadata(src_url)
        if metadata and 'symlink_to' in metadata:
            src_url = metadata['symlink_to']
    src_bucket, src_key = parse_s3_url(src_url)
    if not src_bucket:
        raise S3BucketNotFoundError('Empty bucket name: %r' % src_url)

    if not S3Path(src_url).exists():
        raise FileNotFoundError('File not found: %r' % src_url)

    if not S3Path(src_url).is_file():
        raise S3IsADirectoryError('Is a directory: %r' % src_url)

    dst_url = fspath(dst_url)
    if not dst_url or dst_url.endswith('/'):
        raise S3IsADirectoryError('Is a directory: %r' % dst_url)

    dst_directory = os.path.dirname(dst_url)
    if dst_directory != '':
        os.makedirs(dst_directory, exist_ok=True)

    client = get_s3_client()
    try:
        client.download_file(src_bucket, src_key, dst_url, Callback=callback)
    except Exception as error:  # pragma: no cover
        error = translate_fs_error(error, dst_url)
        error = translate_s3_error(error, src_url)
        raise error


def s3_upload(
        src_url: PathLike,
        dst_url: PathLike,
        callback: Optional[Callable[[int], None]] = None,
        **kwargs) -> None:
    '''
    Uploads a file from local filesystem to s3.
    :param src_url: source fs path
    :param dst_url: target s3 path
    :param callback: Called periodically during copy, and the input parameter is the data size (in bytes) of copy since the last call
    '''
    dst_bucket, dst_key = parse_s3_url(dst_url)
    if not dst_bucket:
        raise S3BucketNotFoundError('Empty bucket name: %r' % dst_url)
    if not dst_key or dst_key.endswith('/'):
        raise S3IsADirectoryError('Is a directory: %r' % dst_url)

    client = get_s3_client()
    with open(src_url, 'rb') as src:
        with raise_s3_error(dst_url):
            client.upload_fileobj(
                src, Bucket=dst_bucket, Key=dst_key, Callback=callback)


def get_content_offset(start: Optional[int], stop: Optional[int], size: int):
    if start is None:
        start = 0
    if stop is None or stop < 0 or start < 0:
        start, stop, _ = slice(start, stop).indices(size)
    if stop < start:
        raise ValueError('read length must be positive')
    return start, stop


def s3_load_content(
        s3_url,
        start: Optional[int] = None,
        stop: Optional[int] = None,
        followlinks: bool = False) -> bytes:
    '''
    Get specified file from [start, stop) in bytes

    :param s3_url: Specified path
    :param start: start index
    :param stop: stop index
    :returns: bytes content in range [start, stop)
    '''

    def _get_object(client, buckey, key, range_str):
        return client.get_object(
            Bucket=bucket, Key=key, Range=range_str)['Body'].read()

    if followlinks:
        metadata = _s3_get_metadata(s3_url)
        if metadata and 'symlink_to' in metadata:
            s3_url = metadata['symlink_to']
    bucket, key = parse_s3_url(s3_url)
    if not bucket:
        raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)
    if not key or key.endswith('/'):
        raise S3IsADirectoryError('Is a directory: %r' % s3_url)

    start, stop = get_content_offset(start, stop, S3Path(s3_url).getsize())
    if start == 0 and stop == 0:
        return b''
    range_str = 'bytes=%d-%d' % (start, stop - 1)

    client = get_s3_client()
    with raise_s3_error(s3_url):
        return patch_method(
            _get_object,
            max_retries=max_retries,
            should_retry=s3_should_retry,
        )(client, bucket, key, range_str)


class S3Cacher(FileCacher):
    cache_path = None

    def __init__(
            self, path: str, cache_path: Optional[str] = None, mode: str = 'r'):
        if mode not in ('r', 'w', 'a'):
            raise ValueError('unacceptable mode: %r' % mode)
        if mode in ('r', 'a'):
            if cache_path is None:
                cache_path = generate_cache_path(path)
            s3_download(path, cache_path)
        self.name = path
        self.mode = mode
        self.cache_path = cache_path

    def _close(self):
        if self.cache_path is not None and \
            os.path.exists(self.cache_path):
            if self.mode in ('w', 'a'):
                s3_upload(self.cache_path, self.name)
            os.unlink(self.cache_path)


@SmartPath.register
class S3Path(URIPath):

    protocol = "s3"

    def access(
            self, mode: Access = Access.READ,
            followlinks: bool = False) -> bool:
        '''
        Test if path has access permission described by mode
        Using head_bucket(), now READ/WRITE are same.

        :param mode: access mode
        :returns: bool, if the bucket of s3_url has read/write access.
        '''
        s3_url = self.path_with_protocol
        if followlinks:
            metadata = _s3_get_metadata(s3_url)
            if metadata and 'symlink_to' in metadata:
                s3_url = metadata['symlink_to']
        bucket, _ = parse_s3_url(s3_url)  # only check bucket accessibility
        if not bucket:
            raise Exception("No available bucket")
        if not isinstance(mode, Access):
            raise TypeError(
                'Unsupported mode: {} -- Mode should use one of the enums belonging to:  {}'
                .format(mode, ', '.join([str(a) for a in Access])))
        if mode not in (Access.READ, Access.WRITE):
            raise TypeError('Unsupported mode: {}'.format(mode))
        client = get_s3_client()
        try:
            client.head_bucket(Bucket=bucket)
        except Exception as error:
            error = translate_s3_error(error, s3_url)
            if isinstance(error, (S3PermissionError, S3FileNotFoundError,
                                  S3BucketNotFoundError)):
                return False
            raise error
        return True

    def exists(self, followlinks: bool = False) -> bool:
        '''
        Test if s3_url exists

        If the bucket of s3_url are not permitted to read, return False

        :returns: True if s3_url eixsts, else False
        '''
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:  # s3:// => True, s3:///key => False
            return not key

        return self.is_dir() or self.is_file(followlinks)

    def getmtime(self, followlinks: bool = False) -> float:
        '''
        Get last-modified time of the file on the given s3_url path (in Unix timestamp format).
        If the path is an existent directory, return the latest modified time of all file in it. The mtime of empty directory is 1970-01-01 00:00:00

        If s3_url is not an existent path, which means s3_exist(s3_url) returns False, then raise S3FileNotFoundError

        :returns: Last-modified time
        :raises: S3FileNotFoundError, UnsupportedError
        '''
        return self.stat(followlinks=followlinks).mtime

    def getsize(self, followlinks: bool = False) -> int:
        '''
        Get file size on the given s3_url path (in bytes).
        If the path in a directory, return the sum of all file size in it, including file in subdirectories (if exist).
        The result excludes the size of directory itself. In other words, return 0 Byte on an empty directory path.

        If s3_url is not an existent path, which means s3_exist(s3_url) returns False, then raise S3FileNotFoundError

        :returns: File size
        :raises: S3FileNotFoundError, UnsupportedError
        '''
        return self.stat(followlinks=followlinks).size

    def glob(self, recursive: bool = True,
             missing_ok: bool = True) -> List[str]:
        '''Return s3 path list in ascending alphabetical order, in which path matches glob pattern
        Notes: Only glob in bucket. If trying to match bucket with wildcard characters, raise UnsupportedError

        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
        :raises: UnsupportedError, when bucket part contains wildcard characters
        :returns: A list contains paths match `s3_pathname`
        '''
        return list(self.iglob(recursive=recursive, missing_ok=missing_ok))

    def glob_stat(
            self,
            recursive: bool = True,
            missing_ok: bool = True,
            followlinks: bool = False) -> Iterator[FileEntry]:
        '''Return a generator contains tuples of path and file stat, in ascending alphabetical order, in which path matches glob pattern
        Notes: Only glob in bucket. If trying to match bucket with wildcard characters, raise UnsupportedError

        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
        :raises: UnsupportedError, when bucket part contains wildcard characters
        :returns: A generator contains tuples of path and file stat, in which paths match `s3_pathname`
        '''
        s3_pathname = fspath(self.path_with_protocol)

        iterables = []
        for group_s3_pathname_1 in _group_s3path_by_bucket(s3_pathname):
            for group_s3_pathname_2 in _group_s3path_by_prefix(
                    group_s3_pathname_1):
                iterables.append(
                    _s3_glob_stat_single_path(
                        group_s3_pathname_2,
                        recursive,
                        missing_ok,
                        followlinks=followlinks))

        generator = chain(*iterables)
        return _create_missing_ok_generator(
            generator, missing_ok,
            S3FileNotFoundError('No match file: %r' % s3_pathname))

    def iglob(self, recursive: bool = True,
              missing_ok: bool = True) -> Iterator[str]:
        '''Return s3 path iterator in ascending alphabetical order, in which path matches glob pattern
        Notes: Only glob in bucket. If trying to match bucket with wildcard characters, raise UnsupportedError

        :param recursive: If False, `**` will not search directory recursively
        :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
        :raises: UnsupportedError, when bucket part contains wildcard characters
        :returns: An iterator contains paths match `s3_pathname`
        '''
        s3_glob_stat_iter = self.glob_stat(
            recursive=recursive, missing_ok=missing_ok)

        def create_generator() -> Iterator[str]:
            for path, _ in s3_glob_stat_iter:
                yield path

        return create_generator()

    def is_dir(self, **kwargs) -> bool:
        '''
        Test if an s3 url is directory
        Specific procedures are as follows:
        If there exists a suffix, of which ``os.path.join(s3_url, suffix)`` is a file
        If the url is empty bucket or s3://

        :returns: True if path is s3 directory, else False
        '''
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:  # s3:// => True, s3:///key => False
            return not key
        prefix = _become_prefix(key)
        client = get_s3_client()
        try:
            resp = client.list_objects_v2(
                Bucket=bucket, Prefix=prefix, Delimiter='/', MaxKeys=1)
        except Exception as error:
            error = translate_s3_error(error, self.path_with_protocol)
            if isinstance(error, (S3UnknownError, S3ConfigError)):
                raise error
            return False

        if not key:  # bucket is accessible
            return True

        if 'KeyCount' in resp:
            return resp['KeyCount'] > 0

        return len(resp.get('Contents', [])) > 0 or \
            len(resp.get('CommonPrefixes', [])) > 0

    def is_file(self, followlinks: bool = False) -> bool:
        '''
        Test if an s3_url is file

        :returns: True if path is s3 file, else False
        '''
        s3_url = self.path_with_protocol
        if followlinks:
            metadata = _s3_get_metadata(s3_url)
            if metadata and 'symlink_to' in metadata:
                s3_url = metadata['symlink_to']
        bucket, key = parse_s3_url(s3_url)
        if not bucket or not key or key.endswith('/'):
            # s3://, s3:///key, s3://bucket, s3://bucket/prefix/
            return False
        client = get_s3_client()
        try:
            client.head_object(Bucket=bucket, Key=key)
        except Exception as error:
            error = translate_s3_error(error, s3_url)
            if isinstance(error, (S3UnknownError, S3ConfigError)):
                raise error
            return False
        return True

    def listdir(self, followlinks: bool = False) -> List[str]:
        '''
        Get all contents of given s3_url. The result is in acsending alphabetical order.

        :returns: All contents have prefix of s3_url in acsending alphabetical order
        :raises: S3FileNotFoundError, S3NotADirectoryError
        '''
        entries = list(self.scandir(followlinks=followlinks))
        return sorted([entry.name for entry in entries])

    def load(self, followlinks: bool = False) -> BinaryIO:
        '''Read all content in binary on specified path and write into memory

        User should close the BinaryIO manually

        :returns: BinaryIO
        '''
        s3_url = self.path_with_protocol
        if followlinks:
            metadata = _s3_get_metadata(s3_url)
            if metadata and 'symlink_to' in metadata:
                s3_url = metadata['symlink_to']
        bucket, key = parse_s3_url(s3_url)
        if not bucket:
            raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)
        if not key or key.endswith('/'):
            raise S3IsADirectoryError('Is a directory: %r' % s3_url)

        buffer = io.BytesIO()
        client = get_s3_client()
        with raise_s3_error(s3_url):
            client.download_fileobj(bucket, key, buffer)
        buffer.seek(0)
        return buffer

    def hasbucket(self) -> bool:
        '''
        Test if the bucket of s3_url exists

        :returns: True if bucket of s3_url eixsts, else False
        '''
        bucket, _ = parse_s3_url(self.path_with_protocol)
        if not bucket:
            return False

        client = get_s3_client()
        try:
            client.head_bucket(Bucket=bucket)
        except Exception as error:
            error = translate_s3_error(error, self.path_with_protocol)
            if isinstance(error, (S3UnknownError, S3ConfigError)):
                raise error
            if isinstance(error, S3FileNotFoundError):
                return False

        return True

    def mkdir(self, exist_ok: bool = False):
        '''
        Create an s3 directory.
        Purely creating directory is invalid because it's unavailable on OSS.
        This function is to test the target bucket have WRITE access.

        :param exist_ok: If False and target directory exists, raise S3FileExistsError
        :raises: S3BucketNotFoundError, S3FileExistsError
        '''
        bucket, _ = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                'Empty bucket name: %r' % self.path_with_protocol)
        if not self.hasbucket():
            raise S3BucketNotFoundError(
                'No such bucket: %r' % self.path_with_protocol)
        if exist_ok:
            if self.is_file():
                raise S3FileExistsError(
                    'File exists: %r' % self.path_with_protocol)
            return
        if self.exists():
            raise S3FileExistsError('File exists: %r' % self.path_with_protocol)

    def move(self, dst_url: PathLike) -> None:
        '''
        Move file/directory path from src_url to dst_url

        :param dst_url: Given destination path
        '''
        for src_file_path, dst_file_path in _s3_scan_pairs(
                self.path_with_protocol, dst_url):
            S3Path(src_file_path).rename(dst_file_path)

    def remove(self, missing_ok: bool = False, **kwargs) -> None:
        '''
        Remove the file or directory on s3, `s3://` and `s3://bucket` are not permitted to remove

        :param missing_ok: if False and target file/directory not exists, raise S3FileNotFoundError
        :raises: S3PermissionError, S3FileNotFoundError, UnsupportedError
        '''
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            if not key:
                raise UnsupportedError(
                    'Remove whole s3', self.path_with_protocol)
            raise S3BucketNotFoundError(
                'Empty bucket name: %r' % self.path_with_protocol)
        if not key:
            raise UnsupportedError('Remove bucket', self.path_with_protocol)
        if not self.exists():
            if missing_ok:
                return
            raise S3FileNotFoundError(
                'No such file or directory: %r' % self.path_with_protocol)

        client = get_s3_client()
        with raise_s3_error(self.path_with_protocol):
            if self.is_file():
                client.delete_object(Bucket=bucket, Key=key)
                return
            prefix = _become_prefix(key)
            total_count, error_count = 0, 0
            for resp in _list_objects_recursive(client, bucket, prefix):
                if 'Contents' in resp:
                    keys = [
                        {
                            'Key': content['Key']
                        } for content in resp['Contents']
                    ]
                    total_count += len(keys)
                    errors = []
                    retries = 2
                    retry_interval = min(0.1 * 2**retries, 30)
                    for i in range(retries):
                        # doc: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_objects
                        if not keys:
                            break
                        response = client.delete_objects(
                            Bucket=bucket, Delete={'Objects': keys})
                        keys = []
                        for error_info in response.get('Errors', []):
                            if s3_error_code_should_retry(
                                    error_info.get('Code')):
                                error_logger.warning(
                                    "retry %s times, removing file: %s, with error %s: %s"
                                    % (
                                        i + 1, error_info['Key'],
                                        error_info['Code'],
                                        error_info['Message']))
                                keys.append({'Key': error_info['Key']})
                            else:
                                errors.append(error_info)
                        time.sleep(retry_interval)
                    for error_info in errors:
                        error_logger.error(
                            "failed remove file: %s, with error %s: %s" % (
                                error_info['Key'], error_info['Code'],
                                error_info['Message']))
                    error_count += len(errors)
            if error_count > 0:
                error_msg = "failed remove path: %s, total file count: %s, failed count: %s" % (
                    self.path_with_protocol, total_count, error_count)
                raise S3UnknownError(
                    Exception(error_msg), self.path_with_protocol)

    def rename(self, dst_url: PathLike) -> None:
        '''
        Move s3 file path from src_url to dst_url

        :param dst_url: Given destination path
        '''
        self.copy(dst_url)
        self.remove()

    def scan(self, missing_ok: bool = True,
             followlinks: bool = False) -> Iterator[str]:
        '''
        Iteratively traverse only files in given s3 directory, in alphabetical order.
        Every iteration on generator yields a path string.

        If s3_url is a file path, yields the file only
        If s3_url is a non-existent path, return an empty generator
        If s3_url is a bucket path, return all file paths in the bucket
        If s3_url is an empty bucket, return an empty generator
        If s3_url doesn't contain any bucket, which is s3_url == 's3://', raise UnsupportedError. walk() on complete s3 is not supported in megfile

        :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
        :raises: UnsupportedError
        :returns: A file path generator
        '''
        scan_stat_iter = self.scan_stat(
            missing_ok=missing_ok, followlinks=followlinks)

        def create_generator() -> Iterator[str]:
            for path, _ in scan_stat_iter:
                yield path

        return create_generator()

    def scan_stat(self, missing_ok: bool = True,
                  followlinks: bool = False) -> Iterator[FileEntry]:
        '''
        Iteratively traverse only files in given directory, in alphabetical order.
        Every iteration on generator yields a tuple of path string and file stat

        :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
        :raises: UnsupportedError
        :returns: A file path generator
        '''
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise UnsupportedError('Scan whole s3', self.path_with_protocol)

        def create_generator() -> Iterator[FileEntry]:
            if not self.is_dir():
                if self.is_file():
                    # On s3, file and directory may be of same name and level, so need to test the path is file or directory
                    yield FileEntry(
                        fspath(self.path_with_protocol),
                        self.stat(followlinks=followlinks))
                return

            if not key.endswith('/') and self.is_file():
                yield FileEntry(
                    fspath(self.path_with_protocol),
                    self.stat(followlinks=followlinks))

            prefix = _become_prefix(key)
            client = get_s3_client()
            with raise_s3_error(self.path_with_protocol):
                for resp in _list_objects_recursive(client, bucket, prefix):
                    for content in resp.get('Contents', []):
                        full_path = s3_path_join(
                            's3://', bucket, content['Key'])
                        yield FileEntry(full_path, _make_stat(content))

        return _create_missing_ok_generator(
            create_generator(), missing_ok,
            S3FileNotFoundError('No match file: %r' % self.path_with_protocol))

    def scandir(self, followlinks: bool = False) -> Iterator[FileEntry]:
        '''
        Get all contents of given s3_url, the order of result is not guaranteed.

        :returns: All contents have prefix of s3_url
        :raises: S3FileNotFoundError, S3NotADirectoryError
        '''
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket and key:
            raise S3BucketNotFoundError(
                'Empty bucket name: %r' % self.path_with_protocol)

        if self.is_file():
            raise S3NotADirectoryError(
                'Not a directory: %r' % self.path_with_protocol)
        elif not self.is_dir():
            raise S3FileNotFoundError(
                'No such directory: %r' % self.path_with_protocol)
        prefix = _become_prefix(key)
        client = get_s3_client()

        # In order to do check on creation,
        # we need to wrap the iterator in another function
        def create_generator() -> Iterator[FileEntry]:
            with raise_s3_error(self.path_with_protocol):

                def generate_s3_path(bucket: str, key: str) -> str:
                    return "s3://%s/%s" % (bucket, key)

                if not bucket and not key:  # list buckets
                    response = client.list_buckets()
                    for content in response['Buckets']:
                        yield FileEntry(
                            content['Name'],
                            StatResult(
                                ctime=content['CreationDate'].timestamp(),
                                isdir=True,
                                extra=content,
                            ))
                    return

                for resp in _list_objects_recursive(client, bucket, prefix,
                                                    '/'):
                    for common_prefix in resp.get('CommonPrefixes', []):
                        yield FileEntry(
                            common_prefix['Prefix'][len(prefix):-1],
                            StatResult(isdir=True, extra=common_prefix))
                    for content in resp.get('Contents', []):

                        if followlinks:
                            content_key = content['Key']
                            src_url = generate_s3_path(bucket, content_key)
                            if S3Path(src_url).is_symlink():
                                content['islnk'] = True
                                yield FileEntry(
                                    content['Key'][len(prefix):],
                                    _make_stat(content))
                            else:
                                yield FileEntry(
                                    content['Key'][len(prefix):],
                                    _make_stat(content))
                        else:
                            yield FileEntry(
                                content['Key'][len(prefix):],
                                _make_stat(content))

        return create_generator()

    def _getdirstat(self) -> StatResult:
        '''
        Return StatResult of given s3_url directory, including: 

        1. Directory size: the sum of all file size in it, including file in subdirectories (if exist).
        The result excludes the size of directory itself. In other words, return 0 Byte on an empty directory path
        2. Last-modified time of directory: return the latest modified time of all file in it. The mtime of empty directory is 1970-01-01 00:00:00

        :returns: An int indicates size in Bytes
        '''
        if not self.is_dir():
            raise S3FileNotFoundError(
                'No such file or directory: %r' % self.path_with_protocol)

        bucket, key = parse_s3_url(self.path_with_protocol)
        prefix = _become_prefix(key)
        client = get_s3_client()
        size = 0
        mtime = 0.0
        with raise_s3_error(self.path_with_protocol):
            for resp in _list_objects_recursive(client, bucket, prefix):
                for content in resp.get('Contents', []):
                    size += content['Size']
                    last_modified = content['LastModified'].timestamp()
                    if mtime < last_modified:
                        mtime = last_modified

        return StatResult(size=size, mtime=mtime, isdir=True)

    def stat(self, followlinks: bool = False) -> StatResult:
        '''
        Get StatResult of s3_url file, including file size and mtime, referring to s3_getsize and s3_getmtime

        Automatically identifies "islnk" of s3_url whether "followlinks" is True or not.
        If s3_url is not an existent path, which means s3_exist(s3_url) returns False, then raise S3FileNotFoundError
        If attempt to get StatResult of complete s3, such as s3_dir_url == 's3://', raise S3BucketNotFoundError

        :returns: StatResult
        :raises: S3FileNotFoundError, S3BucketNotFoundError
        '''
        islnk = False
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                'Empty bucket name: %r' % self.path_with_protocol)

        if not self.is_file():
            return self._getdirstat()

        client = get_s3_client()
        with raise_s3_error(self.path_with_protocol):
            content = client.head_object(Bucket=bucket, Key=key)
            if 'Metadata' in content:
                metadata = dict(
                    (key.lower(), value)
                    for key, value in content['Metadata'].items())
                if metadata and 'symlink_to' in metadata:
                    islnk = True
                    if islnk and followlinks:
                        s3_url = metadata['symlink_to']
                        bucket, key = parse_s3_url(s3_url)
                        content = client.head_object(Bucket=bucket, Key=key)
            stat_record = StatResult(
                islnk=islnk,
                size=content['ContentLength'],
                mtime=content['LastModified'].timestamp(),
                extra=content)
        return stat_record

    def unlink(self, missing_ok: bool = False) -> None:
        '''
        Remove the file on s3

        :param missing_ok: if False and target file not exists, raise S3FileNotFoundError
        :raises: S3PermissionError, S3FileNotFoundError, S3IsADirectoryError
        '''
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket or not key or key.endswith('/'):
            raise S3IsADirectoryError(
                'Is a directory: %r' % self.path_with_protocol)
        if not self.is_file():
            if missing_ok:
                return
            raise S3FileNotFoundError(
                'No such file: %r' % self.path_with_protocol)

        client = get_s3_client()
        with raise_s3_error(self.path_with_protocol):
            client.delete_object(Bucket=bucket, Key=key)

    def walk(self, **kwargs) -> Iterator[Tuple[str, List[str], List[str]]]:
        '''
        Iteratively traverse the given s3 directory, in top-bottom order. In other words, firstly traverse parent directory, if subdirectories exist, traverse the subdirectories in alphabetical order.
        Every iteration on generator yields a 3-tuple: (root, dirs, files)

        - root: Current s3 path;
        - dirs: Name list of subdirectories in current directory. The list is sorted by name in ascending alphabetical order;
        - files: Name list of files in current directory. The list is sorted by name in ascending alphabetical order;

        If s3_url is a file path, return an empty generator
        If s3_url is a non-existent path, return an empty generator
        If s3_url is a bucket path, bucket will be the top directory, and will be returned at first iteration of generator
        If s3_url is an empty bucket, only yield one 3-tuple (notes: s3 doesn't have empty directory)
        If s3_url doesn't contain any bucket, which is s3_url == 's3://', raise UnsupportedError. walk() on complete s3 is not supported in megfile

        :raises: UnsupportedError
        :returns: A 3-tuple generator
        '''
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise UnsupportedError('Walk whole s3', self.path_with_protocol)

        if not self.is_dir():
            return

        stack = [key]
        client = get_s3_client()
        while len(stack) > 0:
            current = _become_prefix(stack.pop())
            dirs, files = [], []
            for resp in _list_objects_recursive(client, bucket, current, '/'):
                for common_prefix in resp.get('CommonPrefixes', []):
                    dirs.append(common_prefix['Prefix'][:-1])
                for content in resp.get('Contents', []):
                    files.append(content['Key'])

            dirs = sorted(dirs)
            stack.extend(reversed(dirs))

            root = s3_path_join('s3://', bucket, current)[:-1]
            dirs = [path[len(current):] for path in dirs]
            files = sorted(path[len(current):] for path in files)
            yield root, dirs, files

    def md5(self, recalculate: bool = False, followlinks: bool = False) -> str:
        '''
        Get md5 meta info in files that uploaded/copied via megfile

        If meta info is lost or non-existent, return None

        :param recalculate: calculate md5 in real-time or return s3 etag
        :returns: md5 meta info
        '''
        bucket, _ = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                'Empty bucket name: %r' % self.path_with_protocol)
        stat = self.stat(followlinks=followlinks)
        if stat.isdir is True:
            hash_md5 = hashlib.md5()  # nosec
            for file_name in self.listdir():
                chunk = S3Path(
                    s3_path_join(
                        self.path_with_protocol,
                        file_name)).md5(recalculate=recalculate).encode()
                hash_md5.update(chunk)
            return hash_md5.hexdigest()
        if recalculate is True:
            with self.open('rb') as f:
                return calculate_md5(f)
        return stat.extra.get('ETag', '')[1:-1]

    def copy(
            self,
            dst_url: PathLike,
            followlinks: bool = False,
            callback: Optional[Callable[[int], None]] = None) -> None:
        ''' File copy on S3
        Copy content of file on `src_path` to `dst_path`.
        It's caller's responsebility to ensure the s3_isfile(src_url) == True

        :param dst_path: Target file path
        :param callback: Called periodically during copy, and the input parameter is the data size (in bytes) of copy since the last call
        '''
        src_url = self.path_with_protocol
        src_bucket, src_key = parse_s3_url(src_url)
        dst_bucket, dst_key = parse_s3_url(dst_url)

        if not src_bucket:
            raise S3BucketNotFoundError('Empty bucket name: %r' % src_url)
        if self.is_dir():
            raise S3IsADirectoryError('Is a directory: %r' % src_url)

        if not dst_bucket:
            raise S3BucketNotFoundError('Empty bucket name: %r' % dst_url)
        if not dst_key or dst_key.endswith('/'):
            raise S3IsADirectoryError('Is a directory: %r' % dst_url)

        if followlinks:
            metadata = _s3_get_metadata(src_url)
            if metadata and 'symlink_to' in metadata:
                src_url = metadata['symlink_to']
                src_bucket, src_key = parse_s3_url(src_url)

        client = get_s3_client()
        try:
            client.copy(
                {
                    'Bucket': src_bucket,
                    'Key': src_key,
                },
                Bucket=dst_bucket,
                Key=dst_key,
                Callback=callback)
        except Exception as error:
            error = translate_s3_error(error, dst_url)
            # Error can't help tell which is problematic
            if isinstance(error, S3BucketNotFoundError):
                if not self.hasbucket():
                    raise S3BucketNotFoundError('No such bucket: %r' % src_url)
            elif isinstance(error, S3FileNotFoundError):
                if not self.is_file():
                    raise S3FileNotFoundError('No such file: %r' % src_url)
            raise error

    def sync(self, dst_url: PathLike, followlinks: bool = False) -> None:
        '''
        Copy file/directory on src_url to dst_url

        :param dst_url: Given destination path
        '''
        for src_file_path, dst_file_path in _s3_scan_pairs(
                self.path_with_protocol, dst_url):
            S3Path(src_file_path).copy(dst_file_path, followlinks=followlinks)

    def symlink(self, dst_url: PathLike) -> None:
        '''
        Create a symbolic link pointing to src_url named dst_url.

        :param dst_url: Desination path
        :raises: S3NameTooLongError, S3BucketNotFoundError, S3IsADirectoryError
        '''
        src_url = self.path_with_protocol
        if len(str(src_url).encode()) > 1024:
            raise S3NameTooLongError('File name too long: %r' % dst_url)
        src_bucket, src_key = parse_s3_url(src_url)
        dst_bucket, dst_key = parse_s3_url(dst_url)

        if not src_bucket:
            raise S3BucketNotFoundError('Empty bucket name: %r' % src_url)
        if not dst_bucket:
            raise S3BucketNotFoundError('Empty bucket name: %r' % dst_url)
        if not dst_key or dst_key.endswith('/'):
            raise S3IsADirectoryError('Is a directory: %r' % dst_url)

        metadata = _s3_get_metadata(src_url)

        if 'symlink_to' in metadata:
            src_url = metadata['symlink_to']
        with raise_s3_error(dst_url):
            client = get_s3_client()
            client.put_object(
                Bucket=dst_bucket,
                Key=dst_key,
                Metadata={"symlink_to": src_url})

    def readlink(self) -> PathLike:
        '''
        Return a string representing the path to which the symbolic link points.

        :returns: Return a string representing the path to which the symbolic link points.
        :raises: S3NameTooLongError, S3BucketNotFoundError, S3IsADirectoryError, S3NotALinkError
        '''
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                'Empty bucket name: %r' % self.path_with_protocol)
        if not key or key.endswith('/'):
            raise S3IsADirectoryError(
                'Is a directory: %r' % self.path_with_protocol)
        metadata = _s3_get_metadata(self.path_with_protocol)

        if not 'symlink_to' in metadata:
            raise S3NotALinkError('Not a link: %r' % self.path_with_protocol)
        else:
            return metadata['symlink_to']

    def is_symlink(self) -> bool:
        '''
        Test whether a path is link

        :returns: True if a path is link, else False
        :raises: S3NotALinkError
        '''
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            return False
        if not key or key.endswith('/'):
            return False
        metadata = _s3_get_metadata(self.path_with_protocol)
        return 'symlink_to' in metadata

    def save(self, file_object: BinaryIO):
        '''Write the opened binary stream to specified path, but the stream won't be closed

        :param file_object: Stream to be read
        '''
        bucket, key = parse_s3_url(self.path_with_protocol)
        if not bucket:
            raise S3BucketNotFoundError(
                'Empty bucket name: %r' % self.path_with_protocol)
        if not key or key.endswith('/'):
            raise S3IsADirectoryError(
                'Is a directory: %r' % self.path_with_protocol)

        client = get_s3_client()
        with raise_s3_error(self.path_with_protocol):
            client.upload_fileobj(file_object, Bucket=bucket, Key=key)

    def open(
            self,
            mode: str = 'r',
            *,
            s3_open_func: Callable[[str, str], BinaryIO] = s3_open,
            **kwargs) -> IO[AnyStr]:
        return s3_open_func(
            self.path_with_protocol, mode,
            **necessary_params(s3_open_func, **kwargs))

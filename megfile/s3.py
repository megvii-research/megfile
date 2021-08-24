import hashlib
import inspect
import io
import os
import re
from collections import defaultdict
from functools import wraps
from itertools import chain
from logging import getLogger as get_logger
from typing import Any, BinaryIO, Callable, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import urlsplit

import boto3
import botocore
import smart_open.s3

from megfile.errors import S3BucketNotFoundError, S3ConfigError, S3FileExistsError, S3FileNotFoundError, S3IsADirectoryError, S3NotADirectoryError, S3PermissionError, S3UnknownError, UnsupportedError, _create_missing_ok_generator
from megfile.errors import _logger as error_logger
from megfile.errors import patch_method, raise_s3_error, s3_should_retry, translate_fs_error, translate_s3_error
from megfile.interfaces import Access, FileCacher, FileEntry, MegfilePathLike, StatResult
from megfile.lib.compat import fspath
from megfile.lib.fnmatch import translate
from megfile.lib.glob import globlize, has_magic, ungloblize
from megfile.lib.joinpath import uri_join
from megfile.lib.s3_buffered_writer import DEFAULT_MAX_BUFFER_SIZE, S3BufferedWriter
from megfile.lib.s3_cached_handler import S3CachedHandler
from megfile.lib.s3_limited_seekable_writer import S3LimitedSeekableWriter
from megfile.lib.s3_pipe_handler import S3PipeHandler
from megfile.lib.s3_prefetch_reader import DEFAULT_BLOCK_SIZE, S3PrefetchReader
from megfile.lib.s3_share_cache_reader import S3ShareCacheReader
from megfile.utils import get_binary_mode, get_content_offset, is_readable, thread_local

MEGFILE_MD5_HEADER = 'megfile-content-md5'

# Monkey patch for smart_open
_smart_open_parameters = inspect.signature(smart_open.s3.open).parameters
if 'resource_kwargs' in _smart_open_parameters:
    # smart_open >= 1.8.1
    def _s3_open(bucket: str, key: str, mode: str):
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
    def _s3_open(bucket: str, key: str, mode: str):
        return smart_open.s3.open(
            bucket,
            key,
            mode,
            s3_session=get_s3_session(),
            endpoint_url=get_endpoint_url())


__all__ = [
    'is_s3',
    's3_buffered_open',
    's3_cached_open',
    's3_copy',
    's3_download',
    's3_access',
    's3_exists',
    's3_getmd5',
    's3_getmtime',
    's3_getsize',
    's3_glob',
    's3_glob_stat',
    's3_hasbucket',
    's3_iglob',
    's3_isdir',
    's3_isfile',
    's3_legacy_open',
    's3_listdir',
    's3_load_content',
    's3_load_from',
    's3_makedirs',
    's3_memory_open',
    's3_open',
    's3_path_join',
    's3_pipe_open',
    's3_prefetch_open',
    's3_remove',
    's3_rename',
    's3_move',
    's3_sync',
    's3_save_as',
    's3_scan',
    's3_scan_stat',
    's3_scandir',
    's3_stat',
    's3_share_cache_open',
    's3_unlink',
    's3_upload',
    's3_walk',
    'S3Cacher',
    'get_s3_client',
    'parse_s3_url',
    'get_endpoint_url',
    'S3BufferedWriter',
    'get_s3_session',
    'S3LimitedSeekableWriter',
    'S3PrefetchReader',
    'S3ShareCacheReader',
]

_logger = get_logger(__name__)

_default_endpoint_url = 'https://s3.amazonaws.com'


def get_endpoint_url() -> str:
    '''Get the endpoint url of S3

    returns: S3 endpoint url
    '''
    oss_endpoint = os.environ.get('OSS_ENDPOINT')
    if oss_endpoint is None:
        oss_endpoint = _default_endpoint_url
    return oss_endpoint


def get_s3_session():
    '''Get S3 session

    returns: S3 session
    '''
    return thread_local('s3_session', boto3.session.Session)


max_pool_connections = 32
max_retries = 10


def _patch_make_request(client: botocore.client.BaseClient):

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
        before_callback=before_callback,
        retry_callback=retry_callback)
    return client


def _patch_send_request():
    # From: https://github.com/boto/botocore/pull/1328
    try:
        import botocore.awsrequest
        original_send_request = botocore.awsrequest.AWSConnection._send_request
    except (AttributeError, ImportError):
        return

    def _send_request(self, method, url, body, headers, *args, **kwargs):
        if headers.get('Content-Length') == '0':
            # From RFC: https://tools.ietf.org/html/rfc7231#section-5.1.1
            # Requirement for clients:
            # - A client MUST NOT generate a 100-continue expectation
            #   in a request that does not include a message body.
            headers.pop('Expect', None)
        original_send_request(self, method, url, body, headers, *args, **kwargs)

    botocore.awsrequest.AWSConnection._send_request = _send_request


_patch_send_request()


def get_s3_client(
        config: Optional[botocore.config.Config] = None,
        cache_key: Optional[str] = None):
    '''Get S3 client

    returns: S3 client
    '''
    if cache_key is not None:
        return thread_local(cache_key, get_s3_client, config)
    client = get_s3_session().client(
        's3', endpoint_url=get_endpoint_url(), config=config)
    client = _patch_make_request(client)
    return client


def is_s3(path: MegfilePathLike) -> bool:
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


def parse_s3_url(s3_url: MegfilePathLike) -> Tuple[str, str]:
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


def _become_prefix(prefix: str) -> str:
    if prefix != '' and not prefix.endswith('/'):
        prefix += '/'
    return prefix


def _make_stat(content: Dict[str, Any]):
    return StatResult(
        size=content['Size'],
        mtime=content['LastModified'].timestamp(),
        extra=content,
    )


def s3_copy(
        src_url: MegfilePathLike,
        dst_url: MegfilePathLike,
        callback: Optional[Callable[[int], None]] = None) -> None:
    ''' File copy on S3
    Copy content of file on `src_path` to `dst_path`.
    It's caller's responsebility to ensure the s3_isfile(src_url) == True

    :param src_path: Source file path
    :param dst_path: Target file path
    :param callback: Called periodically during copy, and the input parameter is the data size (in bytes) of copy since the last call
    '''
    src_bucket, src_key = parse_s3_url(src_url)
    dst_bucket, dst_key = parse_s3_url(dst_url)

    if not src_bucket:
        raise S3BucketNotFoundError('Empty bucket name: %r' % src_url)
    if not src_key or src_key.endswith('/'):
        raise S3IsADirectoryError('Is a directory: %r' % src_url)

    if not dst_bucket:
        raise S3BucketNotFoundError('Empty bucket name: %r' % dst_url)
    if not dst_key or dst_key.endswith('/'):
        raise S3IsADirectoryError('Is a directory: %r' % dst_url)

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
            if not s3_hasbucket(src_url):
                raise S3BucketNotFoundError('No such bucket: %r' % src_url)
        elif isinstance(error, S3FileNotFoundError):
            if not s3_isfile(src_url):
                if s3_isdir(src_url):
                    raise S3IsADirectoryError('Is a directory: %r' % src_url)
                raise S3FileNotFoundError('No such file: %r' % src_url)
        raise error


def s3_isdir(s3_url: MegfilePathLike) -> bool:
    '''
    Test if an s3 url is directory
    Specific procedures are as follows:
    If there exists a suffix, of which ``os.path.join(s3_url, suffix)`` is a file
    If the url is empty bucket or s3://

    :param s3_url: Path to be tested
    :returns: True if path is s3 directory, else False
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket:  # s3:// => True, s3:///key => False
        return not key

    prefix = _become_prefix(key)
    client = get_s3_client()
    try:
        resp = client.list_objects_v2(
            Bucket=bucket, Prefix=prefix, Delimiter='/', MaxKeys=1)
    except Exception as error:
        error = translate_s3_error(error, s3_url)
        if isinstance(error, (S3UnknownError, S3ConfigError)):
            raise error
        return False

    if not key:  # bucket is accessible
        return True

    if 'KeyCount' in resp:
        return resp['KeyCount'] > 0

    return len(resp.get('Contents', [])) > 0 or \
        len(resp.get('CommonPrefixes', [])) > 0


def s3_isfile(s3_url: MegfilePathLike) -> bool:
    '''
    Test if an s3_url is file

    :param s3_url: Path to be tested
    :returns: True if path is s3 file, else False
    '''
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


def s3_access(s3_url: MegfilePathLike, mode: Access = Access.READ) -> bool:
    '''
    Test if path has access permission described by mode
    Using head_bucket(), now READ/WRITE are same.

    :param s3_url: Path to be tested
    :param mode: access mode
    :returns: bool, if the bucket of s3_url has read/write access.
    '''
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
        if isinstance(
                error,
            (S3PermissionError, S3FileNotFoundError, S3BucketNotFoundError)):
            return False
        raise error
    return True


def s3_hasbucket(s3_url: MegfilePathLike) -> bool:
    '''
    Test if the bucket of s3_url exists

    :param path: Path to be tested
    :returns: True if bucket of s3_url eixsts, else False
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket:
        return False

    client = get_s3_client()
    try:
        client.head_bucket(Bucket=bucket)
    except Exception as error:
        error = translate_s3_error(error, s3_url)
        if isinstance(error, (S3UnknownError, S3ConfigError)):
            raise error
        if isinstance(error, S3FileNotFoundError):
            return False

    return True


def s3_exists(s3_url: MegfilePathLike) -> bool:
    '''
    Test if s3_url exists

    If the bucket of s3_url are not permitted to read, return False

    :param path: Path to be tested
    :returns: True if s3_url eixsts, else False
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket:  # s3:// => True, s3:///key => False
        return not key

    return s3_isfile(s3_url) or s3_isdir(s3_url)


max_keys = 1000


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


def s3_scandir(s3_url: MegfilePathLike) -> Iterator[FileEntry]:
    '''
    Get all contents of given s3_url, the order of result is not guaranteed.

    :param s3_url: Given s3 path
    :returns: All contents have prefix of s3_url
    :raises: S3FileNotFoundError, S3NotADirectoryError
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket and key:
        raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)

    if s3_isfile(s3_url):
        raise S3NotADirectoryError('Not a directory: %r' % s3_url)
    elif not s3_isdir(s3_url):
        raise S3FileNotFoundError('No such directory: %r' % s3_url)
    prefix = _become_prefix(key)
    client = get_s3_client()

    # In order to do check on creation,
    # we need to wrap the iterator in another function
    def create_generator() -> Iterator[FileEntry]:
        with raise_s3_error(s3_url):
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

            for resp in _list_objects_recursive(client, bucket, prefix, '/'):
                for common_prefix in resp.get('CommonPrefixes', []):
                    yield FileEntry(
                        common_prefix['Prefix'][len(prefix):-1],
                        StatResult(isdir=True, extra=common_prefix))
                for content in resp.get('Contents', []):
                    yield FileEntry(
                        content['Key'][len(prefix):], _make_stat(content))

    return create_generator()


def s3_listdir(s3_url: str) -> List[str]:
    '''
    Get all contents of given s3_url. The result is in acsending alphabetical order.

    :param s3_url: Given s3 path
    :returns: All contents have prefix of s3_url in acsending alphabetical order
    :raises: S3FileNotFoundError, S3NotADirectoryError
    '''
    entries = list(s3_scandir(s3_url))
    return sorted([entry.name for entry in entries])


def _s3_getdirstat(s3_dir_url: str) -> StatResult:
    '''
    Return StatResult of given s3_url directory, including：

    1. Directory size: the sum of all file size in it, including file in subdirectories (if exist).
    The result exludes the size of directory itself. In other words, return 0 Byte on an empty directory path
    2. Last-modified time of directory：return the latest modified time of all file in it. The mtime of empty directory is 1970-01-01 00:00:00

    :param s3_url: Given s3 path
    :returns: An int indicates size in Bytes
    '''
    if not s3_isdir(s3_dir_url):
        raise S3FileNotFoundError('No such file or directory: %r' % s3_dir_url)

    bucket, key = parse_s3_url(s3_dir_url)
    prefix = _become_prefix(key)
    client = get_s3_client()
    size = 0
    mtime = 0.0
    with raise_s3_error(s3_dir_url):
        for resp in _list_objects_recursive(client, bucket, prefix):
            for content in resp.get('Contents', []):
                size += content['Size']
                last_modified = content['LastModified'].timestamp()
                if mtime < last_modified:
                    mtime = last_modified

    return StatResult(size=size, mtime=mtime, isdir=True)


def s3_stat(s3_url: MegfilePathLike) -> StatResult:
    '''
    Get StatResult of s3_url file, including file size and mtime, referring to s3_getsize and s3_getmtime

    If s3_url is not an existent path, which means s3_exist(s3_url) returns False, then raise S3FileNotFoundError
    If attempt to get StatResult of complete s3, such as s3_dir_url == 's3://', raise UnsupportedError

    :param s3_url: Given s3 path
    :returns: StatResult
    :raises: S3FileNotFoundError, UnsupportedError
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket:
        if not key:
            raise UnsupportedError('Get stat of whole s3', s3_url)
        raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)

    if not s3_isfile(s3_url):
        return _s3_getdirstat(s3_url)

    if not key or key.endswith('/'):
        raise S3FileNotFoundError('No such directory: %r' % s3_url)

    client = get_s3_client()
    with raise_s3_error(s3_url):
        content = client.head_object(Bucket=bucket, Key=key)
        stat_record = StatResult(
            size=content['ContentLength'],
            mtime=content['LastModified'].timestamp(),
            extra=content)
    return stat_record


def s3_getsize(s3_url: MegfilePathLike) -> int:
    '''
    Get file size on the given s3_url path (in bytes).
    If the path in a directory, return the sum of all file size in it, including file in subdirectories (if exist).
    The result exludes the size of directory itself. In other words, return 0 Byte on an empty directory path.

    If s3_url is not an existent path, which means s3_exist(s3_url) returns False, then raise S3FileNotFoundError

    :param s3_url: Given s3 path
    :returns: File size
    :raises: S3FileNotFoundError, UnsupportedError
    '''
    return s3_stat(s3_url).size


def s3_getmtime(s3_url: MegfilePathLike) -> float:
    '''
    Get last-modified time of the file on the given s3_url path (in Unix timestamp format).
    If the path is an existent directory, return the latest modified time of all file in it. The mtime of empty directory is 1970-01-01 00:00:00

    If s3_url is not an existent path, which means s3_exist(s3_url) returns False, then raise S3FileNotFoundError

    :param s3_url: Given s3 path
    :returns: Last-modified time
    :raises: S3FileNotFoundError, UnsupportedError
    '''
    return s3_stat(s3_url).mtime


def s3_upload(
        src_url: MegfilePathLike,
        dst_url: MegfilePathLike,
        callback: Optional[Callable[[int], None]] = None) -> None:
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
        # TODO: when have the 2nd md5 use case, extract this.
        hash_md5 = hashlib.md5()
        for chunk in iter(lambda: src.read(4096), b''):
            hash_md5.update(chunk)
        md5 = hash_md5.hexdigest()
        src.seek(0)

        with raise_s3_error(dst_url):
            # TODO: better design for metadata scheme when we have another metadata field.
            client.upload_fileobj(
                src,
                Bucket=dst_bucket,
                Key=dst_key,
                ExtraArgs={'Metadata': {
                    MEGFILE_MD5_HEADER: md5,
                }},
                Callback=callback)


def s3_download(
        src_url: MegfilePathLike,
        dst_url: MegfilePathLike,
        callback: Optional[Callable[[int], None]] = None) -> None:
    '''
    Downloads a file from s3 to local filesystem.
    :param src_url: source s3 path
    :param dst_url: target fs path
    :param callback: Called periodically during copy, and the input parameter is the data size (in bytes) of copy since the last call
    '''
    src_bucket, src_key = parse_s3_url(src_url)
    if not src_bucket:
        raise S3BucketNotFoundError('Empty bucket name: %r' % src_url)
    if not src_key or src_key.endswith('/'):
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
    except Exception as error:
        error = translate_fs_error(error, dst_url)
        error = translate_s3_error(error, src_url)
        if isinstance(error, S3FileNotFoundError) and s3_isdir(src_url):
            raise S3IsADirectoryError('Is a directory: %r' % src_url)
        raise error


def s3_remove(s3_url: MegfilePathLike, missing_ok: bool = False) -> None:
    '''
    Remove the file or directory on s3, `s3://` and `s3://bucket` are not permitted to remove

    :param s3_url: Given path
    :param missing_ok: if False and target file/directory not exists, raise S3FileNotFoundError
    :raises: S3PermissionError, S3FileNotFoundError, UnsupportedError
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket:
        if not key:
            raise UnsupportedError('Remove whole s3', s3_url)
        raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)
    if not key:
        raise UnsupportedError('Remove bucket', s3_url)
    if not s3_exists(s3_url):
        if missing_ok:
            return
        raise S3FileNotFoundError('No such file or directory: %r' % s3_url)

    client = get_s3_client()
    with raise_s3_error(s3_url):
        if s3_isfile(s3_url):
            client.delete_object(Bucket=bucket, Key=key)
            return
        prefix = _become_prefix(key)
        for resp in _list_objects_recursive(client, bucket, prefix):
            if 'Contents' in resp:
                keys = [{'Key': content['Key']} for content in resp['Contents']]
                client.delete_objects(Bucket=bucket, Delete={'Objects': keys})


def s3_unlink(s3_url: MegfilePathLike, missing_ok: bool = False) -> None:
    '''
    Remove the file on s3

    :param s3_url: Given path
    :param missing_ok: if False and target file not exists, raise S3FileNotFoundError
    :raises: S3PermissionError, S3FileNotFoundError, S3IsADirectoryError
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket or not key or key.endswith('/'):
        raise S3IsADirectoryError('Is a directory: %r' % s3_url)
    if not s3_isfile(s3_url):
        if missing_ok:
            return
        raise S3FileNotFoundError('No such file: %r' % s3_url)

    client = get_s3_client()
    with raise_s3_error(s3_url):
        client.delete_object(Bucket=bucket, Key=key)


def s3_makedirs(s3_url: MegfilePathLike, exist_ok: bool = False):
    '''
    Create an s3 directory.
    Purely creating directory is invalid because it's unavailable on OSS.
    This function is to test the target bucket have WRITE access.

    :param s3_url: Given path
    :param exist_ok: If False and target directory exists, raise S3FileExistsError
    :raises: S3BucketNotFoundError, S3FileExistsError
    '''
    bucket, _ = parse_s3_url(s3_url)
    if not bucket:
        raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)
    if not s3_hasbucket(s3_url):
        raise S3BucketNotFoundError('No such bucket: %r' % s3_url)
    if exist_ok:
        if s3_isfile(s3_url):
            raise S3FileExistsError('File exists: %r' % s3_url)
        return
    if s3_exists(s3_url):
        raise S3FileExistsError('File exists: %r' % s3_url)


def s3_walk(s3_url: MegfilePathLike
           ) -> Iterator[Tuple[str, List[str], List[str]]]:
    '''
    Iteratively traverse the given s3 directory, in top-bottom order. In other words, firstly traverse parent directory, if subdirectories exist, traverse the subdirectories in alphabetical order.
    Every iteration on generator yields a 3-tuple：(root, dirs, files)

    - root: Current s3 path;
    - dirs: Name list of subdirectories in current directory. The list is sorted by name in ascending alphabetical order;
    - files: Name list of files in current directory. The list is sorted by name in ascending alphabetical order;

    If s3_url is a file path, return an empty generator
    If s3_url is a non-existent path, return an empty generator
    If s3_url is a bucket path, bucket will be the top directory, and will be returned at first iteration of generator
    If s3_url is an empty bucket, only yield one 3-tuple (notes: s3 doesn't have empty directory)
    If s3_url doesn't contain any bucket, which is s3_url == 's3://', raise UnsupportedError. walk() on complete s3 is not supported in megfile

    :param path: An s3 path
    :raises: UnsupportedError
    :returns: A 3-tuple generator
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket:
        raise UnsupportedError('Walk whole s3', s3_url)

    if not s3_isdir(s3_url):
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


def s3_scan(s3_url: MegfilePathLike, missing_ok: bool = True) -> Iterator[str]:
    '''
    Iteratively traverse only files in given s3 directory, in alphabetical order.
    Every iteration on generator yields a path string.

    If s3_url is a file path, yields the file only
    If s3_url is a non-existent path, return an empty generator
    If s3_url is a bucket path, return all file paths in the bucket
    If s3_url is an empty bucket, return an empty generator
    If s3_url doesn't contain any bucket, which is s3_url == 's3://', raise UnsupportedError. walk() on complete s3 is not supported in megfile

    :param path: An s3 path
    :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
    :raises: UnsupportedError
    :returns: A file path generator
    '''
    scan_stat_iter = s3_scan_stat(s3_url)

    def create_generator() -> Iterator[str]:
        for path, _ in scan_stat_iter:
            yield path

    return create_generator()


def s3_scan_stat(s3_url: MegfilePathLike,
                 missing_ok: bool = True) -> Iterator[FileEntry]:
    '''
    Iteratively traverse only files in given directory, in alphabetical order.
    Every iteration on generator yields a tuple of path string and file stat

    :param path: Given s3_url
    :param missing_ok: If False and there's no file in the directory, raise FileNotFoundError
    :raises: UnsupportedError
    :returns: A file path generator
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket:
        raise UnsupportedError('Scan whole s3', s3_url)

    def create_generator() -> Iterator[FileEntry]:
        if not s3_isdir(s3_url):
            if s3_isfile(s3_url):
                # On s3, file and directory may be of same name and level, so need to test the path is file or directory
                yield FileEntry(fspath(s3_url), s3_stat(s3_url))
            return

        if not key.endswith('/') and s3_isfile(s3_url):
            yield FileEntry(fspath(s3_url), s3_stat(s3_url))

        prefix = _become_prefix(key)
        client = get_s3_client()
        with raise_s3_error(s3_url):
            for resp in _list_objects_recursive(client, bucket, prefix):
                for content in resp.get('Contents', []):
                    full_path = s3_path_join('s3://', bucket, content['Key'])
                    yield FileEntry(full_path, _make_stat(content))

    return _create_missing_ok_generator(
        create_generator(), missing_ok,
        S3FileNotFoundError('No match file: %r' % s3_url))


def s3_path_join(path: MegfilePathLike, *other_paths: MegfilePathLike) -> str:
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


def s3_glob(
        s3_pathname: MegfilePathLike,
        recursive: bool = True,
        missing_ok: bool = True) -> List[str]:
    '''Return s3 path list in ascending alphabetical order, in which path matches glob pattern
    Notes：Only glob in bucket. If trying to match bucket with wildcard characters, raise UnsupportedError

    :param s3_pathname: May contain shell wildcard characters
    :param recursive: If False，`**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    :raises: UnsupportedError, when bucket part contains wildcard characters
    :returns: A list contains paths match `s3_pathname`
    '''
    return list(
        s3_iglob(s3_pathname, recursive=recursive, missing_ok=missing_ok))


def s3_iglob(
        s3_pathname: MegfilePathLike,
        recursive: bool = True,
        missing_ok: bool = True) -> Iterator[str]:
    '''Return s3 path iterator in ascending alphabetical order, in which path matches glob pattern
    Notes：Only glob in bucket. If trying to match bucket with wildcard characters, raise UnsupportedError

    :param s3_pathname: May contain shell wildcard characters
    :param recursive: If False，`**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    :raises: UnsupportedError, when bucket part contains wildcard characters
    :returns: An iterator contains paths match `s3_pathname`
    '''
    s3_glob_stat_iter = s3_glob_stat(
        s3_pathname, recursive=recursive, missing_ok=missing_ok)

    def create_generator() -> Iterator[str]:
        for path, _ in s3_glob_stat_iter:
            yield path

    return create_generator()


def s3_glob_stat(
        s3_pathname: MegfilePathLike,
        recursive: bool = True,
        missing_ok: bool = True) -> Iterator[FileEntry]:
    '''Return a generator contains tuples of path and file stat, in ascending alphabetical order, in which path matches glob pattern
    Notes：Only glob in bucket. If trying to match bucket with wildcard characters, raise UnsupportedError

    :param s3_pathname: May contain shell wildcard characters
    :param recursive: If False，`**` will not search directory recursively
    :param missing_ok: If False and target path doesn't match any file, raise FileNotFoundError
    :raises: UnsupportedError, when bucket part contains wildcard characters
    :returns: A generator contains tuples of path and file stat, in which paths match `s3_pathname`
    '''
    result = []
    bucket_glob_list = _group_s3path_by_bucket(s3_pathname)

    for globpath in bucket_glob_list:
        result.append(
            _s3_glob_stat_single_path(globpath, recursive, missing_ok))

    iterableres = chain(*result)
    return _create_missing_ok_generator(
        iterableres, missing_ok,
        S3FileNotFoundError('No match file: %r' % s3_pathname))


def _s3_glob_stat_single_path(
        s3_pathname: MegfilePathLike,
        recursive: bool = True,
        missing_ok: bool = True) -> Iterator[FileEntry]:

    bucket, _ = parse_s3_url(s3_pathname)
    if not bucket or has_magic(bucket):
        raise UnsupportedError('Glob whole s3', s3_pathname)

    s3_pathname = fspath(s3_pathname)
    top_dir, wildcard_part = _s3_split_magic(s3_pathname)
    search_dir = wildcard_part.endswith('/')

    def create_generator(_s3_pathname) -> Iterator[FileEntry]:
        if not s3_exists(top_dir):
            return
        if not has_magic(_s3_pathname):
            if s3_isfile(_s3_pathname):
                yield FileEntry(_s3_pathname, s3_stat(_s3_pathname))
            if s3_isdir(_s3_pathname):
                yield FileEntry(_s3_pathname, StatResult(isdir=True))
            return

        # patch glob
        if not recursive:
            # If not recursive, replace ** with *
            _s3_pathname = re.sub(r'\*{2,}', '*', _s3_pathname)

        dirnames = set()
        pattern = re.compile(translate(_s3_pathname))
        bucket, key = parse_s3_url(top_dir)
        prefix = _become_prefix(key)
        client = get_s3_client()
        with raise_s3_error(_s3_pathname):
            for resp in _list_objects_recursive(client, bucket, prefix):
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

    return create_generator(s3_pathname)


def _s3path_change_bucket(path: str, oldname: str, newname: str) -> str:
    newpath = path.replace(oldname, newname, 1)
    return newpath


def _get_all_buckets_name(
        config: Optional[botocore.config.Config] = None,
        cache_key: Optional[str] = None) -> List[str]:
    client = get_s3_client(config, cache_key)
    bucket_names = []
    client = get_s3_client()
    response = client.list_buckets()
    for bucket in response['Buckets']:
        bucket_names.append(bucket["Name"])
    return bucket_names


def _group_s3path_by_bucket_with_wildcard(globpath: str) -> List[str]:
    glob_dict = defaultdict(list)
    bucket_names = _get_all_buckets_name()
    expanded_globpath = ungloblize(globpath)
    for single_glob in expanded_globpath:
        bucket, _ = parse_s3_url(single_glob)
        glob_dict[bucket].append(single_glob)

    group_glob_list = []

    for bucket, glob_list in glob_dict.items():
        globed_path = globlize(glob_list)
        pattern = re.compile(translate(re.sub(r'\*{2,}', '*', bucket)))

        for bucketname in bucket_names:
            if pattern.fullmatch(bucketname) is not None:
                group_glob_list.append(
                    _s3path_change_bucket(globed_path, bucket, bucketname))
    return group_glob_list


def _group_s3path_by_bucket_without_wildcard(globpath: str) -> List[str]:
    glob_dict = defaultdict(list)

    expanded_globpath = ungloblize(globpath)
    for single_glob in expanded_globpath:
        bucket, _ = parse_s3_url(single_glob)
        glob_dict[bucket].append(single_glob)

    group_glob_list = []

    for bucket, glob_list in glob_dict.items():
        group_glob_list.append(globlize(glob_list))
    return group_glob_list


def _group_s3path_by_bucket(globpath: str) -> List[str]:
    bracket_pattern = re.compile(r'{.*?}')
    raw_bucket, _ = parse_s3_url(globpath)
    bucket_match = False
    for substr in bracket_pattern.split(raw_bucket):
        if has_magic(substr):
            bucket_match = True
            break

    if bucket_match:
        return _group_s3path_by_bucket_with_wildcard(globpath)
    else:
        return _group_s3path_by_bucket_without_wildcard(globpath)


def s3_save_as(file_object: BinaryIO, s3_url: MegfilePathLike) -> None:
    '''Write the opened binary stream to specified path, but the stream won't be closed

    :param file_object: Stream to be read
    :param s3_url: Specified target path
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket:
        raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)
    if not key or key.endswith('/'):
        raise S3IsADirectoryError('Is a directory: %r' % s3_url)

    client = get_s3_client()
    with raise_s3_error(s3_url):
        client.upload_fileobj(file_object, Bucket=bucket, Key=key)


def s3_load_from(s3_url: MegfilePathLike) -> BinaryIO:
    '''Read all content in binary on specified path and write into memory

    User should close the BinaryIO manually

    :param s3_url: Specified path
    :returns: BinaryIO
    '''
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


def _s3_binary_mode(s3_open_func):

    @wraps(s3_open_func)
    def wrapper(s3_url, mode: str = 'rb', **kwargs):
        bucket, key = parse_s3_url(s3_url)
        if not bucket:
            raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)

        if not key or key.endswith('/'):
            raise S3IsADirectoryError('Is a directory: %r' % s3_url)

        if 'x' in mode:
            if s3_isfile(s3_url):
                raise S3FileExistsError('File exists: %r' % s3_url)
            mode = mode.replace('x', 'w')

        if 'w' in mode or 'a' in mode:
            if not s3_hasbucket(s3_url):
                raise S3BucketNotFoundError('No such bucket: %r' % s3_url)

        fileobj = s3_open_func(s3_url, get_binary_mode(mode), **kwargs)
        if 'b' not in mode:
            fileobj = io.TextIOWrapper(fileobj)  # pytype: disable=wrong-arg-types
            fileobj.mode = mode
        return fileobj

    return wrapper


@_s3_binary_mode
def s3_prefetch_open(
        s3_url: MegfilePathLike,
        mode: str = 'rb',
        *,
        max_concurrency: Optional[int] = None,
        max_block_size: int = DEFAULT_BLOCK_SIZE) -> S3PrefetchReader:
    '''Open a asynchronous prefetch reader, to support fast sequential read and random read

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        Some parameter setting may perform well：max_concurrency=10 or 20, max_block_size=8 or 16 MB, default value None means using global thread pool

    :param max_concurrency: Max download thread number, None by default
    :param max_block_size: Max data size downloaded by each thread, in bytes, 8MB by default
    :returns: An opened S3PrefetchReader object
    :raises: S3FileNotFoundError
    '''
    if mode != 'rb':
        raise ValueError('unacceptable mode: %r' % mode)

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
        s3_url: MegfilePathLike,
        mode: str = 'rb',
        *,
        cache_key: str = 'lru',
        max_concurrency: Optional[int] = None,
        max_block_size: int = DEFAULT_BLOCK_SIZE) -> S3ShareCacheReader:
    '''Open a asynchronous prefetch reader, to support fast sequential read and random read

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        Some parameter setting may perform well：max_concurrency=10 or 20, max_block_size=8 or 16 MB, default value None means using global thread pool

    :param max_concurrency: Max download thread number, None by default
    :param max_block_size: Max data size downloaded by each thread, in bytes, 8MB by default
    :returns: An opened S3ShareCacheReader object
    :raises: S3FileNotFoundError
    '''
    if mode != 'rb':
        raise ValueError('unacceptable mode: %r' % mode)

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
        s3_url: MegfilePathLike, mode: str, *,
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
        raise ValueError('unacceptable mode: %r' % mode)

    if mode[0] == 'r' and not s3_isfile(s3_url):
        raise S3FileNotFoundError('No such file: %r' % s3_url)

    bucket, key = parse_s3_url(s3_url)
    config = botocore.config.Config(max_pool_connections=max_pool_connections)
    client = get_s3_client(config=config, cache_key='s3_filelike_client')
    return S3PipeHandler(
        bucket, key, mode, s3_client=client, join_thread=join_thread)


@_s3_binary_mode
def s3_cached_open(
        s3_url: MegfilePathLike, mode: str, *,
        cache_path: str) -> S3CachedHandler:
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
        raise ValueError('unacceptable mode: %r' % mode)

    bucket, key = parse_s3_url(s3_url)
    config = botocore.config.Config(max_pool_connections=max_pool_connections)
    client = get_s3_client(config=config, cache_key='s3_filelike_client')
    return S3CachedHandler(
        bucket, key, mode, s3_client=client, cache_path=cache_path)


@_s3_binary_mode
def s3_buffered_open(
        s3_url: MegfilePathLike,
        mode: str,
        *,
        max_concurrency: Optional[int] = None,
        max_buffer_size: int = DEFAULT_MAX_BUFFER_SIZE,
        forward_ratio: Optional[float] = None,
        block_size: int = DEFAULT_BLOCK_SIZE,
        limited_seekable: bool = False,
        buffered: bool = True,
        share_cache_key: Optional[str] = None
) -> Union[S3PrefetchReader, S3BufferedWriter, io.BufferedReader, io.
           BufferedWriter]:
    '''Open an asynchronous prefetch reader, to support fast sequential read

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

        Some parameter setting may perform well：max_concurrency=10 or 20, max_block_size=8 or 16 MB, default value None means using global thread pool

    :param max_concurrency: Max download thread number, None by default
    :param max_buffer_size: Max cached buffer size in memory, 128MB by default
    :param block_size: Size of single block, 8MB by default. Each block will be uploaded or downloaded by single thread.
    :param limited_seekable: If write-handle supports limited seek (both file head part and tail part can seek block_size). Notes：This parameter are valid only for write-handle. Read-handle support arbitrary seek
    :returns: An opened S3PrefetchReader object
    :raises: S3FileNotFoundError
    '''
    if mode not in ('rb', 'wb'):
        raise ValueError('unacceptable mode: %r' % mode)

    bucket, key = parse_s3_url(s3_url)
    config = botocore.config.Config(max_pool_connections=max_pool_connections)
    client = get_s3_client(config=config, cache_key='s3_filelike_client')
    if mode == 'rb':
        # A rough conversion algorithm to align 2 types of Reader / Writer paremeters
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
def s3_memory_open(s3_url: MegfilePathLike, mode: str) -> BinaryIO:
    '''Open a BytesIO to read/write date to specified path

    :param s3_url: Specified path
    :returns: BinaryIO
    '''
    if mode not in ('rb', 'wb'):
        raise ValueError('unacceptable mode: %r' % mode)

    if mode == 'rb':
        return s3_load_from(s3_url)

    buffer = io.BytesIO()
    close_buffer = buffer.close
    bucket, key = parse_s3_url(s3_url)
    config = botocore.config.Config(max_pool_connections=max_pool_connections)
    client = get_s3_client(config=config, cache_key='s3_filelike_client')

    def close():
        try:
            buffer.seek(0)
            # File-like objects are closed after uploading
            # https://github.com/boto/s3transfer/issues/80
            buffer.close = close_buffer
            client.upload_fileobj(buffer, bucket, key)
        except Exception as error:
            raise translate_s3_error(error, s3_url)
        finally:
            close_buffer()

    buffer.close = close
    return buffer


@_s3_binary_mode
def s3_legacy_open(s3_url: MegfilePathLike, mode: str):
    '''Use smart_open.s3.open open a reader / writer

    .. note ::

        User should make sure that reader / writer are closed correctly

        Supports context manager

    :param mode: Mode to open file, either "rb" or "wb"
    :returns: File-Like Object
    '''
    if mode not in ('rb', 'wb'):
        raise ValueError('unacceptable mode: %r' % mode)

    bucket, key = parse_s3_url(s3_url)

    try:
        return _s3_open(bucket, key, mode)
    except Exception as error:
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


def s3_getmd5(s3_url: MegfilePathLike) -> Optional[str]:
    '''
    Get md5 meta info in files that uploaded/copied via megfile

    If meta info is lost or non-existent, return None

    :param s3_url: Specified path
    :returns: md5 meta info
    '''
    bucket, key = parse_s3_url(s3_url)
    if not bucket:
        raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)
    if not key or key.endswith('/'):
        raise S3IsADirectoryError('Is a directory: %r' % s3_url)

    client = get_s3_client()
    with raise_s3_error(s3_url):
        resp = client.head_object(Bucket=bucket, Key=key)
    # boto3 does not lower the key of metadata
    # https://github.com/boto/botocore/issues/1963
    metadata = dict(
        (key.lower(), value) for key, value in resp['Metadata'].items())
    if MEGFILE_MD5_HEADER in metadata:
        return metadata[MEGFILE_MD5_HEADER]
    return None


def s3_load_content(
        s3_url, start: Optional[int] = None,
        stop: Optional[int] = None) -> bytes:
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

    bucket, key = parse_s3_url(s3_url)
    if not bucket:
        raise S3BucketNotFoundError('Empty bucket name: %r' % s3_url)
    if not key or key.endswith('/'):
        raise S3IsADirectoryError('Is a directory: %r' % s3_url)

    start, stop = get_content_offset(start, stop, s3_getsize(s3_url))
    range_str = 'bytes=%d-%d' % (start, stop - 1)

    client = get_s3_client()
    with raise_s3_error(s3_url):
        return patch_method(
            _get_object,
            max_retries=max_retries,
            should_retry=s3_should_retry,
        )(client, bucket, key, range_str)


def s3_rename(src_url: MegfilePathLike, dst_url: MegfilePathLike) -> None:
    '''
    Move s3 file path from src_url to dst_url

    :param src_url: Given source path
    :param dst_url: Given destination path
    '''
    s3_copy(src_url, dst_url)
    s3_remove(src_url)


def _s3_scan_pairs(src_url: MegfilePathLike, dst_url: MegfilePathLike
                  ) -> Iterator[Tuple[MegfilePathLike, MegfilePathLike]]:
    for src_file_path in s3_scan(src_url):
        content_path = src_file_path[len(src_url):]
        if len(content_path) > 0:
            dst_file_path = s3_path_join(dst_url, content_path)
        else:
            dst_file_path = dst_url
        yield src_file_path, dst_file_path


def s3_move(src_url: MegfilePathLike, dst_url: MegfilePathLike) -> None:
    '''
    Move file/directory path from src_url to dst_url

    :param src_url: Given source path
    :param dst_url: Given destination path
    '''
    for src_file_path, dst_file_path in _s3_scan_pairs(src_url, dst_url):
        s3_rename(src_file_path, dst_file_path)


def s3_sync(src_url: MegfilePathLike, dst_url: MegfilePathLike) -> None:
    '''
    Copy file/directory on src_url to dst_url

    :param src_url: Given source path
    :param dst_url: Given destination path
    '''
    for src_file_path, dst_file_path in _s3_scan_pairs(src_url, dst_url):
        s3_copy(src_file_path, dst_file_path)


class S3Cacher(FileCacher):
    cache_path = None

    def __init__(self, path: str, cache_path: str, mode: str = 'r'):
        if mode not in ('r', 'w', 'a'):
            raise ValueError('unacceptable mode: %r' % mode)
        if mode in ('r', 'a'):
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

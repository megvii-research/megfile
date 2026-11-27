import gc
import os
import sys
from pathlib import Path
from typing import Any, BinaryIO
from unittest.mock import Mock

import pytest

from megfile.errors import S3FileNotFoundError
from megfile.lib.s3_cached_handler import S3CachedHandler
from megfile.s3_path import s3_cached_open
from tests.test_s3 import s3_empty_client  # noqa: F401

BUCKET = "bucket"
KEY = "key"
CONTENT = b"block0\n block1\n block2"
CACHE_PATH = "/tmp/tempfile"
LOCAL_PATH = "/tmp/localfile"


@pytest.fixture
def client(s3_empty_client, fs):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    return s3_empty_client


def test_s3_cached_handler_close(tmp_path: Path, s3_empty_client: Any) -> None:
    """Close and abort must both release the cache on the native filesystem."""
    client = s3_empty_client
    client.create_bucket(Bucket=BUCKET)
    url = f"s3://{BUCKET}/{KEY}"
    cache = tmp_path / "cache"
    with s3_cached_open(url, "wb", cache_path=str(cache)) as writer:
        assert not writer.closed
        writer.write(CONTENT)
    assert writer.closed
    assert not cache.exists()

    with s3_cached_open(url, "wb", cache_path=str(cache)) as writer:
        writer.write(b"discard")
        writer.abort()
    assert not cache.exists()

    with s3_cached_open(url, "rb", cache_path=str(cache)) as reader:
        assert not reader.closed
        assert reader.read() == CONTENT
    assert reader.closed
    assert not cache.exists()


def test_s3_cached_handler_mode(client):
    with pytest.raises(ValueError):
        S3CachedHandler(BUCKET, KEY, "r", s3_client=client, cache_path=CACHE_PATH)


def test_s3_cached_handler_fileno(client):
    writer = S3CachedHandler(BUCKET, KEY, "wb", s3_client=client, cache_path=CACHE_PATH)
    assert isinstance(writer.fileno(), int)


def test_s3_cached_handler_generate_cache_path(client):
    writer = S3CachedHandler(BUCKET, KEY, "wb", s3_client=client, cache_path=None)
    assert isinstance(writer._cache_path, str)


def test_cached_open_failed_download(
    tmp_path: Path, s3_empty_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A partial download during open must never be uploaded on finalization."""
    client = s3_empty_client
    client.create_bucket(Bucket=BUCKET)
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    upload = Mock(wraps=client.upload_fileobj)
    monkeypatch.setattr(client, "upload_fileobj", upload)

    def interrupted_download(bucket: str, key: str, stream: BinaryIO) -> None:
        """Leave partial data in the local cache before reporting a failure."""
        stream.write(b"partial")
        raise S3FileNotFoundError(2, "download interrupted")

    monkeypatch.setattr(client, "download_fileobj", interrupted_download)
    cache = tmp_path / "cache"
    with pytest.raises(S3FileNotFoundError, match="download interrupted") as failure:
        s3_cached_open(f"s3://{BUCKET}/{KEY}", "rb+", cache_path=str(cache))

    assert not cache.exists()
    del failure
    gc.collect()
    assert client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read() == CONTENT
    upload.assert_not_called()


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX unlinks the open cache")
def test_cached_open_unlink_failure(
    tmp_path: Path, s3_empty_client: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An unlink error opening a writer must preserve the existing object."""
    client = s3_empty_client
    client.create_bucket(Bucket=BUCKET)
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with monkeypatch.context() as patch:
        patch.setattr(os, "unlink", Mock(side_effect=PermissionError))
        with pytest.raises(PermissionError):
            s3_cached_open(
                f"s3://{BUCKET}/{KEY}", "wb", cache_path=str(tmp_path / "cache")
            )
    gc.collect()
    assert client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read() == CONTENT


def test_s3_cached_handler_read(client: Any) -> None:
    """Read cached data and remove the cache according to platform semantics."""
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)

    with S3CachedHandler(
        BUCKET, KEY, "rb", s3_client=client, cache_path=CACHE_PATH
    ) as reader:
        assert reader.readline() == b"block0\n"
        assert reader.read() == b" block1\n block2"

        if sys.platform != "win32":
            assert not os.path.exists(CACHE_PATH)
    assert not os.path.exists(CACHE_PATH)


def test_s3_cached_handler_cache_file(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)

    with S3CachedHandler(
        BUCKET,
        KEY,
        "rb",
        s3_client=client,
        cache_path=CACHE_PATH,
        remove_cache_when_open=False,
    ) as reader:
        assert reader.readline() == b"block0\n"
        assert reader.read() == b" block1\n block2"

    with open(CACHE_PATH, "rb") as reader:
        assert reader.read() == CONTENT


def test_s3_cached_handler_write(client):
    with S3CachedHandler(
        BUCKET, KEY, "wb", s3_client=client, cache_path=CACHE_PATH
    ) as writer:
        writer.write(CONTENT)

    content = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert content == CONTENT


def test_s3_cached_handler_append(client):
    with S3CachedHandler(
        BUCKET, KEY, "ab", s3_client=client, cache_path=CACHE_PATH
    ) as writer:
        writer.write(CONTENT)

    with S3CachedHandler(
        BUCKET, KEY, "ab", s3_client=client, cache_path=CACHE_PATH
    ) as writer:
        writer.write(CONTENT)

    content = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert content == CONTENT * 2


def test_s3_cached_handler_atomic_abort(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=b"old")

    with pytest.raises(RuntimeError):
        with S3CachedHandler(
            BUCKET, KEY, "wb", s3_client=client, cache_path=CACHE_PATH, atomic=True
        ) as writer:
            assert writer.atomic is True
            writer.write(b"new")
            raise RuntimeError("abort write")

    body = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert body == b"old"


def test_s3_cached_handler_atomic_commit(client):
    with S3CachedHandler(
        BUCKET, KEY, "wb", s3_client=client, cache_path=CACHE_PATH, atomic=True
    ) as writer:
        writer.write(b"atomic commit")

    body = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert body == b"atomic commit"


def assert_ability(fp1, fp2):
    # TODO: pyfakefs writable 返回值是错的, readable 不可读时会抛异常
    # 正确测试以下几项, 需要关掉 pyfakefs
    assert fp1.seekable() == fp2.seekable()
    # assert fp1.readable() == fp2.readable()
    # assert fp1.writable() == fp2.writable()


def assert_read(fp1, fp2, size):
    assert fp1.read(size) == fp2.read(size)


def assert_seek(fp1, fp2, cookie, whence):
    fp1.seek(cookie, whence)
    fp2.seek(cookie, whence)
    assert fp1.tell() == fp2.tell()


def assert_write(fp1, fp2, buffer):
    def load_content(fp):
        fp.flush()
        if isinstance(fp, S3CachedHandler):
            path = fp._cache_path
        else:
            path = fp.name
        with open(path, "rb") as reader:
            return reader.read()

    fp1.write(buffer)
    fp2.write(buffer)
    assert load_content(fp1) == load_content(fp2)


def test_s3_cached_handler_mode_rb(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "rb") as fp1,
        S3CachedHandler(
            BUCKET, KEY, "rb", s3_client=client, cache_path=CACHE_PATH
        ) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_read(fp1, fp2, 5)
        assert_seek(fp1, fp2, 0, 0)
        assert_read(fp1, fp2, 5)
        assert_seek(fp1, fp2, 0, 1)
        assert_read(fp1, fp2, 5)
        assert_seek(fp1, fp2, 0, 2)
        assert_read(fp1, fp2, 5)

        fp2.seek(0)
        assert fp2.readline() == b"block0\n"
        assert list(fp2.readlines()) == [b" block1\n", b" block2"]

        with pytest.raises(IOError):
            fp2.write(b"")
        with pytest.raises(IOError):
            fp2.writelines([])


def test_s3_cached_handler_mode_wb(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "wb") as fp1,
        S3CachedHandler(
            BUCKET,
            KEY,
            "wb",
            s3_client=client,
            cache_path=CACHE_PATH,
            remove_cache_when_open=False,
        ) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_write(fp1, fp2, CONTENT)

        with pytest.raises(IOError):
            fp2.read()
        with pytest.raises(IOError):
            fp2.readline()
        with pytest.raises(IOError):
            fp2.readlines()


def test_s3_cached_handler_mode_ab(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "ab") as fp1,
        S3CachedHandler(
            BUCKET,
            KEY,
            "ab",
            s3_client=client,
            cache_path=CACHE_PATH,
            remove_cache_when_open=False,
        ) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_write(fp1, fp2, CONTENT)


def test_s3_cached_handler_mode_rbp(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "rb+") as fp1,
        S3CachedHandler(
            BUCKET,
            KEY,
            "rb+",
            s3_client=client,
            cache_path=CACHE_PATH,
            remove_cache_when_open=False,
        ) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_read(fp1, fp2, 5)


def test_s3_cached_handler_mode_rbp(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "wb+") as fp1,
        S3CachedHandler(
            BUCKET,
            KEY,
            "wb+",
            s3_client=client,
            cache_path=CACHE_PATH,
            remove_cache_when_open=False,
        ) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_read(fp1, fp2, 5)


def test_s3_cached_handler_mode_rbp(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "ab+") as fp1,
        S3CachedHandler(
            BUCKET,
            KEY,
            "ab+",
            s3_client=client,
            cache_path=CACHE_PATH,
            remove_cache_when_open=False,
        ) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_read(fp1, fp2, 5)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_read(fp1, fp2, 5)

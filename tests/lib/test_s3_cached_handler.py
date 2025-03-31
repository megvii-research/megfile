import os

import pytest

from megfile.lib.s3_cached_handler import S3CachedHandler
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


def test_s3_cached_handler_close(client):
    writer = S3CachedHandler(BUCKET, KEY, "wb", s3_client=client, cache_path=CACHE_PATH)
    assert writer.closed is False
    writer.close()
    assert writer.closed is True

    reader = S3CachedHandler(BUCKET, KEY, "rb", s3_client=client, cache_path=CACHE_PATH)
    assert reader.closed is False
    reader.close()
    assert reader.closed is True


def test_s3_cached_handler_mode(client):
    with pytest.raises(ValueError):
        S3CachedHandler(BUCKET, KEY, "r", s3_client=client, cache_path=CACHE_PATH)


def test_s3_cached_handler_fileno(client):
    writer = S3CachedHandler(BUCKET, KEY, "wb", s3_client=client, cache_path=CACHE_PATH)
    assert isinstance(writer.fileno(), int)


def test_s3_cached_handler_generate_cache_path(client):
    writer = S3CachedHandler(BUCKET, KEY, "wb", s3_client=client, cache_path=None)
    assert isinstance(writer._cache_path, str)


def test_s3_cached_handler_read(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)

    with S3CachedHandler(
        BUCKET, KEY, "rb", s3_client=client, cache_path=CACHE_PATH
    ) as reader:
        assert reader.readline() == b"block0\n"
        assert reader.read() == b" block1\n block2"

        # 文件打开后就被删除
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

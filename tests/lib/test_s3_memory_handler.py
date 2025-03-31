import pytest

from megfile.errors import S3ConfigError
from megfile.lib.s3_memory_handler import S3MemoryHandler
from tests.test_s3 import s3_empty_client  # noqa: F401

BUCKET = "bucket"
KEY = "key"
CONTENT = b"block0\n block1\n block2"
LOCAL_PATH = "/tmp/localfile"


@pytest.fixture
def client(s3_empty_client, fs):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    return s3_empty_client


def test_s3_memory_handler_close(client):
    writer = S3MemoryHandler(BUCKET, KEY, "wb", s3_client=client)
    assert writer.closed is False
    writer.close()
    assert writer.closed is True

    reader = S3MemoryHandler(BUCKET, KEY, "rb", s3_client=client)
    assert reader.closed is False
    reader.close()
    assert reader.closed is True


def test_s3_memory_handler_mode(client):
    with pytest.raises(ValueError):
        S3MemoryHandler(BUCKET, KEY, "w", s3_client=client)


def test_s3_memory_handler_read(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)

    with S3MemoryHandler(BUCKET, KEY, "rb", s3_client=client) as reader:
        assert reader.readline() == b"block0\n"
        assert reader.read() == b" block1\n block2"


def test_s3_memory_handler_write(client):
    with S3MemoryHandler(BUCKET, KEY, "wb", s3_client=client) as writer:
        writer.write(CONTENT)

    content = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert content == CONTENT


def test_s3_memory_handler_append(client):
    with S3MemoryHandler(BUCKET, KEY, "ab", s3_client=client) as writer:
        writer.write(CONTENT)

    with S3MemoryHandler(BUCKET, KEY, "ab", s3_client=client) as writer:
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
        if isinstance(fp, S3MemoryHandler):
            return fp._fileobj.getvalue()
        with open(fp.name, "rb") as reader:
            return reader.read()

    fp1.write(buffer)
    fp2.write(buffer)
    assert load_content(fp1) == load_content(fp2)


def assert_write_lines(fp1, fp2, buffer):
    def load_content(fp):
        fp.flush()
        if isinstance(fp, S3MemoryHandler):
            return fp._fileobj.getvalue()
        with open(fp.name, "rb") as reader:
            return reader.read()

    fp1.writelines([buffer] * 2)
    fp2.writelines([buffer] * 2)
    assert load_content(fp1) == load_content(fp2)


def test_s3_memory_handler_mode_rb(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "rb") as fp1,
        S3MemoryHandler(BUCKET, KEY, "rb", s3_client=client) as fp2,
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


def test_s3_memory_handler_mode_wb(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "wb") as fp1,
        S3MemoryHandler(BUCKET, KEY, "wb", s3_client=client) as fp2,
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


def test_s3_memory_handler_mode_ab(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "ab") as fp1,
        S3MemoryHandler(BUCKET, KEY, "ab", s3_client=client) as fp2,
    ):
        assert_ability(fp1, fp2)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 0)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 1)
        assert_write(fp1, fp2, CONTENT)
        assert_seek(fp1, fp2, 0, 2)
        assert_write(fp1, fp2, CONTENT)
        assert_write_lines(fp1, fp2, CONTENT)


def test_s3_memory_handler_mode_rbp(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "rb+") as fp1,
        S3MemoryHandler(BUCKET, KEY, "rb+", s3_client=client) as fp2,
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


def test_s3_memory_handler_mode_rbp(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "wb+") as fp1,
        S3MemoryHandler(BUCKET, KEY, "wb+", s3_client=client) as fp2,
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


def test_s3_memory_handler_mode_rbp(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with (
        open(LOCAL_PATH, "ab+") as fp1,
        S3MemoryHandler(BUCKET, KEY, "ab+", s3_client=client) as fp2,
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


@pytest.fixture
def error_client(s3_empty_client, fs):
    s3_empty_client.create_bucket(Bucket=BUCKET)

    def fake_head_object(*args, **kwargs):
        raise S3ConfigError()

    s3_empty_client.head_object = fake_head_object
    return s3_empty_client


def test_s3_memory_handler_error(error_client):
    error_client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    with open(LOCAL_PATH, "wb") as writer:
        writer.write(CONTENT)

    with pytest.raises(S3ConfigError):
        with S3MemoryHandler(BUCKET, KEY, "ab", s3_client=error_client):
            pass

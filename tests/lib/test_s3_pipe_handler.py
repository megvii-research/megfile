import os
import sys
from subprocess import check_call

import pytest

from megfile.errors import S3Exception
from megfile.lib.s3_pipe_handler import S3PipeHandler
from tests.test_s3 import s3_empty_client  # noqa: F401

BUCKET = "bucket"
KEY = "key"
CONTENT = b"block0\n block1\n block2"


@pytest.fixture
def client(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    return s3_empty_client


def test_s3_pipe_handler_close(client):
    writer = S3PipeHandler(BUCKET, KEY, "wb", s3_client=client)
    assert writer.closed is False
    writer.close()
    assert writer.closed is True

    reader = S3PipeHandler(BUCKET, KEY, "rb", s3_client=client)
    assert reader.closed is False
    reader.close()
    assert reader.closed is True


def test_s3_pipe_handler_read(client):
    client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)

    with S3PipeHandler(BUCKET, KEY, "rb", s3_client=client) as reader:
        assert reader.name == "s3://bucket/key"
        assert reader.mode == "rb"
        assert reader.readable() is True
        assert reader.writable() is False
        assert reader.tell() == 0
        assert reader.readline() == b"block0\n"
        assert reader.tell() == 7
        assert reader.read() == b" block1\n block2"
        assert reader.tell() == 22
        assert reader.read() == b""

    with pytest.raises(ValueError):
        S3PipeHandler(BUCKET, KEY, "ab", s3_client=client)


def test_s3_pipe_handler_write(client):
    with S3PipeHandler(BUCKET, KEY, "wb", s3_client=client) as writer:
        assert writer.name == "s3://bucket/key"
        assert writer.mode == "wb"
        assert writer.readable() is False
        assert writer.writable() is True
        writer.write(CONTENT)
        assert writer.tell() == 22
        writer.flush()

    content = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert content == CONTENT


def assert_no_timeout(filename, timeout=60):
    # TODO: 改为使用 multiprocessing.Process 方式实现
    # Process 与普通 Python 进程不同的是, 退出时不会进 atexit 注册的 callback,
    # 导致目前已知的死锁问题并不会发生, 因此这里使用了 subprocess 的实现作为 workaround
    path = os.path.join("tests", "timeout", filename)
    check_call([sys.executable, path], env={"PYTHONPATH": "."}, timeout=timeout)


@pytest.mark.skip()
def test_s3_pipe_handler_read_without_close():
    assert_no_timeout("s3_pipe_handler_read_without_close.py")


@pytest.mark.skip()
def test_s3_pipe_handler_write_without_close():
    assert_no_timeout("s3_pipe_handler_write_without_close.py")


@pytest.fixture
def error_client(s3_empty_client):
    def fake(*args, **kwargs):
        raise S3Exception()

    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.download_fileobj = fake
    s3_empty_client.upload_fileobj = fake

    return s3_empty_client


def test_s3_pipe_handler_error(error_client):
    with pytest.raises(S3Exception):
        with S3PipeHandler(BUCKET, KEY, "wb", s3_client=error_client) as writer:
            writer._async_task.join()
            writer._raise_exception()

    with pytest.raises(S3Exception):
        with S3PipeHandler(BUCKET, KEY, "rb", s3_client=error_client) as reader:
            reader._async_task.join()
            reader._raise_exception()

from concurrent.futures import wait
from io import UnsupportedOperation
from threading import Event

import moto
import moto.s3
import pytest

from megfile.lib.s3_buffered_writer import S3BufferedWriter
from tests.test_s3 import s3_empty_client  # noqa: F401

BUCKET = "bucket"
KEY = "key"

CONTENT = b"block0\n block1\n block2"

moto.s3.models.UPLOAD_PART_MIN_SIZE = 5


@pytest.fixture
def client(s3_empty_client, fs):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    return s3_empty_client


def test_s3_buffered_writer_close(client):
    writer = S3BufferedWriter(BUCKET, KEY, s3_client=client)
    assert writer.closed is False
    writer.close()
    assert writer.closed is True

    with pytest.raises(IOError):
        writer.write(b"")


def test_s3_buffered_writer_write(client):
    with S3BufferedWriter(BUCKET, KEY, s3_client=client) as writer:
        writer.write(CONTENT)
        writer.write(b"\n")
        writer.write(CONTENT)

        with pytest.raises(UnsupportedOperation):
            writer.fileno()

    content = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert content == CONTENT + b"\n" + CONTENT


def test_s3_buffered_writer_write_max_worker(client, mocker):
    UNIT = 2**20
    content_size = 16 * 2**20
    content = b"a" * content_size
    with S3BufferedWriter(
        BUCKET,
        KEY,
        s3_client=client,
        max_workers=2,
        block_size=8 * UNIT,
    ) as writer:
        writer.write(content)
        writer.write(b"\n")
        writer.write(content)

    read_content = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert read_content == content + b"\n" + content


def test_s3_buffered_writer_write_put(client, mocker):
    put_object_func = mocker.spy(client, "put_object")

    with S3BufferedWriter(BUCKET, KEY, s3_client=client) as writer:
        writer.write(CONTENT)

    assert not writer._is_multipart
    put_object_func.assert_called_once_with(Bucket=BUCKET, Key=KEY, Body=CONTENT)

    content = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert content == CONTENT


def test_s3_buffered_writer_write_large_bytes(client):
    with S3BufferedWriter(BUCKET, KEY, s3_client=client) as writer:
        writer.write(CONTENT * 10)

    content = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert content == CONTENT * 10


def test_s3_buffered_writer_write_multipart(client, mocker):
    block_size = 10 * 2**20
    content_size = 16 * 2**20
    content = b"a" * content_size

    put_object_func = mocker.spy(client, "put_object")
    create_multipart_upload_func = mocker.spy(client, "create_multipart_upload")
    upload_part_func = mocker.spy(client, "upload_part")
    complete_multipart_upload_func = mocker.spy(client, "complete_multipart_upload")

    with S3BufferedWriter(
        BUCKET, KEY, s3_client=client, block_size=block_size, max_workers=1
    ) as writer:
        writer.write(content)
        writer.write(b"\n")
        writer.write(content)

    assert writer._is_multipart
    # put_object_func.assert_not_called() in Python 3.6+
    assert put_object_func.call_count == 0
    create_multipart_upload_func.assert_called_once_with(Bucket=BUCKET, Key=KEY)
    upload_part_func.assert_any_call(
        Bucket=BUCKET, Key=KEY, Body=content, PartNumber=1, UploadId=writer._upload_id
    )
    upload_part_func.assert_any_call(
        Bucket=BUCKET,
        Key=KEY,
        Body=b"\n" + content,
        PartNumber=2,
        UploadId=writer._upload_id,
    )
    assert upload_part_func.call_count == 2

    complete_multipart_upload_func.assert_called_once_with(
        Bucket=BUCKET,
        Key=KEY,
        UploadId=writer._upload_id,
        MultipartUpload=writer._multipart_upload,
    )

    content_read = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert content_read == content + b"\n" + content


def test_s3_buffered_writer_write_multipart_pending(client, mocker):
    upload_part_event = Event()
    upload_part_func = client.upload_part

    writer = None

    def fake_upload_part(**kwargs):
        upload_part_event.wait()
        upload_part_event.clear()
        return upload_part_func(**kwargs)

    def fake_wait(futures, **kwargs):
        if writer._buffer_size_before_wait is None:
            writer._buffer_size_before_wait = writer._total_buffer_size
        upload_part_event.set()
        return wait(futures, **kwargs)

    mocker.patch.object(client, "upload_part", side_effect=fake_upload_part)
    mocker.patch("megfile.lib.s3_buffered_writer.wait", side_effect=fake_wait)

    writer = S3BufferedWriter(
        BUCKET, KEY, s3_client=client, block_size=5, max_buffer_size=10
    )
    writer._buffer_size_before_wait = None

    writer.write(CONTENT)
    assert writer._buffer_size_before_wait == 22
    writer._buffer_size_before_wait = None
    assert writer._total_buffer_size == 0

    writer.write(b"\n")
    assert writer._buffer_size_before_wait is None
    assert writer._total_buffer_size == 0

    writer.write(CONTENT)
    assert writer._buffer_size_before_wait == 23
    writer._buffer_size_before_wait = None
    assert writer._total_buffer_size == 0

    assert writer._is_multipart


def test_s3_buffered_writer_write_multipart_autoscale(client, mocker):
    block_size = 8 * 2**20
    content_size = 8 * 2**20
    content_repeat = 20
    content = b"a" * content_size

    put_object_func = mocker.spy(client, "put_object")
    create_multipart_upload_func = mocker.spy(client, "create_multipart_upload")
    upload_part_func = mocker.spy(client, "upload_part")
    complete_multipart_upload_func = mocker.spy(client, "complete_multipart_upload")

    with S3BufferedWriter(
        BUCKET, KEY, s3_client=client, block_size=block_size, max_workers=1
    ) as writer:
        for _ in range(content_repeat):
            writer.write(content)
            writer.write(b"\n")

    assert writer._is_multipart
    assert put_object_func.call_count == 0
    create_multipart_upload_func.assert_called_once_with(Bucket=BUCKET, Key=KEY)

    assert upload_part_func.call_count == 16

    complete_multipart_upload_func.assert_called_once_with(
        Bucket=BUCKET,
        Key=KEY,
        UploadId=writer._upload_id,
        MultipartUpload=writer._multipart_upload,
    )

    content_read = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert content_read == (content + b"\n") * content_repeat


def test_s3_buffered_writer_autoscale_block_size(client, mocker):
    with S3BufferedWriter(
        BUCKET,
        KEY,
        s3_client=client,
        block_size=1,
        max_buffer_size=12,
    ) as writer:
        writer._block_autoscale = True

        writer._part_number = 999
        assert writer._block_size == 4

        writer._part_number = 9999
        assert writer._block_size == 8

        writer._part_number = 10000
        assert writer._block_size == 12

        writer._block_autoscale = False
        assert writer._block_size == 1

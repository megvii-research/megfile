import random
from string import ascii_letters

import moto
import moto.s3
import pytest

from megfile.lib.s3_limited_seekable_writer import S3LimitedSeekableWriter
from tests.test_s3 import s3_empty_client  # noqa: F401

BUCKET = "bucket"
KEY = "key"

CONTENT = b"block0\n block1\n block2"

moto.s3.models.UPLOAD_PART_MIN_SIZE = 4


@pytest.fixture
def client(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    return s3_empty_client


def test_seekable(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client)
    assert writer.seekable() is True


def test_commit_on_exit(client):
    with S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client):
        pass

    body = client.get_object(Bucket=BUCKET, Key=KEY)["Body"]
    assert body.read() == b""


def test_write(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client, block_size=4)

    writer.write(b"abc")
    assert writer._head_size == 3
    assert writer._part_number == 0
    assert writer.tell() == 3
    writer.write(b"de")
    assert writer._head_size == 4
    assert writer._part_number == 1
    assert writer._buffer.getvalue() == b"e"
    assert writer.tell() == 5
    writer.write(b"fghij")
    assert writer._part_number == 1
    assert writer._buffer.getvalue() == b"efghij"
    assert writer._multipart_upload["Parts"] == []
    assert writer.tell() == 10
    writer.write(b"klmno")
    assert writer._part_number == 2
    assert writer._buffer.getvalue() == b"lmno"
    assert writer._multipart_upload["Parts"] == [
        {"ETag": '"0f6430efebf97198427b56542a35c88d"', "PartNumber": 2}
    ]
    assert writer.tell() == 15
    writer.write(b"pqrst")
    assert writer._part_number == 3
    assert writer._buffer.getvalue() == b"qrst"
    assert writer._multipart_upload["Parts"] == [
        {"ETag": '"0f6430efebf97198427b56542a35c88d"', "PartNumber": 2},
        {"ETag": '"816cabd1c0334ed363555889d9f4dbe4"', "PartNumber": 3},
    ]
    assert writer.tell() == 20

    # seek to head
    writer.seek(1)
    writer.write(b"BCD")
    assert writer._head_buffer.getvalue() == b"aBCD"
    assert writer.tell() == 4

    # seek to tail
    writer.seek(-4, 2)
    writer.write(b"QR")
    assert writer._buffer.getvalue() == b"QRst"
    assert writer.tell() == 18

    writer.seek(2)
    writer.write(b"ab")
    with pytest.raises(OSError):
        writer.write(b"cd")

    with pytest.raises(OSError):
        writer.seek(5)

    with pytest.raises(OSError):
        writer.seek(5, 3)


def test_s3_buffered_writer_write_large_bytes(client):
    with S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client) as writer:
        writer.write(CONTENT * 10)

    with pytest.raises(IOError):
        writer.seek(0)

    with pytest.raises(IOError):
        writer.write(CONTENT)

    content = client.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    assert content == CONTENT * 10


def test_write_block_size(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client, block_size=4)

    writer.write(b"abcd")
    assert writer.tell() == 4
    writer.write(b"efgh")
    assert writer.tell() == 8
    writer.write(b"ijkl")
    assert writer.tell() == 12
    writer.seek(0)
    writer.write(b"ABCD")
    assert writer.tell() == 4


def test_write_three_blocks(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client)

    writer.write(b"a" * (writer._head_block_size + 1))  # head block + 1
    assert writer._head_size == writer._head_block_size
    assert writer._buffer.getvalue() == b"a"
    writer.write(b"b" * writer._block_size)
    assert writer._part_number == 1
    assert writer._buffer.getvalue() == b"a" + b"b" * writer._block_size
    assert writer._multipart_upload["Parts"] == []
    writer.write(b"c" * writer._block_size)
    assert writer._part_number == 2
    assert writer._buffer.getvalue() == b"c" * writer._block_size
    assert writer._multipart_upload["Parts"] == [
        {"ETag": '"45250eb3de2d486efb93867e061965b3"', "PartNumber": 2}
    ]
    writer.seek(1)
    writer.write(b"A")
    assert writer._head_buffer.getvalue() == b"aA" + b"a" * (
        writer._head_block_size - 2
    )
    writer.seek(0, 2)
    writer.write(b"d")
    writer.close()

    body = client.get_object(Bucket=BUCKET, Key=KEY)["Body"]
    assert (
        body.read()
        == b"aA"
        + b"a" * (writer._head_block_size - 1)
        + b"b" * writer._block_size
        + b"c" * writer._block_size
        + b"d"
    )


def test_write_two_blocks(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client)

    writer.write(b"a" * (writer._head_block_size + 1))  # head block + 1
    assert writer._head_size == writer._head_block_size
    assert writer._buffer.getvalue() == b"a"
    assert writer._part_number == 1
    writer.seek(1)
    writer.write(b"A")
    assert writer._head_buffer.getvalue() == b"aA" + b"a" * (
        writer._head_block_size - 2
    )
    writer.seek(0, 2)
    writer.write(b"c")
    writer.close()

    body = client.get_object(Bucket=BUCKET, Key=KEY)["Body"]
    assert body.read() == b"aA" + b"a" * (writer._head_block_size - 1) + b"c"


def test_write_one_block(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client)

    writer.write(b"a")
    writer.close()

    body = client.get_object(Bucket=BUCKET, Key=KEY)["Body"]
    assert body.read() == b"a"


def test_write_multi_seek(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client, block_size=4)

    writer.write(b"abcde")
    writer.write(b"fghij")
    writer.seek(1)
    writer.write(b"BCD")
    writer.seek(-4, 1)
    writer.write(b"A")
    writer.seek(0, 2)
    writer.write(b"kl")
    writer.seek(2)
    writer.write(b"c")
    writer.seek(0, 2)
    writer.write(b"m")
    assert writer._head_buffer.getvalue() == b"ABcD"
    assert writer._buffer.getvalue() == b"ijklm"


def test_write_multi_seek_head_only(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client)

    writer.write(b"abc")
    writer.seek(-1, 1)
    writer.write(b"C")
    writer.seek(0)
    writer.write(b"A")
    assert writer._head_buffer.getvalue() == b"AbC"

    writer.seek(0, 2)
    writer.write(b"D")
    assert writer._head_buffer.getvalue() == b"AbCD"

    writer.seek(-2, 1)
    writer.write(b"cde")

    assert writer._head_buffer.getvalue() == b"Abcde"
    writer.close()

    body = client.get_object(Bucket=BUCKET, Key=KEY)["Body"]
    assert body.read() == b"Abcde"


def test_write_multi_seek_tail_only(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client, block_size=4)

    writer.write(b"abcdefghijklmno")
    assert writer._head_buffer.getvalue() == b"abcd"
    assert writer._buffer.getvalue() == b"lmno"

    writer.seek(-1, 1)
    writer.write(b"O")
    writer.seek(12)
    writer.write(b"M")
    assert writer._buffer.getvalue() == b"lMnO"

    writer.seek(0, 2)
    writer.write(b"P")
    assert writer._buffer.getvalue() == b"lMnOP"

    writer.seek(-2, 1)
    writer.write(b"opq")

    assert writer._buffer.getvalue() == b"lMnopq"


def test_seek_out_of_head_nor_tail(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client, block_size=4)

    writer.write(b"abcde")
    writer.write(b"fghij")
    writer.write(b"klmno")
    with pytest.raises(Exception) as error:
        writer.seek(-6, 2)
    assert "9" in str(error.value)
    with pytest.raises(Exception) as error:
        writer.seek(9)
    assert "9" in str(error.value)


def test_write_head_part_overflow(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client, block_size=4)

    writer.write(b"abcde")
    writer.seek(1)
    with pytest.raises(Exception) as error:
        writer.write(b"BCDE")
    assert "3" in str(error.value)  # writer._head_block_size - 1
    assert "4" in str(error.value)  # len('BCDE')


def test_write_random(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client)
    data = (random.choice(ascii_letters) * random.randint(1, 100 * 1024 * 1024)).encode(
        "ascii"
    )

    writer.write(data)
    writer.close()

    body = client.get_object(Bucket=BUCKET, Key=KEY)["Body"]
    assert body.read() == data


def test_write_for_autoscaling_block(client):
    writer = S3LimitedSeekableWriter(BUCKET, KEY, s3_client=client)
    data = (random.choice(ascii_letters) * 97 * 1024 * 1024).encode("ascii")

    writer.write(data)
    writer.close()

    body = client.get_object(Bucket=BUCKET, Key=KEY)["Body"]
    assert body.read() == data

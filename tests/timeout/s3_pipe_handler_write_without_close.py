# This file is for `tests/lib/test_s3_pipe_handler.py`

# To test if it will cause deadlock that
# exiting Python process without calling close on S3PipeHandler

from moto import mock_aws

from megfile.lib.s3_pipe_handler import S3PipeHandler
from tests.s3_utils import make_moto_s3_client

BUCKET = "bucket"
KEY = "key"
CONTENT = b" "

with mock_aws():
    client = make_moto_s3_client()
    client.create_bucket(Bucket=BUCKET)
    writer1 = S3PipeHandler(BUCKET, KEY, "wb", s3_client=client)
    writer2 = S3PipeHandler(BUCKET, KEY, "wb", s3_client=client)
    writer1.write(CONTENT)

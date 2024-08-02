# This file is for `tests/lib/test_s3_pipe_handler.py`

# To test if it will cause deadlock that
# exiting Python process without calling close on S3PipeHandler

import boto3
from moto import mock_aws

from megfile.lib.s3_pipe_handler import S3PipeHandler

BUCKET = "bucket"
KEY = "key"
CONTENT = b" "

with mock_aws():
    client = boto3.client("s3")
    client.create_bucket(Bucket=BUCKET)
    writer1 = S3PipeHandler(BUCKET, KEY, "wb", s3_client=client)
    writer2 = S3PipeHandler(BUCKET, KEY, "wb", s3_client=client)
    writer1.write(CONTENT)

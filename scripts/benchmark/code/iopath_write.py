import os
import time

import boto3
from iopath.common.file_io import PathManager
from iopath.common.s3 import S3PathHandler

times = 10240
s3_path = "s3://bucketA/large.txt"
block = b"1" * 1024 * 1024

start = time.time()

path_manager = PathManager()

session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)
client = session.client("s3", endpoint_url=os.environ["OSS_ENDPOINT"])
handler = S3PathHandler()
handler.client = client

path_manager.register_handler(handler)

with path_manager.open(s3_path, "wb") as f:
    for i in range(times):
        f.write(block)

print(time.time() - start)  # write 10GB 91.642

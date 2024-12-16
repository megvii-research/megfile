import os
import time

import boto3
from smart_open import open

times = 10240
s3_path = "s3://bucketA/large.txt"

start = time.time()
session = boto3.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)
with open(
    s3_path,
    "rb",
    transport_params={
        "client": session.client("s3", endpoint_url=os.environ["OSS_ENDPOINT"])
    },
) as f:
    for i in range(times):
        f.read(1024 * 1024)

print(time.time() - start)

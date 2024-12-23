import os
import time

import s3fs

times = 10240
block = b"1" * 1024 * 1024
s3_path = "bucketA/large.txt"

start = time.time()

s3 = s3fs.S3FileSystem(
    endpoint_url=os.environ["OSS_ENDPOINT"],
    key=os.environ["AWS_ACCESS_KEY_ID"],
    secret=os.environ["AWS_SECRET_ACCESS_KEY"],
)

with s3.open(s3_path, "wb") as f:
    for i in range(times):
        f.write(block)

print(time.time() - start)

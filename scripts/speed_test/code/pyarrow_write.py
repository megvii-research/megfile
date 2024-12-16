import os
import time

from pyarrow import fs

times = 10240
block = b"1" * 1024 * 1024
s3_path = "bucketA/large.txt"

start = time.time()

s3 = fs.S3FileSystem(endpoint_override=os.environ["OSS_ENDPOINT"])

with s3.open_output_stream(s3_path) as f:
    for i in range(times):
        f.write(block)

print(time.time() - start)

import os
import time

from pyarrow import fs

times = 10240
s3_path = "bucketA/large.txt"

start = time.time()

s3 = fs.S3FileSystem(endpoint_override=os.environ["OSS_ENDPOINT"])

with s3.open_input_stream(s3_path) as f:
    for i in range(times):
        f.read(1024 * 1024)

print(time.time() - start)

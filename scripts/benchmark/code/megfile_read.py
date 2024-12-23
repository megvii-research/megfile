import time

from megfile import smart_open

times = 10240
s3_path = "s3://bucketA/large.txt"

start = time.time()
with smart_open(s3_path, "rb") as f:
    for i in range(times):
        f.read(1024 * 1024 * 1)

print(time.time() - start)

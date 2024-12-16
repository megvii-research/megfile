import time

from megfile import smart_open

times = 10240
s3_path = "s3://bucketA/large.txt"
block = b"1" * 1024 * 1024

start = time.time()
with smart_open(s3_path, "wb") as f:
    for i in range(times):
        f.write(block)

print(time.time() - start)

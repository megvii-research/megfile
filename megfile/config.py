import os

READER_BLOCK_SIZE = int(os.getenv("MEGFILE_READER_BLOCK_SIZE") or 8 * 2**20)
if READER_BLOCK_SIZE <= 0:
    raise ValueError(
        f"'MEGFILE_READER_BLOCK_SIZE' must bigger than 0, got {READER_BLOCK_SIZE}"
    )
READER_MAX_BUFFER_SIZE = int(os.getenv("MEGFILE_READER_MAX_BUFFER_SIZE") or 128 * 2**20)

# Multi-upload has a maximum of 10,000 parts,
# so the maximum supported file size is MEGFILE_WRITE_BLOCK_SIZE * 10,000 MB,
# the largest object that can be uploaded in a single PUT is 5 TB in aws s3.
WRITER_BLOCK_SIZE = int(os.getenv("MEGFILE_WRITER_BLOCK_SIZE") or 32 * 2**20)
if WRITER_BLOCK_SIZE <= 0:
    raise ValueError(
        f"'MEGFILE_WRITER_BLOCK_SIZE' must bigger than 0, got {WRITER_BLOCK_SIZE}"
    )
# Multi-upload part size must be between 5 MiB and 5 GiB.
# There is no minimum size limit on the last part of your multipart upload.
WRITER_MIN_BLOCK_SIZE = 8 * 2**20
WRITER_MAX_BUFFER_SIZE = int(os.getenv("MEGFILE_WRITER_MAX_BUFFER_SIZE") or 128 * 2**20)

GLOBAL_MAX_WORKERS = int(os.getenv("MEGFILE_MAX_WORKERS") or 8)

# for logging the size of file had read or wrote
BACKOFF_INITIAL = 64 * 2**20  # 64MB
BACKOFF_FACTOR = 4

NEWLINE = ord("\n")

S3_CLIENT_CACHE_MODE = os.getenv("MEGFILE_S3_CLIENT_CACHE_MODE") or "thread_local"

DEFAULT_MAX_RETRY_TIMES = int(os.getenv("MEGFILE_MAX_RETRY_TIMES") or 10)
S3_MAX_RETRY_TIMES = int(
    os.getenv("MEGFILE_S3_MAX_RETRY_TIMES") or DEFAULT_MAX_RETRY_TIMES
)
HTTP_MAX_RETRY_TIMES = int(
    os.getenv("MEGFILE_HTTP_MAX_RETRY_TIMES") or DEFAULT_MAX_RETRY_TIMES
)
HDFS_MAX_RETRY_TIMES = int(
    os.getenv("MEGFILE_HDFS_MAX_RETRY_TIMES") or DEFAULT_MAX_RETRY_TIMES
)
SFTP_MAX_RETRY_TIMES = int(
    os.getenv("MEGFILE_SFTP_MAX_RETRY_TIMES") or DEFAULT_MAX_RETRY_TIMES
)

SFTP_HOST_KEY_POLICY = os.getenv("MEGFILE_SFTP_HOST_KEY_POLICY")

HTTP_AUTH_HEADERS = ("Authorization", "Www-Authenticate", "Cookie", "Cookie2")

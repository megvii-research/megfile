import os
from logging import getLogger

_logger = getLogger(__name__)

DEFAULT_BLOCK_SIZE = int(os.getenv("MEGFILE_BLOCK_SIZE") or 8 * 2**20)

if os.getenv("MEGFILE_MAX_BUFFER_SIZE"):
    DEFAULT_MAX_BUFFER_SIZE = int(os.environ["MEGFILE_MAX_BUFFER_SIZE"])
    if DEFAULT_MAX_BUFFER_SIZE < DEFAULT_BLOCK_SIZE:
        DEFAULT_MAX_BUFFER_SIZE = DEFAULT_BLOCK_SIZE
        _logger.warning(
            "Env 'MEGFILE_MAX_BUFFER_SIZE' is smaller than block size, "
            "will not use buffer."
        )
    DEFAULT_BLOCK_CAPACITY = DEFAULT_MAX_BUFFER_SIZE // DEFAULT_BLOCK_SIZE
    if os.getenv("MEGFILE_BLOCK_CAPACITY"):
        _logger.warning(
            "Env 'MEGFILE_MAX_BUFFER_SIZE' and 'MEGFILE_BLOCK_CAPACITY' are both set, "
            "'MEGFILE_BLOCK_CAPACITY' will be ignored."
        )
elif os.getenv("MEGFILE_BLOCK_CAPACITY"):
    DEFAULT_BLOCK_CAPACITY = int(os.environ["MEGFILE_BLOCK_CAPACITY"])
    DEFAULT_MAX_BUFFER_SIZE = DEFAULT_BLOCK_SIZE * DEFAULT_BLOCK_CAPACITY
else:
    DEFAULT_MAX_BUFFER_SIZE = 128 * 2**20
    DEFAULT_BLOCK_CAPACITY = 16

DEFAULT_MIN_BLOCK_SIZE = int(os.getenv("MEGFILE_MIN_BLOCK_SIZE") or DEFAULT_BLOCK_SIZE)

if os.getenv("MEGFILE_MAX_BLOCK_SIZE"):
    DEFAULT_MAX_BLOCK_SIZE = int(os.environ["MEGFILE_MAX_BLOCK_SIZE"])
    if DEFAULT_MAX_BLOCK_SIZE < DEFAULT_BLOCK_SIZE:
        DEFAULT_MAX_BLOCK_SIZE = DEFAULT_BLOCK_SIZE
        _logger.warning(
            "Env 'MEGFILE_MAX_BLOCK_SIZE' is smaller than block size, will be ignored."
        )
else:
    DEFAULT_MAX_BLOCK_SIZE = max(128 * 2**20, DEFAULT_BLOCK_SIZE)

GLOBAL_MAX_WORKERS = int(os.getenv("MEGFILE_MAX_WORKERS") or 32)
DEFAULT_MAX_RETRY_TIMES = int(os.getenv("MEGFILE_MAX_RETRY_TIMES") or 10)

# for logging the size of file had read or wrote
BACKOFF_INITIAL = 64 * 2**20  # 64MB
BACKOFF_FACTOR = 4

NEWLINE = ord("\n")

S3_CLIENT_CACHE_MODE = os.getenv("MEGFILE_S3_CLIENT_CACHE_MODE") or "thread_local"
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

HTTP_AUTH_HEADERS = ("Authorization", "Www-Authenticate", "Cookie", "Cookie2")

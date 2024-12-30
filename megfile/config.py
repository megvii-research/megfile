import logging
import os
import typing as T


def parse_quantity(quantity: T.Union[str, int]) -> int:
    """
    Parse kubernetes canonical form quantity like 200Mi to a int number.
    Supported SI suffixes:
    base1024: Ki | Mi | Gi | Ti | Pi | Ei
    base1000: "" | k | M | G | T | P | E

    (International System of units; See: http://physics.nist.gov/cuu/Units/binary.html)

    Input:
    quantity: string. kubernetes canonical form quantity

    Returns:
    Int

    Raises:
    ValueError on invalid or unknown input
    """
    if isinstance(quantity, int):
        return quantity

    exponents = {"K": 1, "k": 1, "M": 2, "G": 3, "T": 4, "P": 5, "E": 6}

    number = quantity
    suffix = None
    if len(quantity) >= 2 and quantity[-1] == "i":
        if quantity[-2] in exponents:
            number = quantity[:-2]
            suffix = quantity[-2:]
    elif len(quantity) >= 1 and quantity[-1] in exponents:
        number = quantity[:-1]
        suffix = quantity[-1:]

    try:
        number = int(number)
    except ValueError:
        raise ValueError("Invalid number format: {}".format(number))

    if suffix is None:
        return number

    if suffix.endswith("i"):
        base = 1024
    else:
        base = 1000

    # handle SI inconsistency
    if suffix == "ki":
        raise ValueError("{} has unknown suffix".format(quantity))

    exponent = int(exponents[suffix[0]])
    return number * (base**exponent)  # pytype: disable=bad-return-type


def parse_boolean(value: T.Optional[str], default: bool = False):
    if value is None:
        return default
    return value.lower() in ("true", "yes", "1")


def set_log_level(level: T.Optional[T.Union[int, str]] = None):
    logging.basicConfig(
        level=logging.ERROR,
        format=(
            "%(asctime)s | %(levelname)-8s | "
            "%(name)s:%(funcName)s:%(lineno)d - %(message)s"
        ),
    )
    level = level or os.getenv("MEGFILE_LOG_LEVEL") or logging.INFO
    logging.getLogger("megfile").setLevel(level)


READER_BLOCK_SIZE = parse_quantity(os.getenv("MEGFILE_READER_BLOCK_SIZE") or 8 * 2**20)
if READER_BLOCK_SIZE <= 0:
    raise ValueError(
        f"'MEGFILE_READER_BLOCK_SIZE' must bigger than 0, got {READER_BLOCK_SIZE}"
    )
READER_MAX_BUFFER_SIZE = parse_quantity(
    os.getenv("MEGFILE_READER_MAX_BUFFER_SIZE") or 128 * 2**20
)

# Multi-upload in aws s3 has a maximum of 10,000 parts,
# so the maximum supported file size is MEGFILE_WRITE_BLOCK_SIZE * 10,000,
# the largest object that can be uploaded in a single PUT is 5 TB in aws s3.
WRITER_BLOCK_SIZE = parse_quantity(os.getenv("MEGFILE_WRITER_BLOCK_SIZE") or 8 * 2**20)
if WRITER_BLOCK_SIZE <= 0:
    raise ValueError(
        f"'MEGFILE_WRITER_BLOCK_SIZE' must bigger than 0, got {WRITER_BLOCK_SIZE}"
    )
WRITER_MAX_BUFFER_SIZE = parse_quantity(
    os.getenv("MEGFILE_WRITER_MAX_BUFFER_SIZE") or 128 * 2**20
)
DEFAULT_WRITER_BLOCK_AUTOSCALE = not os.getenv("MEGFILE_WRITER_BLOCK_SIZE")
if os.getenv("MEGFILE_WRITER_BLOCK_AUTOSCALE"):
    DEFAULT_WRITER_BLOCK_AUTOSCALE = parse_boolean(
        os.environ["MEGFILE_WRITER_BLOCK_AUTOSCALE"]
    )

GLOBAL_MAX_WORKERS = int(os.getenv("MEGFILE_MAX_WORKERS") or 8)

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

if os.getenv("MEGFILE_LOG_LEVEL"):
    set_log_level()

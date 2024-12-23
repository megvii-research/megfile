from concurrent.futures import Future
from io import BytesIO
from typing import Optional

from megfile.config import (
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
    S3_MAX_RETRY_TIMES,
)
from megfile.errors import (
    S3FileChangedError,
    S3InvalidRangeError,
    patch_method,
    raise_s3_error,
    s3_should_retry,
)
from megfile.lib.base_prefetch_reader import BasePrefetchReader, LRUCacheFutureManager

__all__ = [
    "S3PrefetchReader",
    "LRUCacheFutureManager",
]


class S3PrefetchReader(BasePrefetchReader):
    """
    Reader to fast read the s3 content.

    This will divide the file content into equalparts of block_size size,
    and will use LRU to cache at most blocks in max_buffer_size memory.

    open(), seek() and read() will trigger prefetch read.
    The prefetch will cached block_forward blocks of data from offset position
    (the position after reading if the called function is read).
    """

    def __init__(
        self,
        bucket: str,
        key: str,
        *,
        s3_client,
        block_size: int = READER_BLOCK_SIZE,
        max_buffer_size: int = READER_MAX_BUFFER_SIZE,
        block_forward: Optional[int] = None,
        max_retries: int = S3_MAX_RETRY_TIMES,
        max_workers: Optional[int] = None,
        profile_name: Optional[str] = None,
    ):
        self._bucket = bucket
        self._key = key
        self._client = s3_client
        self._profile_name = profile_name
        self._content_etag = None

        super().__init__(
            block_size=block_size,
            max_buffer_size=max_buffer_size,
            block_forward=block_forward,
            max_retries=max_retries,
            max_workers=max_workers,
        )

    def _get_content_size(self):
        if self._block_capacity <= 0:
            response = self._client.head_object(Bucket=self._bucket, Key=self._key)
            self._content_etag = response.get("ETag")
            return int(response["ContentLength"])

        try:
            start, end = 0, self._block_size - 1
            first_index_response = self._fetch_response(start=start, end=end)
            if "ContentRange" in first_index_response:
                content_size = int(first_index_response["ContentRange"].split("/")[-1])
            else:
                # usually when read a file only have one block
                content_size = int(first_index_response["ContentLength"])
        except S3InvalidRangeError:
            # usually when read a empty file
            # can use minio test empty file: https://hub.docker.com/r/minio/minio
            first_index_response = self._fetch_response()
            content_size = int(first_index_response["ContentLength"])

        first_future = Future()
        first_future.set_result(first_index_response["Body"])
        self._insert_futures(index=0, future=first_future)
        self._content_etag = first_index_response.get("ETag")
        return content_size

    @property
    def name(self) -> str:
        return "s3%s://%s/%s" % (
            f"+{self._profile_name}" if self._profile_name else "",
            self._bucket,
            self._key,
        )

    def _fetch_response(
        self, start: Optional[int] = None, end: Optional[int] = None
    ) -> dict:
        def fetch_response() -> dict:
            if start is None or end is None:
                return self._client.get_object(Bucket=self._bucket, Key=self._key)

            range_str = f"bytes={start}-{end}"
            response = self._client.get_object(
                Bucket=self._bucket, Key=self._key, Range=range_str
            )
            response["Body"] = BytesIO(response["Body"].read())
            return response

        fetch_response = patch_method(
            fetch_response, max_retries=self._max_retries, should_retry=s3_should_retry
        )

        with raise_s3_error(self.name):
            return fetch_response()

    def _fetch_buffer(self, index: int) -> BytesIO:
        start, end = index * self._block_size, (index + 1) * self._block_size - 1
        response = self._fetch_response(start=start, end=end)
        etag = response.get("ETag", None)
        if self._content_etag and etag and etag != self._content_etag:
            raise S3FileChangedError(
                "File changed: %r, etag before: %s, after: %s"
                % (self.name, self._content_etag, etag)
            )

        return response["Body"]

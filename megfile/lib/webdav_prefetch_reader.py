from io import BytesIO
from typing import Optional

from webdav3.client import Client as WebdavClient
from webdav3.client import Urn

from megfile.config import (
    READER_BLOCK_SIZE,
    READER_MAX_BUFFER_SIZE,
    WEBDAV_MAX_RETRY_TIMES,
)
from megfile.errors import (
    HttpBodyIncompleteError,
    UnsupportedError,
    http_should_retry,
    patch_method,
)
from megfile.lib.base_prefetch_reader import BasePrefetchReader

DEFAULT_TIMEOUT = (60, 60 * 60 * 24)


class WebdavPrefetchReader(BasePrefetchReader):
    """
    Reader to fast read the http content, service must support Accept-Ranges.

    This will divide the file content into equal parts of block_size size, and will use
    LRU to cache at most blocks in max_buffer_size memory.

    open(), seek() and read() will trigger prefetch read.

    The prefetch will cached block_forward blocks of data from offset position
    (the position after reading if the called function is read).
    """

    def __init__(
        self,
        urn: Urn,
        *,
        client: Optional[WebdavClient] = None,
        content_size: Optional[int] = None,
        block_size: int = READER_BLOCK_SIZE,
        max_buffer_size: int = READER_MAX_BUFFER_SIZE,
        block_forward: Optional[int] = None,
        max_retries: int = WEBDAV_MAX_RETRY_TIMES,
        max_workers: Optional[int] = None,
    ):
        self._urn = urn
        self._content_size = content_size
        self._client = client or WebdavClient({})

        super().__init__(
            block_size=block_size,
            max_buffer_size=max_buffer_size,
            block_forward=block_forward,
            max_retries=max_retries,
            max_workers=max_workers,
        )

    def _get_content_size(self) -> int:
        if self._content_size is not None:
            return self._content_size

        first_index_response = self._fetch_response()
        if first_index_response["Headers"].get("Accept-Ranges") != "bytes":
            raise UnsupportedError(
                f"Unsupported server, server must support Accept-Ranges: {self._urn}",
                path=str(self._urn),
            )
        return first_index_response["Headers"]["Content-Length"]

    @property
    def name(self) -> str:
        return str(self._urn)

    def _fetch_response(
        self, start: Optional[int] = None, end: Optional[int] = None
    ) -> dict:
        def fetch_response() -> dict:
            if start is None or end is None:
                with self._client.execute_request(
                    action="download", path=self._urn.quote()
                ) as response:
                    return {
                        "Headers": response.headers,
                        "Cookies": response.cookies,
                        "StatusCode": response.status_code,
                    }

            range_end = end
            if self._content_size is not None:
                range_end = min(range_end, self._content_size - 1)
            headers_ext = [f"Range: bytes={start}-{range_end}"]
            with self._client.execute_request(
                action="download", path=self._urn.quote(), headers_ext=headers_ext
            ) as response:
                if len(response.content) != int(response.headers["Content-Length"]):
                    raise HttpBodyIncompleteError(
                        "The downloaded content is incomplete, "
                        "expected size: %s, actual size: %d"
                        % (
                            response.headers["Content-Length"],
                            len(response.content),
                        )
                    )
                return {
                    "Body": BytesIO(response.content),
                    "Headers": response.headers,
                    "Cookies": response.cookies,
                    "StatusCode": response.status_code,
                }

        fetch_response = patch_method(
            fetch_response,
            max_retries=self._max_retries,
            should_retry=http_should_retry,
        )

        return fetch_response()

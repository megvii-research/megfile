from logging import getLogger as get_logger
from threading import Lock
from typing import NamedTuple, Optional

from megfile.config import (
    DEFAULT_WRITER_BLOCK_AUTOSCALE,
    WRITER_BLOCK_SIZE,
    WRITER_MAX_BUFFER_SIZE,
)
from megfile.errors import raise_s3_error
from megfile.lib.base_multipart_writer import BaseMultipartWriter

_logger = get_logger(__name__)
"""
class PartResult(NamedTuple):

    etag: str
    part_number: int
    content_size: int

in Python 3.6+
"""

_PartResult = NamedTuple(
    "PartResult", [("etag", str), ("part_number", int), ("content_size", int)]
)


class PartResult(_PartResult):
    def asdict(self):
        return {"PartNumber": self.part_number, "ETag": self.etag}


class S3MultipartWriter(BaseMultipartWriter):
    # Multi-upload part size must be between 5 MiB and 5 GiB.
    # There is no minimum size limit on the last part of your multipart upload.
    MIN_BLOCK_SIZE = 8 * 2**20

    def __init__(
        self,
        bucket: str,
        key: str,
        *,
        s3_client,
        block_size: int = WRITER_BLOCK_SIZE,
        block_autoscale: bool = DEFAULT_WRITER_BLOCK_AUTOSCALE,
        max_buffer_size: int = WRITER_MAX_BUFFER_SIZE,
        max_workers: Optional[int] = None,
        profile_name: Optional[str] = None,
        atomic: bool = False,
    ):
        self._bucket = bucket
        self._key = key
        self._client = s3_client
        self._profile_name = profile_name
        self.__upload_id = None
        self.__upload_id_lock = Lock()

        super().__init__(
            block_size=block_size,
            block_autoscale=block_autoscale,
            max_buffer_size=max_buffer_size,
            max_workers=max_workers,
            atomic=atomic,
        )

    @property
    def name(self) -> str:
        protocol = f"s3+{self._profile_name}" if self._profile_name else "s3"
        return f"{protocol}://{self._bucket}/{self._key}"

    @property
    def _is_multipart(self) -> bool:
        return len(self._uploaded_results) > 0 or len(self._uploading_futures) > 0

    @property
    def _upload_id(self) -> str:
        if self.__upload_id is None:
            with self.__upload_id_lock:
                if self.__upload_id is None:
                    with raise_s3_error(self.name):
                        self.__upload_id = self._client.create_multipart_upload(
                            Bucket=self._bucket, Key=self._key
                        )["UploadId"]
        return self.__upload_id

    @property
    def _multipart_upload(self):
        for future in self._uploading_futures:
            result = future.result()
            self._total_buffer_size -= result.content_size
            self._uploaded_results[result.part_number] = result.asdict()
        self._uploading_futures = set()
        return {
            "Parts": [result for _, result in sorted(self._uploaded_results.items())]
        }

    def _upload_buffer(self, part_number, content):
        with raise_s3_error(self.name):
            return PartResult(
                self._client.upload_part(
                    Bucket=self._bucket,
                    Key=self._key,
                    UploadId=self._upload_id,
                    PartNumber=part_number,
                    Body=content,
                )["ETag"],
                part_number,
                len(content),
            )

    def _abort(self):
        _logger.debug("abort file: %r" % self.name)

        if self._is_multipart:
            with raise_s3_error(self.name):
                self._client.abort_multipart_upload(
                    Bucket=self._bucket, Key=self._key, UploadId=self._upload_id
                )

        self._shutdown()

    def _close(self):
        _logger.debug("close file: %r" % self.name)

        if not self._is_multipart:
            with raise_s3_error(self.name):
                self._client.put_object(
                    Bucket=self._bucket, Key=self._key, Body=self._buffer.getvalue()
                )
            self._shutdown()
            return

        self._submit_futures()

        with raise_s3_error(self.name):
            self._client.complete_multipart_upload(
                Bucket=self._bucket,
                Key=self._key,
                MultipartUpload=self._multipart_upload,
                UploadId=self._upload_id,
            )

        self._shutdown()

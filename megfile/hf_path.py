from logging import getLogger as get_logger

from megfile.config import HF_MAX_RETRY_TIMES
from megfile.errors import http_should_retry, patch_method
from megfile.fsspec import BaseFSSpecPath
from megfile.smart import SmartPath
from megfile.utils import cached_classproperty

_logger = get_logger(__name__)

MAX_RETRIES = HF_MAX_RETRY_TIMES


def _patch_huggingface_session():
    from huggingface_hub.utils._http import UniqueRequestIdAdapter

    def after_callback(response, *args, **kwargs):
        if response.status_code in (429, 500, 502, 503, 504):
            response.raise_for_status()
        return response

    def before_callback(method, url, **kwargs):
        _logger.debug(
            "send http request: %s %r, with parameters: %s", method, url, kwargs
        )

    UniqueRequestIdAdapter.send = patch_method(  # pyre-ignore[16]
        UniqueRequestIdAdapter.send,  # pytype: disable=attribute-error
        max_retries=MAX_RETRIES,
        after_callback=after_callback,
        before_callback=before_callback,
        should_retry=http_should_retry,
    )


@SmartPath.register
class HFPath(BaseFSSpecPath):
    protocol = "hf"

    @cached_classproperty
    def filesystem(self):
        from huggingface_hub import HfFileSystem

        _patch_huggingface_session()
        return HfFileSystem()

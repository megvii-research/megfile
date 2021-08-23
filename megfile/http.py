from io import BufferedReader
from typing import Iterable
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from megfile.errors import http_should_retry, patch_method, translate_http_error
from megfile.interfaces import MegfilePathLike
from megfile.lib.compat import fspath
from megfile.utils import binary_open

__all__ = [
    'is_http',
    'http_open',
]

max_retries = 10


def get_http_session(
        timeout: int = 10, status_forcelist: Iterable[int] = (502, 503, 504)):
    session = requests.Session()
    session.timeout = timeout

    def after_callback(response):
        if response.status_code in status_forcelist:
            response.raise_for_status()
        return response

    session.request = patch_method(
        session.request,
        max_retries=max_retries,
        should_retry=http_should_retry,
        after_callback=after_callback,
    )
    return session


def is_http(path: MegfilePathLike) -> bool:
    '''http scheme definition: http(s)://<url>

    :param path: Path to be tested
    :returns: True if path is http url, else False
    '''

    path = fspath(path)
    if not isinstance(path, str) or not (path.startswith('http://') or
                                         path.startswith('https://')):
        return False

    parts = urlsplit(path)
    return parts.scheme == 'http' or parts.scheme == 'https'


@binary_open
def http_open(http_url: str, mode: str = 'rb') -> BufferedReader:
    '''Open a BytesIO to read binary data of given http(s) url

    .. note ::

        Essentially, it reads data of http(s) url to memory by requests, and then return BytesIO to user.

    :param http_url: http(s) url, http(s)://<url>
    :param mode: Only supports 'rb' mode now
    :return: BytesIO initialized with http(s) data 
    '''
    if mode not in ('rb',):
        raise ValueError('unacceptable mode: %r' % mode)

    try:
        response = requests.get(http_url, stream=True, timeout=10.0)
        response.raise_for_status()
    except Exception as error:
        raise translate_http_error(error, http_url)

    response.raw.auto_close = False
    return BufferedReader(response.raw)

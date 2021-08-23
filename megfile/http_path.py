from typing import IO, AnyStr

from .http import http_open
from .interfaces import URIPath
from .smart_path import SmartPath

__all__ = [
    'HttpPath',
    'HttpsPath',
]


@SmartPath.register
class HttpPath(URIPath):

    protocol = "http"

    def open(self, mode: str, **kwargs) -> IO[AnyStr]:
        return http_open(self.path_with_protocol, mode)


@SmartPath.register
class HttpsPath(HttpPath):

    protocol = "https"

from typing import IO, AnyStr

from megfile import http

from .interfaces import StatResult, URIPath
from .smart_path import SmartPath

__all__ = [
    'HttpPath',
    'HttpsPath',
]


@SmartPath.register
class HttpPath(URIPath):

    protocol = "http"

    def open(self, mode: str, **kwargs) -> IO:
        return http.http_open(self.path_with_protocol, mode=mode)

    def stat(self) -> StatResult:
        return http.http_stat(self.path_with_protocol)

    def getsize(self) -> int:
        return http.http_getsize(self.path_with_protocol)

    def getmtime(self) -> float:
        return http.http_getmtime(self.path_with_protocol)


@SmartPath.register
class HttpsPath(HttpPath):

    protocol = "https"

"""Conversions between native paths and megfile file URIs."""

import os
import re


def local_path(uri: str) -> str:
    """Return the native spelling of a file URI or a plain path.

    :param uri: A local path, optionally prefixed with file://.
    """
    if not uri.startswith("file://"):
        return uri
    path = uri[7:]
    if os.name == "nt" and re.match(r"^/[a-zA-Z]:/", path):
        return path[1:]
    return path


def file_uri(path: str) -> str:
    """Format a local path, preserving megfile's relative file URI syntax.

    :param path: A native path or an existing file URI.
    """
    if path.startswith("file://"):
        return path
    if os.name == "nt":
        path = path.replace("\\", "/")
        if re.match(r"^[a-zA-Z]:/", path):
            path = "/" + path
    # UNC paths retain their two leading separators after the protocol.
    return "file://" + path

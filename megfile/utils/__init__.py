import hashlib
import inspect
import math
import os
import uuid
from copy import copy
from functools import cached_property, wraps
from io import (
    BufferedIOBase,
    BufferedRandom,
    BufferedReader,
    BufferedWriter,
    BytesIO,
    StringIO,
    TextIOBase,
    TextIOWrapper,
)
from threading import RLock
from typing import IO, Callable, Optional

from megfile.utils.mutex import ProcessLocal, ThreadLocal


def get_content_size(fileobj: IO, *, intrusive: bool = False) -> int:
    """Get size of File-Like Object

    The File-Like Object must be seekable, otherwise raise IOError
    """
    if isinstance(fileobj, (BytesIO, StringIO)):
        return len(fileobj.getvalue())

    if is_readable(fileobj):
        file = fileobj
        if isinstance(file, TextIOBase):
            file = file.buffer
        if isinstance(file, BufferedIOBase):
            file = file.raw
        if hasattr(file, "_content_size"):
            return getattr(file, "_content_size")  # pyre-ignore[16]

    offset = fileobj.tell()
    if not is_seekable(fileobj) and is_writable(fileobj):
        return offset

    fileobj.seek(0, os.SEEK_END)
    size = fileobj.tell()
    if not intrusive:
        fileobj.seek(offset)
    return size


def is_seekable(fileobj: IO) -> bool:
    """Test if File-Like Object is seekable"""
    if hasattr(fileobj, "seekable"):
        try:
            return fileobj.seekable()
        except Exception:
            return False
    return hasattr(fileobj, "seek")


def is_readable(fileobj: IO) -> bool:
    """Test if File-Like Object is readable"""
    if hasattr(fileobj, "readable"):
        try:
            return fileobj.readable()
        except Exception:
            return False
    return hasattr(fileobj, "read")


def is_writable(fileobj: IO) -> bool:
    """Test if File-Like Object is writable"""
    if hasattr(fileobj, "writable"):
        try:
            return fileobj.writable()
        except Exception:
            return False
    return hasattr(fileobj, "write")


def _is_pickle(fileobj) -> bool:
    """Test if File Object is pickle"""
    if fileobj.name.endswith(".pkl") or fileobj.name.endswith(".pickle"):
        return True

    if "r" in fileobj.mode and "b" in fileobj.mode:
        offset = fileobj.tell()
        fileobj.seek(0)
        data = fileobj.read(2)
        fileobj.seek(offset)
        if len(data) >= 2 and data[0] == 128 and 2 <= data[1] <= 5:
            return True
    return False


def get_content_offset(start: Optional[int], stop: Optional[int], size: int):
    if start is None:
        start = 0
    if stop is None or stop < 0 or start < 0:
        start, stop, _ = slice(start, stop).indices(size)
    if stop < start:
        raise ValueError("read length must be positive")
    return start, stop


def get_name(fileobj, default=None):
    return getattr(fileobj, "name", default or repr(fileobj))


def get_mode(fileobj, default="r"):
    if isinstance(fileobj, BytesIO):
        return "rb+"
    elif isinstance(fileobj, StringIO):
        return "r+"
    return getattr(fileobj, "mode", default)


def shadow_copy(fileobj: IO, intrusive: bool = True, buffered: bool = False):
    """Create a File-Like Object, maintaining file pointer,
    to avoid misunderstanding the position when read / write / seek.

    :param intrusive: If is intrusive. If True, move file pointer to the original
        position after every read / write / seek. If False, then not.
    :param verbose: If True, print log when read / write / seek
    """
    from megfile.lib.shadow_handler import ShadowHandler

    result = ShadowHandler(fileobj, intrusive=intrusive)
    mode = get_mode(fileobj)
    if "b" in mode and (buffered or _is_pickle(result)):
        if "+" in mode:
            result = BufferedRandom(result)
        elif "x" in mode or "w" in mode or "a" in mode:
            result = BufferedWriter(result)
        elif "r" in mode:
            result = BufferedReader(result)
    return result


def lazy_open(path: str, mode: str, open_func: Optional[Callable] = None, **options):
    """Create a File-Like Object, maintaining file pointer, to open a file in lazy mode

    :param intrusive: If is intrusive. If True, move file pointer to the original
        position after every read / write / seek. If False, then not.
    :param verbose: If True, print log when read / write / seek
    """
    from megfile.lib.lazy_handler import LazyHandler

    if open_func is None:
        from megfile.smart import smart_open

        open_func = smart_open
    return LazyHandler(path, mode, open_func=open_func, **options)


def patch_rlimit():
    import resource

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))


thread_local = ThreadLocal()
process_local = ProcessLocal()


def combine(file_objects, name):
    from megfile.lib.combine_reader import CombineReader

    return CombineReader(file_objects, name)


def get_binary_mode(mode: str) -> str:
    """Replace mode parameter in open() with corresponding binary mode"""
    if "t" in mode:
        # rt / wt / rt+ => rb / wb / rb+
        mode = mode.replace("t", "b")
    elif "b" not in mode:
        # r / w / r+ => rb / wb / rb+
        mode = mode[:1] + "b" + mode[1:]
    # rb / wb / r+b => rb / wb / rb+
    return "".join(sorted(mode, key=lambda k: {"b": 1, "+": 2}.get(k, 0)))


def binary_open(open_func):
    """
    Decorator:
    Output according to user-setting mode while calling Open
    """

    @wraps(open_func)
    def wrapper(
        path,
        mode: str = "rb",
        encoding: Optional[str] = None,
        errors: Optional[str] = None,
        **kwargs,
    ):
        fileobj = open_func(path, get_binary_mode(mode), **kwargs)
        if "b" not in mode:
            fileobj = TextIOWrapper(fileobj, encoding=encoding, errors=errors)
            fileobj.mode = mode  # pyre-ignore[41]
        return fileobj

    return wrapper


def get_human_size(size_bytes: float) -> str:
    """Get human-readable size, e.g. `100MB`"""
    if size_bytes < 0:
        raise ValueError("negative size: %r" % size_bytes)
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    index = int(math.floor(math.log(size_bytes, 1024)))
    base = math.pow(1024, index)
    if base == 1:
        size = int(size_bytes)
    else:
        size = round(size_bytes / base, 2)
    return "%s %s" % (size, size_name[index])


def necessary_params(func: Callable, **kwargs):
    params = inspect.signature(func).parameters
    res_kwargs = {}
    var_keyword = False
    for key, value in kwargs.items():
        if key not in params:
            continue
        if params[key].kind is inspect.Parameter.VAR_KEYWORD:
            var_keyword = True
            break
        res_kwargs[key] = value

    if var_keyword:
        res_kwargs = copy(res_kwargs)
    return res_kwargs


def generate_cache_path(filename: str, cache_dir: str = "/tmp") -> str:  # nosec B108
    suffix = os.path.splitext(filename)[1]
    return os.path.join(cache_dir, str(uuid.uuid4()) + suffix)


def _get_class(cls_or_obj) -> type:
    """
    Get the class of an object if one is provided.
    @param cls_or_obj: Either an object or a class.
    @return: The class of the object or just the class again.
    """
    if isinstance(cls_or_obj, type):
        return cls_or_obj
    return type(cls_or_obj)


def calculate_md5(file_object):
    hash_md5 = hashlib.md5()  # nosec
    for chunk in iter(lambda: file_object.read(4096), b""):
        hash_md5.update(chunk)
    return hash_md5.hexdigest()


class classproperty(property):
    """
    The use this class as a decorator for your class property.
    Example:
        @classproperty
        def prop(cls):
            return "value"
    """

    def __get__(self, _, cls) -> object:
        """
        This method gets called when a property value is requested.
        @param cls: The class type of the above instance.
        @return: The value of the property.
        """
        # apply the __get__ on the class
        return super(classproperty, self).__get__(cls)

    # __set__ and __delete__ only work for instance
    # classproperty only support __get__


class cached_classproperty(cached_property):
    """
    The use this class as a decorator for your class property with cache.
    Example:
        @cached_classproperty
        def prop(cls):
            return "value"
    """

    def __init__(self, func: Callable) -> None:
        """
        This method initializes the cached_classproperty instance.
        @param func: The function to be called when the property value is requested.
        """
        super().__init__(func)
        # Python 3.12 removed the lock attribute from cached_property.
        # Maybe we should remove this in the future.
        # See also: https://github.com/python/cpython/pull/101890
        #        https://github.com/python/cpython/issues/87634
        if not hasattr(func, "lock"):
            self.lock = RLock()

    def __get__(  # pyre-ignore[14]
        self,
        _,
        cls,  # pytype: disable=signature-mismatch
    ) -> object:
        """
        This method gets called when a property value is requested.
        @param cls: The class type of the above instance.
        @return: The value of the property.
        """
        if self.attrname is None:
            raise TypeError(  # pragma: no cover
                "Cannot use cached_classproperty instance without calling "
                "__set_name__ on it."
            )
        with self.lock:
            # check if another thread filled cache while we awaited lock
            # cannot use getattr since it will cause RecursionError
            val = cls.__dict__[self.attrname]
            if val is self:
                val = self.func(cls)
                setattr(cls, self.attrname, val)  # pyre-ignore[6]
        return val


def is_domain_or_subdomain(sub, parent):
    if sub == parent:
        return True
    if sub.endswith(f".{parent}"):
        return True
    return False

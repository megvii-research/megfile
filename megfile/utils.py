import inspect
import math
import os
from copy import copy
from functools import wraps
from io import BufferedIOBase, BufferedRandom, BufferedReader, BufferedWriter, BytesIO, StringIO, TextIOBase, TextIOWrapper
from multiprocessing.util import register_after_fork
from threading import RLock as _RLock
from threading import local
from typing import IO, Callable, Optional


def get_content_size(fileobj: IO, *, intrusive: bool = False) -> int:
    ''' Get size of File-Like Object

    The File-Like Object must be seekable, otherwise raise IOError
    '''
    if isinstance(fileobj, (BytesIO, StringIO)):
        return len(fileobj.getvalue())

    if is_readable(fileobj):
        file = fileobj
        if isinstance(file, TextIOBase):
            file = file.buffer
        if isinstance(file, BufferedIOBase):
            file = file.raw
        if hasattr(file, '_content_size'):
            return getattr(file, '_content_size')

    offset = fileobj.tell()
    if not is_seekable(fileobj) and is_writable(fileobj):
        return offset

    fileobj.seek(0, os.SEEK_END)
    size = fileobj.tell()
    if not intrusive:
        fileobj.seek(offset)
    return size


def is_seekable(fileobj: IO) -> bool:
    ''' Test if File-Like Object is seekable'''
    if hasattr(fileobj, 'seekable'):
        try:
            return fileobj.seekable()
        except Exception:
            return False
    return hasattr(fileobj, 'seek')


def is_readable(fileobj: IO) -> bool:
    ''' Test if File-Like Object is readable'''
    if hasattr(fileobj, 'readable'):
        try:
            return fileobj.readable()
        except Exception:
            return False
    return hasattr(fileobj, 'read')


def is_writable(fileobj: IO) -> bool:
    ''' Test if File-Like Object is writable'''
    if hasattr(fileobj, 'writable'):
        try:
            return fileobj.writable()
        except Exception:
            return False
    return hasattr(fileobj, 'write')


def get_content_offset(start: Optional[int], stop: Optional[int], size: int):
    if start is None:
        start = 0
    if stop is None or stop < 0 or start < 0:
        start, stop, _ = slice(start, stop).indices(size)
    if stop < start:
        raise ValueError('read length must be positive')
    return start, stop


def get_name(fileobj, default=None):
    return getattr(fileobj, 'name', default or repr(fileobj))


def get_mode(fileobj, default='r'):
    if isinstance(fileobj, BytesIO):
        return 'rb+'
    elif isinstance(fileobj, StringIO):
        return 'r+'
    return getattr(fileobj, 'mode', default)


def shadow_copy(fileobj: IO, intrusive: bool = True, buffered: bool = True):
    ''' Create a File-Like Object, maintaining file pointer, to avoid misunderstanding the position when read / write / seek 

    :param intrusive: If is intrusive. If True, move file pointer to the original position after every read / write / seek. If False, then not.
    :param verbose: If True, print log when read / write / seek
    '''
    from megfile.lib.shadow_handler import ShadowHandler
    result = ShadowHandler(fileobj, intrusive=intrusive)
    mode = get_mode(fileobj)
    if buffered and "b" in mode:
        if "+" in mode:
            result = BufferedRandom(result)  # pytype: disable=wrong-arg-types
        elif "x" in mode or "w" in mode or "a" in mode:
            result = BufferedWriter(result)  # pytype: disable=wrong-arg-types
        elif "r" in mode:
            result = BufferedReader(result)  # pytype: disable=wrong-arg-types
    return result


def lazy_open(
        path: str, mode: str, open_func: Optional[Callable] = None, **options):
    ''' Create a File-Like Object, maintaining file pointer, to open a file in lazy mode

    :param intrusive: If is intrusive. If True, move file pointer to the original position after every read / write / seek. If False, then not.
    :param verbose: If True, print log when read / write / seek
    '''
    from megfile.lib.lazy_handler import LazyHandler
    if open_func is None:
        from megfile.smart import smart_open
        open_func = smart_open
    return LazyHandler(path, mode, open_func=open_func, **options)


def patch_rlimit():
    import resource
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))


class ThreadLocal:

    def __init__(self):
        local_data = local()
        object.__setattr__(self, '_local', local_data)
        register_after_fork(self, ThreadLocal._reset)

    def _reset(self):
        local_data = local()
        object.__setattr__(self, '_local', local_data)

    def __getattribute__(self, name):
        local_data = object.__getattribute__(self, '_local')
        return local_data.__getattribute__(name)  # pytype: disable=attribute-error

    def __setattr__(self, name, value):
        local_data = object.__getattribute__(self, '_local')
        return local_data.__setattr__(name, value)

    def __delattr__(self, name):
        local_data = object.__getattribute__(self, '_local')
        return local_data.__delattr__(name)


class ProcessLocal:
    """
    Provides a basic per-process mapping container that wipes itself if the current PID changed since the last get/set.
    Aka `threading.local()`, but for processes instead of threads.
    """

    def __init__(self):
        register_after_fork(self, ProcessLocal._reset)

    def _reset(self):
        self.__dict__.clear()


class RLock:

    def __init__(self):
        self._reset()
        register_after_fork(self, RLock._reset)

    def _reset(self):
        self._lock = _RLock()
        self.acquire = self._lock.acquire
        self.release = self._lock.release

    def __enter__(self):
        return self._lock.__enter__()

    def __exit__(self, *args):
        return self._lock.__exit__(*args)


_thread_local_cache = ThreadLocal()

_process_local_cache = ProcessLocal()
_process_local_lock = RLock()


def thread_local(key: str, creator: Callable, *args, **kwargs):

    # Because _threading_local.local are in different thread, users get different dict, so don't need to lock
    if not hasattr(_thread_local_cache, key):
        setattr(_thread_local_cache, key, creator(*args, **kwargs))
    return getattr(_thread_local_cache, key)


def process_local(key: str, creator: Callable, *args, **kwargs):
    with _process_local_lock:
        if not hasattr(_process_local_cache, key):
            setattr(_process_local_cache, key, creator(*args, **kwargs))
        return getattr(_process_local_cache, key)


def combine(file_objects, name):
    from megfile.lib.combine_reader import CombineReader
    return CombineReader(file_objects, name)


def get_binary_mode(mode: str) -> str:
    '''Replace mode parameter in open() with corresponding binary mode'''
    # TODO: some bugs in s3_cached_open, like mode should be rb+ rather than r+b, results in the slightly complicated code logic
    if 't' in mode:
        # rt / wt / rt+ => rb / wb / rb+
        return mode.replace('t', 'b')
    elif 'b' not in mode:
        # r / w / r+ => rb / wb / rb+
        return mode[:1] + 'b' + mode[1:]
    # rb / wb / rb+ => rb / wb / rb+
    return mode


def binary_open(open_func):
    '''
    Decorator:
    Output according to user-setting mode while calling Open
    '''

    @wraps(open_func)
    def wrapper(path, mode: str = 'rb', **kwargs):
        fileobj = open_func(path, get_binary_mode(mode), **kwargs)
        if 'b' not in mode:
            fileobj = TextIOWrapper(fileobj)
            fileobj.mode = mode
        return fileobj

    return wrapper


def get_human_size(size_bytes: float) -> str:
    '''Get human-readable size, e.g. `100MB`'''
    assert size_bytes >= 0, 'negative size: %r' % size_bytes
    if size_bytes == 0:
        return '0 B'
    size_name = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    index = int(math.floor(math.log(size_bytes, 1024)))
    base = math.pow(1024, index)
    if base == 1:
        size = size_bytes
    else:
        size = round(size_bytes / base, 2)
    return '%s %s' % (size, size_name[index])


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


def _get_class(cls_or_obj) -> type:
    """
    Get the class of an object if one is provided.
    @param cls_or_obj: Either an object or a class.
    @return: The class of the object or just the class again.
    """
    if isinstance(cls_or_obj, type):
        return cls_or_obj
    return type(cls_or_obj)


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

    def __set__(self, cls_or_obj, value: object) -> None:
        """
        This method gets called when a property value should be set.
        @param cls_or_obj: The class or instance of which the property should be changed.
        @param value: The new value.
        """
        # call this method only on the class, not the instance
        super(classproperty, self).__set__(_get_class(cls_or_obj), value)

    def __delete__(self, cls_or_obj) -> None:
        """
        This method gets called when a property should be deleted.
        @param cls_or_obj: The class or instance of which the property should be deleted.
        """
        # call this method only on the class, not the instance
        super(classproperty, self).__delete__(_get_class(cls_or_obj))


class cachedproperty:
    """
    A property that is only computed once per instance and then replaces itself
    with an ordinary attribute. Deleting the attribute resets the property.
    Source: https://github.com/bottlepy/bottle/commit/fa7733e075da0d790d809aa3d2f53071897e6f76
    """  # noqa

    def __init__(self, func):
        self.__name__ = func.__name__
        self.__module__ = func.__module__
        self.__doc__ = func.__doc__
        self.__wrapped__ = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = obj.__dict__[self.__name__] = self.__wrapped__(obj)
        return value

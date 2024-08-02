import io
import os
import pickle
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pytest

from megfile.lib.shadow_handler import ShadowHandler
from megfile.s3 import s3_buffered_open
from megfile.smart import smart_open
from megfile.utils import (
    get_content_size,
    is_readable,
    is_seekable,
    is_writable,
    lazy_open,
    process_local,
    shadow_copy,
    thread_local,
)
from tests.test_s3 import s3_empty_client  # noqa: F401

BUCKET = "bucket"
KEY = "key"
CONTENT = b"block0\n block1\n block2"


@pytest.fixture
def client(s3_empty_client, fs):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    return s3_empty_client


def test_fs_abilities():
    # TODO: pyfakefs 提供的文件句柄 readable writable 返回是错的
    with open("/tmp/test_fs_abilities", "wb") as f:
        f.write(b"abcde")

    r = open("/tmp/test_fs_abilities", "rb")
    assert is_readable(r)
    assert not is_writable(r)
    assert is_seekable(r)
    assert get_content_size(r) == 5

    sr = shadow_copy(r)
    assert is_readable(sr)
    assert not is_writable(sr)
    assert is_seekable(sr)
    assert get_content_size(sr) == 5

    r.close()

    w = open("/tmp/test_fs_abilities", "wb")
    assert not is_readable(w)
    assert is_writable(w)
    assert is_seekable(w)
    assert get_content_size(w) == 0

    w.write(b"abcde")
    assert get_content_size(w) == 5
    w.write(b"abcde")
    assert get_content_size(w) == 10

    r = open("/tmp/test_fs_abilities", "r")
    assert is_readable(r)
    assert not is_writable(r)
    assert is_seekable(r)
    assert get_content_size(r) == 10

    sr = shadow_copy(r)
    assert is_readable(sr)
    assert not is_writable(sr)
    assert is_seekable(sr)
    assert get_content_size(sr) == 10

    r.close()

    w = open("/tmp/test_fs_abilities", "w")
    assert not is_readable(w)
    assert is_writable(w)
    assert is_seekable(w)
    assert get_content_size(w) == 0

    w.write("abcde")
    assert get_content_size(w) == 5
    w.write("abcde")
    assert get_content_size(w) == 10

    class FakeIOError:
        def seekable(self):
            raise Exception

        def readable(self):
            raise Exception

        def writable(self):
            raise Exception

    io_object_error = FakeIOError()
    assert is_writable(io_object_error) is False
    assert is_readable(io_object_error) is False
    assert is_seekable(io_object_error) is False

    class FakeIO:
        def seek(self):
            pass

        def read(self):
            pass

        def write(self):
            pass

    io_object = FakeIO()
    assert is_writable(io_object) is True
    assert is_readable(io_object) is True
    assert is_seekable(io_object) is True


def test_shadow_copy_pickle_file(fs):
    with open("test", "wb") as f:
        f.write(b"test")

    with open("test", "rb") as f:
        assert isinstance(shadow_copy(f), ShadowHandler)

    with open("test.pkl", "wb") as f:
        f.mode = "wb"
        assert isinstance(shadow_copy(f), io.BufferedWriter)

    with open("test", "wb") as f:
        f.write(pickle.dumps(b"test"))

    with open("test", "rb") as f:
        f.mode = "rb"
        assert isinstance(shadow_copy(f), io.BufferedReader)

    with open("test", "rb+") as f:
        f.mode = "rb+"
        assert isinstance(shadow_copy(f), io.BufferedRandom)


def test_pipe_abilities():
    r, w = os.pipe()
    r = os.fdopen(r, "rb")
    w = os.fdopen(w, "wb")

    assert is_readable(r)
    assert not is_writable(r)
    assert not is_seekable(r)

    assert not is_readable(w)
    assert is_writable(w)
    assert not is_seekable(w)


def test_s3_abilities(client):
    w = s3_buffered_open("s3://bucket/key", "wb")
    assert not is_readable(w)
    assert is_writable(w)
    assert not is_seekable(w)
    assert get_content_size(w) == 0

    w.write(b"abcde")
    assert get_content_size(w) == 5
    w.write(b"abcde")
    assert get_content_size(w) == 10
    w.close()

    w = s3_buffered_open("s3://bucket/key", "wb", limited_seekable=True)
    assert not is_readable(w)
    assert is_writable(w)
    assert is_seekable(w)
    assert get_content_size(w) == 0

    w.write(b"abcde")
    assert get_content_size(w) == 5
    w.write(b"abcde")
    assert get_content_size(w) == 10
    w.close()

    r = s3_buffered_open("s3://bucket/key", "rb")
    assert is_readable(r)
    assert not is_writable(r)
    assert is_seekable(r)
    assert get_content_size(r) == 10


def test_shadow_copy(client):
    w = s3_buffered_open("s3://bucket/key", "wb")
    sw = shadow_copy(w)
    assert sw.write(b"abcde") == 5
    sw.close()
    w.close()

    r = s3_buffered_open("s3://bucket/key", "rb")
    sr = shadow_copy(r)
    assert sr.seek(0) == 0
    assert sr.read() == b"abcde"
    sr.close()
    r.close()

    rw = s3_buffered_open("s3://bucket/key", "rb+")
    srw = shadow_copy(rw)
    assert srw.seek(0) == 0
    assert srw.read() == b"abcde"
    assert srw.write(b"abcde") == 5
    assert srw.seek(0) == 0
    assert srw.read() == b"abcdeabcde"
    srw.close()
    rw.close()


def assert_same_list(local_func, l1):
    l2 = local_func("list", list)
    assert l2 is l1


def assert_different_list(local_func, l1):
    l3 = local_func("list", list)
    assert l3 is not l1


def test_process_local():
    l1 = process_local("list", list)

    assert_same_list(process_local, l1)

    executor = ThreadPoolExecutor()
    executor.submit(assert_same_list, process_local, l1).result()

    executor = ProcessPoolExecutor()
    executor.submit(assert_different_list, process_local, l1).result()


def test_thread_local():
    l1 = thread_local("list", list)

    assert_same_list(thread_local, l1)

    executor = ThreadPoolExecutor()
    executor.submit(assert_different_list, thread_local, l1).result()

    executor = ProcessPoolExecutor()
    executor.submit(assert_different_list, thread_local, l1).result()


def test_thread_local_recursive():
    def func_3():
        return None

    def func_2():
        thread_local("func_3", func_3)

    def func_1():
        thread_local("func_2", func_2)

    executor = ThreadPoolExecutor()
    executor.submit(func_1).result(timeout=1)


def test_lazy_open(mocker):
    TEST_PATH = "/test"
    funcA = mocker.patch("megfile.lib.lazy_handler.LazyHandler")
    lazy_open(TEST_PATH, "r")
    funcA.assert_called_once_with(TEST_PATH, "r", open_func=smart_open)

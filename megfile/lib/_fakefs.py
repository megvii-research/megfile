import atexit
import errno
import inspect
import os
import stat
import subprocess
from threading import Thread
from uuid import uuid4

try:
    import fuse  # pytype: disable=import-error
except ImportError:  # pragma: no cover
    raise ImportError(
        inspect.cleandoc(
            '''
            Failed to import fuse, the following steps show you how to install it:

                sudo apt install -y fuse libfuse-dev
                pip3 install fuse-python --user
            '''))

if not hasattr(fuse, '__version__'):  # pragma: no cover
    raise RuntimeError(
        "your fuse-py doesn't know of fuse.__version__, probably it's too old.")

fuse.fuse_python_api = (0, 2)


def translate_error_to_errno(error):  # pragma: no cover
    if isinstance(error, FileNotFoundError):
        return -errno.ENOENT
    elif isinstance(error, PermissionError):
        return -errno.EACCES
    elif isinstance(error, EnvironmentError):
        return -errno.ENODEV
    return -errno.ENOSYS


def translate_path(path):  # pragma: no cover
    slices = path.split('/')
    assert slices[0] == '', path
    if slices[1].endswith(':'):
        return slices[1] + '//' + '/'.join(slices[2:])
    return path


class FakeFS(fuse.Fuse):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mountpoint = os.path.join('/tmp/refile/fake', str(uuid4()))
        self.started = False
        self.files = {}

    def getattr(self, path):  # pragma: no cover
        from megfile.smart import smart_isfile, smart_stat

        path = translate_path(path)
        try:
            result = fuse.Stat()
            if smart_isfile(path):
                info = smart_stat(path)
                result.st_mode = stat.S_IFREG | 0o444
                result.st_nlink = 1
                result.st_size = info.size
                result.st_mtime = info.mtime
            else:
                result.st_mode = stat.S_IFDIR | 0o755
                result.st_nlink = 1
        except Exception as error:
            return translate_error_to_errno(error)
        return result

    def open(self, path, flags):  # pragma: no cover
        from megfile.smart import smart_open

        path = translate_path(path)
        accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        if (flags & accmode) != os.O_RDONLY:
            return -errno.EACCES

        if path not in self.files:
            self.files[path] = smart_open(path, 'rb')

    def release(self, path, flags):  # pragma: no cover
        path = translate_path(path)
        if path not in self.files:
            return

        file = self.files[path]
        file.close()
        del self.files[path]

    def read(self, path, size, offset):  # pragma: no cover
        path = translate_path(path)
        if path not in self.files:
            return -errno.ENOENT
        file = self.files[path]
        file.seek(offset)
        return file.read(size)

    def start(self):
        if self.started:
            return
        os.makedirs(self.mountpoint)
        self.parse([self.mountpoint, '-f'], errex=1)
        self.daemon = Thread(target=self.main, daemon=True)
        self.daemon.start()
        atexit.register(self.stop)
        self.started = True

    def stop(self):
        if not self.started:
            return
        for file in self.files.values():
            file.close()
        self.files = {}
        try:
            subprocess.check_call(['fusermount', '-u', self.mountpoint])
        except FileExistsError:
            subprocess.check_call(['sudo', 'umount', self.mountpoint])
        os.rmdir(self.mountpoint)
        atexit.unregister(self.stop)
        self.started = False


fakefs = FakeFS()

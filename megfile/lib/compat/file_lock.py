"""Cross-process locking for SFTP authentication slots."""

import errno
import sys
import time
from collections.abc import Generator
from contextlib import contextmanager


@contextmanager
def file_lock(fd: int) -> Generator[None, None, None]:
    """Hold an exclusive lock while the caller owns the descriptor.

    :param fd: Open, writable descriptor positioned at the start of a lock file.
    """
    if sys.platform == "win32":
        import msvcrt

        # LK_LOCK has a fixed retry limit; authentication can take longer.
        while True:
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                break
            except OSError as error:
                if error.errno not in (errno.EACCES, errno.EDEADLK):
                    raise
                time.sleep(0.1)
        try:
            yield
        finally:
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
    else:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)

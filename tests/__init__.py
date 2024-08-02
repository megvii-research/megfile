from time import time

from megfile.interfaces import StatResult


class Any:
    def __eq__(self, other):
        return True


class Now:
    def __eq__(self, other):
        now = time()
        return -5 < other - now < 5

    def __repr__(self):
        return str(time())


class FakeStatResult(StatResult):
    def __eq__(self, other):
        if any(
            getattr(self, name) != getattr(other, name)
            for name in ("size", "ctime", "mtime", "isdir", "islnk")
        ):
            return False
        return True

import pytest

from megfile.interfaces import StatResult


def test_stat():
    stat = StatResult()
    assert stat.isfile()
    assert not stat.isdir()
    assert not stat.is_symlink()

    stat = StatResult(isdir=True, islnk=True)
    assert stat.isfile()
    assert not stat.isdir()
    assert stat.is_symlink()

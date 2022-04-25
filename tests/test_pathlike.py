import pytest

from megfile.pathlike import FileEntry, StatResult


def test_file_entry():
    stat_result = StatResult(
        size=100, ctime=1.1, mtime=1.2, isdir=False, islnk=False)
    file_entry = FileEntry(name='test', stat=stat_result)
    assert file_entry.is_file() is True
    assert file_entry.is_dir() is False
    assert file_entry.is_symlink() is False

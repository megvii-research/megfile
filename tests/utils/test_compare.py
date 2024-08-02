from megfile.lib import compare
from megfile.pathlike import StatResult


def test_get_sync_type():
    assert compare.get_sync_type("s3", "fs") == "download"
    assert compare.get_sync_type("fs", "s3") == "upload"
    assert compare.get_sync_type("sftp", "fs") == "copy"


def test_compare_time():
    early_stat = StatResult(size=100, ctime=1, mtime=1, isdir=False, islnk=False)
    late_stat = StatResult(size=100, ctime=2, mtime=2, isdir=False, islnk=False)
    assert compare.compare_time(early_stat, late_stat, "upload") is True
    assert compare.compare_time(late_stat, early_stat, "upload") is False
    assert compare.compare_time(early_stat, early_stat, "upload") is True

    assert compare.compare_time(early_stat, late_stat, "download") is False
    assert compare.compare_time(late_stat, early_stat, "download") is True
    assert compare.compare_time(early_stat, early_stat, "download") is True

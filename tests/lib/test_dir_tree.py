from megfile.interfaces import StatResult


def test_stat():
    stat = StatResult()
    assert stat.is_file()
    assert not stat.is_dir()
    assert not stat.is_symlink()

    stat = StatResult(isdir=True, islnk=True)
    assert stat.is_file()
    assert not stat.is_dir()
    assert stat.is_symlink()

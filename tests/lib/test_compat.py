from megfile.lib.compat import fspath


def test_fspath():
    assert fspath(b"test") == "test"
    assert fspath("test") == "test"

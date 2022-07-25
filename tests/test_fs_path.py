from megfile.fs_path import FSPath

FS_PROTOCOL_PREFIX = FSPath.protocol + "://"
TEST_PATH = "/test/dir/file"
TEST_PATH_WITH_PROTOCOL = FS_PROTOCOL_PREFIX + TEST_PATH


def test_from_uri():
    assert FSPath.from_uri(TEST_PATH).path == TEST_PATH
    assert FSPath.from_uri(
        TEST_PATH_WITH_PROTOCOL).path == TEST_PATH_WITH_PROTOCOL


def test_path_with_protocol():
    path = FSPath(TEST_PATH)
    assert path.path_with_protocol == TEST_PATH_WITH_PROTOCOL

    int_path = FSPath(1)
    assert int_path.path_with_protocol == 1

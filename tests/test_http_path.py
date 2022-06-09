# import pytest

# from megfile.http_path import HttpPath

# Http_PROTOCOL_PREFIX = HttpPath.protocol + "://"
# TEST_PATH = "test/aa/bb"
# TEST_PATH_WITH_PROTOCOL = Http_PROTOCOL_PREFIX + TEST_PATH
# path = HttpPath(TEST_PATH)

# def test_open(mocker):
#     funcA = mocker.patch('megfile.http.http_open')
#     path.open(mode='rb')
#     funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL, mode='rb')

# def test_stat(mocker):
#     funcA = mocker.patch('megfile.http.http_stat')
#     path.stat()
#     funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)

# def test_getsize(mocker):
#     funcA = mocker.patch('megfile.http.http_getsize')
#     path.getsize()
#     funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)

# def test_getmtime(mocker):
#     funcA = mocker.patch('megfile.http.http_getmtime')
#     path.getmtime()
#     funcA.assert_called_once_with(TEST_PATH_WITH_PROTOCOL)

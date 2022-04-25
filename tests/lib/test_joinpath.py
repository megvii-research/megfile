from megfile.lib import joinpath


def test_uri_join():
    assert joinpath.uri_join('/test', '/file') == '/test/file'
    assert joinpath.uri_join('/test', 'file/') == '/test/file/'
    assert joinpath.uri_join('/test', '/file/') == '/test/file/'
    assert joinpath.uri_join('/test', 'file') == '/test/file'
    assert joinpath.uri_join('/test/', 'file') == '/test/file'
    assert joinpath.uri_join('/test', 'file/') == '/test/file/'
    assert joinpath.uri_join('/test', '/dir/', '/file/') == '/test/dir/file/'

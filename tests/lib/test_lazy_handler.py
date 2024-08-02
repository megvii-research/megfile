from megfile.lib.lazy_handler import LazyHandler


def test_lazy_handler(fs):
    path = "/file"
    write_mode = "w"
    content = "test"
    with LazyHandler(path=path, mode=write_mode, open_func=open) as handler:
        assert handler.name == path
        assert handler.mode == write_mode
        assert handler.tell() == 0
        assert handler.writable() is True
        handler.write(content)

    read_mode = "r"
    with LazyHandler(path=path, mode=read_mode, open_func=open) as handler:
        assert handler.name == path
        assert handler.mode == read_mode
        assert handler.readable() is True
        assert handler.read() == content
        assert handler.tell() == 4
        assert handler._content_size == 4
        handler.seek(0)
        assert handler.readline(4) == content

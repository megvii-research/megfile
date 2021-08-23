from megfile.interfaces import FileCacher


class FakefsCacher(FileCacher):

    def __init__(self, path):
        from megfile.lib._fakefs import fakefs
        self._path = path
        if not fakefs.started:
            fakefs.start()

    @property
    def cache_path(self):
        from megfile.lib._fakefs import fakefs
        return fakefs.mountpoint + '/' + self._path

    def _close(self):
        pass

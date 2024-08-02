from megfile.utils.mutex import ForkAware, fork_aware


def test_fork_aware():
    class FakeForkAware(ForkAware):
        _test = ""

        def __init__(self):
            super().__init__()
            self._process_id = 0

        def _reset(self):
            self._test = "test"
            pass

        @fork_aware
        def test(self):
            return self._test

    aware = FakeForkAware()
    assert aware.test() == "test"

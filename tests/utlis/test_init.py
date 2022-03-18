import resource
from io import BytesIO, TextIOWrapper

from megfile.utils import _get_class, binary_open, cachedproperty, combine, get_human_size, necessary_params, patch_rlimit


def test_patch_rlimit(mocker):
    funcA = mocker.patch('resource.getrlimit', return_value=('soft', 'hard'))
    funcB = mocker.patch('resource.setrlimit')
    patch_rlimit()
    funcA.assert_called_once_with(resource.RLIMIT_NOFILE)
    funcB.assert_called_once_with(resource.RLIMIT_NOFILE, ('hard', 'hard'))


def test_combine(mocker):
    funcA = mocker.patch('megfile.lib.combine_reader.CombineReader')
    file_objects = [BytesIO]
    combine(file_objects, 'test')
    funcA.assert_called_once_with(file_objects, 'test')


def test_binary_open():

    def fake_open_func(path: str, mode: str, **kwargs):
        return BytesIO(b'test')

    binary_open_func = binary_open(fake_open_func)
    assert isinstance(binary_open_func('test', 'r'), TextIOWrapper)


def test_get_human_size():
    assert get_human_size(0) == '0 B'
    assert get_human_size(1024**2) == '1.0 MB'


def test_necessary_params():

    def func(a, b, c=None, **kwargs):
        pass

    assert necessary_params(
        func, a=1, b=2, c=3, d=4, kwargs=4).get('kwargs') is None


def test_get_class():

    class Test:
        pass

    assert _get_class(Test) == _get_class(Test())

import pickle

from megfile.errors import UnknownError, UnsupportedError


def test_megfile_unknown_error():
    cause = Exception("cause")
    error = UnknownError(cause, "path")
    assert "Exception(" in str(error)
    assert "cause" in str(error)
    assert "path" in str(error)
    assert error.__cause__ is cause


def test_megfile_unknown_error_pickle():
    cause = Exception("cause")
    error = UnknownError(cause, "path")
    error = pickle.loads(pickle.dumps(error))
    assert "Exception(" in str(error)
    assert "cause" in str(error)
    assert "path" in str(error)
    assert str(error.__cause__) == str(cause)


def test_megfile_unsupported_error_pickle():
    error = UnsupportedError("operation", "path")
    error = pickle.loads(pickle.dumps(error))
    assert "path" in str(error)

import importlib
import os

import pytest
from mock import patch


@patch.dict(
    os.environ,
    {
        "MEGFILE_READER_MAX_BUFFER_SIZE": str(4 * 8 * 2**20),
        "MEGFILE_WRITER_BLOCK_SIZE": str(2**20),
        "AWS_SECRET_ACCESS_KEY": "test",
    },
)
def test_config():
    from megfile import config

    importlib.reload(config)

    assert config.READER_MAX_BUFFER_SIZE // 2**20 == 4 * 8
    assert config.WRITER_BLOCK_SIZE == 2**20


@patch.dict(
    os.environ,
    {
        "MEGFILE_READER_BLOCK_SIZE": "0",
    },
)
def test_config_error():
    with pytest.raises(ValueError):
        from megfile import config

        importlib.reload(config)


@patch.dict(
    os.environ,
    {
        "MEGFILE_WRITER_BLOCK_SIZE": "0",
    },
)
def test_config_error2():
    with pytest.raises(ValueError):
        from megfile import config

        importlib.reload(config)

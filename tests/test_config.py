import importlib
import os

import pytest
from mock import patch


@patch.dict(
    os.environ,
    {
        "MEGFILE_READER_MAX_BUFFER_SIZE": str(4 * 8 * 2**20),
        "MEGFILE_WRITER_BLOCK_SIZE": str(2**20),
        "MEGFILE_WRITER_MAX_BUFFER_SIZE": "500Mi",
        "AWS_SECRET_ACCESS_KEY": "test",
    },
)
def test_config():
    from megfile import config

    importlib.reload(config)

    assert config.READER_MAX_BUFFER_SIZE // 2**20 == 4 * 8
    assert config.WRITER_BLOCK_SIZE == 2**20
    assert config.WRITER_MAX_BUFFER_SIZE == 500 * 2**20


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


def test_parse_quantity():
    from megfile.config import parse_quantity

    assert parse_quantity("1Mi") == 2**20
    assert parse_quantity("1M") == 10**6
    assert parse_quantity("1024") == 1024
    assert parse_quantity(1024) == 1024

    with pytest.raises(ValueError):
        parse_quantity("1ki")

    with pytest.raises(ValueError):
        parse_quantity("1kb")

    with pytest.raises(ValueError):
        parse_quantity("Mi")

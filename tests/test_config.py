import importlib
import os

from mock import patch


@patch.dict(
    os.environ, {
        "MEGFILE_MAX_BUFFER_SIZE": str(4 * 8 * 2**20),
        "MEGFILE_BLOCK_CAPACITY": "20",
        "MEGFILE_MAX_BLOCK_SIZE": str(2**20),
        "AWS_SECRET_ACCESS_KEY": "test"
    })
def test_config():
    from megfile import config
    importlib.reload(config)

    assert config.DEFAULT_MAX_BUFFER_SIZE // 2**20 == 4 * 8
    assert config.DEFAULT_BLOCK_CAPACITY == 4
    assert config.DEFAULT_MAX_BLOCK_SIZE == config.DEFAULT_BLOCK_SIZE


@patch.dict(os.environ, {
    "MEGFILE_BLOCK_CAPACITY": "20",
})
def test_config_only_capacity():
    from megfile import config
    importlib.reload(config)

    assert config.DEFAULT_MAX_BUFFER_SIZE // 2**20 == 20 * 8
    assert config.DEFAULT_BLOCK_CAPACITY == 20

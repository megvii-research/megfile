import os
import subprocess

import pytest

from megfile.interfaces import StatResult
from megfile.lib.fakefs import FakefsCacher
from tests.test_s3 import s3_empty_client

BUCKET = 'bucket'
KEY = 'key'

CONTENT = b'block0 block1 block2 block3 block4 '


@pytest.fixture
def client(s3_empty_client):
    s3_empty_client.create_bucket(Bucket=BUCKET)
    s3_empty_client.put_object(Bucket=BUCKET, Key=KEY, Body=CONTENT)
    return s3_empty_client


@pytest.mark.skip
def test_fakefs_stat(client):
    from megfile.lib._fakefs import fakefs

    cacher = FakefsCacher('s3://bucket/key')

    with cacher as path:
        assert path.startswith(fakefs.mountpoint)
        assert subprocess.check_output(['cat', path]) == CONTENT

    with pytest.raises(IsADirectoryError):
        path = os.path.join(fakefs.mountpoint, 'notExist')
        with open(path) as file:
            pass


@pytest.mark.skip
def test_fakefs_close(client):
    from megfile.lib._fakefs import fakefs

    assert os.path.ismount(fakefs.mountpoint) is True
    fakefs.stop()
    assert os.path.ismount(fakefs.mountpoint) is False
    fakefs.start()
    assert os.path.ismount(fakefs.mountpoint) is True

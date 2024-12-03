import sys
import tarfile

from megfile.smart import smart_open

path = sys.argv[1]

with smart_open(path, "rb") as file_object:
    with tarfile.open(mode="r", fileobj=file_object):
        pass

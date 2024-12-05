Define Custom Protocol
======================

Megfile support custom protocols. You can define your own protocol class like this:

```
# custom.py
import io
from typing import IO, AnyStr

from megfile.interfaces import URIPath
from megfile.smart_path import SmartPath

@SmartPath.register
class CustomPath(URIPath):

    protocol = "custom"

    def open(self, mode: str = 'rb', **kwargs) -> IO[AnyStr]:
        return io.BytesIO(b'test')

    ...
```

- `protocol = "custom"` is the name of your custom protocol. Then your path will be like `custom://path/to/file`.
- Implement methods
    - `URIPath` provide some properties and methods like `path_with_protocol`, `path_without_protocol`, `parts`, `parents` and you can use them. You can read more about them in [megfile.pathlike.URIPath](https://github.com/megvii-research/megfile/blob/main/megfile/pathlike.py#L819).
    - smart methods will call your `CustomPath`'s methods automatically, if you have implemented the corresponding method. For example: if you implement `CustomPath.open`, `smart_open` will call it when `path` is `custom://path/to/file`. You can find the corresponding class methods required for smart methods in [megfile.smart_path.SmartPath](https://github.com/megvii-research/megfile/blob/main/megfile/smart_path.py#L28).
- **You must import your custom python file before you use smart methods.** You must make the decorator `@SmartPath.register` effective. Like this:

```
from custom import CustomPath
from megfile import smart_open

with smart_open("custom://path/to/file", "rb") as f:
    assert f.read() == b'test'
```

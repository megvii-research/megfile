megfile - Megvii FILE library
---

[![build](https://github.com/megvii-research/megfile/actions/workflows/on-push.yaml/badge.svg?branch=main)](https://github.com/megvii-research/megfile/actions/workflows/on-push.yaml)
[![docs](https://github.com/megvii-research/megfile/actions/workflows/publish-docs.yml/badge.svg)](https://github.com/megvii-research/megfile/actions/workflows/publish-docs.yml)
[![Latest version](https://img.shields.io/pypi/v/megfile.svg)](https://pypi.org/project/megfile/)
[![Support python versions](https://img.shields.io/pypi/pyversions/megfile.svg)](https://pypi.org/project/megfile/)
[![License](https://img.shields.io/pypi/l/megfile.svg)](https://github.com/megvii-research/megfile/blob/master/LICENSE)

* Docs: http://megvii-research.github.io/megfile

`megfile` provides a silky operation experience with different backends (currently including local file system and s3), which enable you to focus more on the logic of your own project instead of the question of "Which backend is used for this file?"

`megfile` provides:

* Almost unified file system operation experience. Target path can be easily moved from local file system to s3.
* Complete boundary case handling. Even the most difficult (or even you can't even think of) boundary conditions, `megfile` can help you easily handle it.
* Perfect type hints and built-in documentation. You can enjoy the IDE's auto-completion and static checking.
* Semantic version and upgrade guide, which allows you enjoy the latest features easily.

`megfile`'s advantages are:

* `smart_open` can open resources that use various protocols, including fs, s3, http(s) and stdio. Especially, reader / writer of s3 in `megfile` is implemented with multi-thread, which is faster than known competitors.
* `smart_glob` is available on s3. And it supports zsh extended pattern syntax of `[]`, e.g. `s3://bucket/video.{mp4,avi}`.
* All-inclusive functions like `smart_exists` / `smart_stat` / `smart_sync`. If you don't find the functions you want, [submit an issue](https://github.com/megvii-research/megfile/issues).
* Compatible with `pathlib.Path` interface, referring to `S3Path` and `SmartPath`.

## Quick Start

Here's an example of writing a file to s3, syncing to local, reading and finally deleting it.

```python
from megfile import smart_open, smart_exists, smart_sync, smart_remove, smart_glob
from megfile.smart_path import SmartPath

# open a file in s3 bucket
with smart_open('s3://playground/refile-test', 'w') as fp:
    fp.write('refile is not silver bullet')

# test if file in s3 bucket exist
smart_exists('s3://playground/refile-test')

# copy files or directories
smart_sync('s3://playground/refile-test', '/tmp/playground')

# remove files or directories
smart_remove('s3://playground/refile-test')

# glob files or directories in s3 bucket
smart_glob('s3://playground/video-?.{mp4,avi}')

# or in local file system
smart_exists('/tmp/playground/refile-test')

# smart_open also support protocols like http / https
smart_open('https://www.google.com')

# SmartPath interface
path = SmartPath('s3://playground/megfile-test')
if path.exists():
    with path.open() as f:
        result = f.read(7)
        assert result == b'megfile'
```

### Configuration

Before using `megfile` to access files on s3, you need to set up authentication credentials for your s3 account using the AWS CLI or editing the file `~/.aws/config` directly.

```
[default]
aws_secret_access_key = xxx
aws_access_key_id = xxx

s3 =
    addressing_style = virtual
    endpoint_url = http://xxx
```

## Installation

### PyPI

```bash
pip3 install megfile
```

You can specify megfile version as well
```bash
pip3 install "megfile~=0.0"
```

### Build from Source

megfile can be installed from source
```bash
git clone git@github.com:megvii-research/megfile.git
cd megfile
pip3 install -U .
```

### Development Environment

```bash
git clone git@github.com:megvii-research/megfile.git
cd megfile
sudo apt install libgl1-mesa-glx libfuse-dev fuse
pip3 install -r requirements.txt -r requirements-dev.txt
```


## How to Contribute
* We welcome everyone to contribute code to the `megfile` project, but the contributed code needs to meet the following conditions as much as possible:
    
    *You can submit code even if the code doesn't meet conditions. The project members will evaluate and assist you in making code changes*

    * **Code format**: Your code needs to pass **code format check**. `megfile` uses `yapf` as lint tool and the version is locked at 0.27.0. The version lock may be removed in the future
    * **Static check**: Your code needs complete **type hint**. `megfile` uses `pytype` as static check tool. If `pytype` failed in static check, use `# pytype: disable=XXX` to disable the error and please tell us why you disable it.

        *Note* : Because `pytype` doesn't support variable type annation, the variable type hint format introduced by py36 cannot be used.
        > i.e. `variable: int` is invalid, replace it with `variable  # type: int`

    * **Test**: Your code needs complete **unit test** coverage. `megfile` uses `pyfakefs` and `moto` as local file system and s3 virtual environment in unit tests. The newly added code should have a complete unit test to ensure the correctness

* You can help to improve `megfile` in many ways:
    * Write code.
    * Improve [documentation](https://github.com/megvii-research/megfile/blob/main/docs).
    * Report or investigate [bugs and issues](https://github.com/megvii-research/megfile/issues).
    * If you find any problem or have any improving suggestion, [submit a new issuse](https://github.com/megvii-research/megfile/issues) as well. We will reply as soon as possible and evaluate whether to adopt.
    * Review [pull requests](https://github.com/megvii-research/megfile/pulls).
    * Star `megfile` repo.
    * Recommend `megfile` to your friends.
    * Any other form of contribution is welcomed.

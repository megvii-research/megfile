megfile - Megvii FILE library
---

[![Build](https://github.com/megvii-research/megfile/actions/workflows/run-tests.yml/badge.svg?branch=main)](https://github.com/megvii-research/megfile/actions/workflows/run-tests.yml)
[![Documents](https://github.com/megvii-research/megfile/actions/workflows/publish-docs.yml/badge.svg)](https://github.com/megvii-research/megfile/actions/workflows/publish-docs.yml)
[![Codecov](https://img.shields.io/codecov/c/gh/megvii-research/megfile)](https://app.codecov.io/gh/megvii-research/megfile/)
[![Latest version](https://img.shields.io/pypi/v/megfile.svg)](https://pypi.org/project/megfile/)
[![Support python versions](https://img.shields.io/pypi/pyversions/megfile.svg)](https://pypi.org/project/megfile/)
[![License](https://img.shields.io/pypi/l/megfile.svg)](https://github.com/megvii-research/megfile/blob/master/LICENSE)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/5233/badge)](https://bestpractices.coreinfrastructure.org/projects/5233)

* Docs: http://megvii-research.github.io/megfile

`megfile` provides a silky operation experience with different backends, which enable you to focus more on the logic of your own project instead of the question of "Which backend is used for this file?"

## Why megfile

* Same interfaces as the python standard library, low learning curve
* Faster file read and write operations
* Excellent error retry mechanism to help you handle network issues
* Supports popular protocols, even making it easy to use the same protocol with different endpoints
* Stable and secure, with high CI test coverage, used by multiple industry giants
* Perfect type hints and built-in documentation. You can enjoy the IDE's auto-completion and static checking

## Support Protocols
- fs(local filesystem)
- s3
- sftp
- http
- stdio
- hdfs: `pip3 install 'megfile[hdfs]'`
- webdav: `pip3 install 'megfile[webdav]'`

## Quick Start

The interfaces of `megfile` correspond to those in the Python standard library. For example, `open` -> `smart_open` and `pathlib.Path` -> `SmartPath`. You only need to [configure the protocol settings](https://megvii-research.github.io/megfile/configuration.html) and provide the path in the corresponding format to use them conveniently.

Path string in `megfile` almost is `protocol://path/to/file`, for example `s3://bucketA/key`. More details see [path format document](https://megvii-research.github.io/megfile/path_format.html).

Here's an example of writing a file to s3 / fs, syncing to local, reading and finally deleting it.

### Functional Interface
```python
from megfile import smart_open, smart_exists, smart_sync, smart_remove, smart_glob

# open a file in s3 bucket
with smart_open('s3://playground/megfile-test', 'w') as fp:
    fp.write('megfile is not silver bullet')

# test if file in s3 bucket exist
smart_exists('s3://playground/megfile-test')

# or in local file system
smart_exists('/tmp/playground/megfile-test')

# copy files or directories
smart_sync('s3://playground/megfile-test', '/tmp/playground/megfile-test')

# remove files or directories
smart_remove('s3://playground/megfile-test')

# glob files or directories in s3 bucket
smart_glob('s3://playground/megfile-?.{mp4,avi}')
```

### SmartPath Interface

`SmartPath` has a similar interface with `pathlib.Path`.

```python
from megfile.smart_path import SmartPath

path = SmartPath('s3://playground/megfile-test')
if path.exists():
    with path.open() as f:
        result = f.read(7)
        assert result == b'megfile'
```

### Command Line Interface
```bash
$ pip install 'megfile[cli]'  # install megfile cli requirements

$ megfile --help  # see what you can do

$ megfile ls s3://playground/
$ megfile ls -l -h s3://playground/

$ megfile cat s3://playground/megfile-test

$ megfile cp s3://playground/megfile-test /tmp/playground/megfile-test
```

## Installation

### PyPI

```bash
pip3 install megfile

# for cli support
pip3 install 'megfile[cli]'

# for hdfs support
pip3 install 'megfile[hdfs]'

# for webdav support
pip3 install 'megfile[webdav]'
```

## Configuration

Using `s3` as an example, the following describes the configuration methods. For more details, please refer to [Configuration](https://megvii-research.github.io/megfile/configuration.html).

You can use environments and configuration file for configuration, and priority is that environment variables take precedence over configuration file.

### Use environments
You can use environments to setup authentication credentials for your `s3` account:
- `AWS_ACCESS_KEY_ID`: access key
- `AWS_SECRET_ACCESS_KEY`: secret key
- `AWS_SESSION_TOKEN`: session token
- `OSS_ENDPOINT` / `AWS_ENDPOINT_URL_S3` / `AWS_ENDPOINT_URL`: endpoint url of s3
- `AWS_S3_ADDRESSING_STYLE`: addressing style

### Use command
You can update config file with `megfile` command easyly:
[megfile config s3 [OPTIONS] AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY](https://megvii-research.github.io/megfile/cli.html#megfile-config-s3) 

```
$ megfile config s3 accesskey secretkey

# for aliyun oss
$ megfile config s3 accesskey secretkey \
--addressing-style virtual \
--endpoint-url http://oss-cn-hangzhou.aliyuncs.com
```

You can get the configuration from `~/.aws/credentials`, like:
```
[default]
aws_secret_access_key = accesskey
aws_access_key_id = secretkey

s3 =
    addressing_style = virtual
    endpoint_url = http://oss-cn-hangzhou.aliyuncs.com
```

### Create aliases
```
# for volcengine tos
$ megfile config s3 accesskey secretkey \
--addressing-style virtual \
--endpoint-url https://tos-s3-cn-beijing.ivolces.com \
--profile-name tos

# create alias
$ megfile config alias tos s3+tos
```

You can get the configuration from `~/.config/megfile/aliases.conf`, like:
```
[tos]
protocol = s3+tos
```

You can use alias in path, like `tos://bucket/key`, the same as `s3+tos://bucket/key`.

## Benchmark
[![10GiB](https://github.com/megvii-research/megfile/blob/main/scripts/benchmark/10GiB.png?raw=true)](https://megvii-research.github.io/megfile/benchmark.html)
[![10MiB](https://github.com/megvii-research/megfile/blob/main/scripts/benchmark/10MiB.png?raw=true)](https://megvii-research.github.io/megfile/benchmark.html)

## How to Contribute
* We welcome everyone to contribute code to the `megfile` project, but the contributed code needs to meet the following conditions as much as possible:

    *You can submit code even if the code doesn't meet conditions. The project members will evaluate and assist you in making code changes*

    * **Code format**: Your code needs to pass **code format check**. `megfile` uses `ruff` as lint tool
    * **Static check**: Your code needs complete **type hint**. `megfile` uses `pytype` as static check tool. If `pytype` failed in static check, use `# pytype: disable=XXX` to disable the error and please tell us why you disable it.

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

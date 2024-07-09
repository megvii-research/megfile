Hdfs Configuration
==================

Please use command `pip install 'megfile[hdfs]'` to install hdfs requirements.
You can use environments and configuration file for configuration, and priority is that environment variables take precedence over configuration file.

### Use environments
You can use environments to setup authentication credentials and other configuration items:
- `HDFS_USER`: hdfs user
- `HDFS_URL`: The url can be configured to support High Availability namenodes of **WebHDFS**, simply add more URLs by delimiting with a semicolon (`;`)
- `HDFS_ROOT`: hdfs root directory when using relative path
- `HDFS_TIMEOUT`: request hdfs server timeout
- `HDFS_TOKEN`: hdfs token if hdfs server require
- `HDFS_CONFIG_PATH`: hdfs config file, default is `~/.hdfscli.cfg`
- `MEGFILE_HDFS_MAX_RETRY_TIMES`: hdfs request max retry times when catch error which may fix by retry, default is `10`

### Use command
You can update config file with `megfile` command easyly:
[megfile config hdfs [OPTIONS] URL](https://megvii-research.github.io/megfile/cli.html#megfile-config-hdfs) 

```bash
$ megfile config hdfs http://127.0.0.1:50070 --user admin --root '/' --token xxx
```

You can get the configuration from `~/.hdfscli.cfg`, like:
```ini
[global]
default.alias = default

[default.alias]
url = http://127.0.0.1:50070
user = admin
root = /
token = xxx
```
Most information about configuration file: https://hdfscli.readthedocs.io/en/latest/quickstart.html#configuration

### Config for different hdfs server
You can operate hdfs files in different hdfs server.
For example, you have two hdfs server with different url. With configuration, you can use path with profile name like `hdfs+profile_name://bucket/key` to operate different hdfs server:
```python
from megfile import smart_sync

smart_sync('hdfs+profile1://path/to/file', 'hdfs+profile2://path/to/file')
```

#### Using environment
You need use `PROFILE_NAME__` prefix, like: 

- `PROFILE1__HDFS_USER`
- `PROFILE1__HDFS_URL`
- `PROFILE1__HDFS_ROOT`
- `PROFILE1__HDFS_TIMEOUT`
- `PROFILE1__HDFS_TOKEN`

#### Using command:
```bash
megfile config hdfs http://127.0.0.1:8000 --user admin \
--root /b --token aaa --profile-name profile1

megfile config hdfs http://127.0.0.1:8001 --user admin \
--root /a --token bbb --profile-name profile2
```

Then the configuration file's content will be:

```ini
[global]
default.alias = default

[default.alias]
url = http://127.0.0.1:8000
user = admin
root = /a
token = aaa

[test.alias]
url = http://127.0.0.1:8001
user = admin
root = /b
token = bbb
```

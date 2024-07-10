Path Format
===========

**In path, brackets** `[]` **means this part is optional.**

### fs
- An integer file descriptor of the file, e.g. `0`
- Absolute or relative file path, e.g. `/root`, `root`
- File path with protocol, e.g. `file://root`

### s3
- `s3[+profile_name]://bucket/key`

### http
- Http uri, e.g. `https://megvii-research.github.io/megfile/`

##### set cookies, headers and other parameters
You can set `cookies`, `headers` and other parameters of `requests.request` in `HttpPath`'s property `request_kwargs`, like:

```
from megfile import HttpPath, smart_copy

url = HttpPath('https://megvii-research.github.io/megfile/')
url.request_kwargs = {
    'cookies': {'key': 'value'}, 
    'headers': {'key': 'value'},
}
smart_copy(url, 'index.html')
```

### stdio
- `stdio://-`
- `stdio://0`
- `stdio://1`
- `stdio://2`

### sftp

Relative path will be assumed relative to the directory that is setted by sftp server.

- Absolute path: `sftp://[username[:password]@]hostname[:port]//file_path`
- Relative path: `sftp://[username[:password]@]hostname[:port]/file_path`

### hdfs

If root is relative or unset, the relative path will be assumed relative to the user’s home directory.

- Absolute path: `hdfs[+profile_name]:///path/to/file`
- Relative path: `hdfs[+profile_name]://path/to/file`

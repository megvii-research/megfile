**In path, brackets** `[]` **means this part is optional.**

### fs
- An integer file descriptor of the file, e.g. `0`
- Absolute or relative file path, e.g. `/root`, `root`
- File path with protocol, e.g. `file://root`

### s3
- `s3[+profile_name]://bucket/key`

### http
- Http uri, e.g. `https://www.google.com`

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

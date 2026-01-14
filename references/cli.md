# Command Line Interface

## megfile

Client for megfile.

If you install megfile with `--user`,
you also need configure `$HOME/.local/bin` into `$PATH`.

### Usage

```shell
megfile [OPTIONS] COMMAND [ARGS]...
```

### Options

### --debug

Enable debug mode.

### --log-level <log_level>

Set logging level.

* **Options:**
  DEBUG | INFO | WARNING | ERROR

### cat

Concatenate any files and send them to stdout.

### Usage

```shell
megfile cat [OPTIONS] PATH
```

### Arguments

### PATH

Required argument

### completion

Return the completion file

### Usage

```shell
megfile completion [OPTIONS] COMMAND [ARGS]...
```

#### bash

Update the config file for bash

### Usage

```shell
megfile completion bash [OPTIONS]
```

#### fish

Update the config file for fish

### Usage

```shell
megfile completion fish [OPTIONS]
```

#### zsh

Update the config file for zsh

### Usage

```shell
megfile completion zsh [OPTIONS]
```

### config

Return the config file

### Usage

```shell
megfile config [OPTIONS] COMMAND [ARGS]...
```

#### alias

Update the config file for aliases

### Usage

```shell
megfile config alias [OPTIONS] NAME PROTOCOL_OR_PATH
```

### Options

### -p, --path <path>

megfile config file, default is ~/.config/megfile/megfile.conf

### --no-cover

Not cover the same-name config

### Arguments

### NAME

Required argument

### PROTOCOL_OR_PATH

Required argument

#### env

Update the config file for envs

### Usage

```shell
megfile config env [OPTIONS] EXPR
```

### Options

### -p, --path <path>

megfile config file, default is ~/.config/megfile/megfile.conf

### --no-cover

Not cover the same-name config

### Arguments

### EXPR

Required argument

#### hdfs

Update the config file for hdfs

### Usage

```shell
megfile config hdfs [OPTIONS] URL
```

### Options

### -p, --path <path>

hdfs config file, default is $HOME/.hdfscli.cfg

### -n, --profile-name <profile_name>

hdfs config file

### -u, --user <user>

user name

### -r, --root <root>

hdfs path’s root dir

### -t, --token <token>

token for requesting hdfs server

### -o, --timeout <timeout>

request hdfs server timeout, default 10

### --no-cover

Not cover the same-name config

### Arguments

### URL

Required argument

#### s3

Update the config file for s3

### Usage

```shell
megfile config s3 [OPTIONS] AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY
```

### Options

### -p, --path <path>

s3 config file, default is $HOME/.aws/credentials

### -n, --profile-name <profile_name>

s3 config file

### -e, --endpoint-url <endpoint_url>

endpoint-url

### -st, --session-token <session_token>

session-token

### -as, --addressing-style <addressing_style>

addressing-style

### -sv, --signature-version <signature_version>

signature-version

### --no-cover

Not cover the same-name config

### Arguments

### AWS_ACCESS_KEY_ID

Required argument

### AWS_SECRET_ACCESS_KEY

Required argument

### cp

Copy files from source to dest, skipping already copied.

### Usage

```shell
megfile cp [OPTIONS] SRC_PATH DST_PATH
```

### Options

### -r, --recursive

Command is performed on all files or objects under the specified directory or prefix.

### -T, --no-target-directory

treat dst_path as a normal file.

### -g, --progress-bar

Show progress bar.

### --skip

Skip existed files.

### Arguments

### SRC_PATH

Required argument

### DST_PATH

Required argument

### edit

Edit the file.

### Usage

```shell
megfile edit [OPTIONS] PATH
```

### Options

### -e, --editor <editor>

Editor to use.

### Arguments

### PATH

Required argument

### head

Concatenate any files and send first n lines of them to stdout.

### Usage

```shell
megfile head [OPTIONS] PATH
```

### Options

### -n, --lines <lines>

print the first NUM lines

### Arguments

### PATH

Required argument

### ll

List all the objects in the path.

### Usage

```shell
megfile ll [OPTIONS] PATH
```

### Options

### -f, --full

Displays the full path of each file.

### -r, --recursive

Command is performed on all files or objects under the specified directory or prefix.

### Arguments

### PATH

Required argument

### ls

List all the objects in the path.

### Usage

```shell
megfile ls [OPTIONS] PATH
```

### Options

### -l, --long

List all the objects in the path with size, modification time and path.

### -f, --full

Displays the full path of each file.

### -r, --recursive

Command is performed on all files or objects under the specified directory or prefix.

### -h, --human-readable

Displays file sizes in human readable format.

### Arguments

### PATH

Required argument

### md5sum

Produce an md5sum file for all the objects in the path.

### Usage

```shell
megfile md5sum [OPTIONS] PATH
```

### Arguments

### PATH

Required argument

### mkdir

Make the path if it doesn’t already exist.

### Usage

```shell
megfile mkdir [OPTIONS] PATH
```

### Arguments

### PATH

Required argument

### mtime

Return the mtime and number of objects in remote:path.

### Usage

```shell
megfile mtime [OPTIONS] PATH
```

### Arguments

### PATH

Required argument

### mv

Move files from source to dest.

### Usage

```shell
megfile mv [OPTIONS] SRC_PATH DST_PATH
```

### Options

### -r, --recursive

Command is performed on all files or objects under the specified directory or prefix.

### -T, --no-target-directory

treat dst_path as a normal file.

### -g, --progress-bar

Show progress bar.

### --skip

Skip existed files.

### Arguments

### SRC_PATH

Required argument

### DST_PATH

Required argument

### rm

Remove files from path.

### Usage

```shell
megfile rm [OPTIONS] PATH
```

### Options

### -r, --recursive

Command is performed on all files or objects under the specified directory or prefix.

### Arguments

### PATH

Required argument

### size

Return the total size and number of objects in remote:path.

### Usage

```shell
megfile size [OPTIONS] PATH
```

### Arguments

### PATH

Required argument

### stat

Return the stat and number of objects in remote:path.

### Usage

```shell
megfile stat [OPTIONS] PATH
```

### Arguments

### PATH

Required argument

### sync

Make source and dest identical, modifying destination only.

### Usage

```shell
megfile sync [OPTIONS] SRC_PATH DST_PATH
```

### Options

### -f, --force

Copy files forcible, ignore same files.

### --skip

Skip existed files.

### -w, --worker <worker>

Number of concurrent workers.

### -g, --progress-bar

Show progress bar.

### -v, --verbose

Show more progress log.

### -q, --quiet

Not show any progress log.

### Arguments

### SRC_PATH

Required argument

### DST_PATH

Required argument

### tail

Concatenate any files and send last n lines of them to stdout.

### Usage

```shell
megfile tail [OPTIONS] PATH
```

### Options

### -n, --lines <lines>

print the last NUM lines

### -f, --follow

output appended data as the file grows

### Arguments

### PATH

Required argument

### to

Write bytes from stdin to file.

### Usage

```shell
megfile to [OPTIONS] PATH
```

### Options

### -a, --append

Append to the given file

### -o, --stdout

File content to standard output

### Arguments

### PATH

Required argument

### touch

Make the file if it doesn’t already exist.

### Usage

```shell
megfile touch [OPTIONS] PATH
```

### Arguments

### PATH

Required argument

### version

Return the megfile version.

### Usage

```shell
megfile version [OPTIONS]
```

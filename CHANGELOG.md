## 2.1.1.post1 - 2023.07.04
- fix
    - fix dst_path check in SftpPath's methods

## 2.1.1 - 2023.07.03
- fix
    - fix `ls -r` not display directories
    - fix `SftpPath.cwd`
    - fix sftp exec command method's return data

## 2.1.0 - 2023.06.26
- feat
    - `smart_sync` will raise `IsADirectoryError` when src_path is a file and dst_path is a directory
- fix
    - fix `sftp_upload`, `sftp_download`, `sftp_copy` path check, when path is not sftp or is dir, will raise error
    - `sftp_copy` makedir if `dst_path`'s dir not exist

## 2.0.7 - 2023.06.16
- perf
    - retry ConnectionError in sftp retry

## 2.0.6.post1 - 2023.06.16
- fix
    - fix sftp retry bug when catch EOFError 
    - fix the bug from new `urlsplit` in py3.11.4
    - fix the path list's order returned by fs glob, now return path list in ascending alphabetical order

## 2.0.6 - 2023.06.13
- fix
    - fix sftp connect timeout after long time
- perf
    - `smart_sync` and other sync methods will ignore same files
    - `smart_sync` will raise `FileNotFound` error when src_path is not exist

## 2.0.5.post1 - 2023.05.11
- fix
    - fix `SftpPath.rename` error log

## 2.0.5 - 2023.05.11
- feat
    - support python 3.11
- perf
    - cli support s3 log
- fix
    - fix `is_dir` and `is_file` of `SftpPath` when file not found

## 2.0.4 - 2023.04.12
- feat
    - s3 path support custom profile name, like `s3[+profile_name]://bucket/key`
    - remove `smart-open` from requirements
    - `smart_sync` support `map_func` parameter for concurrent
    - add `smart_concat`
- perf
    - reduce the number of `s3_open`'s requests

## 2.0.3 - 2023.03.22
- feat
    - add smart_lstat
    - smart_scandir support ‘with’ operate
- fix
    - fix smart_sync error when file name in dir is empty str
    - fix stat properties default value
    - fix smart_load_content when path is not fs or s3

## 2.0.2 - 2023.03.13
- support s3 endpoint env: AWS_ENDPOINT

## 2.0.1 - 2023.03.01
- cli 
    - `megfile ls` support glob path
    - `megfile cp`, `megfile mv` and `megfile sync` support `-g`, `--progress-bar`
    - perf err output
    - `megfile sync` support glob path
- close ssh connection before process exit

## 2.0.0 - 2023.02.10
- path classes align with `pathlib.Path`
    - methods(`glob`, `iglob`, `glob_stat`, `resolve`, `home`, `cwd`, `readlink`) in all path classes return path object
    - methods(`glob`, `iglob`, `glob_stat`) in all path classes add `pattern` parameter. Functions(like `smart_glob`, `s3_glob`) not change.
    - `relative_to`'s parameter `other` in all path classes change to `*other`
    - `FSPath.parts` align with `pathlib.Path.parts`, return value `parts[0]` will not be `file://` any more.
    - `mkdir` in all path classes add parameters(`mode=0o777`, `parents=False`), Functions(like `smart_makedirs`) not change.
- change `s3_symlink`, `S3Path.symlink`, `s3_rename`, `S3Path.rename` parameter name, change `src_url`, `dst_url` to `src_path`, `dst_path`
- change `fs_stat`, `FSPath.stat`, `s3_stat`, `S3Path.stat` parameter name, change `followlinks` to `follow_symlinks`
- `FileEntry` add method `inode`
- `StatResult` add properties(`st_mode`, `st_ino`, `st_dev`, `st_nlink`, `st_uid`, `st_gid`, `st_size`, `st_atime`, `st_mtime`, `st_ctime`, `st_atime_ns`, `st_mtime_ns`, `st_ctime_ns`)
- support sftp protocol

## 1.0.2 - 2022.09.22
- remove `smart_getmd5_by_paths` method
- retry when catch `botocore.exceptions.ResponseStreamingError`
- remove `followlinks` parameter in rename, move, remove; make behavior same as standard library
- fix `smart_rename` bug, when rename file cross platform or device

## 1.0.1 - 2022.08.04
- fix open mode with + in different order
- sort `smart_getmd5_by_paths` parameter paths

## 1.0.0 - 2022.07.25
- refactor code
- add `smart_getmd5_by_paths`
- change of symlink's parameters position

## 0.1.2 - 2022.04.26
- handle s3 remove file errors
- support s3 symlink

## 0.1.1 - 2022.01.14
- fix smart api bug

## 0.1.0 - 2022.01.14

- update get_md5, s3 use etag and support dir
- fix py35 test about moto
- add fs symlink support
- support python 3.10

## 0.0.11 - 2021.12.08

- `smart_open` support read and write pipe

## 0.0.10 - 2021.11.29

- add info log about environ OSS_ENDPOINT and oss config file
- `smart_getsize` and `smart_getmtime` support http
- update cli cp and mv, make them like cp and mv in linux
- fix sed warning in macOS
- add some test code
- add error code callback to _patch_make_request
- Generate cache_path automatically

## 0.0.9 - 2021.10.11

- `megfile.s3` retries when server returns 500 - 503
- remove `megfile.lib.fakefs`

## 0.0.8 - 2021.09.15

- `megfile.s3.s3_memory_open` support ab / rb+ / wb+ / ab+ mode
- `megfile.s3.s3_open` support ab / rb+ / wb+ / ab+ mode (by using s3_memory_open)
- Speed up `s3_glob`
- Accept `s3.endpoint_url` in aws config file

## 0.0.7 - 2021.09.06

- __[Breaking]__ Rename `megfile.interfaces.MegfilePathLike` to `megfile.interfaces.PathLike`
- Fix ungloblize

## 0.0.6 - 2021.09.01

- __[Breaking]__ Rename `megfile.s3.MEGFILE_MD5_HEADER` to `megfile.s3.content_md5_header`
- __[Breaking]__ Remove `megfile.lib.get_image_size`, `megfile.smart.smart_load_image_metadata` and `megfile.smart.IMAGE_EXTNAMES`

## 0.0.5 - 2021.08.31

- Refactor `process_local` / `thread_local`, remove dependency on  `multiprocessing.utils.register_after_fork`

## 0.0.4 - 2021.08.29

- Speed up `s3_glob`

## 0.0.3 - 2021.08.24

- First release of `megfile`

CHANGELOG
=========

## 0.0.10 - 2021.11.29

- add info log about environ OSS_ENDPOINT and oss config file
- smart_getsize and smart_getmtime support http
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

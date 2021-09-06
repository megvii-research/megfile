CHANGELOG
=========

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

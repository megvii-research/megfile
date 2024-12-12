Common Configuration
====================

### Environment configurations

All size unit is bytes, and support kubernetes canonical form quantity like `200Mi`.

- `MEGFILE_READER_BLOCK_SIZE`: default block size of read operate, unit is bytes, default is `8Mi`
- `MEGFILE_READER_MAX_BUFFER_SIZE`: max read buffer size, unit is bytes, default is `128Mi`
- `MEGFILE_WRITER_BLOCK_SIZE`:
    - default block size of write operate, unit is bytes, default is `8Mi`
    - In S3, the block size automatically increases with the amount of data written if you don’t set `MEGFILE_WRITER_BLOCK_SIZE` and don’t set `MEGFILE_WRITER_BLOCK_AUTOSCALE` to false. The largest file size you can write under these conditions is `500Gi`. If you need to write a larger file to S3, you should set a larger block size. Note that AWS S3's multipart upload supports a maximum of 10,000 parts, so the maximum supported file size is `MEGFILE_WRITE_BLOCK_SIZE` * 10,000.
- `MEGFILE_WRITER_MAX_BUFFER_SIZE`: max write buffer size, unit is bytes, default is `128Mi`
- `MEGFILE_WRITER_BLOCK_AUTOSCALE`: whether to automatically increase the block size; the default is `true`. However, if you set `MEGFILE_WRITER_BLOCK_SIZE`, it will be set to `false`.
- `MEGFILE_MAX_WORKERS`: max threads will be used, default is `8`
- `MEGFILE_MAX_RETRY_TIMES`: default max retry times when catch error which may fix by retry, default is `10`.

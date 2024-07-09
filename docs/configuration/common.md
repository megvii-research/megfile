Common Configuration
====================

### Environment configurations
- `MEGFILE_BLOCK_SIZE`: default block size of read and write operate, unit is bytes, default is `8MB`
- `MEGFILE_MIN_BLOCK_SIZE`: 
    - min write block size, unit is bytes, default is equal to `MEGFILE_BLOCK_SIZE`
    - If you need write big size file, you should set `MEGFILE_MIN_BLOCK_SIZE` to a big value.
- `MEGFILE_MAX_BLOCK_SIZE`: max write block size, unit is bytes, default is `128MB`
- `MEGFILE_MAX_BUFFER_SIZE`: max read buffer size, unit is bytes, default is `128MB`
- `MEGFILE_MAX_WORKERS`: max threads will be used, default is `32`
- `MEGFILE_BLOCK_CAPACITY`: 
    - default cache capacity of block, default is `16`
    - if `MEGFILE_MAX_BUFFER_SIZE` and `MEGFILE_BLOCK_CAPACITY` are both set, `MEGFILE_BLOCK_CAPACITY` will be ignored
- `MEGFILE_S3_CLIENT_CACHE_MODE`: s3 client cache mode, `thread_local` or `process_local`, default is `thread_local`, **it's a experimental feature.**
- `MEGFILE_MAX_RETRY_TIMES`: default max retry times when catch error which may fix by retry, default is `10`

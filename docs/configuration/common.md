Common Configuration
====================

### Environment configurations
- `MEGFILE_READER_BLOCK_SIZE`: default block size of read operate, unit is bytes, default is `8MB`
- `MEGFILE_READER_MAX_BUFFER_SIZE`: max read buffer size, unit is bytes, default is `128MB`
- `MEGFILE_WRITER_BLOCK_SIZE`:
    - default block size of write operate, unit is bytes, default is `32MB`
    - If you need write big size oss file, you should set to a big value.
- `MEGFILE_WRITER_MAX_BUFFER_SIZE`: max write buffer size, unit is bytes, default is `128MB`
- `MEGFILE_MAX_WORKERS`: max threads will be used, default is `8`
- `MEGFILE_MAX_RETRY_TIMES`: default max retry times when catch error which may fix by retry, default is `10`.

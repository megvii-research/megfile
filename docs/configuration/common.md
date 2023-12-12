### Environment configurations
- `MEGFILE_BLOCK_SIZE`: block size in some `open` func, like `http_open`, `s3_open`, default is `8MB`
- `MEGFILE_MAX_BLOCK_SIZE`: max block size in some `open` func, like `http_open`, `s3_open`, default is `block size * 16`
- `MEGFILE_MAX_BUFFER_SIZE`: max buffer size in some `open` func, like `http_open`, `s3_open`, default is `block size * 16`
- `MEGFILE_MAX_WORKERS`: max threads will be used, default is `32`
- `MEGFILE_BLOCK_CAPACITY`: default cache capacity of block and concurrency, default is `16`
- `MEGFILE_S3_CLIENT_CACHE_MODE`: s3 client cache mode, `thread_local` or `process_local`, default is `thread_local`, **it's a experimental feature.**

## Benchmark

Benchmark was performed on an 8-core, 32G RAM virtual machine, using docker-compose to start MinIO with a rate limit of 100 MiB/s per connection.

[Script and related files](https://github.com/megvii-research/megfile/tree/main/scripts/benchmark)

### version

```
megfile==4.0.1
iopath==0.1.10
pyarrow==18.1.0
s3fs==2024.10.0
smart_open==7.0.5
```

### Result

![10GiB](https://github.com/megvii-research/megfile/blob/main/scripts/benchmark/10GiB.png?raw=true)
![10MiB](https://github.com/megvii-research/megfile/blob/main/scripts/benchmark/10MiB.png?raw=true)

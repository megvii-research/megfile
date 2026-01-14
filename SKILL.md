---
name: megfile
description: Use megfile’s APIs, SmartPath, configuration, and CLI to perform filesystem tasks easy across local FS, S3/OSS-style object storage, SFTP, WebDAV, HTTP, HDFS, and stdio.
---

# Megfile

## Overview

Guide to using megfile’s unified APIs, SmartPath, configuration, and CLI for file operations across various backends, like local FS, S3/OSS-compatible object storage, SFTP, WebDAV, HTTP, HDFS, and stdio.

## Quick Start
- Install base: `pip install megfile`; add extras per backend (`megfile[cli]`, `megfile[hdfs]`, `megfile[webdav]`).
- Configure credentials/endpoints (env vars > config files). S3 examples: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional `AWS_ENDPOINT_URL` / `OSS_ENDPOINT` / `AWS_ENDPOINT_URL_S3`, `AWS_S3_ADDRESSING_STYLE`.
- Path format: `protocol://bucket/key` or alias (e.g., `tos://`); bare POSIX paths are treated as `file://`.
- Import functional APIs (`from megfile import smart_open, smart_sync, ...`) or SmartPath (`from megfile.smart_path import SmartPath`).

## Supported Protocols & Extras
- Local FS (`file://` or bare paths) — base install.
- S3/OSS-compatible (`s3://`, plus aliases) — base install.
- SFTP (`sftp://`) — install `megfile[cli]` or `megfile` with SFTP deps.
- HTTP/HTTPS (`http://`, `https://`) — base install.
- Stdio (`stdio://`) — base install.
- HDFS (`hdfs://`) — install `megfile[hdfs]`.
- WebDAV (`webdav://`) — install `megfile[webdav]`.
- Full Protocol path format reference: `references/path_format.md`.

## Core Tasks

### File IO
- `smart_open(path, mode='r', encoding=None, **options)`: open binary/text handles.
- Convenience loaders/savers: `smart_load_content` / `smart_save_content` (bytes), `smart_load_text` / `smart_save_text` (str), `smart_save_as(file_obj, path)`, `smart_load_from(path)` returns BinaryIO.
- `smart_combine_open(glob, mode='rb', open_func=smart_open)`: sequentially reads multiple files as one stream.

### Existence & Metadata
- `smart_exists`, `smart_isfile`, `smart_isdir`, `smart_islink`, `smart_isabs`.
- `smart_access(path, mode=Access.READ/WRITE)`.
- `smart_stat` / `smart_lstat` (via SmartPath), `smart_getsize`, `smart_getmtime`, `smart_getmd5(recalculate=False, followlinks=False)`.

### Listing & Globbing
- Directory traversal: `smart_listdir`, `smart_scandir`, `smart_walk`, `smart_scan`, `smart_scan_stat` (returns FileEntry/StatResult).
- Pattern matching: `smart_glob`, `smart_iglob`, `smart_glob_stat` for stat-rich globbing.

### Data Transfer & Lifecycle
- Copy/sync: `smart_copy`, `smart_sync`, `smart_sync_with_progress` (progress-friendly wrapper), `smart_concat` to merge multiple sources.
- Moves/deletes: `smart_move`, `smart_rename`, `smart_remove`, `smart_unlink`, `smart_touch`, `smart_makedirs`.
- Links: `smart_symlink`, `smart_readlink`.

### Path Utilities
- `smart_path_join`, `smart_abspath`, `smart_realpath`, `smart_relpath`, `smart_isabs`.
- SmartPath mirrors pathlib semantics but routes to the right backend: `path = SmartPath("s3://bucket/key"); path.exists(); path.open(mode="rb")`.

### Caching
- `smart_cache(path, cacher=SmartCacher, **options)`: cache remote resources locally for tools that only support local files. 

## Configuration
- Use CLI helpers to persist credentials/endpoints; environment variables take precedence.
- Profiles enable multiple endpoints (e.g., `s3+prod://...`). See `references/configuration/` for protocol-specific flags and env vars.
- Full config reference: `references/configuration/`.

## CLI Essentials
- Install CLI extras: `pip install 'megfile[cli]'`.
- Common commands (ls/cp/sync/stat/md5sum/mkdir/rm/touch) mirror POSIX semantics across backends.
- Completion scripts: `megfile completion zsh`.
- Full command list and flags: `references/cli.md`.

## Usage Notes
- Prefer smart_* for protocol-agnostic code paths; avoid branching per backend.
- Ensure required extras are installed for target protocols before invoking APIs.
- For high-volume sync/copy, supply `map_func` (e.g., `ThreadPoolExecutor.map`) and `callback` to report progress.
- Use aliases via `megfile config alias <alias> <protocol>` to shorten paths (e.g., `tos://`).

## References
- API surface: `references/megfile.smart.md` and `references/megfile.smart_path.md`.
- Configuration flags, env vars, and profiles: `references/configuration/`
- CLI commands and flags: `references/cli.md`
- Full Protocol path format reference: `references/path_format.md`.
- Glob patterns reference: `references/advanced/glob.md`.

WebDAV Configuration
==================

Megfile does not support locking for now, because the current package in use does not support locking. Currently, `webdavclient3` is the best available option. In the future, we may consider coding our own implementation or adopting a better package.

### Use environments
You can use environments to setup authentication credentials:

- `WEBDAV_USERNAME`
- `WEBDAV_PASSWORD`
- `WEBDAV_TOKEN`
- `WEBDAV_TOKEN_COMMAND`: command to refresh token
- `WEBDAV_TIMEOUT`: timeout setting for webdav client, default is `30` seconds

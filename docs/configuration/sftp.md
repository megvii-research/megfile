Sftp Configuration
==================

Sftp is a little different from other protocols, because you can set some configurations in path(`sftp://[username[:password]@]hostname[:port]/file_path`). **But we suggest you not to use password in path.** You can use environments setting configuration, and priority is that path settings take precedence over environments.

### Use environments
You can use environments to setup authentication credentials:

- `SFTP_USERNAME`
- `SFTP_PASSWORD`
- `SFTP_PRIVATE_KEY_PATH`: ssh private key path
- `SFTP_PRIVATE_KEY_TYPE`: algorithm of ssh key
- `SFTP_PRIVATE_KEY_PASSWORD`: if don't have passwd, not set this environment
- `SFTP_MAX_UNAUTH_CONN`: this enviroment is about sftp server's MaxStartups configuration, for connect to sftp server concurrently.
- `MEGFILE_SFTP_MAX_RETRY_TIMES`: sftp request max retry times when catch error which may fix by retry, default is `10`
- `MEGFILE_SFTP_HOST_KEY_POLICY`: defining the policy when the SSH server's hostname is not in either the system host keys or the application's keys. Value should be one of `auto`, `reject`, `warning`, default is `reject`.
    - `auto`: Automatically add the host key
    - `reject`: Reject the connection
    - `warning`: Warn the user, but allow the connection

Sftp is a little different from other protocols, because you can set some configurations in path(`sftp://[username[:password]@]hostname[:port]/file_path`). **But we suggest you not to use password in path.** You can use enviroments setting configuration, and priority is that path settings take precedence over enviroments.

### Use enviroments
You can use enviroments to setup authentication credentials:

- `SFTP_USERNAME`
- `SFTP_PASSWORD`
- `SFTP_PRIVATE_KEY_PATH`: ssh private key path
- `SFTP_PRIVATE_KEY_TYPE`: algorithm of ssh key
- `SFTP_PRIVATE_KEY_PASSWORD`: if don't have passwd, not set this environment
- `SFTP_MAX_UNAUTH_CONN`: this enviroment is about sftp server's MaxStartups configuration, for connect to sftp server concurrently.

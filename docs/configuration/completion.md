Auto Completion
===============

### Environment configurations

Megfile provides command-line auto-completion, which can be written into the corresponding shell configuration file by using the `megfile completion your_shell` command.

For example, using `megfile completion zsh` can write the completion command into `~/.zshrc`. After opening a new shell, the auto-completion feature will be available.

```bash
megfile completion your_shell

# zsh, for example
megfile completion zsh

# open a new shell
megfile ls [TAB]  # will show avaiables paths
```

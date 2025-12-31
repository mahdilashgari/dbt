# Improving Autocomplete on macOS for Zsh

This guide will help you set up and improve autocomplete on macOS for Zsh, including support for dbt.

## Step 1: Install `zsh-autocomplete`

Install `zsh-autocomplete` using Homebrew:

```sh
brew install zsh-autocomplete
```

`zsh-autocomplete` provides real-time type-ahead completion for Zsh.

For more details, see the [zsh-autocomplete GitHub page](https://github.com/marlonrichert/zsh-autocomplete)

## Step 2: Configure `zsh-autocomplete`

Add the following line at the **top** of your `.zshrc` file (before any calls to `compdef`):

```sh
source $HOMEBREW_PREFIX/share/zsh-autocomplete/zsh-autocomplete.plugin.zsh
```

Remove any calls to `compinit` from your `.zshrc` file.

## Step 3: Add dbt Autocomplete

Download the dbt completion script:

```sh
mkdir -p ~/.zsh/completions
curl -o ~/.zsh/completions/_dbt https://raw.githubusercontent.com/dbt-labs/dbt-completion.bash/master/_dbt
```

Add the path to your completions folder in your `.zshrc` file at a second line:

```sh
fpath=(~/.zsh/completions $fpath)
```

For more details, see the [dbt-completion GitHub page](https://github.com/dbt-labs/dbt-completion.bash).

## Step 4: Rebuild Zsh Completion Cache

After saving the changes, rebuild the Zsh completion cache to recognize the new `_dbt` file:

```sh
rm -f ~/.zcompdump; zsh
```

## Step 5: Restart Zsh

Restart your Zsh session to apply the changes.

```sh
exec zsh
```

You should now have improved autocomplete on macOS for Zsh, including support for dbt.
# Plugin commands

`flz plugin` installs plugin packages, enables plugins and connects relays
to them. What a plugin is, and how its settings and hooks work, is explained in
[`plugins.md`](../plugins/plugins.md); this page shows the commands.

Each section shows, for the verbs used most, a command with its answer.
Unless a section names another page, the answers
come from the echo of [Up and running](../../getting_started/up_and_running.md#run-the-echo).
How `flz` reaches the controller, its options and its exit codes are on the
[CLI](cli.md) page, and every verb in one list is the
[command reference](command_reference.md).

## `flz plugin`

Installs plugin packages, enables plugins and connects relays to them. Explained in [`plugins.md`](../plugins/plugins.md), [`hooks.md`](../plugins/hooks.md), [`installing_plugins.md`](../plugins/installing_plugins.md), [Connect a plugin](../../getting_started/connect_a_plugin.md).

`flz plugin help` prints the group's verbs, and the [command
reference](command_reference.md) lists each with its usage line. It also
explains plugins, packages and hooks, with examples.

`flz plugin help <provider>` lists one provider's settings and hooks, and
`flz plugin list --providers` the providers there are. The echo's file plugins
from [`plugin_examples.md`](../plugins/plugin_examples.md#a-file-in-a-file-out): an output that writes everything the
echo emits to a file, then the plugins and their relays, then the plugin
taken away again:

```sh
$ flz plugin enable writer relay_file_output --relay_id=my_echo --hook=on_output --Xpath=echo-out.txt
```

```output
Enabling plugin...
Plugin writer enabled.
Plugin writer bound to my_echo.
```

```sh
$ flz relay send my_echo --text hello
```

```output
Delivered to 'my_echo'.
```

```sh
$ cat storage/app/files/default/echo-out.txt
```

```output
echo: hello
```

```sh
$ flz plugin list
```

```output
writer  file_sink  bound: my_echo  runs on: flezha@127.0.0.1
```

```sh
$ flz plugin unbind writer --relay_id=my_echo
```

```output
Plugin writer unbound from my_echo.
```

```sh
$ flz plugin disable writer
```

```output
Plugin writer disabled.
```

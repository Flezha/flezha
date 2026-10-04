# Plugin commands

`flz plugin` installs plugin packages, enables plugins and connects operators
to them. What a plugin is, and how its settings and hooks work, is explained in
[`plugins.md`](../basics/plugins.md); this page shows the commands.

Each section shows what the group's help prints and, for the verbs used most,
a command with its answer. Unless a section names another page, the answers
come from the echo of the [tutorials](../../tutorials/echo/run_the_echo.md).
How `flz` reaches the controller, its options and its exit codes are on the
[CLI](cli.md) page, and every verb in one list is the
[command reference](command_reference.md).

## `flz plugin`

Installs plugin packages, enables plugins and connects operators to them. Explained in [`plugins.md`](../basics/plugins.md), [`hooks.md`](../advanced/hooks.md), [`installing_plugins.md`](../advanced/installing_plugins.md), tutorial [Feed the echo a file](../../tutorials/echo/feed_the_echo_a_file.md).

```sh
$ flz plugin help
```

```output
flz plugin - Plugin management commands

USAGE:
    flz plugin <COMMAND>
    flz plugin enable <name> <provider_id> [--single] [X<key>=<value>]... [OPTIONS]
    flz plugin bind <name> --operator_id <op> [--hook=<name>]... [--channel <c>]

COMMANDS:
    install    Place a plugin package (.fpz) from a path or an https:// URL
    uninstall  Remove an installed plugin package
    sync       Make the plugin root hold exactly the packages FLEZHA_PLUGINS
               declares
    enable     Enable a plugin by name in a namespace
    bind       Connect an operator to an enabled plugin
    unbind     Take an operator's connection to a plugin away
    update     Change an enabled plugin's settings
    disable    Turn a plugin off
    list       List the plugins enabled in a namespace (text or json),
               `--installed` packages (text or json) or `--providers`
               (text only)
    help       Show help for a specific plugin provider

PLUGINS:
    A plugin has a name of its own in its namespace and is enabled once, with
    no operator; nothing runs until an operator is bound to it. A plugin runs
    on each controller where an operator bound to it runs, and one copy there
    serves every such operator when its provider can be shared. `--single`
    runs one copy in the whole cluster instead. A plugin whose last operator
    leaves stops and stays enabled; only `disable` turns it off. A provider
    that cannot be shared serves one operator, or one partition group.

    `enable --operator_id <op>` enables and binds in one command. `update`
    changes a running plugin in place when its provider declares the changed
    settings live, and restarts it otherwise, naming the operators bound to it.

PACKAGES:
    install, uninstall and sync change the plugin root, never a running
    controller, and work with no controller running. A change takes effect
    when the controller restarts; `flz plugin list --installed` shows each
    package as loaded, refused, pending or removed. An upgrade is an install
    of a newer package of the same id. While FLEZHA_PLUGINS is set, install
    and uninstall are refused: the declared list is the node's only truth. An
    operator bound to a plugin whose package a controller lacks runs there
    without it, and its health says so.

HOOKS:
    A hook is a point at which the operator calls its plugin, named by the
    callback the provider defines: on_input sends every payload the operator
    ingests, on_output every payload it emits, on_interval runs on a timer.
    `flz plugin help <provider_id>` lists the hooks a provider has. A hook
    belongs to a binding.

    `--hook=<name>` names one and may be repeated. A hook's sub-options follow
    it as `--<name>.<key>=<value>`. on_interval has no default interval, so
    it needs `--on_interval.interval_ms=<ms>`.

    A binding naming no hook has no hooks. A plugin that only receives still
    feeds its operator: its connection is held by the provider's own worker,
    which no hook governs.

    enable reads --namespace, --single, --operator_id, --hook, --channel and
    the X-prefixed provider settings; bind reads --namespace, --operator_id,
    --hook and --channel; unbind --namespace and --operator_id; update
    --namespace and the X-prefixed settings; disable --namespace. Each
    refuses any other option.

EXAMPLES:
    flz plugin install ./flezha_example_plugin-0.1.0.fpz
    flz plugin list --installed
    flz plugin enable trades operator_websocket \
        --Xuri=ws://localhost:9000/feed --Xchannel=trades
    flz plugin bind trades --operator_id my_op
    flz plugin bind trades --operator_id other_op --channel ticks
    flz plugin enable stamp example_stamp --operator_id my_op \
        --Xpath=/tmp/stamped.txt --hook=on_output
    flz plugin update trades --Xsubscription_items='["BTC/EUR","ETH/EUR"]'
    flz plugin list --namespace default
    flz plugin unbind trades --operator_id other_op
    flz plugin disable trades
```

`flz plugin help <provider>` lists one provider's settings and hooks, and
`flz plugin list --providers` the providers there are. The echo's file plugins
from [`plugins.md`](../basics/plugins.md#examples): a sink that writes everything the
echo emits to a file, then the plugins and their operators, then the plugin
taken away again:

```sh
$ flz plugin enable writer operator_file_sink --operator_id=my_echo --hook=on_output --Xpath=echo-out.txt
```

```output
Enabling plugin...
Plugin writer enabled.
Plugin writer bound to my_echo.
```

```sh
$ flz operator send my_echo --text hello
```

```output
Delivered to 'my_echo'.
```

```sh
$ cat app_data/files/echo-out.txt
```

```output
echo: hello
```

```sh
$ flz plugin list
```

```output
writer  file_sink  bound: my_echo  runs on: idle
```

```sh
$ flz plugin unbind writer --operator_id=my_echo
```

```output
Plugin writer unbound from my_echo; it stays enabled.
```

```sh
$ flz plugin disable writer
```

```output
Plugin writer disabled.
```

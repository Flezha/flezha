# Enabling and binding

`flz plugin` enables a plugin by name and binds relays to it. This page covers the
commands, their flags and hooks, changing a running plugin and seeing what runs.
[Plugins](plugins.md) has the model, and [Plugin commands](../cli/plugin_commands.md)
shows what each command prints.

## The commands

| Command | What it does |
| --- | --- |
| `flz plugin enable <name> <provider> [--single] [--X<option>=<value>]...` | enable a plugin by name; with `--relay_id` it also binds that relay |
| `flz plugin bind <name> --relay_id <op> [--hook=<hook>]... [--channel <c>]` | bind a relay to an enabled plugin |
| `flz plugin unbind <name> --relay_id <op>` | take the binding away; the plugin stays enabled |
| `flz plugin update <name> [--X<option>=<value>]...` | change an enabled plugin's options |
| `flz plugin disable <name>` | turn the plugin off |
| `flz plugin list` | the plugins enabled in the namespace |
| `flz plugin list --providers` | the providers this controller can enable (text only) |
| `flz plugin list --installed` | the installed packages |
| `flz plugin help <provider>` | a provider's options and hooks |

Every verb takes `--namespace` (default `default`). `enable` reads `--namespace`,
`--single`, `--relay_id`, `--hook`, `--channel` and the X-prefixed settings; `bind`
reads `--namespace`, `--relay_id`, `--hook` and `--channel`; `unbind` reads
`--namespace` and `--relay_id`; `update` reads `--namespace` and the X-prefixed
settings; `disable` reads `--namespace`. Each refuses any other option rather than
dropping it.

## Three kinds of flags

`flz plugin enable` takes three kinds of flags:

| Flag | Belongs to | For example |
|---|---|---|
| `--relay_id`, `--namespace`, `--single`, `--channel` | the command: which relay the plugin is bound to, and how | `--relay_id=trade_watcher` |
| `--hook` | the binding: when the controller calls the plugin | `--hook=on_output` |
| `--X<option>` | the plugin: its own settings, one flag each | `--Xuri=ws://localhost:4500/` |

The `X` keeps a plugin's settings apart from the command's own flags: `--Xchannel` is the
plugin's `channel` setting, not an option of `flz`. A setting is written
`--X<option>=<value>`, `--X<option> <value>`, or without its dashes as
`X<option>=<value>`. A value is plain text, unless it is written as JSON, a list in
`[...]` or an object in `{...}`, in which case the plugin gets it as JSON. Quote such a
value, so the shell passes it as one word.

Every provider lists its settings, required ones first, then its type and the hooks it
supports:

```sh
$ flz plugin help relay_websocket
```

```output
Websocket Plugin Help

USAGE:
    flz plugin enable <name> relay_websocket [OPTIONS]

REQUIRED PARAMETERS (X-prefixed):
    --Xuri <value>    The websocket endpoint to connect to

OPTIONAL PARAMETERS (X-prefixed):
    --Xplugin_id <value>    Plugin id; defaults to the id it was enabled under
    --Xignore_pattern <value>    Substring marking a frame as a heartbeat rather than data
...
```

With `--json`, `flz plugin help` prints the whole answer: beside the screen, the
plugin type and the hooks, `options` lists each option the enable takes, with its
type, whether it is required, its default, its bounds and its allowed values.

## Names, enables and binds

- A name is unique in its namespace. Enabling a name that is enabled with the same
  provider leaves the existing plugin untouched and says so (its options are not
  compared); with another provider it is refused with `plugin.id_taken`.
- `enable ... --relay_id <op>` enables and binds in one command. A bind that fails there
  leaves the plugin enabled, and the output says so before the refusal.
- A bind to a name that is not enabled is refused with `plugin.not_found`; an unbind of a
  binding the relay does not have, with `plugin.not_bound`. Binding a controller plugin
  is refused with `plugin.not_bindable`.
- `--channel` on a bind names the entry channel that relay's records arrive on, when it
  is not the one the plugin was enabled with: one plugin can feed `trades` on one relay
  and `ticks` on another. A source's entry channel, the bind's or the plugin's own, that
  the runner hosting the relay did not announce is refused with `api.channel_unknown`, at a
  bind, a `relay start --plugins` and a `plugin update` alike; a relay whose runner
  announced no channels is not checked.
- Options belong to the enable and the update; a bind that carries `--X` settings is
  refused.

## Hooks

A hook is the moment the controller calls a plugin, and it belongs to a binding. A source
needs none: its connection is held by the provider's own worker, which starts whether or
not the binding names a hook.

| Hook | The plugin is called |
|---|---|
| none | never; a source pushes its input in by itself |
| `on_output` | with everything the relay sends out |
| `on_input` | with everything the relay takes in |
| `on_interval` | on a timer, every `interval_ms`, which it requires |

`--hook=<name>` names one and may be repeated. A hook takes settings of its own, written
after it with the hook's name and a dot: `--hook=on_interval --on_interval.interval_ms=5000`.
`flz plugin help <provider>` lists the hooks a provider supports, and
[`hooks.md`](hooks.md) has every hook.

## Change a running plugin

```sh
$ flz plugin update trades --Xsubscription_items='["EUR/USD","GBP/USD"]'
```

```output
Plugin trades updated in place.
```

`flz plugin update` validates the new options against the provider's declaration and
records them. When every changed option is one the provider declares live, every running
copy applies it in place, with no restart and no reconnect. Otherwise every running copy
restarts with the new options, and the command prints a warning naming the bound relays:

```output
Plugin trades updated; its running copies restarted.
warning: restarted under a, b
```

An update that changes nothing prints `Plugin trades is unchanged.`

## See what runs

`flz plugin list` shows each plugin in the namespace: its name, its provider's name,
whether it is `single`, the relays bound to it, and the controllers it runs on, or
`idle`. A controller plugin's row also names its hooks and the controllers its copy lags
on. `--json` prints the same rows as JSON.

```sh
$ flz plugin list --namespace trading
```

```output
exchange  websocket  bound: trade_watcher  runs on: flezha@127.0.0.1
```

`flz relay info` lists every relay in the namespace and, under each one, the plugins
bound to it: the plugin's name, its provider module, the hook it is on, what the provider
reports about itself (a file plugin's full path, a websocket's subscriptions), and whether
it is healthy. When a relay shows `degraded`, this is where to look for the plugin
that is not healthy.

```sh
$ flz relay info
```

```output
Fetching relay information for namespace 'default'...
Relay information for namespace 'default':

Relay: my_echo
  Run: flow
  Durable: local (sleep_after: false)
  Phase: running
  Health: healthy
  Runner ID: default_b2e389ab1c13
  Group ID: default
  Uptime (ms): 7249
  Default Input Type: 2
  Waiting callers: 0
  Plugins:
    - reader: Flezha.Plugin.Provider.File.Source
        path: /opt/flezha/storage/app/files/default/echo-in.txt
        health: :healthy
    - writer: Flezha.Plugin.Provider.File.Sink
        hook: on_output []
        path: /opt/flezha/storage/app/files/default/echo-out.txt
        health: :healthy
```

`reader` has no hook line, because a source is not sent anything.

## At a relay's start

`flz relay start` (and `flz schedule add`) take `--plugins`, a JSON array of plugin
specs to bind at start. Each spec names a plugin (`name`) and the hooks the relay is
bound with (`hooks`, which a source may leave out). A spec that also names a `provider`,
with its `opts` and `single`, enables the plugin when it is not enabled yet; a spec that
only binds may name the binding's `channel` in its `opts`. Any other key is refused.

```sh
$ flz relay start order_executor --id order_1 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order \
  --plugins '[{"name": "order_1_file", "provider": "relay_file_output", "opts": {"path": "order_1.txt"}, "hooks": [["on_output", {}]]}]'
```

```output
Started order_1
```

A relay entry declared with plugin rows of the earlier form (plugins carried on the
relay rather than enabled by name) starts without them, and one warning names the
relay and the old plugin ids: enable and bind them again. Nothing converts them.

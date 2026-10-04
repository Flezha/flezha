# Hooks

A hook is a moment at which the controller calls a plugin. An operator plugin is called at
points in an operator's life: a record coming in, a record going out, a timer, a request.
A controller plugin is called when something happens in its namespace: an operator
started, stopped or failed, an input was dead-lettered, a plugin turned unhealthy. A hook
named `on_...` tells the plugin that something happened.

Hooks exist so that a plugin does only the work it was asked for. A file sink is handed
what an operator emits because its binding names `on_output`; the same provider bound on
`on_input` writes what the operator ingests instead. A source needs no hook at all: its
connection is held by its provider's own worker, which no hook governs.

## How it works

Each hook has a kind, which decides how its plugins are called:

| Kind | Hooks | How the plugins are called |
| --- | --- | --- |
| transform | `on_input`, `on_output` | in a chain, in `position` order, each handed the record the one before passed on |
| fold | `on_operator_init` | in a chain, each handed what the ones before accumulated |
| timer | `on_interval` | every `interval_ms`, per plugin |
| request | `on_message` | the one plugin whose `key` the request names, which answers it |
| fanout | `on_preview`, `on_snapshot`, `on_terminate`, and the five controller hooks | each plugin on its own, with nothing passed between them |

A hook belongs to a binding for an operator plugin: two operators bound to one plugin can
be bound on different hooks. A controller plugin's hooks belong to the plugin itself,
named when it is enabled and changed with `flz plugin update`.

A provider implements the hooks it can be called on, and only those. A provider is either
an operator plugin or a controller plugin, and a module that defines a hook of the other
type does not compile. `flz plugin help <provider>` lists the hooks a provider supports.

## Operator hooks

| Hook | The plugin is called | Its options |
| --- | --- | --- |
| none | never; a source pushes its input in by itself | |
| `on_input` | with every record the operator takes in | `position` |
| `on_output` | with every record the operator sends out on a sink | `position` |
| `on_interval` | on a timer | `interval_ms`, required: it has no default |
| `on_message` | with a request addressed to its `key`, which it answers | `key`, required |
| `on_operator_init` | once, when the operator starts, to contribute to its initial values | `position`, `timeout_ms` (default `5000`), `on_error` (`fail`, the default, or `empty`) |
| `on_snapshot` | with each snapshot of the operator's state, or nothing when one failed | |
| `on_terminate` | when the operator terminates, with the reason | |
| `on_preview` | with a part or a dead-letter notice, for listeners; no sink implements it | |

An output reaches `on_output` only when it leaves the operator on a sink: an operator
decides in its code what leaves it, and a plugin can only take what does. An
`on_operator_init` plugin can only be attached when its operator starts, with
`flz operator start <factory> --plugins '<json-array>'` (`plugin.init_after_start`
otherwise). Two plugins cannot answer the same `key` on `on_message`
(`plugin.hook_key_taken`).

### Naming hooks on the command line

`--hook=<name>` names a hook and may be repeated. A hook's options follow it as
`--<name>.<key>=<value>`:

```sh
$ flz plugin bind ticker --operator_id=prices --hook=on_interval --on_interval.interval_ms=5000
```

In a plugin spec (`--plugins` at an operator's start) a hook is a name, a
`[name, options]` pair, or an object with a `name` and an `opts` object:

```json
"hooks": ["on_terminate", ["on_output", {}], {"name": "on_interval", "opts": {"interval_ms": 5000}}]
```

`position` orders a chain and is refused on any other kind
(`plugin.hook_position_refused`). Each refusal names what is wrong:

| Code | When |
| --- | --- |
| `plugin.unknown_hook` | no hook has that name; the message lists the hooks this plugin takes |
| `plugin.hook_not_implemented` | the provider does not implement that hook |
| `plugin.unknown_hook_option` | the hook's kind has no such option |
| `plugin.hook_option_invalid` | the option does not accept the value |
| `plugin.hook_key_missing` | an `on_message` hook without its `key` |
| `plugin.hook_invalid`, `plugin.hook_options_not_object` | a hook written in none of the shapes above |

## Controller plugins and their hooks

A controller plugin is bound to no operator. It runs on every controller that holds its
namespace and has its provider, from its enable until it is disabled, and it never stops
for want of operators; with `--single` it runs one copy in the whole cluster, moved like a
`single` operator plugin. It is told what happens in its own namespace, and only there,
through five hooks:

| Hook | When | Its own fields |
| --- | --- | --- |
| `on_operator_started` | a deliberate start (by a user, a schedule, a tasklet or the admin SDK) has declared the operator and it runs; not a restart, a move, a wake or a partition rescale | `operator`, `factory` |
| `on_operator_stopped` | a deliberate stop or a completion; not a partition rescale | `operator`, `reason` (`stopped` or `completed`) |
| `on_operator_failed` | the operator's health reads parked, or failed because its runner is lost; once, until it reads healthy, degraded or suspended again or is stopped, so a park retried on later ticks is not repeated | `operator`, `error` (the error code of the park's reason) |
| `on_dead_letter` | an input is newly dead-lettered | `operator`, `entry` (the DLQ id), `channel`, `error` |
| `on_plugin_unhealthy` | a plugin bound to an operator turns from healthy to unhealthy | `plugin`, `operator`, `error` |

Every event also carries `hook`, `namespace`, `controller`, `at` (UTC, ISO 8601) and
`text`, one line a person can read, and never a record's data: a dead letter names its
entry, which `flz dlq show` reads. An event is delivered to a plugin at most once per
occurrence.

**Where an event is heard.** A copy without `single` receives an event from the controller
it runs on, and from no other, so an operator that parks on one controller of three is
reported once, by that controller's copy. A `single` copy hears every controller. An event
on a controller with no copy of the plugin is not delivered to it.

**What it costs.** Emitting a hook event never waits for a plugin. On a controller where
nothing wants hook events, an emit sends no message and writes nothing. Each copy holds
its waiting events in a queue bounded by `FLEZHA_PLUGIN_BACKLOG_BYTES` (default `4194304`)
and `FLEZHA_PLUGIN_BACKLOG_FRAMES` (default `4096`); on overflow it drops the oldest, its
health reports `plugin_lagging` with the count dropped, and `flz plugin list` shows the
controllers it lags on. Each hook runs in a process of its own, so a callback that blocks
holds back only its own plugin.

A start and a stop are also the operator's own observability events, `operator_started`
and `operator_stopped` of source `flezha_operator`, so `flz observe tail flezha_operator`
and the exporters show them with no plugin enabled.

## Example

The webhook plugin is the controller plugin that ships: it POSTs each event as one JSON
object to a URL. It is enabled with its hooks, one `--hook` each:

```sh
$ flz plugin enable alerts hook_webhook \
  --Xurl=https://hooks.example.test/services/T000/B000/XXXX \
  --hook on_operator_failed --hook on_dead_letter --namespace prod
```

`flz plugin list --namespace prod` shows the plugin with its hooks and the controllers it
runs on. Its hooks are replaced with an update, and binding it to an operator is refused
with `plugin.not_bindable`:

```sh
$ flz plugin update alerts --hook on_operator_started --namespace prod
```

An operator that parks in `prod` then produces one request whose body is:

```json
{
  "hook": "on_operator_failed",
  "namespace": "prod",
  "controller": "flezha@ctrl-2",
  "at": "2026-10-02T13:41:16.662637Z",
  "operator": "orders",
  "error": "runner.not_connected",
  "text": "operator orders in prod failed: runner.not_connected"
}
```

`text` lets a Slack-style incoming webhook take the event as it is.
[`hook_webhook.md`](../../plugins/hook_webhook.md) has its options, retries and telemetry.
An operator hook is shown on [`plugins.md`](../basics/plugins.md): a file sink bound with
`--hook=on_output`.

## Limits

- **Delivery is best effort.** An event waiting in a copy whose controller stops is lost,
  and so is one that happens in the first moments after an enable, before the controller
  has heard of the new copy. While a `single` copy moves to another controller, the old and
  the new copy both run for a moment, so an event can arrive twice. The dead-letter queue
  and `flz operator list` remain the record of what happened.
- **A controller plugin hears only its namespace**, and enabling one is authorized in that
  namespace like any plugin.
- **A hook returns quickly.** A provider that does slow I/O hands the event to its own
  worker, as `hook_webhook` does.
- **A controller hook only listens.** It is told that something happened; it cannot refuse
  or change what the controller does.

## See also

- [`plugins.md`](../basics/plugins.md): the plugin model, enabling and binding
- [`writing_a_plugin.md`](writing_a_plugin.md): implementing a hook, and a controller plugin of your own
- [`hook_webhook.md`](../../plugins/hook_webhook.md): the webhook plugin
- [Controller environment variables](../../configuration/environment_variables.md#runner-and-operator-lifecycle): `FLEZHA_PLUGIN_BACKLOG_BYTES` and `FLEZHA_PLUGIN_BACKLOG_FRAMES`
- [`../../errors.md`](../../errors.md): every `plugin.*` code

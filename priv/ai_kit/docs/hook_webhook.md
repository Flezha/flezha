# Webhook plugin

The webhook plugin tells another service what happens in a namespace: a chat channel, an
alerting tool, or any service that takes an HTTP POST. It is a controller plugin, so no
relay is bound to it: it sends every hook event it is enabled for as one JSON object to
a URL. `Flezha.Plugin.Provider.HookWebhook` is its provider, under the provider id
`hook_webhook`.

| | |
| --- | --- |
| Type | controller plugin |
| Ships | built in |
| Shareable | no: it is bound to no relay; one copy runs on each controller of its namespace, or one in the cluster with `--single` |
| Partitioned relays | not applicable |
| Durability | none: best effort, an event is a notice, not a record |

## Options

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `url` | string | none | yes | Where each event is POSTed: http or https, with a host and no credentials |
| `headers` | headers | `%{}` | no | Request headers, as a JSON object; never logged |
| `timeout_ms` | int | `5000` | no | Budget for one request |
| `retries` | int | `2` | no | Retries of a 429, a 5xx, a timeout or a transport error, after 1 s and then 5 s |
<!-- provider-options:end -->

## Example

```sh
$ flz plugin enable alerts hook_webhook \
  --Xurl=https://hooks.example.test/services/T000/B000/XXXX \
  --hook on_relay_failed --hook on_dead_letter --namespace prod
```

`flz plugin list --namespace prod` shows the plugin with its hooks and the controllers it
runs on. `flz plugin update alerts --hook on_relay_started --namespace prod` replaces its
hooks; `flz plugin bind` of it is refused with `plugin.not_bindable`.

## Contract

| | |
| --- | --- |
| Hooks | `on_relay_started`, `on_relay_stopped`, `on_relay_failed`, `on_dead_letter`, `on_plugin_unhealthy`, each named with `--hook` |
| Runs | one copy on every controller that holds the namespace, each sending what happened on its own controller; one copy in the cluster with `--single` |
| Request | `POST` to `url`, the event as a JSON object, `content-type: application/json`, one connection kept open, events in the order they happened on the controller |
| Retries | a 429, a 5xx, a timeout or a transport error, after 1 s and then 5 s, up to `retries` |
| Delivery | best effort: an event is a notice, not a record |

## Checks at enable

The options are checked when the plugin is enabled, with the HTTP poll plugin's URL rules,
and no connection is opened then: a URL with credentials is `plugin.url_has_credentials`,
one without a host `plugin.url_without_host`, any scheme but `http` and `https`
`plugin.url_scheme_unsupported`, and `headers` that are not a JSON object of strings
`plugin.headers_invalid`. Put a token in `headers` or in the URL's path, never in its
userinfo. `timeout_ms` is 1 to 60000 and `retries` 0 to 5.

## The event

Every event carries:

| Field | Value |
| --- | --- |
| `hook` | the hook's name |
| `namespace` | the namespace it happened in |
| `controller` | the controller it happened on |
| `at` | the time the controller saw it, UTC, ISO 8601 |
| `text` | one line a person reads, so a Slack-style incoming webhook takes the event as it is |

and the fields of its hook:

| Hook | When | Fields |
| --- | --- | --- |
| `on_relay_started` | a deliberate start (a user, a schedule, a tasklet or the admin SDK) has declared the relay and it runs; not a restart, a move, a wake or a partition rescale | `relay`, `factory` |
| `on_relay_stopped` | a deliberate stop or a completion; not a partition rescale. `controller` is the one that ran the stop | `relay`, `reason` (`stopped` or `completed`) |
| `on_relay_failed` | the relay's health turned halted, or failed because its runner was lost; once, until it is healthy again or stopped | `relay`, `error` (an error code) |
| `on_dead_letter` | a record was newly dead-lettered | `relay`, `entry` (the DLQ entry, as `flz dlq show` takes it), `channel`, `error` |
| `on_plugin_unhealthy` | a plugin bound to a relay turned from healthy to unhealthy | `plugin`, `relay`, `error` |

A start and a stop are also the relay's own observability events, `relay_started` and
`relay_stopped` of source `flezha_relay`, so `flz observe tail
flezha_relay` and the exporters show them with no plugin enabled.

No event carries a record's data: a dead letter names its entry, which `flz dlq show` reads
with an `ops` token. An `error` the error catalogue does not hold, such as a provider's own
health reason, is `internal.uncatalogued`.

```json
{
  "hook": "on_relay_failed",
  "namespace": "prod",
  "controller": "flezha@ctrl-2",
  "at": "2026-10-02T13:41:16.662637Z",
  "relay": "orders",
  "error": "runner.not_connected",
  "text": "relay orders in prod failed: runner.not_connected"
}
```

## Retries and drops

A 2xx completes an event. A 429, a 5xx, a timeout or a transport error is sent again after
1 s and then after 5 s, at most `retries` times, and is then dropped; any other status is
dropped at once. Only the status of a response is read: an unfinished body past 8 KiB is
left unread and its connection closed. The connection is opened again after a failed exchange or a
closed one. Each drop is
counted, and logged at warning level once per plugin and status, naming the plugin and the
URL's host: the URL's path and query and the headers may hold a token and are never logged,
nor shown in the status of the plugin's copy or its worker.

## Delivery

Each copy holds the events it has not sent in a bounded queue, under
`FLEZHA_PLUGIN_BACKLOG_BYTES` and `FLEZHA_PLUGIN_BACKLOG_FRAMES`. A receiver that is slow or
does not answer holds back only this plugin: nothing in Flezha waits for it. When the queue
is full the oldest events are dropped, and `flz plugin list` shows the controllers the
plugin lags on.

Delivery is best effort. An event waiting on a controller that stops is lost, and so is one
that happens in the first moments after an enable, before the controller's hook output plugin has
heard of the new copy. While a `single` copy moves to another controller, the old and the
new copy both run for a moment, so an event can be sent twice. The DLQ and
`flz relay list` remain the record of what happened.

## Telemetry

What each signal says on the observability bus, and the series an exporter names a metric by:

<!-- bridge-signals:begin flezha_hook_webhook -->
| Source | Signal | Kind | Series | Meaning |
| --- | --- | --- | --- | --- |
| `flezha_hook_webhook` | `failed` | event |  | A delivery or a read failed. |
| `flezha_hook_webhook` | `overflowed` | metric | `flezha_hook_webhook_overflowed` | The plugin's queue was full, so items were dropped. |
| `flezha_hook_webhook` | `retried` | metric | `flezha_hook_webhook_retried` | A delivery failed and will be tried again; nothing is lost yet. |
| `flezha_hook_webhook` | `shipped` | metric | `flezha_hook_webhook_shipped` | The plugin passed items on, into its relay or out to where it sends them. |
<!-- bridge-signals:end -->

| Event | Source | Measurements | Metadata |
| --- | --- | --- | --- |
| `[:flezha, :plugin, :hook_webhook, :shipped]` | `:flezha_hook_webhook` metric `shipped` | `count` | `plugin` |
| `[:flezha, :plugin, :hook_webhook, :retried]` | `:flezha_hook_webhook` metric `retried` | `count` | `plugin`, `reason` (the status or the transport error) |
| `[:flezha, :plugin, :hook_webhook, :failed]` | `:flezha_hook_webhook` event `failed` | `count` | `plugin`, `reason` |

`retried` is one attempt that will be retried; `failed` is an event given up. These are the
names every provider reports; the plugin sends through the worker the HTTP push plugin uses.

## Limits

One URL per plugin, one request per event: no batching, no request signing. Enable a second
plugin for a second receiver.

Like the HTTP plugins, the plugin dials whatever address `url` names, loopback, private networks and
cloud metadata services such as `169.254.169.254` included: there is no address rule.
Enabling it takes `plugin.enable`, which the `app` role holds, so on a controller several
tenants share, anyone with `app` in a namespace can have the controller's machine make
requests for them. Grant `app` only to callers you would let do that, or block those
addresses at the host's firewall.

## See also

- [`hooks.md`](../topics/plugins/hooks.md): hooks and controller plugins
- [`plugins.md`](../topics/plugins/plugins.md): the plugin model
- [`writing_a_plugin.md`](../topics/plugins/writing_a_plugin.md): a controller plugin of your own

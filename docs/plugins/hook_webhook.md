# Webhook plugin

For users who want to hear about what happens in a namespace somewhere else: a chat
channel, an alerting tool, any service that takes an HTTP POST.
`Flezha.Plugin.Provider.HookWebhook` (provider id `hook_webhook`) is a controller plugin:
it is bound to no operator, and it sends every hook event it is enabled for as one JSON
object to a URL. This page covers the contract, the parameters, the request, the event
fields, retries, delivery and telemetry.

## Contract

| | |
| --- | --- |
| Plugin type | `:controller` |
| Hooks | `on_operator_started`, `on_operator_stopped`, `on_operator_failed`, `on_dead_letter`, `on_plugin_unhealthy`, each named with `--hook` |
| Runs | one copy on every controller that holds the namespace, each sending what happened on its own controller; one copy in the cluster with `--single` |
| Request | `POST` to `url`, the event as a JSON object, `content-type: application/json`, one connection kept open, events in the order they happened on the controller |
| Retries | a 429, a 5xx, a timeout or a transport error, after 1 s and then 5 s, up to `retries` |
| Delivery | best effort: an event is a notice, not a record |

```sh
flz plugin enable alerts hook_webhook \
  --Xurl=https://hooks.example.test/services/T000/B000/XXXX \
  --hook on_operator_failed --hook on_dead_letter --namespace prod
```

`flz plugin list --namespace prod` shows the plugin with its hooks and the controllers it
runs on. `flz plugin update alerts --hook on_operator_started --namespace prod` replaces its
hooks; `flz plugin bind` of it is refused with `plugin.not_bindable`.

## Parameters

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `url` | string | &mdash; | yes | Where each event is POSTed: http or https, with a host and no credentials |
| `headers` | json | `%{}` | no | Request headers, as a JSON object; never logged |
| `timeout_ms` | int | `5000` | no | Budget for one request |
| `retries` | int | `2` | no | Retries of a 429, a 5xx, a timeout or a transport error, after 1 s and then 5 s |
<!-- provider-options:end -->

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
| `on_operator_started` | a deliberate start (a user, a schedule, a tasklet or the admin SDK) has declared the operator and it runs; not a restart, a move, a wake or a partition rescale | `operator`, `factory` |
| `on_operator_stopped` | a deliberate stop or a completion; not a partition rescale. `controller` is the one that ran the stop | `operator`, `reason` (`stopped` or `completed`) |
| `on_operator_failed` | the operator's health turned parked, or failed because its runner was lost; once, until it is healthy again or stopped | `operator`, `error` (an error code) |
| `on_dead_letter` | a record was newly dead-lettered | `operator`, `entry` (the DLQ entry, as `flz dlq show` takes it), `channel`, `error` |
| `on_plugin_unhealthy` | a plugin bound to an operator turned from healthy to unhealthy | `plugin`, `operator`, `error` |

No event carries a record's data: a dead letter names its entry, which `flz dlq show` reads
with an `ops` token. An `error` the error catalogue does not hold, such as a provider's own
health reason, is `internal.uncatalogued`.

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

## Retries and drops

A 2xx completes an event. A 429, a 5xx, a timeout or a transport error is sent again after
1 s and then after 5 s, at most `retries` times, and is then dropped; any other status is
dropped at once. The connection is opened again after a failed exchange. Each drop is
counted, and logged at warning level once per plugin and status, naming the plugin and the
URL's host: the URL's path and query and the headers may hold a token and are never logged.

## Delivery

Each copy holds the events it has not sent in a bounded queue, under
`FLEZHA_PLUGIN_BACKLOG_BYTES` and `FLEZHA_PLUGIN_BACKLOG_FRAMES`. A receiver that is slow or
does not answer holds back only this plugin: nothing in Flezha waits for it. When the queue
is full the oldest events are dropped, and `flz plugin list` shows the controllers the
plugin lags on.

Delivery is best effort. An event waiting on a controller that stops is lost, and so is one
that happens in the first moments after an enable, before the controller's hook sink has
heard of the new copy. While a `single` copy moves to another controller, the old and the
new copy both run for a moment, so an event can be sent twice. The DLQ and
`flz operator list` remain the record of what happened.

## Telemetry

| Event | Source | Measurements | Metadata |
| --- | --- | --- | --- |
| `[:flezha, :plugin, :hook_webhook, :sent]` | `:flezha_hook_webhook` metric `sent` | `count` | `plugin` |
| `[:flezha, :plugin, :hook_webhook, :failed]` | `:flezha_hook_webhook` event `failed` | `count` | `plugin`, `reason` (the status or the transport error) |
| `[:flezha, :plugin, :hook_webhook, :dropped]` | `:flezha_hook_webhook` metric `dropped` | `count` | `plugin`, `reason` |

`failed` is one attempt that will be retried; `dropped` is an event given up.

## Limits

One URL per plugin, one request per event: no batching, no request signing. Enable a second
plugin for a second receiver.

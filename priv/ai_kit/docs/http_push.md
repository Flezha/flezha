# HTTP push plugin

The HTTP push plugin sends a relay's outputs to an HTTP endpoint: a REST API, a webhook
receiver, or any service that takes a request per record. It hooks `on_output`, or
`on_input` for what the relay ingested, and sends each payload as one request.
`Flezha.Plugin.Provider.HttpPush` is its provider, under the provider id
`relay_http_push`.

| | |
| --- | --- |
| Type | relay plugin |
| Ships | built in |
| Shareable | yes: every relay bound to it on a controller sends through one worker, one queue and its connections |
| Partitioned relays | allowed: each member has its own copy |
| Durability | a delivery-guaranteed output stays in the outbox until a 2xx; a plain output is dropped and counted once its retries are spent |

## Options

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `url` | string | none | yes | Where each payload is sent: http or https, with a host and no credentials |
| `method` | enum | `"POST"` | no | `POST` or `PUT` |
| `headers` | json | `%{}` | no | Request headers, as a JSON object; never logged |
| `content_type` | string | `"application/octet-stream"` | no | The request's `content-type` |
| `channel` | names | none | no | The channel this plugin is attached to: only payloads naming it are sent. Several names, comma-separated, send them all |
| `channel_out` | names | none | no | Send only payloads whose metadata channel matches; the explicit spelling |
| `timeout_ms` | int | `10000` | no | Budget for one request, connect to last byte |
| `max_in_flight` | int | `1` | no | Requests outstanding at once, one connection each; above 1 order is not kept |
| `retries` | int | `5` | no | Retries of a 408, a 429, a 5xx, a timeout or a transport error |
| `retry_backoff_ms` | int | `250` | no | First delay between retries |
| `retry_max_backoff_ms` | int | `10000` | no | Ceiling for that delay, and for a `retry-after` |
| `delivery_timeout_ms` | int | `30000` | no | Time from a payload's first attempt after which it is not retried again |
| `queue_max` | int | `10000` | no | Payloads waiting to be sent |
| `queue_max_bytes` | int | `67108864` | no | Payload bytes waiting to be sent |
| `queue_overflow` | enum | `:drop_nothing` | no | What a full queue does: make the dispatch wait, drop the oldest, or drop the newest |
| `queue_block_timeout_ms` | int | `5000` | no | How long a dispatch waits on a full queue before the payload overflows |
| `failure_threshold` | int | `3` | no | Consecutive failed requests before the plugin reports unhealthy |
<!-- provider-options:end -->

## Example

```sh
$ flz plugin enable orders_out relay_http_push \
  --Xurl=https://api.example.test/orders \
  --Xheaders='{"authorization": "Bearer <token>"}' \
  --Xcontent_type=application/json --Xchannel=orders
$ flz plugin bind orders_out --relay_id orders --hook on_output
```

## Contract

| | |
| --- | --- |
| Hooks | `on_output`, `on_input` |
| Request | `POST` (or `PUT`) to `url`, the payload as the body unchanged, `content_type` as its `content-type`, the configured `headers` and the output's identity headers |
| Connections | `max_in_flight` (default 1), each kept open between requests; with one, payloads are sent in the order they were dispatched |
| Shared | every relay bound to the plugin on a controller sends through one worker, one queue and its connections; each member of a partitioned relay has its own |
| Plain output | queued and sent; dropped and counted once its retries are spent |
| Guaranteed output | held in the outbox until a 2xx; parked as a dead letter when it is refused or its retries run out |
| Redirects | never followed |

## Checks at enable

The options are checked when the plugin is enabled and no connection is opened then. `url`
follows the HTTP poll plugin's rules: credentials in it are `plugin.url_has_credentials`, no
host `plugin.url_without_host`, a scheme but `http` and `https`
`plugin.url_scheme_unsupported`. `headers` must be a JSON object of text or number values,
and `plugin.headers_invalid`, whose `header` detail names the header, refuses a name the
request sets itself (`content-type`, `content-length`, `host`, `transfer-encoding`,
`connection`, `idempotency-key`, or any starting `flz-`), a name that is not an HTTP token,
and a value outside visible ASCII, space and tab. Put a token in `headers`, never in the
URL's userinfo.

## The request

Every request carries the output's identity, each header present when its metadata is:

| Header | Metadata | Value |
| --- | --- | --- |
| `flz-key` | `flz.key` | the natural key of a `.keyed()` channel |
| `flz-cell` | `flz.cell` | `<relay>/<tasklet>/<cell key>` |
| `flz-seq` | `flz.seq` | the output's sequence in its Cell |
| `flz-epoch` | `flz.epoch` | the outbox epoch |
| `idempotency-key` | all three of `flz.cell`, `flz.seq`, `flz.epoch` | `<cell>/<seq>/<epoch>`, the Kafka producer's `flezha.dedup_key` |

Every delivery-guaranteed output carries an `idempotency-key`, the same on every retry and
every redrive, so a receiver that keeps the keys it has seen drops a repeat. A value is
percent-encoded outside visible ASCII and at its `%` characters, and is sent unchanged
otherwise, so a key in another script reaches the receiver intact.

## Delivery

The plugin follows its relay's settings. A plain output makes no delivery promise: the
dispatch queues it and goes on, and an output that still fails after its retries is
dropped, counted, and logged at warning level once per plugin and reason. A
delivery-guaranteed output (a relay with `effectively_once`) is held in the outbox
before it is queued, confirmed on a 2xx answer, and otherwise parked in the relay's DLQ
with the error `http_push.rejected` (its `status` detail) or `http_push.exhausted` (its
`reason` detail: the last status or transport error), never dropped; `flz dlq replay` sends
it again. One still queued when the plugin stops is parked with `http_push.exhausted` and
the reason `stopped`. While no worker of the plugin runs on the controller, a guaranteed
output waits in the outbox and a plain one is dropped and counted.

A receiver that ignores `idempotency-key` sees a retried guaranteed output twice when its
first answer was lost: delivery is at least once at the receiver unless it dedups.

## The queue

The plugin holds at most `queue_max` payloads and `queue_max_bytes` payload bytes that wait
to be sent, under `queue_overflow`:

| Policy | A payload that does not fit |
| --- | --- |
| `drop_nothing` (default) | the dispatch waits until there is room, at most `queue_block_timeout_ms`, and the payload then overflows |
| `drop_oldest` | the oldest waiting plain payloads are dropped to make room, then the oldest guaranteed one |
| `drop_newest` | the new payload overflows |

The dispatch waits only while the queue is full; payloads already on their way when it fills
are admitted past the bound. Every overflow is counted as `overflowed`, and a guaranteed
output that overflows is parked with `http_push.exhausted` and the reason `queue_full`.

## Answers and retries

| Answer | What happens |
| --- | --- |
| 2xx | delivered; a guaranteed output is confirmed |
| 408, 429, 5xx, a timeout, a closed connection, a connect failure | retried along the backoff curve, on a fresh connection after a transport error |
| any other status, a 3xx included | given up at once: a guaranteed output is parked with `http_push.rejected` |

A retry waits `retry_backoff_ms`, doubling (jittered) up to `retry_max_backoff_ms`, or longer
when the answer carries a `retry-after` in seconds, up to `retry_max_backoff_ms`. An output
is retried while its attempts after the first stay within `retries` and the time since its
first attempt within `delivery_timeout_ms`, and is then given up. A retry keeps its
connection's place, so with `max_in_flight` 1 nothing overtakes it. A redirect is not
followed: it would replay the configured headers, which may carry a token, to another
host.

## Health

The plugin reports `{:unhealthy, :send_stalled}` once `failure_threshold` requests in a row
have failed, and is healthy again after the next 2xx.

## Telemetry

| Event | Source | Measurements | Metadata |
| --- | --- | --- | --- |
| `[:flezha, :plugin, :http_push, :shipped]` | `:flezha_http_push` metric `shipped` | `count` | `plugin` |
| `[:flezha, :plugin, :http_push, :retried]` | `:flezha_http_push` metric `retried` | `count` | `plugin`, `reason` (the status or the transport error) |
| `[:flezha, :plugin, :http_push, :failed]` | `:flezha_http_push` event `failed` | `count` | `plugin`, `reason` |
| `[:flezha, :plugin, :http_push, :overflowed]` | `:flezha_http_push` metric `overflowed` | `count` | `plugin`, `policy` |

`failed` is an output given up: a plain one dropped, a guaranteed one parked, or a plain one
no worker was running for (`reason: :not_running`).

## Security

No log line, telemetry event or process status holds the URL's path or query or a header
value; the host stands for the endpoint. TLS is verified against the system CA store. The
plugin sends to whatever host `url` names, private and loopback addresses included: like
the HTTP poll plugin it has no address rule, so who may enable plugins decides where data
may go.

## Limits

One URL per plugin and one request per payload: no batching, no body template, no request
signing, no HTTP/2. Enable a second plugin for a second endpoint.

## See also

- [`hook_webhook.md`](hook_webhook.md): hook events to an HTTP endpoint
- [`plugins.md`](../topics/plugins/plugins.md): the plugin model
- [`durability.md`](../topics/core/durability.md): guaranteed outputs and the outbox

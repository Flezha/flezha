# Websocket subscription source

For operators running `Flezha.Plugin.Provider.Websocket` (provider id
`operative_websocket`) who need to change what a live socket is subscribed to
without tearing it down. By default the plugin subscribes with a static
`post_connect` payload fixed at enable time; changing it means a disable and an
enable, which drops every other stream on that connection and spends the
upstream's reconnect budget. A plugin that sets `subscription_url` (or a
literal `subscription_items` list) instead *converges*: it polls the desired
set, diffs it against what it has applied, and sends subscribe/unsubscribe
frames for the difference on the connection it already holds. This doc covers
the source contract, the templates, the reconcile and commit rules, both
watchdog tiers, durability, parameters, the partitioned-member share,
observability and limits.

A plugin with no source configured is the plain static provider: no poll
timer, no applied set, and no subscription state on the per-frame path.

## Contract

| | |
|---|---|
| Plugin type | `:io_binding` |
| Source | one `http://` or `https://` URL, `GET` only, redirects not followed — or a literal `subscription_items` list |
| Source response | `{"version": <integer>, "items": [<string>, …]}` |
| Item strings | opaque — never parsed, validated or interpreted |
| Frames | operator-supplied templates, rendered per delta |
| Commit | only on a successful acknowledgement (or on send in ack-free mode) |
| Retry | re-derive on the next cycle — there is no retry state |
| Durability | the applied set is persisted to the plugin cspec after every commit |
| Failure posture | a failed poll is *no information*: never an unsubscribe |

## The source

```json
{"version": 7, "items": ["BTC/USD", "ETH/USD", "SOL/USD"]}
```

`version` is informational: the provider diffs `items`, so a repeated version
with changed items still reconciles and an advanced version with identical
items sends nothing. A response that is not JSON, has no `items` list, or lists
a non-string is a failed poll (`:invalid_json`, `:missing_items`,
`:items_must_be_strings`). The upstream is the only party that knows its own
symbol namespace: an item it rejects is rendered unchanged and refused there.

The poll is one `Flezha.Plugin.Provider.HttpFetch` GET, shared with the
[HTTP poll plugin](http_poll.md): streamed body cancelled at 1 MiB, 10 s
timeout, conditional GET on the server's ETag (a not-modified answer reconciles
against the set already decoded), verified TLS, redirects not followed. Both
bounds are fixed, not tunable. The fetch runs in a short-lived monitored child
of the connection process, so a slow or hanging source never stops the socket
being read; a poll tick that lands while a fetch is outstanding is dropped, so
a slow endpoint stretches the cadence rather than stacking requests.

## Templates carry the protocol

```text
--Xsubscribe_template='{"method":"subscribe","params":{"channel":"ticker","symbol":{{items}}},"req_id":{{req_id}}}'
--Xunsubscribe_template='{"method":"unsubscribe","params":{"channel":"ticker","symbol":{{items}}},"req_id":{{req_id}}}'
```

| Placeholder | Substituted with |
|---|---|
| `{{items}}` | the delta as a sorted JSON array, e.g. `["ETH/USD"]` |
| `{{req_id}}` | a fresh integer, increasing from 1 |

The provider's whole understanding of the domain is "a set of strings, some
added, some removed"; everything exchange-specific lives in the templates and
the source response.

## Reconcile

Each cycle computes `added = desired − applied` and `removed = applied −
desired`, renders the matching template for each non-empty side and sends the
frames on the **existing** connection. Both empty sends nothing, so a steady
source costs one HTTP request per interval and zero frames. The provider never
exits, tears down the socket or reconnects to apply a change. A delta attempted
while the socket is transiently unestablished is skipped with a `:warning` and
re-derived next cycle.

## Commit is acknowledgement-gated

A delta folds into the applied set only when the upstream answers: an inbound
JSON object whose `"req_id"` equals the one sent is that request's
acknowledgement, and reports success unless it carries `"error"` or
`"success": false`. A rejection drops the request and records
`{:rejected, req_id}` as the last error.

Failures, rejections and timeouts are not tracked as retry state: the applied
set stands, the next cycle re-derives an identical delta and re-sends it under
a fresh `req_id`. Convergence is the retry mechanism. An unanswered request is
abandoned once its delta has been re-sent, so an acknowledgement arriving a
full interval late cannot commit a set the source has since moved on from.

Acknowledgement frames are ordinary inbound frames: they reach the operative's
input stream and touch `last_data_frame_at` only through the `ignore_pattern`
classification. Put a substring of them in `ignore_pattern` if they must not
count as data liveness, exactly as for a heartbeat.

`subscription_ack_mode: "none"` commits a delta on successful send, for
protocols with no correlation id. Drift is still corrected one cycle later, but
a subscription the upstream rejected reads as applied until the data-liveness
watchdog says otherwise.

## Both watchdog tiers re-assert the current set

The plugin runs a two-tier watchdog whether or not a source is configured:

| Tier | Knob | Trigger | Action |
|---|---|---|---|
| connection liveness | `idle_timeout_ms` (default `30000`; `0` disables) | no frame of any kind — data, heartbeat, ping/pong — for the window | the connection process exits `{:shutdown, :idle_timeout}` and the host restarts the plugin through its retry path; emits `[:flezha, :ws, :idle_reconnect]` |
| data liveness | `resubscribe_after_ms` (default `0` = off) | socket alive but no non-`ignore_pattern` data frame for the window | re-sends the subscription on the existing connection, at most once per window; emits `[:flezha, :ws, :resubscribe]` |

Connect and the data-liveness re-subscribe send the same thing: the rendered
`subscribe_template` over the current **applied** set when a source is
configured, the `post_connect` payload when one is not. A re-assertion carries
the reserved `req_id` `0`, outside the in-flight space, so its acknowledgement
— however late — can never be mistaken for a pending delta's. Connect also runs
a reconcile, which is what lets a literal `subscription_items` source subscribe
with no poll to wait for.

The poll timer is separate from the watchdog tick, and connect re-arms it by
cancelling the previous one first, so a reconnect never leaves a second poll
chain running.

## Durability

The applied set is written to the plugin cspec after every successful commit —
the same registry row the enable lives in — so three restarts resume with what
the socket was actually subscribed to:

| Restart | Restored from |
|---|---|
| Plugin restart (`Extension.Table.handle_down/5`) | the extension table's stored opts, which `init_provider/1` re-runs from |
| Operative handover | the same stored opts, carried with the plugin |
| Controller restart | the cspec, re-subscribed by `Plugins.prepare/2` |

The write happens after the acknowledgement, so a crash between send and
acknowledgement degrades to a re-send, never to a subscription nobody
remembers. The cspec write runs in a task, off the Overwatch loop. A failed
poll writes nothing and never unsubscribes; an **explicitly empty** `items`
array is a real instruction and unsubscribes everything.

## Parameters

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `uri` | string | &mdash; | yes | The websocket endpoint to connect to |
| `plugin_id` | string | &mdash; | no | Plugin id; defaults to the URI |
| `ignore_pattern` | string | &mdash; | no | Substring marking a frame as a heartbeat rather than data |
| `post_connect` | json | `%{}` | no | Frame sent once the socket opens |
| `ws_opts` | json | `[]` | no | Options passed to the socket library |
| `payload_metadata` | json | `%{}` | no | Metadata merged into every injection |
| `idle_timeout_ms` | int | `30000` | no | Silence on the connection before it is reopened; 0 disables |
| `resubscribe_after_ms` | int | `0` | no | Silence of DATA frames before the subscription is reasserted; 0 disables |
| `subscription_url` | string | &mdash; | no | Endpoint the desired item set is polled from |
| `subscription_items` | list | &mdash; | no | Literal desired item set, instead of polling one |
| `subscription_interval_ms` | int | `30000` | no | How often the item source is polled |
| `subscription_headers` | json | `%{}` | no | Headers for the item source poll |
| `subscribe_template` | string | &mdash; | no | Frame template carrying the items to subscribe |
| `unsubscribe_template` | string | &mdash; | no | Frame template carrying the items to drop |
| `subscription_ack_mode` | enum | `"required"` | no | `"none"` commits a delta on successful send instead of on the server's ack |
<!-- provider-options:end -->

### Refusals at enable time

Each of these fails the enable and opens no connection:

- a `subscription_url` whose scheme is not `http`/`https`, or that has no host
- a `subscription_url` embedding credentials — headers are the place for a secret
- `subscription_interval_ms` below the 1000 ms floor
- a source without **both** templates: a set that can only grow is not a
  reconcile loop, and discovering the gap at the first removal on a live socket
  is the failure this refuses
- a template with no `{{items}}` placeholder
- malformed `subscription_headers`
- an unrecognised `subscription_ack_mode`
- both `subscription_url` and `subscription_items` — a source is polled or literal
- a `subscription_items` value that is not a list of non-empty strings
- any unrecognised option

## A partitioned member subscribes to its share only

When the plugin's opts carry a partition identity — injected by a partitioned
start's or group enable's plugin fan-out, never written by hand — the desired
set is filtered, between decode and reconcile, to items whose key group falls
in the member's range:

```text
group(item) = fnv1a128(item) mod 256
```

Everything downstream derives from the filtered set: the applied set, the delta
frames, the durable `applied_subscriptions`, the connect-time re-assert. Each
member asks upstream for only its own items, so nothing is received and
discarded and total bandwidth equals one connection's, split. A literal
`subscription_items` list passes through the same filter; `post_connect` stays
an opaque boot frame and is never filtered.

The item string must equal what the operative's `key_by` extracts. That is the
one convention partitioning rests on, and the SDK turns a violation into a park
before any state for that key is created — see
[`partitioned_operatives.md`](../sdk/partitioned_operatives.md).

## Observability

`flz operative info` renders the provider's `inspect_state/1` beneath the
plugin: the applied set, the desired set, requests in flight, the last
reconcile outcome and the last error.

```text
  Plugins:
    - kraken_ws: Flezha.Plugin.Provider.Websocket
        applied: BTC/USD, ETH/USD
        desired: BTC/USD, ETH/USD
        inflight: 0
        last_reconcile: %{added: 1, removed: 0, sent: true}
        last_error: (none)
```

A connection process that cannot be reached reports the last persisted applied
set with `last_error: :no_connection` (or `:probe_timeout`). `flz plugin info`
is the installed-package surface and has no live set to report.

A stalled subscription source is deliberately **not** unhealthy: the socket
keeps streaming what it already applied, and tearing it down would lose that to
fix nothing. It surfaces as `last_error`; `health/1` keeps answering from the
two watchdog tiers alone.

## Telemetry

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :ws, :reconcile]` | `%{added: integer(), removed: integer()}` | `%{plugin_id: binary()}` |
| `[:flezha, :ws, :idle_reconnect]` | `%{silence_ms: integer()}` | `%{plugin_id: binary()}` |
| `[:flezha, :ws, :resubscribe]` | `%{data_silence_ms: integer()}` | `%{plugin_id: binary()}` |

`:reconcile` fires only on a cycle that sent at least one frame, so a steady
source is silent and a flapping one is loud — a source that flaps is a defect
in the source.

## Limits

- **The poll interval is the floor on how fast a change takes effect.** This
  is a convergence loop, not a push channel; shorten the interval for tighter
  bounds.
- **One plugin owns one socket owns one set.** Nothing reconciles
  subscriptions across plugins or operatives.
- **A source outage freezes the applied set.** The socket keeps streaming, and
  reconcile resumes when the source returns.

## See also

- [`http_poll.md`](http_poll.md) — the other `HttpFetch` consumer
- [`lifecycle.md`](lifecycle.md) — `init_provider/1` vs `activate/1`, host-injected identity
- [`../sdk/partitioned_operatives.md`](../sdk/partitioned_operatives.md) — key groups and the `key_by` contract

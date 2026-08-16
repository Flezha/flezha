# Websocket subscription source: changing a live socket's subscription set

`Flezha.Plugin.Provider.Websocket` subscribes to its upstream with a static
`post_connect` payload: one JSON frame, fixed at plugin-enable time, sent on
connect and resent by the data-liveness watchdog. To change what that socket is
subscribed to, an operator had to `flz plugin disable` and `flz plugin enable`
— a full teardown that drops **every other stream on that connection** to add or
remove one item, and spends the upstream's reconnect budget on a change that
needs no reconnect at all. (Kraken, for one, allows roughly 150 connects per ten
minutes per IP, then bans for ten.)

The consequence was that operators avoided runtime subscription changes and ran
one operative per item instead — one connection, one set of topics, one recovery
unit each — which is precisely the waste a multiplexed socket exists to avoid.

A plugin that sets `subscription_url` instead **converges**: it polls that URL
for the desired set, diffs it against what it has applied, and sends subscribe
and unsubscribe frames for the difference on the connection it already holds.

Nothing here changes a plugin that does not configure it. With no
`subscription_url` there is no poll timer, no applied set, and no subscription
state on the per-frame path — `handle_in/2` matches its ordinary clause against
a state map that has no `:inflight` key at all.

## Contract

| | |
|---|---|
| Plugin type | `:io_binding` (unchanged) |
| Source | one `http://` or `https://` URL, `GET` only, redirects not followed |
| Source response | `{"version": <integer>, "items": [<string>, …]}` |
| Item strings | **opaque** — never parsed, validated or interpreted |
| Frames | operator-supplied templates, rendered per delta |
| Commit | only on a successful acknowledgement (or on send in ack-free mode) |
| Retry | re-derive on the next cycle — there is no retry state |
| Durability | the applied set is persisted to the plugin cspec after every commit |
| Failure posture | a failed poll is *no information*: never an unsubscribe |

## The source contract

The endpoint returns the desired set and nothing about delivery:

```json
{"version": 7, "items": ["BTC/USD", "ETH/USD", "SOL/USD"]}
```

`version` is informational — the provider diffs the items, so a repeated
version with changed items still reconciles, and an advanced version with
identical items still sends nothing. Item strings are opaque: a value the
upstream will reject is rendered into the template unchanged and refused by the
upstream, which is the only party that knows its own symbol namespace.

The set's owner publishes intent and never tracks what landed. That is what
lets it be a static file, an S3 object, or a database-backed endpoint with no
knowledge of Flezha at all.

## Templates carry the protocol

Everything exchange-specific lives in configuration:

```
--Xsubscribe_template   '{"method":"subscribe","params":{"channel":"ticker","symbol":{{items}}},"req_id":{{req_id}}}'
--Xunsubscribe_template '{"method":"unsubscribe","params":{"channel":"ticker","symbol":{{items}}},"req_id":{{req_id}}}'
```

| Placeholder | Substituted with |
|---|---|
| `{{items}}` | the delta as a JSON array, e.g. `["ETH/USD"]` |
| `{{req_id}}` | a fresh monotonically increasing integer |

The provider's entire understanding of the domain is "a set of strings, some
added, some removed". That is deliberate: it is what keeps a market-data
requirement from becoming market-data code in a general framework.

## Reconcile

Each cycle computes `added = desired − applied` and `removed = applied −
desired`, renders the matching template for each non-empty side, and sends the
frames on the **existing** connection. Both empty sends nothing, so a steady
source costs one HTTP request per interval and zero frames.

The provider never exits its process, tears down the socket, or reconnects to
apply a change. A send attempted while the socket is transiently unestablished
is skipped with a `:warning` and retried on the next cycle.

## Commit is acknowledgement-gated

A delta folds into the applied set only when the upstream answers. The
correlation contract is the `{{req_id}}` the template carried: an inbound JSON
object whose `"req_id"` equals the one sent is that request's acknowledgement,
and reports success unless it carries `"error"` or `"success": false`.

Failures and timeouts are not tracked as retry state. They simply leave the
applied set unchanged, and the next cycle re-derives an identical delta and
re-sends it under a fresh `req_id`. **Convergence is the retry mechanism.**

An unanswered request is abandoned once its delta has been re-sent, so an
acknowledgement arriving a full interval late cannot commit a set the source has
since moved on from.

Acknowledgement frames are ordinary inbound frames: they reach the operative's
input stream and affect `last_data_frame_at` only through the existing
`ignore_pattern` classification. A deployment that does not want acknowledgements
counting as data liveness puts a substring of them in `ignore_pattern`, exactly
as it would for a heartbeat.

### Protocols with no correlation id

`subscription_ack_mode: "none"` commits a delta on successful send. Drift is
still corrected one cycle later — the desired set is re-diffed against what the
provider believes it applied — but a subscription the upstream rejected will be
recorded as applied until something else surfaces it. The honest signal there is
the data-liveness watchdog: no data on a subscription that reads as applied.

## Connect and both watchdog tiers assert the current set

`handle_connect/3` and the data-liveness re-subscribe both send the rendered
`subscribe_template` over the **current applied set** when a source is
configured, and the `post_connect` payload when one is not.

This closes a correctness edge that predates the feature: the data-liveness
watchdog resent `post_connect`, the *boot-time* payload. Any runtime change
would have been silently reverted by the very mechanism meant to heal a stalled
subscription.

A re-assertion carries the reserved `req_id` `0`, outside the in-flight space
(which starts at 1), so its acknowledgement — however late — can never be
mistaken for a pending delta's.

## Durability

The applied set is written to the plugin cspec after every successful commit,
through the same registry path the enable record uses. Three restarts therefore
resume with what the socket was actually subscribed to:

| Restart | Restored from |
|---|---|
| Plugin restart (`Extension.Table.handle_down/5`) | the extension table's stored opts, which `attempt_restart/3` re-runs `init_provider/1` from |
| Operative handover | the same stored opts, carried with the plugin |
| Controller restart | the cspec, re-applied by the plugin manager |

The write happens *after* the acknowledgement, so a crash between send and
acknowledgement degrades to a re-send, never to a subscription nobody
remembers.

A failed poll never writes anything and never unsubscribes. An **explicitly
empty** `items` array is a real instruction and does unsubscribe everything —
the distinction between "the source said nothing is wanted" and "the source
could not be read" is the whole point of treating a failure as no information.

## Parameters

| Option | Default | Meaning |
|---|---|---|
| `subscription_url` | — | the desired-set endpoint; unset (and no `subscription_items`) means the static `post_connect` provider, unchanged |
| `subscription_items` | — | a literal item list feeding the same desired/reconcile machinery, for a set that needs no endpoint; mutually exclusive with `subscription_url` |
| `subscription_interval_ms` | `30000` | poll cadence; minimum `1000` |
| `subscription_headers` | `{}` | static request headers (JSON object via CLI) — the place for a bearer token |
| `subscribe_template` | — | required with `subscription_url`; must contain `{{items}}` |
| `unsubscribe_template` | — | required with `subscription_url`; must contain `{{items}}` |
| `subscription_ack_mode` | `"required"` | `"none"` commits on send for protocols that echo no id |

### Refusals, at enable time

Each of these fails the enable and opens no connection:

- a `subscription_url` whose scheme is not `http`/`https`, or that has no host
- a `subscription_url` embedding credentials — headers are the place for a secret
- `subscription_interval_ms` below the 1000 ms floor
- a `subscription_url` without **both** templates: a set that can only grow is
  not a reconcile loop, and discovering the gap at the first removal, on a live
  socket, in production, is exactly the failure this refuses
- a template with no `{{items}}` placeholder
- malformed `subscription_headers`
- an unrecognised `subscription_ack_mode`
- declaring both `subscription_url` and `subscription_items` — a source is
  either polled or literal
- a `subscription_items` value that is not a list of non-empty strings
- any unrecognised option

## A partitioned member subscribes to its share only

When the plugin's opts carry a partition identity — injected by a partitioned
start's plugin fan-out, never written by hand — the desired set is filtered,
between decode and reconcile, to items whose key group falls in the member's
range:

```
group(item) = fnv1a128(item) mod 256
```

Everything downstream derives from the filtered set: the applied set, the delta
frames, the durable `applied_subscriptions`, the connect-time re-assert. So
each member asks upstream for only its own items — **no data is received and
discarded**, and total bandwidth equals today's single connection, split. A
literal `subscription_items` list passes through the same filter;
`post_connect` remains an opaque boot frame and is never filtered.

The item string must equal what the operative's `key_by` extracts. That is the
one convention partitioning rests on, and the SDK turns a violation into a
first-frame park naming it — see
[`partitioned_operatives.md`](partitioned_operatives.md).

## Observability

`flz operative info` reports each plugin's provider-supplied state beneath it —
for this provider, the applied set, the desired set, the number of requests in
flight, the last reconcile outcome and the last error:

```
  Plugins:
    - kraken_ws: Flezha.Plugin.Provider.Websocket
        subscription: on_terminate []
        applied: BTC/USD, ETH/USD
        desired: BTC/USD, ETH/USD
        inflight: 0
        last_reconcile: %{added: 1, removed: 0, sent: true}
        last_error: (none)
```

(`flz plugin info` is the *installed-package* surface — it describes a `.fpz`
and its provided modules, and has no operative to report a live set against.)

A stalled subscription source is deliberately **not** unhealthy: the socket
keeps streaming what it already applied, and tearing it down would lose that to
fix nothing. It surfaces as `last_error` here instead.

## Telemetry

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :ws, :reconcile]` | `%{added: integer(), removed: integer()}` | `%{plugin_id: binary()}` |
| `[:flezha, :ws, :idle_reconnect]` | `%{silence_ms: integer()}` | `%{plugin_id: binary()}` |
| `[:flezha, :ws, :resubscribe]` | `%{data_silence_ms: integer()}` | `%{plugin_id: binary()}` |

`:reconcile` fires only on a cycle that sent at least one frame, so a steady
source is silent and a flapping one is loud — which is the signal an operator
wants, since a source that flaps is a defect in the source.

## Lifecycle and threading

The poll runs in a short-lived monitored child of the connection process, so a
slow or hanging source cannot stop the socket being read. The poll timer is
separate from the watchdog tick — the two cadences answer different questions —
and is re-armed idempotently on connect, so a reconnect cannot leave a second
poll chain running.

The HTTP request itself is `Flezha.Plugin.Provider.HttpFetch`, shared with
[the HTTP poll plugin](http_poll_plugin.md): streamed body cancelled at a cap,
conditional `GET` on the server's ETag, verified TLS, redirects not followed.

## Limits

- **The poll interval is the floor on how fast a change takes effect.** This is
  a convergence loop, not a push channel; callers that need tighter bounds
  shorten the interval.
- **One plugin owns one socket owns one set.** Nothing reconciles subscriptions
  across plugins or operatives.
- **A source outage freezes the applied set.** Correct by construction: the
  socket keeps streaming, and reconcile resumes when the source returns.

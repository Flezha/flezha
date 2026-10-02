# HTTP poll plugin

For users who want a slowly-changing HTTP resource — runtime configuration, reference
data, any external state — delivered onto an operator's entry channel, and for authors
reading it as the smallest two-phase source provider. `Flezha.Plugin.Provider.HttpPoll`
(provider id `operator_http_poll`) polls one URL on an interval and injects the body as
an ordinary journaled entry frame, only when its content changed. This page covers the
contract, parameters, change detection, confirmed delivery, failure semantics, lifecycle,
telemetry and limits.

## Contract

| | |
| --- | --- |
| Plugin type | `:io_binding` |
| Hooks | `:on_terminate`: the poller drives itself; the hook is there because a plugin spec must name at least one |
| Shared | yes: one poller per controller for every operator bound to the plugin, or one in the whole cluster with `--single`. Each member of a partitioned operator runs a poller of its own |
| Upstream | one `http://` or `https://` URL, `GET` only, redirects not followed |
| Injection | the response body, on the entry channel `:channel` (or `:channel_in`) names, only when its SHA-256 differs from the last delivered one |
| Delivery | the confirming ingest — the hash advances only once the operator has taken the payload |
| Seed | the first successful poll after a (re)start always injects |
| Failure posture | inject nothing, keep the last hash, count toward health |

Because the frame is journaled like any other entry input, replay determinism, the
effectively-once tier and a steps operator's durable input behave exactly as for any other source.

```elixir
Flezha.Service.PluginManager.enable_plugin(
  "signal-config",
  Flezha.Plugin.new(
    id: "signal-config",
    provider: Flezha.Plugin.Provider.HttpPoll,
    opts: %{url: "https://api.internal/internal/signal-config", channel: "config_in", interval_ms: 30_000},
    namespace: "default",
    operator_id: "detector",
    hooks: [{:on_terminate, []}]
  )
)
```

As a plugin spec at operator start (`flz operator start --plugins`):

```json
[
  {
    "name": "signal-config",
    "provider": "operator_http_poll",
    "opts": {
      "url": "https://api.internal/internal/signal-config",
      "channel": "config_in",
      "interval_ms": "30000",
      "headers": "{\"authorization\":\"Bearer ${TOKEN}\"}"
    },
    "hooks": ["on_terminate"]
  }
]
```

`channel` names the entry channel the body is injected on. `channel_in` is the
explicit spelling of the same name, for a user writing one vocabulary
across every provider; one of the two is required, and writing both is refused.
`channel_out` is not a parameter here: the poller never acts on the records
passing through the operator, so the option would have nothing to read.

Either spelling takes exactly one name. A body is injected on exactly one channel, so
several names comma-separated — `trades,quotes` — are refused when the plugin is enabled
rather than read as a channel whose name contains a comma.

## Parameters

<!-- provider-options:begin -->
| Option | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `url` | string | &mdash; | yes | The resource to GET each interval |
| `channel` | names | &mdash; | no | The channel this plugin is attached to: the entry channel the body is injected on, and one name. Required, unless `channel_in` names it |
| `channel_in` | names | &mdash; | no | Entry channel the body is injected on, and one name; the explicit spelling of `channel`. Required, unless `channel` names it |
| `plugin_id` | string | &mdash; | no | Plugin id; defaults to the id it was enabled under |
| `headers` | json | `%{}` | no | Request headers, as a JSON object |
| `interval_ms` | int | `60000` | no | Milliseconds between polls |
| `timeout_ms` | int | `10000` | no | Budget for one request |
| `max_body_bytes` | int | `1048576` | no | Largest body accepted before the poll is refused |
| `failure_threshold` | int | `3` | no | Consecutive failed polls before the plugin reports unhealthy |
| `idle_after_ms` | int | `0` | no | Age of the last settled poll past which the source is idle; 0 derives it from `interval_ms` |
<!-- provider-options:end -->

**One of `channel` and `channel_in` is required.** The Required column can only
speak of one row at a time, so it reads `no` for both; a plugin naming neither is
refused when it is enabled, with a message naming both spellings.

Any other key is refused at attach rather than ignored — a POST body, an auth flow, a
backoff ladder — so a config asking for something the provider does not offer fails
loudly. The check runs while the options are prepared, in
`Flezha.Plugin.ProviderBehaviour.Options.normalise_opts/2`, which the extension table
calls before `init_provider/1` on the CLI path and the `--plugins` spec path alike.

Every log line, telemetry label and attach error carries the URL with its userinfo and
query stripped; config endpoints routinely carry an API key in the query string, and
telemetry labels are exported to the observability sinks. Put secrets in `headers`.

### Per-partition URLs

`url` may carry `{{partition_index}}` and `{{partition_count}}`, substituted once at
`init_provider/1` from the identity a partitioned start's plugin fan-out injects:

```text
url: "https://api.example/trades?shard={{partition_index}}&of={{partition_count}}"
```

Partition 1 of 3 polls `…?shard=1&of=3`. Validation runs on the rendered URL, so a
substitution that lands in the scheme or host is refused at init. A placeholder on a
plugin with no partition identity is refused, naming the placeholder. See
[../sdk/partitioned_operators.md](../sdk/partitioned_operators.md).

## Change detection

The gate is the SHA-256 of the raw response bytes: JSON, text and binary all work, and an
application wanting field-level semantics puts a version inside the body. When the server
offers an `ETag`, the next request carries `If-None-Match` and a `304` is a successful
unchanged poll — bandwidth only; the local hash stays the injection gate.

### The seed

A fresh poller holds no hash, so its first success always injects. Provider state is not
portable across a handover, and the seed is what converges an operator that was just
started, recovered, migrated or had its plugin restarted onto current content within one
interval. The cost: a rejoining operator may see content it already had.
**Consumers of a polled channel must treat its frames idempotently** — free for
configuration-shaped payloads. The frames are journaled either way, so replay stays
deterministic.

## Delivery is confirmed

Injection is `GenServer.call(overwatch, {:ingest, body, %{"channel" => entry_channel}})`,
where `entry_channel` is the name `channel` or `channel_in` resolved to when the plugin was
enabled. It is never the fire-and-forget `:io_in` cast, which drops silently in ordinary
states — a parked operator, an unbound stream, a failed durable input-log append. A refused
or not-ready ingest is a failure like any other: it counts toward health and leaves the
hash alone, so the next poll re-offers the same content. `{:ok, :absorbed}` (a duplicate the operator
already holds) and `{:ok, :suspended}` (journaled, delivered on wake) both count as
delivered.

## Failures

Timeouts, connection errors, any status outside 2xx (including 3xx), a body over
`max_body_bytes`, a body that is not valid UTF-8, and a refused ingest are failures. A
failure injects nothing and retains the last delivered hash — being unable to re-read the
resource does not invalidate what the operator already holds.

- Non-UTF-8 is a failure rather than a frame because the SDKs seal entry payloads to
  strings.
- A failing poll leaves the `ETag` uncommitted. Storing a validator for a rejected body
  would make the next request conditional on it, the server would answer `304`, the
  failure run would reset, and the plugin would sit healthy, silent and empty.
- The size cap is enforced as the body streams in; the request is cancelled at the
  boundary, so a misbehaving endpoint costs the cap, not the response.
- Redirects are not followed: `:httpc` would replay the static headers — where a bearer
  token lives — to a host the provider never validated, and would drop an `https://`
  request's verification on a downgrade. A `3xx` is reported as `{:http_status, code}`.

Consecutive failures accumulate. At `failure_threshold`, `health/1` reports
`{:unhealthy, {:poll_failing, reason}}` and the convergence loop escalates to a plugin
restart. The next success clears the run on the poll that reports it, and injects the
content if it changed during the outage — one recovery, one frame. A plugin that
convergence already restarted carries no hash and re-seeds, so recovery by escalation
costs one frame whether or not the content moved.

## Lifecycle and threading

- `init_provider/1` validates and normalises with no upstream I/O, so a slow or
  unreachable endpoint cannot stall plugin preparation.
- `activate/1` — after the operator's SDK side is bound — starts the poller and returns;
  the first request is scheduled, not executed.
- Every HTTP wait runs in a short-lived monitored child of the poller. The host's dispatch
  path is never inside a request, and the poller stays answerable while one is
  outstanding, so `health/1` can report on the hanging endpoint that would otherwise
  block it.
- A tick that lands while a request is outstanding is dropped: a slow endpoint stretches
  the cadence instead of stacking requests.
- The host tracks the poller, kills it when the plugin is removed and re-runs both phases when it
  dies; the poller monitors the host in turn and cannot outlive the operator it feeds.

## Telemetry

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:flezha, :plugin, :http_poll, :shipped]` | `bytes`, `latency_ms` | `plugin_id`, `channel`, `seed` |
| `[:flezha, :plugin, :http_poll, :unchanged]` | `latency_ms` | `plugin_id`, `channel` |
| `[:flezha, :plugin, :http_poll, :failed]` | `consecutive_failures`, `latency_ms` | `plugin_id`, `channel`, `reason` |

All three bridge to the observability bus under source `:flezha_http_poll`: `:injected`
and `:unchanged` as metrics, `:failed` as an event, because its `reason` carries
per-poll values (an observed body size, an ingest verdict) that would be unbounded
cardinality on a metric. Alert on the absence of `:injected` / `:unchanged` or on the
plugin's health verdict, not on a failure-rate metric.

## Limits

- **`GET` only.** No POST, streaming, webhooks, or auth beyond static headers.
- **No per-item diffing.** A changed byte anywhere re-injects the whole body.
- **`channel` is not validated at attach.** Channel names are an SDK-side topology fact
  that never reaches the controller (`AddOperatorResponse.metadata` carries capability
  facts only), and attach happens before the operator binds. An undeclared name surfaces
  on the runner as a dead-letter park under that name, exactly as a mistyped
  `flz operator send --channel` does: `flz dlq list` shows the frames and
  `flz dlq replay` re-injects them once a channel of that name exists.
- **HTTPS is verified** against the system CA store; a host with no usable store fails the
  poll rather than dropping to an unverified connection.
- **Requests share the default `:httpc` profile** with the rest of the controller;
  per-request options are explicit and nothing global is mutated, but pooling is shared.

## See also

- [lifecycle.md](lifecycle.md) — the two-phase provider contract this plugin follows
- [websocket_subscription.md](websocket_subscription.md) — the same fetch semantics used for a subscription source
- [../sdk/partitioned_operators.md](../sdk/partitioned_operators.md) — the partition identity behind `{{partition_index}}`
- [../operations/observability.md](../operations/observability.md) — where the bridged events land

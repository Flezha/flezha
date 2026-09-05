# Operative-forward plugin

For operators composing work across operatives, and for plugin authors who need
a sink of this shape. `Flezha.Plugin.Provider.OperativeForward` (provider id
`operative_forward`) is the member of the sink set whose destination is another
operative in the same namespace rather than an external endpoint. It is a
configuration change on the source operative only: the payload arrives on the
target's ordinary input stream, and the target cannot tell a peer from a source
adapter. This doc covers the contract, parameters, refusals, partitioned
targets, the worker model, the verdicts it acts on, telemetry and health. The
cross-hop delivery guarantee itself is specified in
[`../resilience/data_durability.md`](../resilience/data_durability.md)
"Operative to operative" and is not repeated here.

## Contract

| | |
|---|---|
| Plugin type | `:io_binding` |
| Extension points | `:on_output` (feed a downstream operative), `:on_input` (pass-through tap) |
| Destination | an operative in the **same namespace** |
| Delivery | asynchronous; the callback enqueues and returns |
| Retry | until the target confirms, capped exponential backoff |
| Loss posture | delay, never drop — no delivery deadline |
| Guarantee | derived from the source's outbox, never configured — there is no ack-mode option |

```elixir
Flezha.Service.PluginManager.enable_plugin(
  "coder->critic",
  Flezha.Plugin.new(
    id: "coder->critic",
    provider: Flezha.Plugin.Provider.OperativeForward,
    opts: %{target_operative_id: "critic"},
    namespace: "default",
    operative_id: "coder",
    subscriptions: [{:on_output, []}]
  )
)
```

The identity stamps (`flz.seq`, `flz.key`, `flz.epoch`) cross the hop
unmodified; the plugin never rewrites metadata. A delivery-guaranteed output is
taken under an outbox hold when it is enqueued and confirmed when the target's
ingest confirms, which is what lets the source's outbox cursor wait on the hop.

## Parameters

| Option | Required | Default | Meaning |
|---|---|---|---|
| `:target_operative_id` | yes | — | the operative (or partition group) to deliver into |
| `:queue_max` | no | `10_000` | queue bound; past it the enqueueing caller parks |
| `:retry_backoff_ms` | no | `250` | first retry delay |
| `:retry_max_backoff_ms` | no | `5_000` | backoff cap |
| `:call_timeout_ms` | no | `5_000` | per-attempt confirming-call timeout |
| `:plugin_id` | no | `"<source>-><target>"` | worker key component |

Unknown options are rejected by `build/1` rather than ignored, so a config
asking for a delivery mode this provider deliberately does not offer fails
loudly instead of reading as accepted.

## Enable-time refusals

| Condition | Result |
|---|---|
| `:target_operative_id` missing | error naming the option; no worker started |
| target equals the source operative | error identifying the self-target |
| target resolves in a **different** namespace | error naming the namespace boundary |
| target does not resolve anywhere | **accepted** — it may not be started yet, and the worker retries |

Longer cycles (`A → B → A`) are deliberately not refused: an orchestrator and
its critics feeding each other terminate by operative logic, and topology alone
cannot tell a productive loop from a runaway one. Queue-depth telemetry is the
signal to watch.

## A partitioned target routes per message

When `:target_operative_id` names a partition group, `init_provider/1`
resolves the member set and their key-group ranges and keeps **one worker per
member**, keyed `{plugin_id, member}`. Each message routes by `group(flz.key)`
to the member whose range owns it, so repeated outputs for one key always take
the same member. Per-member workers rather than one router: a worker is one
queue with one in-flight slot, so a shared one would head-of-line-block every
partition behind the slowest, while per-key ordering only needs per-member
FIFO.

A message with **no `flz.key`** — an emit on a channel not declared `.keyed()`,
or any workflow-outbox output — is parked via the DLQ store with
`partitioned target requires keyed emit`, naming the source, the target and the
contract. It is never round-robined and never payload-hashed: both scatter one
key across partitions and fail downstream, where the cause is invisible. A key
that hashes to a group no member owns parks the same way.

A target with one member (or an ordinary operative) receives everything, keyed
or not. Disabling the plugin stops **every** worker, and `health/1` reports the
sickest of them.

### When the target rescales

`flz operative scale` on a partitioned target nudges every forward pointing at
it to re-run `init_provider/1`, so nothing keeps routing by the old ranges:

- a surviving member keeps its worker and everything still queued in it;
- a vanished member hands its queue back before its worker stops, and each
  payload is re-routed to whoever owns its key now. The hand-over runs while
  the plugin is being re-prepared, so no emit can be routed in between and a
  re-homed payload lands ahead of anything emitted afterwards — per-key order
  survives the rescale;
- a payload whose key belongs to no member after the rescale is parked, as above.

A source this controller cannot reach at that moment keeps the old member set
until it next re-prepares; the rescale logs it rather than failing, since the
rescale itself has already succeeded.

## Nothing ever blocks one Overwatch on another

The callback enqueues into a long-lived per-`{plugin_id, target}` worker and
returns. The worker performs the confirming delivery in a short-lived monitored
process, never in its own `handle_call`, so it always services an enqueue
promptly. Were delivery inline, two operatives forwarding to each other would
wedge the moment both had a message in flight.

```text
   A.Overwatch                                  B.Overwatch
   ─────────────                                ─────────────
   :on_output ──enqueue (call, returns)──► ForwardWorker
                                              │ confirming delivery + retry
                                              └────────────► ingest, verdict
                                                                   │
                                                          :on_input → journal_input
                                                                   → durable inbox
```

Order is preserved: one payload is in flight at a time, and a retryable
verdict re-arms the same head of the queue. Past `:queue_max` the enqueueing
caller parks until a delivery frees space, which back-pressures the source
Overwatch.

## Verdicts

The worker delivers through
`Flezha.Service.OperativeManager.send_io_confirming/5`, which runs the target's
single pre-delivery ingest path — the `:on_input` extension chain, then the
input journal — and answers what happened.

| Verdict | Meaning | Worker action |
|---|---|---|
| `:ok` | forwarded to the bound runner, or durably journaled | done |
| `{:ok, :absorbed}` | filtered by an `:on_input` plugin, or absorbed as a duplicate | done |
| `{:ok, :suspended}` | durable in the target's inbox; delivered when it rehydrates | done |
| `{:error, :not_ready}` | target parked with an `init_error`, or unbound with no durable log | retry |
| `{:error, :not_found}` | no pid registered for `(namespace, operative_id)` | retry |
| `{:error, :refused}` | the durable log refused the append | retry |

A call that exits (timeout, dead target) or a delivery process that dies
without reporting is retryable too (`{:unreachable, _}`,
`{:delivery_crashed, _}`). The verdicts and the ingest entry points they belong
to are specified in
[`rust_operative_architecture.md`](../sdk/rust_operative_architecture.md)
"Ingest entry points".

"Filtered" and "duplicate" are successes because the receiver has decided the
payload needs no further delivery; a deterministic filter would filter the
retry too. A suspended target is a success, not an outage: the payload is
durable in its inbox and the append itself wakes it. The plain
`{:io_in, _, _}` cast keeps its silent drop for a parked operative; a sender
that needs the payload held uses the confirming path.

## `:on_message` or `:on_output` forward?

| | `:on_message` + `MessageForward` | `:on_output` + `OperativeForward` |
|---|---|---|
| Shape | synchronous request/reply | asynchronous data flow |
| Destination | an `on_message/2` handler on the target's Overwatch | the operative's **input stream** |
| Durability | none — the reply is the result | the target's durable inbox / input log |
| Caller | any process that can reach the Overwatch | the source operative's output |

Reach for `:on_message` to **query** an operative; reach for this plugin to
**feed** one with work that belongs in its input stream and must survive a
restart of either end. See [`message_handlers.md`](message_handlers.md).

## Telemetry

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :plugin, :operative_forward, :delivered]` | `%{count: 1}` | `+ :outcome` (`:delivered`, `:absorbed`, `:suspended`) |
| `[:flezha, :plugin, :operative_forward, :retry]` | `%{attempt:, backoff_ms:}` | `+ :reason` |
| `[:flezha, :plugin, :operative_forward, :queue_depth]` | `%{depth:}` | on every enqueue |
| `[:flezha, :plugin, :operative_forward, :saturated]` | `%{depth:}` | the queue reached `:queue_max` |

Every event carries `:plugin_id`, `:namespace`, `:source_operative_id` and
`:target_operative_id`. All four are bridged onto the observability bus as the
`:flezha_operative_forward` source, so a saturating cycle is visible without
parsing log lines.

## Health

The provider is healthy while its worker — every worker, for a partitioned
target — is alive. A delivery failure is not unhealthiness: an unreachable peer
is retried, and convergence is already healing it. The first deferred delivery
of a run logs a warning with the reason and the backoff.

Disabling the plugin stops the worker for good — it is a `:transient` child,
so the host's `:shutdown` of the owned pid is final rather than a restart. A
re-prepared plugin (Overwatch restart) reuses its live worker, so
queued-but-undelivered payloads survive it.

## Implementation notes

The worker is built from the seams every queue-holding provider shares —
`WorkerSup` for supervision, `ProviderBehaviour.park_or_reply/2` /
`release_waiters/1` for the overflow policy, and the `:forward_deliver` row of
`Flezha.Config.Backoff` for the ladder. See [`lifecycle.md`](lifecycle.md) "Shared helpers for queue-holding
workers" before writing another sink of this shape; its two rules (never
deliver from the worker's own `handle_call`, and pick `restart` deliberately)
are both load-bearing here.

## See also

- [`../resilience/data_durability.md`](../resilience/data_durability.md) — the cross-hop guarantee and what survives a controller restart
- [`message_handlers.md`](message_handlers.md) — the synchronous alternative
- [`../sdk/partitioned_operatives.md`](../sdk/partitioned_operatives.md) — key groups and rescale

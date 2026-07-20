# Operative-forward plugin: one operative's output into another's input

`Flezha.Plugin.Provider.OperativeForward` is the member of the sink set whose
destination is another operative in the same namespace rather than an external
endpoint. It composes work across operatives without a round trip through a
broker — and without a delivery guarantee Flezha does not already own.

Everything about it is a configuration change on the source operative: the
target operative is unaware it is being fed by a peer rather than by a source
adapter, because the payload arrives on its ordinary input stream.

## Contract

| | |
|---|---|
| Plugin type | `:io_binding` |
| Extension points | `:on_output` (feed a downstream operative), `:on_input` (pass-through tap) |
| Destination | an operative in the **same namespace** |
| Delivery | asynchronous; the callback enqueues and returns |
| Retry | until the target confirms, capped exponential backoff |
| Loss posture | delay, never drop — no DLQ routing, no delivery deadline |

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

## Parameters

| Option | Required | Default | Meaning |
|---|---|---|---|
| `:target_operative_id` | yes | — | the operative to deliver into |
| `:queue_max` | no | `10_000` | queue bound; past it the enqueueing caller parks |
| `:retry_backoff_ms` | no | `250` | first retry delay |
| `:retry_max_backoff_ms` | no | `5_000` | backoff cap |
| `:call_timeout_ms` | no | `5_000` | per-attempt confirming-call timeout |
| `:plugin_id` | no | `"<source>-><target>"` | worker key component |

Unknown options are **rejected** by `build/1` rather than ignored. In
particular there is no delivery-mode or ack-mode option: see
"The guarantee is derived, never configured" below.

## Enable-time refusals

| Condition | Result |
|---|---|
| `:target_operative_id` missing | error naming the option; no worker started |
| target equals the source operative | error identifying the self-target |
| target resolves in a **different** namespace | error naming the namespace boundary |
| target does not resolve anywhere | **accepted** — it may not be started yet, and the worker retries |

Longer cycles are deliberately not refused. `A → B → A` is the motivating
pattern (an orchestrator and its critics feeding each other); it terminates by
operative logic, and the platform cannot tell a productive loop from a runaway
one by topology alone. Queue-depth telemetry is the signal an operator watches
instead.

## Nothing ever blocks one Overwatch on another

The `:on_output` callback enqueues into a long-lived per-`{plugin_id, target}`
worker and returns. The worker performs the confirming delivery **in a
short-lived monitored process**, never in its own `handle_call`, so it always
services an enqueue promptly.

```
   A.Overwatch                                  B.Overwatch
   ─────────────                                ─────────────
   :on_output ──enqueue (call, returns)──► ForwardWorker
                                              │ confirming delivery + retry
                                              └────────────► ingest, verdict
                                                                   │
                                                          :on_input → journal_input
                                                                   → durable inbox
```

That is the single most important property in the design: were delivery
performed inline, two operatives forwarding to each other would wedge the moment
both had a message in flight.

Order is preserved — one payload is in flight at a time, and a retryable verdict
re-arms the same head of the queue.

## Verdicts

The worker delivers through `Flezha.Service.OperativeManager.send_io_confirming/5`,
which runs the target's single pre-delivery ingest path — the `:on_input`
extension chain, then `journal_input/2` — and answers what happened.

| Verdict | Meaning | Worker action |
|---|---|---|
| `:ok` | forwarded to the bound runner, or durably journaled | done |
| `{:ok, :absorbed}` | filtered by an `:on_input` plugin, or absorbed as a duplicate | done |
| `{:ok, :suspended}` | durable in the target's inbox; delivered when it rehydrates | done |
| `{:error, :not_ready}` | target parked with an `init_error`, or unbound with no durable log | retry |
| `{:error, :not_found}` | no pid registered for `(namespace, operative_id)` | retry |
| `{:error, :refused}` | the durable log refused the append | retry |

"Filtered" and "duplicate" are successes because both mean the receiver has
decided the payload needs no further delivery. Retrying either is at best wasted
work and at worst an infinite loop, since a deterministic filter filters the
retry too.

A suspended target is a success, not an outage: the payload is durable in its
inbox and the append itself wakes it. That is what makes the agent cycle cheap —
a parked receiver costs nothing.

The pre-existing `{:io_in, _, _}` cast is unchanged, including its silent drop
for a parked operative. A sender that needs the payload held uses the confirming
path.

## The guarantee is derived, never configured

```
   Source has a transactional outbox        Source has no outbox
   (effectively-once, or durable bounded)   (plain unbounded)
   ────────────────────────────────────     ──────────────────────
   identity stamped (flz.seq / flz.key)     no identity ledger
   worker retries until confirmed           worker retries until confirmed
   receiver dedups by flz.msg_id / hash     receiver dedups iff durable
   ⇒ effectively-once across the hop        ⇒ at-least-once, best effort
```

An acknowledgement is only worth having if the sender can act on it, and acting
means re-sending from a durable record. That record is the transactional outbox,
which exists for exactly those sender kinds. Offering an "ack mode" to a sender
with nothing to re-send from would sell a guarantee that cannot be honoured.

The identity stamps (`flz.seq`, `flz.key`, `flz.epoch`) cross the hop unmodified
— the plugin never rewrites metadata.

See `docs/resilience/data_durability.md` for the restart limitation.

## `:on_message` or `:on_output` forward?

Two ways to reach another operative, and the distinction is real:

| | `:on_message` + `MessageForward` | `:on_output` + `OperativeForward` |
|---|---|---|
| Shape | synchronous request/reply | asynchronous data flow |
| Destination | the SDK factory's `handle_message` callback | the operative's **input stream** |
| Durability | none — the reply is the result | the target's durable inbox / input log |
| Caller | any cluster-reachable caller | the source operative's output |

Reach for `:on_message` to **query** an operative — a side-channel question whose
answer you need now. Reach for this plugin to **feed** an operative — work that
belongs in its input stream and must survive a restart of either end.

## Telemetry

| Event | Measurements | Metadata |
|---|---|---|
| `[:flezha, :plugin, :operative_forward, :delivered]` | `%{count: 1}` | `+ :outcome` |
| `[:flezha, :plugin, :operative_forward, :retry]` | `%{attempt:, backoff_ms:}` | `+ :reason` |
| `[:flezha, :plugin, :operative_forward, :queue_depth]` | `%{depth:}` | |
| `[:flezha, :plugin, :operative_forward, :saturated]` | `%{depth:}` | |

Every event carries `:plugin_id`, `:namespace`, `:source_operative_id`, and
`:target_operative_id`. All four are bridged onto the observability bus as the
`:flezha_operative_forward` source, so a saturating cycle is visible without
parsing log lines.

## Health

The provider is healthy while its worker is alive. A delivery failure is not
unhealthiness: an unreachable peer is retried, and convergence is already
healing it.

Disabling the plugin stops the worker for good — it is a `:transient` child, so
the host's shutdown of the owned pid is final rather than a restart.

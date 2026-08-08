# The poll-based SDK core (`flz_core`)

`flz_core` is the language-agnostic runner kernel: transport, ordering,
durability, retries, timers, credit, routing, and the bounded driver live in
one Rust crate. A host language SDK drives it through exactly one invocation
surface:

```text
loop:
  activation = core.poll(timeout)     # blocks inside the core
  result     = run_user_code(...)     # host threads, host semantics
  core.complete(result)
```

The core **never calls into host code and never owns a host thread** (the
FFI log callback is the one permitted exception). Python keeps asyncio, Go
keeps goroutines, Java keeps JVM threads; each SDK writes the loop above
idiomatically. The Rust SDK itself is the first host: its typed layer spawns
poll workers (`FLEZHA_WORKER_POOL_SIZE`, default = available parallelism)
and consumes the same engine as a crate — the dogfood rule that keeps the
boundary from eroding.

## Activation / ActivationResult

The boundary contract lives in `flz_core::schema` and is postcard-encoded;
field order IS the wire contract and `SCHEMA_VERSION` gates it. Every kind
is pinned by hex byte fixtures (`schema.rs fixture_tests`) that each shim
must reproduce byte-identically.

An **Activation** targets exactly one entity and carries:

- `Work(items)` — an ordered per-entity batch of `Input` items (payload
  bytes, metadata, `consumed_offset`, replay flag, core-evaluated `now`)
  and `Timer` firings (kind, `scheduled_at`; its `now` IS `scheduled_at`,
  so a chained `timer_after` anchors replay-stably on the firing that woke
  it). Timer firings are validated against the pending set at assembly —
  a stale entry fires nothing.
- One of the five host-participating command kinds — `RestoreFold`,
  `SnapshotRequest`, `LiveStateRead`, `ApplyState`, `MigrationSerialize` —
  subject to the same per-entity exclusivity. The other 14 control commands
  never surface (pinned: a `Ping` generates no activation).
- `KeyRequest` — batched keying for unkeyed entry payloads bound for a
  keyed edge, answered before anything reaches an entity mailbox.

An **ActivationResult** carries, per processed item, the effect record the
flusher consumes exactly as it consumes a typed drain today: host-encoded
change bytes (core-opaque), outputs `(edge | plugin, bytes, key?)`,
`consumed_offset`, timer ops, checkpoint flag, and a verdict
`ok | retryable_error | non_retryable_error`.

## Per-entity exclusivity and ordering

The core never hands out two activations for the same entity concurrently —
the single-active-drainer CAS gate, expressed at the boundary. An entity
with an outstanding activation reappears in poll results only after its
completion; its inputs are delivered in enqueue order across successive
activations. Batch assembly also owns the consumed-floor skip, replay-slice
buffering and event-time-ordered flush, and late-input admission.

**Event-time-clocked classes get ONE admitted input per activation** (and a
closed replay slice drains one member per round): chained timer
registrations land only at completion, so wider batches would make
due-firing interleave depend on batch timing — and a correction replay must
fold firings at the same positions the live run did. Wall/logical classes
batch freely.

## The retry→park ladder

The engine drives the ladder from completion verdicts: a `retryable_error`
re-hands the same item at the head of the line after the existing backoff
(`base × 2^(n-1)` ±50% jitter, capped), with the unprocessed batch tail
held behind it; a `non_retryable_error` — or a failing final attempt —
parks the payload through the existing DLQ path, and a park closes the
checkpoint interval. Attempt counts and `RETRIES_TOTAL` ride the existing
telemetry.

## Producer-side transforms and keys (D4)

Consumer-edge transform chains and `key_by` evaluate on the producing side
— for byte-level hosts, in the completion's outputs (`(edge, key)` is all
the core routes on); for the in-process Rust host, at the one routing seam
(`flz_core::routing`) both live delivery and the controller's replay slice
travel, which is what keeps replay determinism free. A transform or key
failure is contained once and enters the ladder.

## The C ABI (`flz_core_ffi`)

Context lifecycle: `flz_core_context_new(shim_schema_version)` →
`flz_core_declare_class` (JSON) → `flz_core_start(namespace)` →
`flz_core_poll` / `flz_core_complete` → `flz_core_shutdown` →
`flz_core_context_free`.

- **Version negotiation happens at creation**: a shim built against a
  different `SCHEMA_VERSION` gets NULL and `flz_core_init_error()` names
  both versions — before any controller connection. Silent runtime skew is
  impossible.
- **Any host thread may poll; any host thread may complete**, including a
  different one than the thread that polled. No affinity.
- `flz_core_poll` blocks up to its timeout and returns a bounded-margin
  timeout result; **shutdown wakes every blocked poller promptly** with a
  distinguishable status. Unpolled work follows the existing durability
  rules (journaled controller-side, re-forwarded on the next bind).
- Ownership: whoever allocates frees. Activation buffers the core hands out
  are freed with `flz_core_buf_free`.

The CI scenario (`flz_core_ffi/tests/abi_scenario.rs`) drives connect,
AddOperative over the real control path, input activations, a timer firing,
and shutdown entirely through the C ABI — a capability reachable in the
crate API but not the ABI is a defect, not a backlog item.

### Host classes (v1 scope)

`flz_core_declare_class` declares single-mode procedures fed by
bytes-passthrough entry channels, with optional plugin-bound outputs,
wall-clock `now` (stamped core-side at translation), processing-time
timers, and optional persistence (host-encoded change bytes framed by the
core). Keyed edges, event-time clocks, restore folds, live reads,
inter-procedure edges and bounded lifecycles for host classes land with the
first full host SDK; their activation kinds and byte fixtures are already
part of the schema.

### In-process residuals

Two host-code-on-core-thread seams remain for the crate-consuming Rust host
and are invisible to the C ABI: `TypedBuffer<S>` serializes `S::Change` on
the flusher's linger thread, and erased entry parsers / edge closures
evaluate on the wire-reader/routing thread. Both are the documented
in-process forms of what the byte boundary removes for FFI hosts.

## What stays per-language, forever

The authoring surface (Procedure traits, the graph DSL), State and channel
codecs, the poll loop's integration with the host's concurrency primitives,
idiomatic errors, and the testing harness — the code that *should* feel
native, an estimated 3–5k lines per SDK.

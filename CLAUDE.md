# Flezha repo notes

Detailed per-change implementation notes (gotchas, load-bearing decisions, test
maps) live in `docs/dev_notes/<change-id>.md` — one file per OpenSpec change.
Before touching an area listed below, READ its notes file first; the gotchas
there are deliberately not repeated in `docs/`.

Maintenance rule: when a change lands, write its notes to
`docs/dev_notes/<change-id>.md` and add ONE index line here (newest first).
Never inline the notes in this file — it must stay small enough to load whole.

## Per-change notes index

- [Durable operator intent](docs/dev_notes/durable-operator-intent.md) — intent = the hub declared set (pairs with process_hub `declared-children-and-remote-manifest`); every deliberate stop MUST ride `stop_children`; cspec stays `:transient` (upstream loosened); re-place uses `migrate_child`, never stop+start
- [Wire-frame limits](docs/dev_notes/wire-frame-limits.md) — reject a declared length BEFORE allocating, pre-auth; one `max_frame_bytes()` in flz_transport for both parsers; a bad header poisons the stream, there is no resync
- [Authn/authz finished end to end](docs/dev_notes/finish-platform-authn-authz.md) — three-way enforcement posture; identity ONLY from the transport; namespace-prefixed state at the `Registry.class_dir/1` seam; the monotonic-time disk-cache bug
- [Tiered procedure authoring](docs/dev_notes/add-tiered-procedure-authoring.md) — three tiers, explicit wins; `emits` is the ONLY source of the ctx-shape output type; `SimpleProcedure`'s method-level `where Self: Procedure` breaks the supertrait cycle
- [`route_latest` building block](docs/dev_notes/add-route-latest-building-block.md) — a document reaches each keyed Entity on that key's OWN traffic, never a remembered-set fan-out; Java codec must come from `desc.kryoTypes()`
- [One plugin-sink method: `sink` + `sealed`](docs/dev_notes/unify-plugin-sink-api.md) — terminality is DERIVED from the wiring; `sealed()` must not touch the builder; the `sink(name)` arg is inert at runtime
- [Operative kinds are `stream` and `workflow`](docs/dev_notes/rename-operative-kinds-to-stream-and-workflow.md) — three senses of the old kind word and only one renamed; `scripts/check-kind-naming.sh` is the permanent gate; archive keeps old ids
- [Websocket subscription source](docs/dev_notes/add-websocket-subscription-source.md) — delta frames on the already-open socket; ack-gated commit, retry is convergence; every reconnect must cancel the previous poll timer
- [Per-Entity timers](docs/dev_notes/entity-timers.md) — timers are FOLDED STATE (Change→WAL→replay); a quiet Entity never fires event-time timers; compaction must re-state the pending set after reading the seq
- [Live Entity read](docs/dev_notes/entity-state-live-query.md) — `flz state get`; a read is NOT a compaction (no drain, seq stamped in metadata); the capped evicted-key trace answers evicted-vs-never-existed
- [Stream transforms + edge keying](docs/dev_notes/stream-dsl-keyby-and-transforms.md) — transforms fuse into ONE closure per edge at the routing seam; `entity_key` deleted, keying is `.key_by(f)` on the wiring; transform-after-key refused
- [Multi-input procedures](docs/dev_notes/add-multi-input-procedures.md) — merge maps at the ROUTING seam so replay determinism is free; workflow merge over an entry channel refused; the old Java resume-compensation gap is closed by the shared core driver
- [Procedure-requested spawn](docs/dev_notes/add-procedure-requested-spawn.md) — `ctx.spawn_workflow`; namespace comes from the ControlChannel, never the wire; the seed rides `opts[:seed_input]` inside the bind
- [Operative-to-operative messaging](docs/dev_notes/operative-to-operative-messaging.md) — the route existed, only a VERDICT was missing; ForwardWorker must never deliver from its own handle_call (four-way deadlock)
- [Runner entry point + dynamic type set](docs/dev_notes/sdk-runner-entrypoint-redesign.md) — runner-as-handle, slot claimed at `start()`; e2e harness kills runners by closing stdin; committed dist jars embed the api classes
- [Java SDK parity](docs/dev_notes/java-sdk-parity.md) — the unblocker was `AddOperativeResponse.metadata`; Kryo changes inside postcard frames; the Java-side internals were since absorbed into `flz_core` (poll-core rehost)
- [Checkpoint ack-granularity](docs/dev_notes/effectively-once-checkpoint-granularity.md) — producer-side only, interval replay IS crash-resume replay; a park CLOSES the interval; a restore discards the open interval
- [Stream late-input correction](docs/dev_notes/stream-late-input-correction.md) — THERE IS NO SPLICE: rewound baseline + event-time-sorted slice on the existing replay; NEVER reset the event-time high-water on a replay
- [Workflow loop rewind](docs/dev_notes/workflow-loop-rewind.md) — `rewound` marker, not truncation; undo is derived from the journal; a rewind does NOT replay inputs (undo-and-continue)
- [Workflow entity retry](docs/dev_notes/workflow-entity-retry.md) — strictly pre-commit retry ladder reusing the stream backoff; a journaled proc is never retried
- [Workflow durable loop + suspension](docs/dev_notes/workflow-durable-loop-and-suspension.md) — loop modes Once/PerInput/Chained; durable inbox offset = WAL seq + 1; suspension holds zero runner resources, rehydrate = bind
- [Workflow staged commit + outbox](docs/dev_notes/workflow-staged-commit-and-outbox.md) — `ctx.emit` never hits the wire inside a structured proc; journal state decides what is owed; undo record journaled BEFORE the done marker
- [Workflow proc-journal](docs/dev_notes/workflow-proc-journal.md) — per-instance `proc.journal`, journal-then-ack `ProcCommit`; inter-proc codecs required for durable structured classes
- [Dead-letter queue](docs/dev_notes/platform-dead-letter-queue.md) — retry-then-park; tier-on parks ride the effect record atomically; replay is a fresh identity, no epoch bump
- [Postgres sink plugin](docs/dev_notes/effectively-once-output-postgres-sink.md) — one transaction per batch, `(epoch, seq)` guard makes re-drives no-ops; test service `postgres-sink` on host port 5433
- [Deterministic effects + output identity](docs/dev_notes/deterministic-effects-and-output-identity.md) — `ctx.now()`/`ctx.stable_id()` contract; clocks evaluated once per invocation; keyed channels stamp `flz.entity`/`flz.seq`/`flz.key`
- [Snapshot compaction is now real](docs/dev_notes/entity-compaction-is-a-stub.md) — both SDKs answered `CompactEntity` with a stub; `snapshot_seq` MUST be `change_seq - 1` after draining; postcard/Kryo, never the JSON serializer
- [Per-Entity state persistence](docs/dev_notes/operative-state-persistence.md) — WAL layout, wire payload-ids, dynamic entity mode + destructive eviction, credit-grant flow control, `flz state` CLI, config knobs
- [Last-runner migration keeps its state](docs/dev_notes/fix-last-runner-migration-state-loss.md) — runner ownership is NODE-LOCAL; the binding commits only after the ping; an empty handover must not suppress the WAL restore
- [Auto-restore on boot](docs/dev_notes/auto-restore-on-boot.md) — recovery is an ORPHAN RECONCILE, no marker; `dump/1` includes stopped rows — use `live_registry_dump/1`; the reconcile grace IS the boot gate
- [Convergence self-heal loop](docs/dev_notes/consume-recovery-mode-and-self-heal.md) — now `Convergence.Loop` (Intent→Probe→Diff→Planner→Heal primitives); /healthz contract; output-stall → forced rebind; runner takeover rides the same loop
- [Data-plane liveness instrumentation](docs/dev_notes/data-plane-liveness-instrumentation.md) — NIF driver self-report + BEAM counters + stall classifier (deepest-wedged-layer-wins); verdicts now feed convergence remediation
- [Graceful shutdown](docs/dev_notes/controller-graceful-shutdown.md) — quiesce → drain → finalise; orchestrator grace period MUST be `timeout + 15s`, bump in lockstep
- [Observability foundation](docs/dev_notes/platform-observability-foundation.md) — Signal envelope + bus + sinks + bridges; emit runs in the caller and never blocks under `:drop`
- [OTP backend sources](docs/dev_notes/platform-observability-otp-backend.md) — beam_vm/host/processhub/transport pollers; `flz observe` CLI
- [Observability export](docs/dev_notes/platform-observability-export.md) — hand-rolled Prometheus listener + OTLP/HTTP via `:mint`; OTLP overflow policy is `:block`
- [Durable audit log](docs/dev_notes/platform-audit-log.md) — JSONL + INDEX, fsync per record by default; Writer closes before the FsyncBatcher flush
- [Platform authn/authz foundation](docs/dev_notes/platform-authn-authz.md) — roles/tokens/quotas/bootstrap; policy point `AuthZ.authorize/3`; bootstrap claim flips `:enabled` durably

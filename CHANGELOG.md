# Changelog

## Unreleased

### Added

- **An Entity's current value is readable without a serving tier**
  (`entity-state-live-query`). `flz state get <operative/procedure[/entity_key]>`
  returns one live Entity's in-memory state as JSON together with the
  `change_seq` it was serialized at. Until now the only read was
  `flz state at`, which serves the controller's WAL at a point in the *past*, so
  anything needing the current value had to wait for it to reach a sink and read
  that copy — a second source of truth, always behind, free to diverge.

  The value is the freshest one there is: the SDK serializes under the Entity's
  state lock and does **not** drain pending changes, so the reply includes
  changes applied but not yet flushed, and is therefore at or ahead of what is
  durable. The seq is how a caller tells versions apart and sees how far ahead of
  durability it is. A read triggers no compaction, snapshot or WAL truncation.

  An Entity that is not held in memory is **refused naming why** — idle-evicted,
  instance suspended, no Entity by that key, or Operative unbound — and points at
  `flz state at` as the durable path. There is deliberately no durable fallback:
  dynamic-mode eviction is destructive (the recreate cold-starts), the controller
  cannot decode the SDK's WAL bytes, and a fallback would mean binding a runner
  and restoring an Entity as a side effect of a read. `entity_evicted` and
  `entity_not_found` are distinguished by a bounded per-SDK trace of what the
  reaper evicted, since eviction otherwise leaves nothing behind.

  The reply is bounded by the existing `:wal_high_watermark_bytes`; over it the
  read is refused with the byte count and the selector that would narrow it. No
  new configuration key, no new control command, no new payload-id: `EntityState`
  gained two optional selector fields, and an unset selector encodes
  byte-identically, so the migration/handover caller is untouched. Dynamic-mode
  Entities are covered on both runtimes, closing a gap in the serialization
  requirement. Authorized as `:"state.get"` (viewer). Docs:
  `docs/entity_state_live_query.md`; examples:
  `sdk/rust/examples/live_state_read.rs`, Java `E11_LiveStateRead`.

- **An operative can be fed from an HTTP resource**
  (`add-http-poll-source-provider`). The built-in `operative_http_poll` source
  provider (`Flezha.Plugin.Provider.HttpPoll`) polls a URL on an interval and
  injects the response body as an ordinary journaled entry frame — but only when
  the body's SHA-256 changes, so a steady endpoint costs one request per interval
  and zero frames. The first successful poll after the plugin starts always
  injects, which converges an operative that has just been started, recovered, or
  handed over onto current content within one interval; consumers of a polled
  channel must therefore treat its frames idempotently. Until now runtime
  configuration could reach a running operative only by restarting it with new
  `init_values` or by an operator hand-pushing `flz operative send`.

  Delivery uses the operative's confirming ingest and the hash advances only once
  the operative has taken the payload, so a frame dropped by a parked operative
  or an unbound stream is re-offered on the next poll instead of being lost until
  the endpoint next changes. Timeouts, connection errors, statuses outside 2xx
  (redirects included — they are not followed, since `:httpc` would replay the
  configured headers to an unvalidated host and downgrade TLS), an oversized
  body, a non-UTF-8 body, and a refused ingest all inject nothing, retain the
  last delivered hash and ETag, and count toward the standard provider health
  contract; the next success clears the run and delivers the change the outage
  hid. `ETag`/`If-None-Match` is passed through for bandwidth, with the local hash
  still the injection gate. The size cap aborts the transfer as the body arrives,
  so it bounds controller memory and not merely what the operative receives.
  HTTPS is verified against the system CA store. Attach refuses a non-http
  scheme, a hostless URL, credentials embedded in the URL, a sub-second interval,
  a timeout that could outlast its own interval, a non-positive tunable, a body
  cap above the ceiling, and any unknown option — the last on `init_provider/1`,
  the path a `--plugins` spec actually takes.

  **Controller-only**: no SDK, wire, or journal change. See
  `docs/http_poll_plugin.md`.

- **A procedure can consume more than one channel**
  (`add-multi-input-procedures`). `c.merge::<I>()` (Rust) / `c.merge(I.class)`
  (Java) subscribes two or more source channels to one procedure whose `Input`
  is an author-declared sum type, each subscription supplying a pure mapping
  from its payload into that type. Entry channels and inter-procedure channels
  are both valid sources on an unbounded operative, freely mixed, so a detector
  can take its data feed and
  a live config channel without the operative being restarted with new
  environment. Per-source FIFO is preserved and nothing is ordered across
  sources; `entities_for` sees the mapped value; the mappings run identically on
  live delivery and WAL replay, so a warm restart restores the same state.
  Fewer than two sources, the same channel subscribed twice, or a source that
  would re-wire an already-declared edge all fail operative definition with a
  named topology error. A **bounded** operative merges its procedures' output
  streams — its entry channel seeds the root from the spawn args and names no
  channel, so an entry channel as a bounded merge source is refused by name.
  Single-input wiring is unchanged.

  **No wire change and no controller change**: the merge is a subscription-time
  construct in the SDK, frames are still journaled per source channel, and the
  controller has no model of a channel's consumers. See
  `docs/multi_input_procedures.md`.

- **A procedure can ask for a bounded operative**
  (`add-procedure-requested-spawn`). `ctx.spawn_bounded` (Rust) /
  `ctx.spawnBounded` (Java) spawns-or-delivers a bounded instance on one
  `(namespace, instance_id)` from inside any procedure: a free id starts the
  factory with the payload as its first input, a taken id receives the payload
  as one more input (`if_running: deliver`) or refuses (`error`). Concurrent
  requests race-resolve at the operative registry to exactly one instance.
  Failures — quota, unknown factory, unreachable controller — resolve the
  caller's async handle and never fault the requesting procedure; an
  uninspected handle logs the failure once. Spawning is a live-processing side
  effect: a replayed invocation issues nothing. Until now a bounded operative
  could only be started from outside the cluster, so every pipeline that needed
  one grew its own external dispatcher. See
  `docs/procedure_requested_spawn.md`.

  **Wire**: two additive `CommandRequest` variants, `SpawnBoundedRequest`
  (tag 34, SDK → controller) and `SpawnBoundedResponse` (tag 35,
  controller → SDK). Additive-tag compatibility as with every prior wire
  change: an older controller ignores the unknown variant (the requester times
  out), so the controller image and both SDKs are released together.

- `flz schedule add --plugins <json>` — a schedule entry can now carry the
  same plugin-spec array `flz operative start --plugins` accepts
  (`schedule-entries-carry-plugins`). The spec is validated against the
  enabled providers at `add` time, stored verbatim, and decoded at every
  firing; each firing stamps its namespace and instance id on every plugin
  and prefixes the plugin id with the instance id so overlapping slots never
  collide. Without this a cron-fired bounded job had no output channel at all
  — a bounded instance finishes in milliseconds, so plugins must be attached
  at spawn. A spec that fails to decode at fire time starts nothing and is
  reported on `[:flezha, :schedule, :spawn_failed]` rather than firing a run
  whose output is dropped. The schedule row gained a field, so the
  delete-`schedules.dets`-and-re-add note below applies to this change too.

### Changed (BREAKING)

- Scheduled bounded spawns are now driven by **one schedule process per
  schedule, hosted in a dedicated `:flz_schedules` ProcessHub**, replacing the
  per-controller polling evaluator (`single-owner-schedule-processes`). The
  hub's `Singularity` redundancy makes exactly one process exist cluster-wide
  per schedule name, so a slot fires once without leader election and without
  cross-node state sync: every controller asserts the definitions its own
  node-local store holds and duplicate assertions no-op. This closes the
  documented multi-controller double-fire hole. `Flezha.Schedule.Evaluator`
  is removed; the schedule entry's `last_slot` watermark is gone (a process
  holding an armed timer *is* the watermark) and with it
  `FLEZHA_SCHEDULE_TICK_MS`. **A `schedules.dets` written by an earlier build
  must be deleted (`rm app_data/schedules/schedules.dets`) and its schedules
  re-added** — the row shape changed and no migration is provided.
  `flz schedule list` gains a `NODE` column. On start a schedule process
  fires the single most recent slot inside `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS`
  to cover restart and migration gaps; older slots are still never
  back-filled.

- New guarded SDK entry point in both SDKs (`runner-lifecycle-singleton-api`,
  superseded before release by `sdk-runner-entrypoint-redesign` — the shipping
  surface is the one described here). `Flezha.newInstance(namespace)` (Java) /
  `flezha_runner::new(namespace)` (Rust) and the
  `Flezha.registerUnbounded`/`registerBounded` /
  `register_unbounded::<R>()`/`register_bounded::<T>()` registration surface
  are removed with no back-compat shim. The replacement is a runner handle —
  no builder, no freeze step:
  `Flezha.createRunner(ns).defineUnbounded(A.class).run()` (Java) /
  `Flezha::create_runner(ns)?.define_unbounded::<A>()?.run()?` (Rust).
  `RunnerBuilder` and both Java registry facades
  (`UnboundedOperativeRegistry`/`BoundedOperativeRegistry`) are gone.
  The declared set is per-runner and starts empty each generation;
  `define_*`/`remove_*` (`removeUnbounded`/`removeBounded` /
  `remove_unbounded::<R>()`/`remove_bounded::<T>()`) are legal before and
  after `start()`, a post-start change shipping the full factory set as a
  fire-and-forget `UpdateFactories`. A removal refuses while live instances
  of the factory exist (`OperativeTypeInUseException` /
  `FlezhaError::OperativeTypeInUse`). Starting with zero declared classes is
  legal.
  The singleton slot is claimed at `start()`, not at create: any number of
  unstarted runners may exist, and a second `start()`/`run()` fails loudly
  (`IllegalStateException` / `FlezhaError::AlreadyRunning`) naming the running
  namespace instead of silently corrupting the process-global
  identity/sink/credit state; after `close()`/`Drop` a new runner may start.
  `create_runner` is what returns the `FlezhaRunner` handle — `start()`
  returns `Result<&Self, FlezhaError>` (bind it: a chained
  `…create_runner(ns)?.start()?` drops the temporary at statement end),
  `run()` = `start()` + `awaitTermination()` (standalone binaries — the
  `Thread.currentThread().join()` / stdin-loop keep-alive hacks are gone),
  and `Flezha.currentRunner()` / `Flezha::current_runner()` exposes the
  active runner. Rust declaration, connect, and lifecycle failures are now
  ONE typed `Result` error, `FlezhaError` (wrapping `TopologyBuildError` /
  `BuildBoundedError`), instead of panics and the removed
  `RunnerStartError`/`RunnerError`; the per-issue topology enum is
  `TopologyIssue`. Java's annotation processor generates
  `GeneratedUnboundedOperatives.defineAll(FlezhaRunner)`.
  Naming rules for future SDK surface: `docs/sdk_naming_conventions.md`.

### Fixed

- A controller that lost **every runner** for a namespace could neither keep its
  operatives nor move them, and the healing machinery hid the damage
  (`fix-last-runner-migration-state-loss`). Three defects compounded:
  `hub_migrate` had been reduced to a vacancy check that no-ops exactly when the
  hub still holds operatives — i.e. whenever there is something to migrate;
  `:runner_migrate` committed the binding *before* the runner answered, so a
  failed heal left the operative naming a **live but unreachable** runner, which
  made `Convergence.Diff` stop classifying it misbound and emptied the delta
  permanently; and because a handover serializes its state from the **runner**,
  a migration caused by that runner's death shipped an empty payload while still
  marking `arrived_via_hotswap?`, which suppressed the WAL restore *and* the
  explicit repair path. The binding is now committed only on success, the flag
  only when the handover carried state, and `hub_migrate` drains the hub
  (`HubManager.drain_hub/2`, the seam graceful shutdown already used) so
  operatives leave through a live HotSwap handover. New
  `Flezha.State.WalTransfer` moves what the handover cannot: a
  destination-pulled, chunked `:erpc` copy of the Entity's durable files,
  written to a `.transferring` sibling that is fsynced and atomically renamed,
  with names resolved through the Entity's own file set so a peer cannot read
  arbitrary paths. `state_root` stays node-local and is never assumed shared.
  One-shot at migration time, not replication — a controller that dies together
  with its disk still takes its state with it.

- A durable bounded operative whose procedures hold **no per-Entity state**
  went silent after its first turn. The proc-journal is instance-scoped but rode
  `RestoreInitState.proc_journal`, a per-Entity frame — and such a class has no
  Entity WAL, so the controller shipped no restore at all. Every wake resumed
  at iteration 0, and each turn's commit collided with iteration 0's
  `outbox_flushed` marker (the flush marker is keyed `{type, proc_idx, iter}`),
  so its staged output was journaled and never delivered. Only the first turn
  of such an operative was ever emitted; it bit any instance that parked
  between turns, on both SDKs.
  `ProcJournalState` moves from `RestoreInitState` (field 6, removed) to
  `AddOperative` (field 6, added) — instance-scoped state on the bind, beside
  `resume_entity_count`, delivered once per bind instead of once per persisted
  Entity and before the driver spawns. No compatibility shim: an SDK and a
  controller must be upgraded together.

### Changed

- An operative may now declare **more than one entry channel** (both SDKs). The
  `sdk-graph-authoring` graph-shape rule is relaxed from "exactly one entry" to
  "one or more entries, every node reachable from some entry"; the
  `MultipleEntryChannels` topology error is removed. The runtime already routed
  each inbound frame to its entry by `metadata["channel"]` (falling back to the
  first-declared entry), so this only lifts the build-time refusal — enabling a
  single operative to ingest several external sources (e.g. a market pipeline
  reading a trade feed and an order-book feed on distinct channels). Bounded
  operatives keep their single-entry linear-chain rule.

- Plugin subscriptions added via `flz plugin enable` now survive any
  Overwatch (re)start automatically. The ProcessHub child-spec is the
  source of truth for an operative's enabled plugins; the runtime
  `Overwatch.state.extensions` table is rebuilt from it on every
  `init/1`. Previously, plugins enabled after `flz operative start`
  were dropped on runner-pod restart, agent restart with intact
  ProcessHub state, or any in-place restart that ran through
  `HubManager.alter_child_data/2` (e.g. `kubectl rollout restart
  daemonset/<sdk-daemonset>`), forcing runners to re-run the enable
  commands manually before the data plane recovered. See
  `openspec/specs/plugin-subscription-persistence/spec.md`.

  **Upgrade note for clusters with already-running operatives.**
  Operatives whose plugins were enabled dynamically before this
  upgrade have an empty `opts[:plugins]` in the cspec. They keep
  working as long as no Overwatch (re)start happens before the next
  `flz plugin enable` / `flz plugin disable` proc, which canonicalises
  the cspec. Runners who want to canonicalise immediately can run
  `flz plugin disable <id> && flz plugin enable <id>` for each
  plugin. No automatic migration job is shipped.

### Added

- Controller-native cron scheduling of bounded operatives
  (`add-scheduled-operative-spawns`). `flz schedule add <name> --cron
  "<expr>" --tz <iana> --factory <factory_id> [--namespace ns]
  [--init_values json]` registers a durable schedule (node-local DETS at
  `app_data/schedules/schedules.dets`; names are capped at 11 bytes so the
  composed instance id fits the 32-byte operative id limit); `list` /
  `remove` / `run <name>` complete the family. A per-controller evaluator
  (`Flezha.Schedule.Evaluator`) fires each matching slot through the
  standard bounded start path with deterministic instance id
  `<name>@<slot-iso>` — a duplicate fire attempt no-ops on the id, which is
  the cluster-wide exactly-once guard. Slots are evaluated in the entry's
  IANA time zone (new `tz` dependency): a spring-forward gap slot skips, a
  fall-back ambiguous slot fires once at the first occurrence. Slots older
  than `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS` (default 120 s) at evaluation —
  e.g. missed while the controller was down — are skipped and logged with
  their timestamps, never back-filled; a still-running previous slot never
  blocks the next one (ids differ; bound runtime via `max_duration`). Only
  bounded operatives are schedulable. Telemetry: `[:flezha, :schedule,
  :fired | :skipped | :spawn_failed]`. Operator guide in
  [`docs/operations/schedules.md`](docs/operations/schedules.md).

- Hardened first-party Kafka producer plugin
  (`Flezha.Plugin.Provider.Kafka.Producer`). The previous thin
  `KafkaEx.produce`-per-call wrapper is replaced with a per-`(plugin_id,
  topic, partition)` `ProducerWorker` GenServer that runs linger-based
  batching, bounded outbound queueing with configurable overflow strategy
  (`:drop_oldest` / `:drop_newest` / `:block`), automatic retries with
  exponential backoff for every transient broker error class, active
  metadata refresh on `:not_leader_for_partition` / `:no_broker` /
  `:unknown_topic_or_partition`, idempotent producer semantics via Produce
  v5+ (`InitProducerId` + per-partition sequence numbers), and auto-create
  on first publish for unknown topics. Underlying `KafkaEx.New.Client`
  workers are deduplicated across plugin instances by connection identity
  via the new `Flezha.Plugin.Provider.Kafka.WorkerRegistry`. New plugin
  parameters: `:retries`, `:retry_backoff_ms`, `:retry_max_backoff_ms`,
  `:delivery_timeout_ms`, `:linger_ms`, `:max_batch_bytes`, `:queue_max`,
  `:queue_overflow`, `:queue_block_timeout_ms`, `:idempotent`,
  `:auto_create_topic`, `:auto_create_partitions`,
  `:auto_create_replication_factor`, `:topic_configs`,
  `:metadata_refresh_timeout_ms`. Six new telemetry events under the
  `[:flezha, :plugin, :kafka, _, _]` prefix surface every state
  transition. Synchronous counters available via
  `Flezha.Plugin.Provider.Kafka.Producer.stats/1`. Full runner-facing
  contract in [`docs/kafka-producer.md`](docs/kafka-producer.md).

### Changed (BREAKING)

- `:required_acks` default for the Kafka producer plugin is now `-1`
  (acks=all). Was `1`. Combined with the new default `:idempotent =
  true`, this is the durability-correct default — retries cannot create
  duplicates on the broker, and acks=all stays equivalent to acks=1 in
  steady state on clusters with `min.insync.replicas = 1`. Callers that
  depended on the old fast-but-lossy single-broker-ack latency profile
  should explicitly set `required_acks: 1, idempotent: false` per the
  migration table in [`docs/kafka-producer.md`](docs/kafka-producer.md).
- `Flezha.Plugin.Provider.Kafka.Producer.init_provider/1` now returns
  `{:ok, opts, producer_pid}` where `producer_pid` is the
  `ProducerWorker` GenServer (was the kafka_ex worker pid). The
  underlying kafka_ex worker is shared across plugin instances and is no
  longer directly addressable from plugin opts. Plugin host code that
  relied on holding a direct kafka_ex worker pid should use
  `Producer.stats/1` or attach a `:telemetry` handler instead.

- SDKs ship a final state snapshot to the agent on graceful shutdown so
  rolling deploys preserve the latest state. Rust and Java SDKs now emit
  one `RunnerShutdownNotification{operative_id, serialized_state}`
  per running operative on the control channel when the SDK is dropped,
  receives SIGTERM (Kubernetes pre-stop), or is closed explicitly via
  `Flezha.close()` (Java) / handle drop (Rust); the agent's
  `Flezha.Runner.ControlChannel` overwrites
  `Flezha.Operative.Overwatch.state.snapshot` with the supplied binary
  and emits an `RunnerShutdownAck`. Per-operative ack wait is bounded
  at 2 s, parallelised across operatives. SIGKILL bypasses as expected;
  agent falls back to the last periodic snapshot. Disabled-snapshot
  deployments (`overwatch_state_snapshot_interval_ms == 0`, e.g.
  `flezha_market`) get a one-shot shutdown handoff that survives until
  the next `AddOperative` reissue. New protobuf messages
  `RunnerShutdownNotification` (tag 12) and `RunnerShutdownAck`
  (tag 13) are additive — older agents/SDKs interoperate by falling
  back to the periodic-snapshot freshness shipped by
  `heal-connections-on-restart`.
- Runner ↔ agent connections now heal automatically across either-side
  restarts; operatives resume traffic within 5 s of reconnect without
  runner intervention. Stream rebind is enforced as a hard contract on
  `Flezha.Runner.StreamChannel`, the UDS/QUIC acceptors atomically
  bind incoming streams only to nil-ref channels, and
  `Flezha.Runner.FleetManager.runner_connect/3` re-issues
  `AddOperative` for operatives ProcessHub still has tagged to a dead
  runner_id matching the connecting runner's profile. Rust and
  Java SDK `AddOperative` handlers are now idempotent on duplicate
  `operative_id` (log + skip).
- `mix test.connection_healing` opt-in alias that runs
  `test/scripts/connection_healing.sh` against the dev compose stack to
  exercise both restart paths end-to-end. Requires Docker on the host;
  not part of the default `mix test` budget.
- `test/flezha/deployment/k8s_manifest_test.exs` k8s manifest sanity test
  that asserts the daemonset / configmap fields the connection-healing
  flow relies on (`flezha-sockets` volume, `FLEZHA_SOCKET_FILES_PATH`,
  `FLEZHA_RUNNER_KEEPALIVE`) stay well-formed.

## 0.2.0 — Cluster snapshot & restore (replaces DETS recovery)

Restart-survival is now runner-driven via `flz cluster snapshot/restore`.
The previous DETS-based automatic recovery subsystem has been removed
along with the SYSMODE coordinator GenServer that drove it.

### Added

- New top-level CLI commands `flz cluster snapshot <file>` and
  `flz cluster restore <file>` for explicit, runner-driven cluster-wide
  state capture and restore. Both commands accept `--namespace NS` for
  per-namespace procedures; default behaviour without the flag is
  cluster-wide. (Both verbs were later removed; `flz boot
  import-legacy-snapshot` reads the format they produced.)
- `Flezha.Cluster.Snapshot` and `Flezha.Cluster.Snapshot.Coordinator`
  Elixir modules implementing the gather/apply RPC fan-out and the
  atomic ETF file write.
- `Flezha.Cluster.Snapshot.PluginRecoverability` predicate that decides
  which plugins survive a snapshot/restore cycle.
- `Flezha.Operative.Builder` now recognises `:initial_state_binary` in
  opts; `Flezha.Operative.Overwatch` sends a new `EntityApplyState`
  control command to the compute engine before the first input is
  forwarded so a re-spawned operative begins with primed state.
- New protobuf message `EntityApplyState` and corresponding handler
  in the Rust and Java SDKs. Default impl returns the canonical
  `apply_state_not_supported` error so older SDK builds are detectable;
  the agent logs once per operative on apply failure.
- `:flezha, :snapshot_state_cap_bytes` configuration key (default 8 MB),
  with `FLEZHA_SNAPSHOT_STATE_CAP_BYTES` env-var override.

### Removed (BREAKING)

- The entire DETS-based recovery subsystem:
  `Flezha.Recovery.Store`, `Flezha.Recovery.PluginStore`,
  `Flezha.Recovery.Coordinator`, `Flezha.Recovery.SysMode`. The agent no
  longer writes child specs to a DETS file on operative start, no longer
  reads DETS at boot, no longer maintains a SYSMODE state machine, and
  no longer exchanges SYSMODE with peers. Recovery from an unattended
  crash now requires the runner to run `flz cluster restore` against
  a previously-captured snapshot file.
- `Flezha.Operative.InitState.Providers.Sibling` along with its
  registration in `:enabled_plugins`, its capability spec
  (`operative-init-state-sibling`), and the `lib/flezha/operative/init_state/`
  tree. Restart-survival init-state seeding is now handled by
  `flz cluster restore` priming `:initial_state_binary` directly. The
  generic `operative-init-state` provider chain remains for non-snapshot
  sources (HTTP, S3, computed defaults, custom modules implementing
  `on_operative_init/2`).
- Application env keys: `:flezha, :recovery_window_ms`,
  `:flezha, :recovery_data_dir`, `:flezha, :recovery_enabled`,
  `:flezha, :recovery_autostart`, `:flezha, :prefer_explicit_restore?`.
  Env-var overrides removed: `FLEZHA_RECOVERY_*`, `FLEZHA_DATA_DIR`,
  `FLEZHA_PREFER_EXPLICIT_RESTORE`.
- ProcessHub `post_children_start_hook` and `child_unregistered_hook`
  callbacks into the recovery coordinator.
- `failure-recovery` capability and its spec.
- `scripts/recovery_e2e.sh` shell-driven recovery smoke test.
- `docker/prod/docker-compose.local-test.yml` overlay.

### Plugin installer

- `Flezha.Plugin.Installer.Store` no longer co-locates its DETS file
  under the (now-removed) `recovery_data_dir`. New config knob:
  `:flezha, :plugin_installer_data_dir` (defaults to the installer's
  `:plugin_root`, which itself defaults to `app_data/plugins`).

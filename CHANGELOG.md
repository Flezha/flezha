# Changelog

## Unreleased

### Changed

- Plugin subscriptions added via `flz plugin enable` now survive any
  Overwatch (re)start automatically. The ProcessHub child-spec is the
  source of truth for an operative's enabled plugins; the runtime
  `Overwatch.state.extensions` table is rebuilt from it on every
  `init/1`. Previously, plugins enabled after `flz operative start`
  were dropped on executor-pod restart, agent restart with intact
  ProcessHub state, or any in-place restart that ran through
  `HubManager.alter_child_data/2` (e.g. `kubectl rollout restart
  daemonset/<sdk-daemonset>`), forcing executors to re-run the enable
  commands manually before the data plane recovered. See
  `openspec/specs/plugin-subscription-persistence/spec.md`.

  **Upgrade note for clusters with already-running operatives.**
  Operatives whose plugins were enabled dynamically before this
  upgrade have an empty `opts[:plugins]` in the cspec. They keep
  working as long as no Overwatch (re)start happens before the next
  `flz plugin enable` / `flz plugin disable` op, which canonicalises
  the cspec. Executors who want to canonicalise immediately can run
  `flz plugin disable <id> && flz plugin enable <id>` for each
  plugin. No automatic migration job is shipped.

### Added

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
  `Flezha.Plugin.Provider.Kafka.Producer.stats/1`. Full executor-facing
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
  one `ExecutorShutdownNotification{operative_id, serialized_state}`
  per running operative on the control channel when the SDK is dropped,
  receives SIGTERM (Kubernetes pre-stop), or is closed explicitly via
  `Flezha.close()` (Java) / handle drop (Rust); the agent's
  `Flezha.Executor.ControlChannel` overwrites
  `Flezha.Operative.Overwatch.state.snapshot` with the supplied binary
  and emits an `ExecutorShutdownAck`. Per-operative ack wait is bounded
  at 2 s, parallelised across operatives. SIGKILL bypasses as expected;
  agent falls back to the last periodic snapshot. Disabled-snapshot
  deployments (`overwatch_state_snapshot_interval_ms == 0`, e.g.
  `flezha_market`) get a one-shot shutdown handoff that survives until
  the next `AddOperative` reissue. New protobuf messages
  `ExecutorShutdownNotification` (tag 12) and `ExecutorShutdownAck`
  (tag 13) are additive — older agents/SDKs interoperate by falling
  back to the periodic-snapshot freshness shipped by
  `heal-connections-on-restart`.
- Executor ↔ agent connections now heal automatically across either-side
  restarts; operatives resume traffic within 5 s of reconnect without
  executor intervention. Stream rebind is enforced as a hard contract on
  `Flezha.Executor.StreamChannel`, the UDS/QUIC acceptors atomically
  bind incoming streams only to nil-ref channels, and
  `Flezha.Executor.FleetManager.executor_connect/3` re-issues
  `AddOperative` for operatives ProcessHub still has tagged to a dead
  executor_id matching the connecting executor's profile. Rust and
  Java SDK `AddOperative` handlers are now idempotent on duplicate
  `operative_id` (log + skip).
- `mix test.connection_healing` opt-in alias that runs
  `test/scripts/connection_healing.sh` against the dev compose stack to
  exercise both restart paths end-to-end. Requires Docker on the host;
  not part of the default `mix test` budget.
- `test/flezha/deployment/k8s_manifest_test.exs` k8s manifest sanity test
  that asserts the daemonset / configmap fields the connection-healing
  flow relies on (`flezha-sockets` volume, `FLEZHA_SOCKET_FILES_PATH`,
  `FLEZHA_EXECUTOR_KEEPALIVE`) stay well-formed.

## 0.2.0 — Cluster snapshot & restore (replaces DETS recovery)

Restart-survival is now executor-driven via `flz cluster snapshot/restore`.
The previous DETS-based automatic recovery subsystem has been removed
along with the SYSMODE coordinator GenServer that drove it.

### Added

- New top-level CLI commands `flz cluster snapshot <file>` and
  `flz cluster restore <file>` for explicit, executor-driven cluster-wide
  state capture and restore. Both commands accept `--namespace NS` for
  per-namespace operations; default behaviour without the flag is
  cluster-wide. See [`docs/cluster-snapshot.md`](docs/cluster-snapshot.md).
- `Flezha.Cluster.Snapshot` and `Flezha.Cluster.Snapshot.Coordinator`
  Elixir modules implementing the gather/apply RPC fan-out and the
  atomic ETF file write.
- `Flezha.Cluster.Snapshot.PluginRecoverability` predicate that decides
  which plugins survive a snapshot/restore cycle.
- `Flezha.Operative.Builder` now recognises `:initial_state_binary` in
  opts; `Flezha.Operative.Overwatch` sends a new `SegmentApplyState`
  control command to the compute engine before the first input is
  forwarded so a re-spawned operative begins with primed state.
- New protobuf message `SegmentApplyState` and corresponding handler
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
  crash now requires the executor to run `flz cluster restore` against
  a previously-captured snapshot file.
- `Flezha.Operative.InitState.Providers.Sibling` along with its
  registration in `:enabled_plugins`, its capability spec
  (`relay-init-state-sibling`), and the `lib/flezha/operative/init_state/`
  tree. Restart-survival init-state seeding is now handled by
  `flz cluster restore` priming `:initial_state_binary` directly. The
  generic `relay-init-state` provider chain remains for non-snapshot
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

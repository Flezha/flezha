# cluster-snapshot-restore Specification

## Purpose

The cluster-snapshot-restore capability provides executor-driven, explicit snapshot and restore of an entire Flezha cluster's relay and plugin state. It defines two CLI commands (`flz cluster snapshot <file>` and `flz cluster restore <file>`), a self-contained ETF snapshot file format, and a per-relay state-priming SDK contract (`Apply`). It is the only restart-survival mechanism in the agent — there is no automatic at-boot replay.

## Requirements

### Requirement: `flz cluster snapshot <file>` SHALL produce a single ETF artefact describing the entire reachable cluster

The CLI command `flz cluster snapshot <file> [--peer-timeout-ms N] [--strict]` SHALL be added at the top level of the `flz` Rust CLI under `cli/src/cluster.rs`, dispatched from `cli/src/main.rs`. The CLI invocation forwards via the existing `rpc` boundary used by `cli/src/relay.rs:64` to the Elixir entry point `Flezha.Cluster.Snapshot.Coordinator.snapshot/2`.

The coordinator SHALL:

1. Capture a snapshot of the ProcessHub hash-ring as `%{relay_id => expected_node}`.
2. Enumerate `Node.list([:visible, :this])` and call `:rpc.call(peer, Flezha.Cluster.Snapshot, :local_state, [], peer_timeout_ms)` against each node in parallel.
3. Merge per-node responses; reconcile mismatches between ring-snapshot and observed node by trusting the per-node response and logging WARN naming the relay and the divergence.
4. Build the payload `%{taken_at_ms, cluster: %{nodes: [...]}, relays: [...], plugins: [...], captured_states: %{relay_id => binary()}, strict_mode: bool, initiator_node: node()}`.
5. Encode as `:erlang.term_to_binary({:flezha_snapshot, 1, payload}, [:compressed])` (compression level 6 implied by `:compressed`).
6. Atomically write: `File.write!(<file>.tmp, binary)`, then `File.rename!(<file>.tmp, <file>)`. Same-filesystem rename is required.

#### Scenario: Single-node cluster snapshot
- **GIVEN** a single-node Flezha agent with two relays and three enabled plugins
- **WHEN** the executor runs `flz cluster snapshot /tmp/x.flz`
- **THEN** `/tmp/x.flz` exists, `:erlang.binary_to_term/1` decodes it to `{:flezha_snapshot, 1, payload}` with `payload.relays` containing two entries, `payload.plugins` containing three entries, the file's mtime is within 1s of the snapshot call, and the CLI exits 0 with summary `snapshot: 2 relays, 3 plugins, <K>KB written to /tmp/x.flz`

#### Scenario: Multi-node cluster snapshot
- **GIVEN** a three-node cluster with one relay on each node
- **WHEN** the executor runs `flz cluster snapshot /tmp/x.flz` on node A
- **THEN** node A issues two parallel RPCs (to B and C), all three contribute their local relay + state, the merged file's `relays` list has three entries each tagged with `last_observed_node`, and reconciliation logs no WARN because no migrations occurred during the gather

#### Scenario: Default best-effort skips an unreachable peer
- **GIVEN** a three-node cluster where node C is unreachable
- **WHEN** the executor runs `flz cluster snapshot /tmp/x.flz` on node A (no `--strict`)
- **THEN** the gather logs WARN `[cluster] peer C unreachable: :timeout`, the snapshot is written with relays from A and B only, the CLI exits 0, and the summary names the missing peer

#### Scenario: --strict aborts on an unreachable peer
- **GIVEN** the same three-node cluster with C unreachable
- **WHEN** the executor runs `flz cluster snapshot /tmp/x.flz --strict`
- **THEN** the coordinator returns `{:error, {:strict_mode, [:c]}}`, the CLI exits non-zero with `snapshot failed: peer C unreachable in strict mode`, and `/tmp/x.flz` is unchanged from before the call (no torn write — `.tmp` was deleted on failure)

#### Scenario: Atomic rename prevents torn reads
- **GIVEN** an active snapshot is being written to `/tmp/x.flz` (write to `.tmp`, in-flight rename)
- **WHEN** a concurrent reader opens `/tmp/x.flz`
- **THEN** the reader observes either the previous successful snapshot's bytes or the new bytes — never a partially-written file

#### Scenario: Per-relay state size cap
- **GIVEN** a relay whose `Overwatch.create_snapshot/2` returns 12 MB binary, exceeding the configurable cap (default 8 MB)
- **WHEN** the executor runs `flz cluster snapshot /tmp/x.flz`
- **THEN** the relay entry is written with `captured_state_binary: nil`, a WARN line names the relay id and the size, and the snapshot file does not include that binary

### Requirement: `flz cluster restore <file>` SHALL apply a snapshot atomically across the cluster

The CLI command `flz cluster restore <file> [--warn-if-older-than DURATION] [--namespace NS]` SHALL read the snapshot file, validate its schema version, and apply the contained relays and plugins to their target nodes via the existing per-node lifecycle paths. State binaries SHALL be primed before the first input.

The coordinator SHALL:

1. `File.read!(file)`; decompress + `:erlang.binary_to_term/1`.
2. Validate the outer tuple is `{:flezha_snapshot, 1, payload}`. Any other version is a hard error: exit non-zero with `unsupported snapshot version: got <N>, supports [1]`.
3. Check `payload.taken_at_ms`; if older than `warn_if_older_than_ms`, log WARN naming the snapshot age and the threshold; otherwise log INFO with the age.
4. For each `relay` entry: determine `target_node` (last_observed_node if reachable, else ProcessHub-placed). RPC `Flezha.Cluster.Snapshot.apply_local_state/1` on the target with `{:relay, builder, captured_state_binary}`. The Overwatch path attaches `:initial_state_binary` to the builder's opts.
5. For each `plugin` entry: skip if not `recoverable?`; RPC `apply_local_state/1` on the target with `{:plugin, plugin_struct}`. Underlying call: `PluginManager.enable_plugin/2`.
6. Optionally filter by `--namespace`: when set, only entries with matching namespace are applied; the rest are logged at INFO and skipped.

#### Scenario: Happy-path restore primes captured state
- **GIVEN** `/tmp/x.flz` was written 5 minutes ago and contains one relay with a 14 KB captured state binary
- **WHEN** the executor runs `flz cluster restore /tmp/x.flz`
- **THEN** the relay is re-spawned on its last_observed_node, the compute engine receives the captured binary as initial state via the new `:initial_state_binary` option, the first input post-restore is processed against the primed state, and the CLI exits 0 with summary `restore: 1 relays, 0 plugins`

#### Scenario: Stale snapshot logs WARN but still restores
- **GIVEN** `/tmp/x.flz` was written 30 hours ago
- **WHEN** the executor runs `flz cluster restore /tmp/x.flz --warn-if-older-than 86400s`
- **THEN** a WARN line names the snapshot age (`30h ago`) and the threshold (`24h`), the restore proceeds, and the CLI exits 0 with the standard summary plus a final WARN line `staleness: snapshot was 30h old at restore`

#### Scenario: Schema-version mismatch is a hard error
- **GIVEN** `/tmp/x.flz` decodes to `{:flezha_snapshot, 2, _payload}` (a hypothetical future version)
- **WHEN** the executor runs `flz cluster restore /tmp/x.flz` against a v1-supporting agent
- **THEN** the CLI exits non-zero with `unsupported snapshot version: got 2, supports [1]` and no relays are touched

#### Scenario: Non-recoverable plugin is skipped with WARN
- **GIVEN** `Flezha.Cluster.Snapshot.PluginRecoverability.recoverable?/1` returns `false` for a plugin captured in the snapshot (e.g. a PID-bound MonitoredIOForward)
- **WHEN** the executor runs `flz cluster restore /tmp/x.flz`
- **THEN** the plugin is omitted, a WARN line names the plugin id and the provider, and the restore proceeds for the remaining plugins

#### Scenario: Captured state binary missing primes default state
- **GIVEN** a relay entry has `captured_state_binary: nil` (per the size-cap clause)
- **WHEN** restore re-spawns it
- **THEN** the relay is started without `:initial_state_binary`, a one-time INFO line names the relay id and notes "no captured state — starting empty", and processing begins from the default state

#### Scenario: Compute-engine SDK does not implement state apply
- **GIVEN** a relay's factory_id is served by an SDK build that does not implement the `Apply` command
- **WHEN** restore attempts to prime its state
- **THEN** the spawn succeeds with empty state, a one-time WARN per factory_id names the SDK and the factory_id with `apply-state command not supported by this SDK build — state primed empty for this factory`, and the executor can decide whether to upgrade

#### Scenario: --namespace filters restore
- **GIVEN** a snapshot file containing relays in namespaces `:default` and `:test`
- **WHEN** the executor runs `flz cluster restore /tmp/x.flz --namespace test`
- **THEN** only `:test`-namespace entries are applied; `:default` entries are listed at INFO `[restore] skipping <N> relays in namespace :default (filtered)` and not touched

#### Scenario: Atomic write protects against half-applied restore
- **GIVEN** restore is mid-flight on a five-relay snapshot, three already applied
- **WHEN** node A (the initiator) crashes
- **THEN** the three already-applied relays are running normally on their target nodes; the remaining two are not started; an executor-issued restore can resume safely — re-applying the three already-running ones returns `:already_registered`, the remaining two complete the operation

### Requirement: `Flezha.Relay.Overwatch` SHALL accept `:initial_state_binary` to prime new relay state

The relay spawn path in `Flezha.Relay.Overwatch` (and its underlying `start_link/1`-like constructor) SHALL accept an `:initial_state_binary` option (binary) in `Flezha.Relay.Builder.opts`. When present, the spawn pipeline SHALL deliver this binary to the compute engine via a new `:partition_apply_state` command, mirroring the existing `:partition_state` command used by `create_snapshot/2`.

The compute engine SDK contract gains exactly one new operation:

- **Apply**: SDK receives `{:apply_state, binary}` from the agent and SHALL feed it to the relay's GenServer state before the first input is processed. SDKs that do not implement Apply SHALL respond `{:error, :apply_state_not_supported}`.

When the SDK responds with `{:error, :apply_state_not_supported}`, the agent logs a one-time WARN per factory_id (so log noise is bounded), the relay proceeds with empty default state, and processing continues.

#### Scenario: Apply primes state before first input
- **GIVEN** a relay is spawned with `opts: [initial_state_binary: <<bin>>]`
- **WHEN** the compute engine receives `:apply_state` and then the first `:input`
- **THEN** the SDK applies the state first, the input is processed against the applied state, and the resulting output reflects history embedded in the binary

#### Scenario: Apply not supported by SDK falls back gracefully
- **GIVEN** the SDK build returns `{:error, :apply_state_not_supported}` to `:apply_state`
- **WHEN** restore primes a relay on this SDK
- **THEN** spawn proceeds, a one-time WARN names the factory and SDK, and the relay begins with default empty state

### Requirement: ETF file format SHALL use the `{:flezha_snapshot, version, payload}` envelope

The on-disk file SHALL be the binary produced by `:erlang.term_to_binary({:flezha_snapshot, 1, payload}, [:compressed])`. `version` is a `pos_integer()`. `payload` is a map with the following keys:

- `taken_at_ms :: integer()` — `System.system_time(:millisecond)` at gather start.
- `cluster :: %{nodes: [node()]}` — the live cluster as observed by the initiator.
- `relays :: [%{relay: %Flezha.Relay.Builder{}, last_observed_node: node()}]`.
- `plugins :: [%{plugin: %Flezha.Plugin{}, last_observed_node: node()}]`.
- `captured_states :: %{relay_id :: String.t() => binary()}` — keyed by relay id; values are the SDK-opaque snapshot binaries returned by `Overwatch.create_snapshot/2`.
- `strict_mode :: boolean()` — what `--strict` was at gather time (informational).
- `initiator_node :: node()` — the node that wrote the file.

Reading code SHALL pattern-match on the outer tuple. A file that fails to decode (corrupted, wrong format) SHALL be rejected with a hard error naming the file path and the underlying reason; restore SHALL not partially apply.

#### Scenario: Forward-incompatible file is rejected
- **GIVEN** an agent supports schema version `1`
- **WHEN** the file's outer tuple is `{:flezha_snapshot, 2, _}`
- **THEN** restore exits non-zero with the unsupported-version message and no state is touched

#### Scenario: Corrupted file is rejected
- **GIVEN** the snapshot file's bytes are truncated or otherwise invalid ETF
- **WHEN** restore reads the file
- **THEN** `:erlang.binary_to_term/1` raises, the coordinator catches with `{:error, {:decode_failed, reason}}`, the CLI exits non-zero naming the file and the reason, and SYSMODE is not notified

### Requirement: New CLI subcommands SHALL be documented and discoverable

`flz cluster --help` and `flz --help` SHALL list the new subcommands. The Rust CLI's existing help-text generation pattern (see `cli/src/relay.rs` and friends) SHALL be extended for `cluster.rs` with a top-level summary plus per-subcommand usage. A new doc file `docs/cluster-snapshot.md` SHALL describe:

- The two commands and all flags.
- The file format envelope at a high level (no need to publish the full term layout — executors don't need it).
- The restart-survival model: cluster-snapshot/restore is the only mechanism, planned-shutdown vs crash-recovery flows.
- Operational guidance: how to force a cold start (delete the file), how to inspect a snapshot's contents (`erl -noshell -eval '...binary_to_term...'` one-liner), how to handle staleness.

#### Scenario: Help text includes the new commands
- **WHEN** an executor runs `flz cluster --help`
- **THEN** the output lists `snapshot` and `restore` subcommands with their flag summaries, each pointing to `docs/cluster-snapshot.md` for details

### Requirement: Snapshot file paths use executor_id

Cluster-snapshot files SHALL include per-Executor state under directory paths keyed by `executor_id` (replacing the prior `compute_engine_id` path component). All snapshot artifacts produced by the snapshot/restore subsystem SHALL use the new path scheme; restoration SHALL read from the new scheme exclusively.

#### Scenario: Snapshot bundle contains executor_id-keyed directories
- **WHEN** `flz controller snapshot create` is invoked on a Controller with Executors `exec-1` and `exec-2`
- **THEN** the produced snapshot bundle SHALL contain top-level directories `exec-1/` and `exec-2/` for the Executor-scoped state, not `compute_engine_*` directories

#### Scenario: Restore reads executor_id-keyed paths
- **WHEN** `flz controller snapshot restore <bundle>` is invoked
- **THEN** the restore process SHALL look for state under `executor_id` directories; absence of legacy `compute_engine_id` directories MUST NOT cause restore to fail

### Requirement: Snapshot manifest references Controllers and Executors by canonical role names

The snapshot manifest (the JSON / metadata file inside a cluster-snapshot bundle) SHALL identify the orchestrator role as `controller` and SDK process roles as `executor`. Manifest fields previously named `agent_*` or `compute_engine_*` SHALL be renamed `controller_*` and `executor_*` respectively.

#### Scenario: Manifest fields use canonical names
- **WHEN** a snapshot manifest is produced
- **THEN** the manifest SHALL contain a `controller_id` field, an `executors` array (each entry having `executor_id`), and SHALL NOT contain any `agent_id` or `compute_engine_id` fields

# remote-durability-tier Specification

## Purpose
TBD - created by archiving change add-remote-durability-tier. Update Purpose after archive.
## Requirements
### Requirement: Backend selection with a byte-identical default
The controller SHALL select the durability backend from configuration (`FLEZHA_DURABILITY_BACKEND`, values `none | postgres | s3`), defaulting to `none`, and with `none` the system SHALL behave byte-identically to a build without the tier: no new processes on write paths, no new dependencies loaded, no latency change on any ack path.

#### Scenario: Default configuration is inert
- **WHEN** the controller boots with no durability backend configured
- **THEN** no tier processes ship artifacts, no backend connection is attempted, and every ack path gates only on local fsync as before

#### Scenario: Backend selected by configuration
- **WHEN** `FLEZHA_DURABILITY_BACKEND=postgres` is set with valid connection options
- **THEN** the tier starts, connects, and shipping begins at the next trigger boundary

### Requirement: Backends implement blob storage only
A backend SHALL implement exactly the blob-level contract — `put`, `get`, `list`, `delete`, and epoch-conditional `put_if` — and SHALL NOT implement or duplicate any WAL semantics (framing, folding, truncation, torn-tail repair). All artifact and manifest semantics SHALL live in the tier, shared across backends.

#### Scenario: A backend is a dumb store
- **WHEN** a new backend module is added
- **THEN** it implements only the five blob operations and the tier's shipping, manifest, and recovery behavior work against it unchanged

### Requirement: Shipped artifact scope covers the operative's full durable identity
The tier SHALL ship, as immutable artifacts: compaction snapshots, seq-addressed WAL record ranges (record bundles keyed `(entity, epoch, seq_range)` read from the entity WAL at ship time — never file byte ranges, since the local WAL file is rewritten in place by compaction and correction), proc-journal suffixes, durable-inbox suffixes, the effectively-once durable input log, the ProcessHub registry cspec, and the replayable source position each state cut is consistent with. Local files SHALL remain authoritative; shipping never mutates or deletes local state.

#### Scenario: The log a cut resumes from ships with the cut
- **WHEN** a state cut ships for an operative that has a durable input log
- **THEN** the input log ships with it, so a hydrated Entity has both its consumed floor and the inputs above that floor to replay

#### Scenario: An operative's existence survives with its state
- **WHEN** the registry cspec for an operative is mutated and the tier is enabled
- **THEN** the updated cspec ships as an artifact, so a recovering controller learns both that the operative exists and its full configuration

#### Scenario: Source position ships with the state cut
- **WHEN** a checkpoint interval closes for a Kafka-fed stream operative
- **THEN** the shipped manifest entry records the source offset consistent with that cut, so replay after hydration resumes from it

### Requirement: Shipping triggers reuse existing boundaries plus an RPO backstop
The tier SHALL ship at existing boundaries — compaction completion, checkpoint-interval close, and workflow ProcCommit/IterCommit barriers — and SHALL additionally run a periodic backstop sweep every `ship_interval_ms` (default 30 000) that ships unshipped record ranges for entities that reached no other boundary. Shipping SHALL be per-entity and uncoordinated: no cross-entity barrier or alignment.

#### Scenario: Compaction snapshot ships
- **WHEN** a compaction completes for an entity
- **THEN** the snapshot and the segments it supersedes are reflected in a new shipped manifest entry for that entity

#### Scenario: Quiet entity is bounded by the backstop
- **WHEN** an entity has unshipped record ranges and hits no compaction, checkpoint, or barrier for `ship_interval_ms`
- **THEN** the backstop sweep ships those ranges within one sweep period

### Requirement: Manifest-last atomicity with epoch supersession
For every ship, artifact blobs SHALL be uploaded before the per-entity manifest entry, and the manifest entry SHALL be written last via an epoch-conditional write. Manifest entries SHALL be epoch-stamped: an entry at a higher epoch supersedes overlapping seq ranges from lower epochs, and recovery SHALL follow the highest-epoch consistent chain. Recovery SHALL read only the manifest; blobs not referenced by a live manifest chain SHALL be invisible to recovery and eligible for garbage collection, and GC SHALL never delete a manifest-referenced artifact.

#### Scenario: Torn upload is invisible
- **WHEN** the controller crashes after uploading artifact blobs but before the manifest write
- **THEN** recovery observes the previous manifest entry only, and the orphaned blobs are later garbage-collected without affecting any hydration

#### Scenario: A correction rewind supersedes shipped ranges
- **WHEN** a correction executes `rewind_to(seq, epoch+1)` on an entity whose pre-rewind records were already shipped
- **THEN** the next ship writes a manifest entry at the new epoch superseding the shipped ranges above the rewind seq, and a subsequent hydration reconstructs the post-rewind state, never the superseded one

### Requirement: Async shipping never touches the hot path
In async mode (all stream classes, and workflow classes that do not opt into sync), shipping SHALL run off the ack paths: a slow or failing backend SHALL NOT delay any append, fsync, ack, or watermark advance. Failed ships SHALL retry with the shared exponential backoff ladder while local operation continues.

#### Scenario: Backend outage does not stall dataflow
- **WHEN** the remote backend becomes unreachable while a stream operative processes input
- **THEN** throughput and ack latency are unchanged, shipping retries with backoff, and the RPO alarm (below) fires when the target is exceeded

### Requirement: Per-class synchronous mode for workflow operatives
A workflow class SHALL be able to declare synchronous durability via the class-declaration path, in which case the controller SHALL complete the remote ship of the barrier's journal suffix before replying to that ProcCommit/IterCommit; the SDK-observed ack thereby implies remote durability (RPO = 0). A stream class declaring synchronous mode SHALL be refused at registration.

#### Scenario: Sync workflow commit is remote-durable before ack
- **WHEN** a sync-declared workflow class reaches an IterCommit and the remote ship succeeds
- **THEN** the ack is sent only after the backend confirms the manifest write

#### Scenario: Crash between local append and remote ship
- **WHEN** the controller crashes after the local journal append but before the remote ship of a sync-mode barrier
- **THEN** the SDK, never having received the ack, re-runs the uncommitted proc on resume and no divergent state is exposed

#### Scenario: Stream class cannot opt into sync
- **WHEN** a stream class declares synchronous durability
- **THEN** registration is refused with an error naming the class and the reason

### Requirement: Recovery hydrates from the manifest into the existing restore path
When a controller binds an operative whose local durable state is absent but whose manifest exists in the configured backend, it SHALL hydrate the manifest-referenced artifacts into the local layout and then run the existing restore-and-replay path unchanged. Hydration SHALL support fetching a subset of a procedure's entities by entity key.

#### Scenario: Node-loss recovery
- **WHEN** a fresh controller with an empty state directory and the same backend configuration boots
- **THEN** it hydrates the registry cspec at boot before hub start, the orphan reconcile then starts the restored operatives, bind-time hydration fetches each entity's state artifacts, journal, and inbox, the existing restore path runs, and the system resumes with zero loss for sync-mode workflow classes and at most one ship interval of loss for stream classes

#### Scenario: Partial hydration by entity key
- **WHEN** hydration is requested for a named subset of a procedure's entity keys
- **THEN** only those entities' artifacts are fetched and restored

#### Scenario: Hydration restores the local facts, not only the data
- **WHEN** an entity is hydrated whose shipped manifest was written under a non-zero WAL-owner epoch and which carried saga compensation markers
- **THEN** the hydrated entity SHALL report that same epoch and those same markers locally, so its next manifest write is not refused as stale and a resumed workflow compensates rather than resuming forward past a rollback it owes
- **AND** the restored outbox flush cursor SHALL NOT lead the restored records, so no effect record is dropped from the boot re-drive

### Requirement: Remote fencing via epoch-conditional writes
Every manifest write SHALL carry the writer's epoch and be applied conditionally; a write from a stale epoch SHALL be refused by the backend contract, and a controller receiving such a refusal SHALL stop shipping for that entity and raise a fencing alarm rather than retry blindly.

#### Scenario: Split-brain claimant is fenced
- **WHEN** two controllers both attempt to ship manifests for the same entity and one holds a stale epoch
- **THEN** the stale controller's conditional write is refused, it ceases shipping that entity, and an alarm-grade event is emitted

### Requirement: RPO breach is loud
The tier SHALL track per-entity time since last successful ship against the RPO target and SHALL emit an alarm-grade telemetry event when breached, surfaced in `flz controller status` and `/healthz`. A shipping stall SHALL never be observable only by inspecting the backend.

#### Scenario: Stalled shipper alarms
- **WHEN** shipping for an entity fails past the RPO target while local writes continue
- **THEN** the RPO-exceeded event fires with the entity identity and lag, and controller status reports the breach

### Requirement: Disabling the tier is lossless
Setting the backend to `none` on a controller with intact local state SHALL be a complete rollback: local state remains authoritative and no local data is altered or lost. Remote artifacts SHALL merely go stale.

#### Scenario: Rollback by configuration
- **WHEN** an operator disables the tier and restarts the controller
- **THEN** all operatives restore from local state exactly as before the tier existed


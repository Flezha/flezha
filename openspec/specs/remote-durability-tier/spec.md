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
The tier SHALL ship every durable artifact an operative needs to resurrect elsewhere: its Entity WALs and snapshots, its proc-journal and durable inbox, its input log, and the registry cspec.

An operative's artifacts SHALL be located through both of the identities it is stored under — its instance id, which keys the proc-journal and durable inbox, and its resolved operative class, which keys the Entities and input log. Where the two differ, a request naming one SHALL NOT silently return only the artifacts stored under it; in particular a synchronous-durability barrier SHALL NOT report success having shipped neither the Entity state nor the input log, and the per-class synchronous-durability gate SHALL resolve for a caller holding either identity.

The registry cspec SHALL be stored per writing controller rather than under one shared key, because each controller holds its own replica of a gossiped registry and all of them are legitimate writers. Recovery SHALL select one replica deterministically.

#### Scenario: The log a cut resumes from ships with the cut
- **WHEN** an effectively-once operative closes a checkpoint interval
- **THEN** both the Entity's state cut and the input log covering its consumed offset are shipped, so a hydrated Entity has a log to replay from

#### Scenario: An operative's existence survives with its state
- **WHEN** a controller boots with an empty state directory against a backend holding shipped artifacts
- **THEN** it hydrates the registry cspec before any hub starts, so the operatives it names are started and their state hydrated on bind

#### Scenario: Source position ships with the state cut
- **WHEN** an Entity's shipped cut is hydrated onto a fresh controller
- **THEN** its consumed offset, WAL-owner epoch and compensation markers are restored with it, not only its records

#### Scenario: A synchronous barrier ships both identity spaces
- **WHEN** an operative whose instance id and resolved class differ reaches a commit barrier under synchronous durability
- **THEN** the barrier ships the artifacts stored under both identities before it is acknowledged

#### Scenario: Two controllers' registry replicas do not collide
- **WHEN** two controllers in one namespace each ship their own registry cspec replica
- **THEN** both are stored without either overwriting the other, and a cold controller hydrates exactly one of them

### Requirement: Shipping triggers reuse existing boundaries plus an RPO backstop
The tier SHALL ship at existing boundaries — compaction completion, checkpoint-interval close, and workflow ProcCommit/IterCommit barriers — and SHALL additionally run a periodic backstop sweep every `ship_interval_ms` (default 30 000) that ships unshipped record ranges for entities that reached no other boundary. Shipping SHALL be per-entity and uncoordinated: no cross-entity barrier or alignment.

#### Scenario: Compaction snapshot ships
- **WHEN** a compaction completes for an entity
- **THEN** the snapshot and the segments it supersedes are reflected in a new shipped manifest entry for that entity

#### Scenario: Quiet entity is bounded by the backstop
- **WHEN** an entity has unshipped record ranges and hits no compaction, checkpoint, or barrier for `ship_interval_ms`
- **THEN** the backstop sweep ships those ranges within one sweep period

### Requirement: Manifest-last atomicity with epoch supersession
For every ship, artifact blobs SHALL be uploaded before the per-entity manifest entry, and the manifest entry SHALL be written last via a conditional write. Manifest entries SHALL be epoch-stamped: an entry at a higher epoch supersedes overlapping seq ranges from lower epochs, and recovery SHALL follow the highest-epoch consistent chain. Recovery SHALL read only the manifest; blobs not referenced by a live manifest chain SHALL be invisible to recovery and eligible for garbage collection, and GC SHALL never delete a manifest-referenced artifact.

Artifact names SHALL incorporate a hash of their content, so that two controllers holding one reference address different keys and neither can overwrite the other's bytes. Because blobs are uploaded before the manifest entry, a writer that has already lost the reference's lineage will upload before it can discover that; position-derived names would let it replace the winning writer's records inside the winning manifest.

Garbage collection SHALL NOT delete an artifact younger than the configured collection grace, whatever its scope, and SHALL otherwise be scoped to the references the collecting node holds local state for. An artifact SHALL fail both tests before removal. Neither is the whole safety argument: the grace is what makes "unreferenced" safe to read as "orphaned" while a peer may be mid-ship, and the scoping bounds blast radius. Artifacts whose reference no node holds local state for SHALL still be reclaimed, past a threshold strictly longer than the grace, so that scoping cannot leak storage indefinitely.

#### Scenario: Torn upload is invisible
- **WHEN** the controller crashes after uploading artifact blobs but before the manifest write
- **THEN** recovery observes the previous manifest entry only, and the orphaned blobs are later garbage-collected without affecting any hydration

#### Scenario: A correction rewind supersedes shipped ranges
- **WHEN** a correction executes `rewind_to(seq, epoch+1)` on an entity whose pre-rewind records were already shipped
- **THEN** the next ship writes a manifest entry at the new epoch superseding the shipped ranges above the rewind seq, and a subsequent hydration reconstructs the post-rewind state, never the superseded one

#### Scenario: One node's collection does not race another node's ship
- **WHEN** two controllers share a durability backend and one is between uploading a blob and writing the manifest entry that references it
- **THEN** a collection pass on the other controller leaves that blob in place, and the resulting manifest references an artifact that still exists

#### Scenario: A young artifact is never collected
- **WHEN** a collection pass encounters an unreferenced artifact younger than the collection grace
- **THEN** it is left for a later pass, whether or not it falls inside the collecting node's scope

#### Scenario: Collection still reclaims
- **WHEN** an unreferenced artifact is older than the abandoned threshold and no controller holds local state for its reference
- **THEN** it is eventually collected, so an abandoned lineage does not leak storage forever

#### Scenario: A losing writer cannot overwrite the winner's bytes
- **WHEN** two controllers hold one reference at the same epoch and both ship the same seq range with different records
- **THEN** the two uploads land under different keys, and the artifacts the live manifest names still contain the records that writer shipped

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
Recovery SHALL rebuild local durable files from the manifest and then run the existing restore-and-replay path unchanged. Local state SHALL always take precedence: a reference with local durable files SHALL NOT be overwritten.

A manifest that references an artifact the backend does not hold SHALL be reported as an alarm-grade event naming the reference and the missing artifact. It SHALL NOT be degraded into a silent cold start, because a corrupt lineage and an unreachable backend are different conditions calling for different operator responses.

#### Scenario: Node-loss recovery
- **WHEN** a controller dies with its disk and a fresh controller boots against the same backend
- **THEN** it hydrates the shipped artifacts into the local layout and the ordinary restore-and-replay brings each operative back at its last shipped cut

#### Scenario: Partial hydration by entity key
- **WHEN** hydration is requested for named entity keys of one procedure
- **THEN** only those entities' artifacts are fetched

#### Scenario: Hydration restores the local facts, not only the data
- **WHEN** an Entity is hydrated from the manifest
- **THEN** its consumed offset, epoch and compensation markers are seeded alongside its records

#### Scenario: A manifest naming a missing artifact is alarmed
- **WHEN** hydration fetches a manifest that references an artifact the backend no longer holds
- **THEN** an alarm-grade event naming the reference and the artifact is emitted, distinct from the event raised when the backend is unreachable

### Requirement: Remote fencing via a claim counter
Every manifest write SHALL be applied conditionally against a claim counter held only in the remote store. The owner of a reference SHALL extend its own lineage at the claim it holds; any other writer SHALL bid strictly above it, and the backend SHALL apply a bid only when the stored counter is strictly lower, so that simultaneous claimants resolve to exactly one winner. A controller whose write is refused SHALL stop shipping that reference rather than retry.

The fencing token SHALL NOT be any value that travels with the local state it protects. The WAL-owner epoch does travel — it is copied when state is transferred between controllers and restored verbatim by hydration — and therefore SHALL NOT be used as the guard; it remains in the manifest as the supersession fact recovery reads.

Taking over a reference's lineage SHALL require that the controller has adopted the operative, not merely that it holds local state for it: state transfer copies rather than moves, so a controller that has lost an operative still holds its files. Losing a claim SHALL be reported as a routine handover rather than an alarm, and SHALL NOT leave the reference counted as having unshipped progress.

#### Scenario: Split-brain claimant is fenced
- **WHEN** two controllers both attempt to ship manifests for the same entity and one does not hold the claim
- **THEN** that controller's conditional write is refused and it ceases shipping that entity

#### Scenario: A controller that lost an operative cannot take its lineage back
- **WHEN** an operative migrates to another controller, which adopts it and ships, while the source controller still holds every local file
- **THEN** the source controller's backstop sweep does not reclaim the lineage, and the manifest continues to name the adopting controller

#### Scenario: A restart extends rather than bids
- **WHEN** a controller restarts and ships a reference it already owned
- **THEN** it extends its own lineage without consuming a claim, and is never refused against itself

#### Scenario: A handover does not raise an RPO breach
- **WHEN** a controller with unshipped local progress loses a reference's claim to another writer
- **THEN** it stops shipping that reference and reports no RPO breach for it, so a migration does not latch a permanent alarm

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


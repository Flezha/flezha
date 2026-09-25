# remote-durability-tier Specification

## Purpose
TBD - created by archiving change add-remote-durability-tier. Update Purpose after archive.
## Requirements
### Requirement: Backends implement blob storage only
A backend SHALL implement exactly the blob-level contract — `put`, `get`, `list`, `delete`, and epoch-conditional `put_if` — and SHALL NOT implement or duplicate any WAL semantics (framing, folding, truncation, torn-tail repair). All artifact and manifest semantics SHALL live in the tier, shared across backends.

#### Scenario: A backend is a dumb store
- **WHEN** a new backend module is added
- **THEN** it implements only the five blob operations and the tier's shipping, manifest, and recovery behavior work against it unchanged

### Requirement: Shipped artifact scope covers the operator's full durable identity
The tier SHALL ship every durable artifact an operator needs to resurrect elsewhere: its Cell WALs and snapshots, its task-journal, its durable input log, and the registry cspec. There SHALL be no separate inbox artifact: a bounded instance's post-start input is the same input log an unbounded operator on the effectively-once tier ships, and a bounded operator's commit SHALL enqueue it for shipping with the journal suffix.

An operator's artifacts SHALL be located through both of the identities it is stored under, its instance id, which keys the task-journal, and its resolved operator class, which keys the Cells and the input log. Where the two differ, a request naming one SHALL NOT silently return only the artifacts stored under it; in particular a synchronous-durability barrier SHALL NOT report success having shipped neither the Cell state nor the input log, and the per-class synchronous-durability gate SHALL resolve for a caller holding either identity.

The registry cspec SHALL be stored per writing controller rather than under one shared key, because each controller holds its own replica of a gossiped registry and all of them are legitimate writers. Recovery SHALL select one replica deterministically.

#### Scenario: The log a cut resumes from ships with the cut
- **WHEN** an effectively-once operator closes a checkpoint interval
- **THEN** both the Cell's state cut and the input log covering its consumed offset are shipped, so a hydrated Cell has a log to replay from

#### Scenario: A bounded operator's input ships with its commit
- **WHEN** a durable bounded instance commits an iteration
- **THEN** its journal suffix and its input log are enqueued for shipping, and a controller hydrating the instance finds the input the instance has not consumed in that log

#### Scenario: An operator's existence survives with its state
- **WHEN** a controller boots with an empty state directory against a backend holding shipped artifacts
- **THEN** it hydrates the registry cspec before any hub starts, so the operators it names are started and their state hydrated on bind

#### Scenario: Source position ships with the state cut
- **WHEN** a Cell's shipped cut is hydrated onto a fresh controller
- **THEN** its consumed offset, WAL-owner epoch and compensation markers are restored with it, not only its records

#### Scenario: A synchronous barrier ships both identity spaces
- **WHEN** an operator whose instance id and resolved class differ reaches a commit barrier under synchronous durability
- **THEN** the barrier ships the artifacts stored under both identities before it is acknowledged

#### Scenario: Two controllers' registry replicas do not collide
- **WHEN** two controllers in one namespace each ship their own registry cspec replica
- **THEN** both are stored without either overwriting the other, and a cold controller hydrates exactly one of them

### Requirement: Shipping triggers reuse existing boundaries plus an RPO backstop
The tier SHALL ship at existing boundaries — compaction completion, checkpoint-interval close, and bounded operator TaskCommit/IterCommit barriers — and SHALL additionally run a periodic backstop sweep every `ship_interval_ms` (default 30 000) that ships unshipped record ranges for cells that reached no other boundary. A bounded operator barrier's ship SHALL cover the Cell records the journal entry names as well as the journal suffix, through the operator's full artifact set; no boundary snapshot is requested at an iteration commit, so a bounded operator Cell's snapshot ships when the Watchdog's compaction lands, as an unbounded operator Cell's does. Shipping SHALL be per-cell and uncoordinated: no cross-cell barrier or alignment.

#### Scenario: Compaction snapshot ships
- **WHEN** a compaction completes for a cell
- **THEN** the snapshot and the segments it supersedes are reflected in a new shipped manifest entry for that cell

#### Scenario: A stage barrier ships the records it names
- **WHEN** a durable bounded operator's `task_committed` entry naming a Cell record at seq 5 is acked
- **THEN** that Cell's range through seq 5 and the journal suffix are enqueued for shipping together

#### Scenario: Quiet cell is bounded by the backstop
- **WHEN** a cell has unshipped record ranges and hits no compaction, checkpoint, or barrier for `ship_interval_ms`
- **THEN** the backstop sweep ships those ranges within one sweep period

### Requirement: Manifest-last atomicity with epoch supersession
For every ship, artifact blobs SHALL be uploaded before the per-cell manifest entry, and the manifest entry SHALL be written last via a conditional write. Manifest entries SHALL be epoch-stamped: an entry at a higher epoch supersedes overlapping seq ranges from lower epochs, and recovery SHALL follow the highest-epoch consistent chain. Recovery SHALL read only the manifest; blobs not referenced by a live manifest chain SHALL be invisible to recovery and eligible for garbage collection, and GC SHALL never delete a manifest-referenced artifact.

Artifact names SHALL incorporate a hash of their content, so that two controllers holding one reference address different keys and neither can overwrite the other's bytes. Because blobs are uploaded before the manifest entry, a writer that has already lost the reference's lineage will upload before it can discover that; position-derived names would let it replace the winning writer's records inside the winning manifest.

Garbage collection SHALL NOT delete an artifact younger than the configured collection grace, whatever its scope, and SHALL otherwise be scoped to the references the collecting node holds local state for. An artifact SHALL fail both tests before removal. Neither is the whole safety argument: the grace is what makes "unreferenced" safe to read as "orphaned" while a peer may be mid-ship, and the scoping bounds blast radius. Artifacts whose reference no node holds local state for SHALL still be reclaimed, past a threshold strictly longer than the grace, so that scoping cannot leak storage indefinitely.

#### Scenario: Torn upload is invisible
- **WHEN** the controller crashes after uploading artifact blobs but before the manifest write
- **THEN** recovery observes the previous manifest entry only, and the orphaned blobs are later garbage-collected without affecting any hydration

#### Scenario: A correction rewind supersedes shipped ranges
- **WHEN** a correction executes `rewind_to(seq, epoch+1)` on a cell whose pre-rewind records were already shipped
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
In async mode (all unbounded classes, and bounded classes that do not opt into sync), shipping SHALL run off the ack paths: a slow or failing backend SHALL NOT delay any append, fsync, ack, or watermark advance. Failed ships SHALL retry with the shared exponential backoff ladder while local operation continues.

#### Scenario: Backend outage does not stall dataflow
- **WHEN** the remote backend becomes unreachable while an unbounded operator processes input
- **THEN** throughput and ack latency are unchanged, shipping retries with backoff, and the RPO alarm (below) fires when the target is exceeded

### Requirement: Per-class synchronous mode for bounded operators
A bounded class SHALL be able to declare synchronous durability via the class-declaration path, in which case the controller SHALL complete the remote ship of the barrier's journal suffix and of every Cell record the barrier's entry names before replying to that TaskCommit/IterCommit; the SDK-observed ack thereby implies remote durability (RPO = 0) of the stage's state and output, not of the marker alone. The gate SHALL be the commit worker's one gate, applied after the entry's local sync and before the ack, and no other path SHALL gate on the remote ship. The declaration SHALL have exactly one spelling per SDK, the class-level one; no SDK's channel builder SHALL carry a synchronous-durability call. An unbounded class declaring synchronous mode SHALL be refused at registration under the name `SyncDurabilityOnUnbounded`, whether the SDK judges the class itself or the shared core does.

#### Scenario: Sync bounded operator commit is remote-durable before ack
- **WHEN** a sync-declared bounded class reaches an IterCommit and the remote ship succeeds
- **THEN** the ack is sent only after the backend confirms the manifest write

#### Scenario: A sync stage commit ships its records before the ack
- **WHEN** a sync-declared bounded class reaches a TaskCommit naming a Cell record at seq 5
- **THEN** the ack is sent only after the backend confirms the manifest covering that Cell's range through seq 5 and the journal suffix

#### Scenario: Crash between local append and remote ship
- **WHEN** the controller crashes after the local journal append but before the remote ship of a sync-mode barrier
- **THEN** the SDK, never having received the ack, re-runs the uncommitted task on resume and no divergent state is exposed

#### Scenario: Unbounded class cannot opt into sync
- **WHEN** an unbounded class declares synchronous durability, in any SDK whose spelling lets an unbounded class write it
- **THEN** registration is refused with an error naming the class and `SyncDurabilityOnUnbounded`

#### Scenario: One spelling per SDK
- **WHEN** the Python, Ruby, PHP and Node channel builders are searched for a synchronous-durability call
- **THEN** none carries one, and each SDK's bounded class declares the mode through its class-level setting alone

### Requirement: Recovery hydrates from the manifest into the existing restore path
Recovery SHALL rebuild local durable files from the manifest and then run the existing restore-and-replay path unchanged. Local state SHALL always take precedence: a reference with local durable files SHALL NOT be overwritten.

A manifest that references an artifact the backend does not hold SHALL be reported as an alarm-grade event naming the reference and the missing artifact. It SHALL NOT be degraded into a silent cold start, because a corrupt lineage and an unreachable backend are different conditions calling for different user responses.

#### Scenario: Node-loss recovery
- **WHEN** a controller dies with its disk and a fresh controller boots against the same backend
- **THEN** it hydrates the shipped artifacts into the local layout and the ordinary restore-and-replay brings each operator back at its last shipped cut

#### Scenario: Partial hydration by cell key
- **WHEN** hydration is requested for named cell keys of one tasklet
- **THEN** only those cells' artifacts are fetched

#### Scenario: Hydration restores the local facts, not only the data
- **WHEN** a Cell is hydrated from the manifest
- **THEN** its consumed offset, epoch and compensation markers are seeded alongside its records

#### Scenario: A manifest naming a missing artifact is alarmed
- **WHEN** hydration fetches a manifest that references an artifact the backend no longer holds
- **THEN** an alarm-grade event naming the reference and the artifact is emitted, distinct from the event raised when the backend is unreachable

### Requirement: Remote fencing via a claim counter
Every manifest write SHALL be applied conditionally against a claim counter held only in the remote store. The owner of a reference SHALL extend its own lineage at the claim it holds; any other writer SHALL bid strictly above it, and the backend SHALL apply a bid only when the stored counter is strictly lower, so that simultaneous claimants resolve to exactly one winner. A controller whose write is refused SHALL stop shipping that reference rather than retry.

The fencing token SHALL NOT be any value that travels with the local state it protects. The WAL-owner epoch does travel — it is copied when state is transferred between controllers and restored verbatim by hydration — and therefore SHALL NOT be used as the guard; it remains in the manifest as the supersession fact recovery reads.

Taking over a reference's lineage SHALL require that the controller has adopted the operator, not merely that it holds local state for it: state transfer copies rather than moves, so a controller that has lost an operator still holds its files. Losing a claim SHALL be reported as a routine handover rather than an alarm, and SHALL NOT leave the reference counted as having unshipped progress.

#### Scenario: Split-brain claimant is fenced
- **WHEN** two controllers both attempt to ship manifests for the same cell and one does not hold the claim
- **THEN** that controller's conditional write is refused and it ceases shipping that cell

#### Scenario: A controller that lost an operator cannot take its lineage back
- **WHEN** an operator migrates to another controller, which adopts it and ships, while the source controller still holds every local file
- **THEN** the source controller's backstop sweep does not reclaim the lineage, and the manifest continues to name the adopting controller

#### Scenario: A restart extends rather than bids
- **WHEN** a controller restarts and ships a reference it already owned
- **THEN** it extends its own lineage without consuming a claim, and is never refused against itself

#### Scenario: A handover does not raise an RPO breach
- **WHEN** a controller with unshipped local progress loses a reference's claim to another writer
- **THEN** it stops shipping that reference and reports no RPO breach for it, so a migration does not latch a permanent alarm

### Requirement: RPO breach is loud
The tier SHALL track per-cell time since last successful ship against the RPO target and SHALL emit an alarm-grade telemetry event when breached, surfaced in `flz controller status` and `/healthz`. A shipping stall SHALL never be observable only by inspecting the backend.

#### Scenario: Stalled shipper alarms
- **WHEN** shipping for a cell fails past the RPO target while local writes continue
- **THEN** the RPO-exceeded event fires with the cell identity and lag, and controller status reports the breach

### Requirement: Disabling the tier is lossless
Setting the backend to `none` on a controller with intact local state SHALL be a complete rollback: local state remains authoritative and no local data is altered or lost. Remote artifacts SHALL merely go stale.

#### Scenario: Rollback by configuration
- **WHEN** a user disables the tier and restarts the controller
- **THEN** all operators restore from local state exactly as before the tier existed

### Requirement: Platform durable stores ship with the tier

Beyond the operator's durable identity, the tier SHALL ship the platform
stores a replacement controller needs to resurrect completely: the dead-letter
queue's parked records and the authn/authz state (the roles/tokens/quotas
store, namespace claims, and the durable bootstrap-enabled flag). These SHALL
ship under the writing controller's claim with the same manifest-last
atomicity and fencing as operator artifacts. Recovery SHALL hydrate authz
state before any hub starts or any verb is accepted, so a resurrected
controller enforces from its first accepted connection and the one-shot
bootstrap claim cannot be re-run. Every other durable store under the state
root SHALL be either in shipped scope or named on a documented
accepted-exclusions list (initially: the audit log, delegated to external log
shipping); an undocumented gap is a spec violation, not an accepted loss.

#### Scenario: A dead controller's parked failures survive it

- **WHEN** a controller dies with its disk after parking records to the DLQ
- **THEN** a replacement controller hydrates the parked records with their
  owning operators, and `flz dlq` operations see them

#### Scenario: A resurrected controller stays locked

- **WHEN** a replacement controller hydrates from the backend after the
  original died with its disk
- **THEN** authz state is restored before any connection is accepted: existing
  tokens work, enforcement posture is unchanged, and the bootstrap claim
  reports already-claimed

#### Scenario: Exclusions are documented, not silent

- **WHEN** a durable store exists under the state root that the tier does not
  ship
- **THEN** it appears on the accepted-exclusions list with its alternative
  durability story, and tier documentation reflects it

### Requirement: Backend selection defaults to none and announces the posture

The controller SHALL select the durability backend from configuration
(`FLEZHA_DURABILITY_BACKEND`, values `none | postgres | s3 | local_path`), and
SHALL default to `none` in every environment when it is unset. A boot that takes
that default SHALL log a warning naming the risk it accepts and the valid moves
(configure a backend, or set `none` explicitly), and SHALL NOT refuse to start.
With `none` in force the system SHALL behave byte-identically to a build without
the tier (no new processes on write paths, no new dependencies loaded, no
latency change on any ack path), and the local-only posture SHALL be surfaced on
`/healthz` and `flz controller status` whether `none` was defaulted or set
explicitly, so that a fleet audit can find these nodes without having observed a
boot. An unrecognised value SHALL resolve to `none` rather than fail the boot, and SHALL log a warning naming the value that was not understood and the values that are.

#### Scenario: Unset backend warns and defaults to none

- **WHEN** a production release boots with `FLEZHA_DURABILITY_BACKEND` unset
- **THEN** the controller starts with the tier off, and logs a warning naming the
  env variable, the supported backends, the explicit `none` opt-out, and the risk
  that this node's disk holds the only copy of its durable state

#### Scenario: A defaulted posture is as visible as a declared one

- **WHEN** a controller boots with `FLEZHA_DURABILITY_BACKEND` unset
- **THEN** `/healthz` and `flz controller status` report the local-only
  durability posture identically to a controller booted with `none` set
  explicitly

#### Scenario: Explicit opt-out is inert but visible

- **WHEN** a production controller boots with `FLEZHA_DURABILITY_BACKEND=none`
- **THEN** no tier processes ship artifacts, no backend connection is
  attempted, every ack path gates only on local fsync, and `/healthz` and
  `flz controller status` report the local-only durability posture

#### Scenario: Backend selected by configuration

- **WHEN** `FLEZHA_DURABILITY_BACKEND=postgres` is set with valid connection
  options
- **THEN** the tier starts, connects, and shipping begins at the next trigger
  boundary

#### Scenario: The tier is not what makes a restart durable

- **WHEN** a controller running with the tier off is stopped and restarted with
  its state root intact
- **THEN** every Cell resumes from its local snapshot and tail WAL, and no
  durable state is lost

### Requirement: A selected backend that cannot work refuses the boot

A controller SHALL verify at boot that a selected durability backend holds the options it
requires, and SHALL refuse to start naming the backend and the missing option when it does
not. A backend selected but unusable SHALL NOT reach a running state in which it reports a
remote-tier posture and ships nothing. This refusal SHALL apply only to a backend the
user selected: an unset backend defaults to `none` and warns, and an unrecognised name
resolves to `none` and warns, neither of which is a refusal.

#### Scenario: An object-store backend without a bucket refuses

- **WHEN** a controller boots with `FLEZHA_DURABILITY_BACKEND=s3` and no bucket configured
- **THEN** it does not start, and the refusal names the backend and the missing option

#### Scenario: A filesystem backend without a path refuses

- **WHEN** a controller boots with `FLEZHA_DURABILITY_BACKEND=local_path` and no path configured
- **THEN** it does not start, and the refusal names the backend and the missing option

#### Scenario: A misconfigured backend never reports a remote posture

- **WHEN** a selected backend is missing a required option
- **THEN** the controller does not reach a state where `/healthz` or `flz controller status`
  reports a remote-tier durability posture

#### Scenario: A complete backend configuration is unaffected

- **WHEN** a controller boots with a selected backend and every required option present
- **THEN** the check passes, the tier starts, and the boot is identical to today's


# Remote durability tier

For operators deciding how a Controller's durable state is protected and running one with a
remote backend: the mandatory production decision, the tier and per-backend configuration,
what ships and when, atomicity and collection, the recovery point per operative kind and the
per-class synchronous mode, how recovery runs, who owns a remote lineage, the alarms, the
drills, rollback and the exact scope. The model itself is in
[`../architecture/kernel.md`](../architecture/kernel.md#the-remote-durability-tier).

Every durable byte flezha owns lives on the owning controller's local disk under `app_data/`
— Entity WALs and snapshots, proc-journals and durable inboxes, the input log, the registry
cspec that records which Operatives exist, the parked failures and the control-plane stores.
`Flezha.State.WalTransfer` moves a subset once, at migration time; nothing replicates
continuously, so a controller that dies **with its disk** takes its Operatives' state and the
record of their existence with it. The tier closes that by shipping immutable artifacts to a
remote backend at boundaries that already exist and hydrating them back when a controller
binds an Operative whose local state is absent. It is **not** a pluggable WAL: every ack still
gates on local fsync, the local files stay authoritative, shipping never mutates or deletes
them, and turning the tier off is a complete rollback.

## Another controller is not a substitute

Adding a controller — a peer on another machine, or a second on this one — buys capacity and
isolation and does **not** protect the state on this controller's disk. The hub's redundancy
strategy is `Singularity`: each Operative runs on exactly one node with no replicas, so no
peer shadows another's Operatives. And when a peer does adopt them,
`Flezha.State.WalTransfer` pulls destination-driven over `:erpc` against the source node, so a
controller that died with its disk answers nothing and the adopting controller starts cold.
This tier is the mechanism that survives that loss. Co-locating controllers on one host is
documented in [`deployment.md` § Several controllers on one machine](deployment.md), which
carries the same statement.

## A production controller must choose

`FLEZHA_DURABILITY_BACKEND` has no production default. A prod release with it unset refuses
to boot — `Flezha.State.DurabilityTier.children/0` prints the message to stderr and raises
before the supervision tree is built — naming the two valid moves:

```text
FLEZHA_DURABILITY_BACKEND=postgres   # or s3, or local_path: durable state leaves this disk
FLEZHA_DURABILITY_BACKEND=none       # explicit opt-out: the local disk is the only copy
```

With `none` nothing is started, no dependency is loaded and every ack path gates on local
fsync exactly as without the tier; `/healthz` then carries `"durability":"local_only"` on
every response and `flz controller status` prints `durability_posture: local_only` — not an
alarm, but how a fleet audit finds the nodes whose durable state has one copy. Dev and test
default to `none` with no refusal. Any other value (a misspelling included) also resolves to
`none` rather than crash-looping the controller, so check the posture after changing it.
Postgres is the recommended backend (single-statement conditional write, server-side clock
for collection's grace); S3 suits deployments that already run object storage; `local_path`
needs a root that outlives the cluster, such as an NFS mount. Rollback is lossless: set
`none` and restart — nothing on disk changes and the remote artifacts merely go stale.

## Configuration

`config :flezha, :durability_tier, [...]`, populated from the environment by
`config/runtime.exs`. Non-positive or non-integer values fall back to the default.

| Key                      | Env                                       | Default                            | Meaning                                                                                                         |
| ------------------------ | ----------------------------------------- | ---------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `:backend`               | `FLEZHA_DURABILITY_BACKEND`               | prod: unset (refused); else `none` | `none`, `postgres`, `s3`, `local_path`; or `{Module, opts}` in config for a custom backend                      |
| `:ship_interval_ms`      | `FLEZHA_DURABILITY_SHIP_INTERVAL_MS`      | `30000`                            | Backstop sweep for references that reach no boundary — the bound on how stale a quiet Entity's remote copy gets |
| `:rpo_target_ms`         | `FLEZHA_DURABILITY_RPO_TARGET_MS`         | `120000`                           | Unshipped-progress age at which `rpo_exceeded` fires (two sweeps, so one missed sweep is not an alarm)         |
| `:gc_interval_ms`        | `FLEZHA_DURABILITY_GC_INTERVAL_MS`        | `3600000`                          | Orphan-blob collection period                                                                                    |
| `:gc_grace_ms`           | `FLEZHA_DURABILITY_GC_GRACE_MS`           | `3600000`                          | Nothing younger is ever collected, by anyone                                                                     |
| `:gc_abandoned_after_ms` | `FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS` | `604800000`                        | Age past which artifacts outside this node's scope are reclaimed too                                             |
| —                        | `FLEZHA_DURABILITY_WRITER_ID`             | `node()`                           | This controller's identity as a writer of remote lineages                                                        |

**`FLEZHA_DURABILITY_WRITER_ID`** must be stable across restarts of the same controller and
must **not** be carried to a replacement that starts with an empty disk — that one genuinely
is a new claimant. `node()` is right under a StatefulSet or a fixed host; set it explicitly
under a Deployment, where the pod name changes on every reschedule and an unnamed controller
would take each of its own lineages over again, one claim at a time.

### Postgres (`backend: :postgres`)

One table of `bytea` blobs keyed by artifact key with an epoch column, so the conditional
manifest write is one statement. `postgrex` is already a tree dependency.

| Option                    | Env                                           | Default                                                                                                  |
| ------------------------- | --------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `:hostname` / `:port`     | `FLEZHA_DURABILITY_PG_HOST` / `_PORT`         | `localhost` / `5432`                                                                                     |
| `:database` / `:username` | `FLEZHA_DURABILITY_PG_DATABASE` / `_USERNAME` | `flezha` / `flezha`                                                                                      |
| `:password_env`           | `FLEZHA_DURABILITY_PG_PASSWORD_ENV`           | — (names the env var holding the password; a literal `:password` in config is accepted, prefer the env form) |
| `:ssl`                    | `FLEZHA_DURABILITY_PG_SSL`                    | `false`                                                                                                  |
| `:table`                  | `FLEZHA_DURABILITY_PG_TABLE`                  | `flezha_durability_artifacts`                                                                            |
| `:auto_create`            | `FLEZHA_DURABILITY_PG_AUTO_CREATE`            | `true` (creates the table on connect)                                                                    |
| `:pool_size`              | — (config only)                               | `2`                                                                                                      |

The password travels only inside the connection's `:configure` closure: never in child args,
process state or a log line.

### S3-compatible (`backend: :s3`)

AWS Signature V4 over `:mint`, path-style addressing; works against S3, MinIO and anything
speaking the same verbs. The conditional write is `If-Match` on the observed ETag (or
`If-None-Match: *` when absent), with the claim counter in `x-amz-meta-epoch`.
`Flezha.State.DurabilityTier.Backend.S3.create_bucket/1` provisions the bucket: a one-shot
operator call, not part of any ship path.

| Option                   | Env                                  | Default                                  |
| ------------------------ | ------------------------------------ | ---------------------------------------- |
| `:endpoint`              | `FLEZHA_DURABILITY_S3_ENDPOINT`      | —                                        |
| `:bucket`                | `FLEZHA_DURABILITY_S3_BUCKET`        | —                                        |
| `:region`                | `FLEZHA_DURABILITY_S3_REGION`        | `us-east-1`                              |
| `:access_key_id`         | `FLEZHA_DURABILITY_S3_ACCESS_KEY_ID` | —                                        |
| `:secret_access_key_env` | `FLEZHA_DURABILITY_S3_SECRET_ENV`    | — (names the env var holding the secret) |
| `:prefix`                | `FLEZHA_DURABILITY_S3_PREFIX`        | `flezha`                                 |

### LocalPath (`backend: :local_path`)

Dependency-free filesystem storage: one file per artifact key under the root, written
atomically (fsynced temp file + rename), the claim counter in a `.claim` sidecar.
**The path must outlive the cluster** — a root on a cluster node's own disk shares that
disk's fate. A plain filesystem has no compare-and-swap either, so the fencing write is
read-then-write: correct under the tier's normal one-writer-per-lineage operation, but a
deployment that needs hard multi-writer fencing should use Postgres or S3.

| Option  | Env                      | Default |
| ------- | ------------------------ | ------- |
| `:path` | `FLEZHA_DURABILITY_PATH` | —       |

## What ships, and when

| Artifact                     | Shape                                                                                                                        | Trigger                                                                                  |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Entity WAL records           | seq-addressed record bundle                                                                                                  | checkpoint-interval close (checkpoint-ack classes), compaction, backstop                 |
| Entity snapshots             | the `*.snap.<seq>` file as-is                                                                                                | compaction completion                                                                    |
| Proc-journal / durable inbox | seq-addressed bundle (same substrate)                                                                                        | `ProcCommit` / `IterCommit`                                                              |
| Input log                    | seq-addressed bundle                                                                                                         | checkpoint-interval close (with the Entity cut), backstop                                |
| Registry cspec               | whole payload, per writer                                                                                                    | cspec mutation, backstop                                                                 |
| DLQ parked records           | whole payload: `entries.jsonl` + payload blobs, one artifact per `<operative_class>/<procedure_id>`                          | every park, drop, replay-mark and retention drop (`Dlq.Store.dir_changed/2`), backstop   |
| Control-plane stores         | whole payload: authz DETS files (`state`, `roles`, `namespaces`, `tokens`, `quota_counters`) and `schedules.dets`, per writer | backstop                                                                                 |
| Declared operative intent    | `declared/<hub_id>`, version-guarded                                                                                         | every change to the hub's declared list (`Flezha.State.DurabilityTier.DeclaredManifest`) |

A shipped WAL artifact is a **seq range read at ship time**, never a file byte range:
compaction (`WalWriter.truncate_up_to`) and correction (`rewind_to/3`) rewrite the live WAL in
place. There is no barrier alignment and no cross-Entity coordination — an effect record
already binds state change and consumed offset per Entity, so every shipped cut is
independently consistent. The parked set ships as the **whole directory** on purpose: a
resolved entry has to disappear from the backend, and a per-entry artifact would resurrect it
on hydration; an emptied set still commits a cut. The authz file list is explicit rather than
a directory scan — that is the security boundary keeping `bootstrap.token` and the
`REBOOTSTRAP` marker on the node. The declared list needs no configuration: `HubManager`
wires the hub's `remote_manifest` whenever the tier is on, and the store never overwrites a
higher version.

## Manifest-last atomicity and collection

Blobs upload first under content-addressed names; the per-reference manifest entry naming
them is written last through the backend's claim-conditional `put_if/4`. Recovery reads
**only** the manifest, so a crash between the two leaves the previous entry authoritative and
the new blobs invisible — orphans, collected later. Collection never deletes a
manifest-referenced key: a collector bug can leak storage but never lose state. Because
"uploaded but not yet referenced" looks like an orphan from another controller, an artifact
fails two tests before removal: **age** — nothing younger than `gc_grace_ms` is collected in
any scope (the 1 h default is bounded by the slowest plausible ship: both backends time out
at 30 s, the retry ladder caps at 30 s, and a failed manifest write re-uploads its blobs,
refreshing their age) — and **scope** — a node collects the references it holds local state
for, and artifacts of a reference no node holds any more (a stopped Operative, state that
moved) are reclaimed past `gc_abandoned_after_ms` (7 d). The two thresholds must differ or
the scoping contributes nothing.

Manifest entries mirror local truth rather than accumulating. After **compaction** the ranges
at or below the WAL's new floor are dropped (the snapshot covers them) and the truncated tail
is re-covered from the local WAL. After a **correction** (`rewind_to(seq, epoch + 1)`) the
ranges above `seq` stop being referenced and the next ship writes at the higher epoch;
hydration follows the highest-epoch chain and cannot reconstruct the superseded past.

## RPO by kind

| Kind     | Mode                   | Loss on losing the controller's disk                |
| -------- | ---------------------- | --------------------------------------------------- |
| Workflow | synchronous durability | **none** — the barrier is remote before it is acked |
| Workflow | default (async)        | at most the work since the last shipped barrier     |
| Stream   | checkpoint-ack         | at most the current, unclosed interval              |
| Stream   | per-input              | at most one `ship_interval_ms`                      |

### Synchronous mode

A workflow class declares it in the SDK: Rust `const SYNC_DURABILITY: bool = true;` on the
`WorkflowOperative` impl; Java overrides `WorkflowOperative.syncDurability()` to return
`true`. The controller then completes the remote ship of every artifact of the instance
(`DurabilityTier.commit_sync/1`) before replying to its `ProcCommit`/`IterCommit`, so the
acknowledgement the driver blocks on implies remote durability — one remote round trip per
barrier. A crash between the local append and the remote ship is the ordinary
uncommitted-proc case: the SDK never saw the ack and re-runs the procedure on resume. A ship
failure stalls the workflow at its barrier — visible, bounded by the ack budget, and what RPO
0 means. **The guarantee starts at the second barrier**: the gate is armed by the class
declaration, which the controller learns from the `AddOperative` response, and a fast first
procedure can reach its barrier before that; that barrier ships asynchronously and, unacked,
is re-run on resume like any other.

**Refusals at registration**, by class name: a stream class declaring it (a remote round trip
per input acknowledgement is not a guarantee anyone can keep), and a non-persistent class (no
commit barrier to ship). An operator can force a class either way without an SDK change —
`config :flezha, :sync_durability, %{"payment-flow" => false}`. Sync mode is inert while the
tier is off: a class cannot wait on a backend that does not exist.

## Recovery

When a controller binds an Operative whose local durable state is absent, the bind hydrates
the manifest-referenced artifacts into the local layout and then runs the **existing**
restore-and-replay path unchanged. Nothing is ever hydrated over local state; local is
authoritative, always. Two orderings matter:

- **The cspec and the control-plane stores hydrate at boot, before any hub starts**
  (`Hydrator.hydrate_on_boot/0`, a one-shot child ahead of `Flezha.Service.HubManager`).
  ProcessHub opens `registry.dets` at hub start, so a cspec landing later would be invisible
  to the orphan reconcile; the authz stores land before `AuthZ.Supervisor` opens them and
  before the transport accepts a connection, so a resurrected controller enforces from its
  first accepted verb and the one-shot bootstrap reads already-claimed.
- **The input log and the parked failures hydrate with the Operative, on bind** — never at
  boot, or every node would hold another node's parks and `flz dlq list` would duplicate
  them. Without the input log a hydrated Entity would hold a consumed floor with nothing to
  replay from, and the next input, landing below the floor, would be skipped.

The manifest is per-Entity, so a narrowed request costs exactly what it names: `flz state at`
and its siblings hydrate one entity key to answer about an Operative held only remotely. Node
loss end to end: a fresh controller boots with the same `:durability_tier` configuration,
hydrates every shipped cspec and control store before its first hub starts, the orphan
reconcile starts the Operatives, each bind hydrates that Operative's Entities, proc-journal,
inbox, input log and parks, and the ordinary restore runs — sync-mode workflow classes with
zero loss, stream classes from their last shipped cut.

## Who owns a lineage

Several controllers sharing one backend is the intended configuration, so every write must be
correct with a peer writing at the same time. Each per-reference manifest entry carries an
**owner** and a monotone **claim counter**, and every manifest write is conditional on it: the
owner **extends** its lineage at the claim it holds (`{:extend, n}`, applied unless the stored
counter is strictly higher); anyone else **bids** one above (`{:claim, n}`, applied only if the
stored counter is strictly lower — a compare-and-set, so two nodes bidding the same number
cannot both win).

- **The WAL epoch is not the fencing token.** It lives in the `.outbox.dedup` side-file, which
  `WalTransfer` copies and `Hydrator` restores verbatim, so after a migration both nodes hold
  the same epoch. It stays in the entry as the supersession fact; the claim counter exists only
  in the remote store, which is exactly why it can separate writers.
- **A bid requires an adoption.** `WalTransfer.pull_operative/3` copies and nothing deletes
  the source, so after a migration the losing node still holds every file; if having the state
  were enough, its next backstop sweep would take the lineage straight back. The bind
  (`Lifecycle.bind/3` → `DurabilityTier.adopt_operative/1`) is what says "this node runs it".
- **Losing a claim is ordinary.** The superseded node stops shipping that reference, reports
  `superseded_by` and clears its pending-progress mark — otherwise every handover would leave a
  permanent RPO breach. Nothing local is touched.
- **Blob names are content-addressed** (`r-<epoch>-<from>-<to>-<sha256>`, `s-<seq>-<sha256>`,
  `f-<sha256>`). Blobs go up before the manifest write, so a writer that has already lost a
  lineage uploads before it can discover that; position-addressed names would let it overwrite
  the winner's records. Identical bytes mean the same key, so a re-ship stays idempotent.
- **The cspec and the control-plane stores are keyed per writer** (`c/<hub_id>/<writer>`,
  `p/<store>/<writer>`), not claimed: every controller holds its own replica of a node-local
  store. Hydration takes the newest by `shipped_at_ms` (a per-node wall clock; skew can pick a
  slightly older, equivalent replica, and the orphan reconcile heals the difference).

## Failure posture and alarms

Async shipping never touches an ack path: a slow or failing backend delays no append, fsync,
ack or watermark advance; failed ships retry on the
`Flezha.Plugin.ProviderBehaviour.retry_backoff_ms/3` ladder (500 ms base, 30 s cap) while
local operation continues. What degrades is the guarantee, and that is loud:

| Event (`[:flezha, :state, …]`) | Grade  | Meaning                                                                                                                                                                                                       |
| ------------------------------ | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `:rpo_exceeded`                | alarm  | `Flezha.State.Watchdog`: a reference's local progress is unshipped past `rpo_target_ms`. Latched per reference, cleared when it catches up. `%{lag_ms, target_ms}`, `%{ref}`                                   |
| `:artifact_missing`            | alarm  | A manifest names an artifact the backend does not hold: that lineage is corrupt. **Detection only** — the next ship starts at `shipped_through + 1` and never re-uploads what is missing; act while a node still has a local copy |
| `:hydration_failed`            | event  | The backend was unreachable during hydration — wait and retry, as opposed to the above                                                                                                                        |
| `:ship_failed`                 | event  | One failed attempt; `%{attempt, backoff_ms}`, `%{reason}`                                                                                                                                                     |
| `:claim_superseded`            | info   | Another writer took the lineage over; one per handover. `%{holder}`                                                                                                                                           |
| `:artifact_shipped`            | metric | `%{bytes, records}`                                                                                                                                                                                           |
| `:hydrated`                    | event  | One reference rebuilt locally                                                                                                                                                                                 |
| `:durability_gc`               | metric | One collection pass                                                                                                                                                                                           |

All but `:claim_superseded` reach the observability bus through `TelemetryBridge` under the
`:flezha_durability` source. `flz controller status` reports `durability_backend`,
`durability_posture`, `durability_rpo_breaches` and `durability_superseded`; `/healthz`
reports `degraded` with reason `durability_rpo_exceeded` on a breach — never `503`, because
local durability is intact and a restart cannot reach a backend that is not answering.

## Drills

The kill-the-disk drill is `test/system/remote_durability_e2e_test.exs`: a sync workflow
class and a checkpoint stream class run against a real Postgres, their entire local state
directory is destroyed mid-run, and both must come back — the workflow with its committed
procedure skipped rather than re-run, the stream with its checkpointed state intact.

```bash
docker compose -f docker/dev/docker-compose.yml up -d postgres-sink
POSTGRES_INTEGRATION=1 mix test --include system --include postgres_integration \
  test/system/remote_durability_e2e_test.exs
```

`test/scripts/durability_posture.sh` is the prod-image half: the refusal on an unset backend,
the inert opt-out and its `local_only` posture, and a fresh container with an empty `app_data`
coming back with its parks, authz state and schedules. To verify alarming live, pause the
backend and watch `rpo_exceeded` fire and `flz controller status` report the breach.

## Scope

Shipped — everything a replacement controller needs to come back as itself:

| Store                                                                            | On disk                 | Artifact                             |
| -------------------------------------------------------------------------------- | ----------------------- | ------------------------------------ |
| Entity state, proc-journals, durable inboxes, input log                          | `app_data/state/`       | seq-addressed                        |
| Registry cspec                                                                   | `app_data/process_hub/` | whole payload, per writer            |
| Declared operative intent                                                        | *(remote only)*         | `declared/<hub_id>`, version-guarded |
| DLQ parked records                                                               | `app_data/dlq/`         | whole payload, claim-fenced          |
| Authz state (roles, tokens, namespaces, quota counters, the durable enabled flag) | `app_data/auth/`        | whole payload, per writer            |
| Schedule definitions                                                             | `app_data/schedules/`   | whole payload, per writer            |

Excluded, each with its alternative. A durable store missing from both lists is a defect:

| Excluded                                         | Why                                                                                         | Alternative                                                         |
| ------------------------------------------------ | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Audit log (`app_data/audit/`)                    | Append-only with its own retention; shipping it would make the tier a log shipper           | External log shipping of the JSONL records                          |
| `app_data/auth/bootstrap.token`                  | One-shot plaintext secret; a resurrected controller reads already-claimed from `state.dets` | Re-issued by the ordinary bootstrap flow if no admin binding exists |
| `app_data/auth/REBOOTSTRAP`                      | Emergency-recovery marker an operator writes by hand, with the controller stopped           | Re-created by hand                                                  |
| TLS material (`app_data/certs/`)                 | Provisioned key material; private keys must not enter a shared backend                      | Provisioned with the deployment                                     |
| Installed plugin artifacts (`app_data/plugins/`) | Content-addressed build output                                                              | Re-installed on demand                                              |
| Sockets (`app_data/sockets/`)                    | Not durable; recreated at boot                                                              | —                                                                   |

## Extending the tier

An artifact class is a row in `Flezha.State.DurabilityTier.Manifest`'s `@classes` table (key
tag and identity segment count, so the key encoder and its inverse cannot drift) plus an
`Artifact.payload_files/1` clause (whole payload) or a `wal_path/1` clause (seq-addressed);
shipping, manifest-last, fencing, hydration and collection are shared. A backend implements
`Flezha.State.DurabilityTier.Backend` — `put/3`, `get/2`, `list/2` (returning `{key, age_ms}`,
the age from the store's own clock where it has one), `delete/2`, the claim-conditional
`put_if/4` answering `{:error, {:claim_held, stored}}` when refused, and `child_spec/1`
(`:ignore` for a connectionless store) — and nothing else; select it with
`backend: {MyBackend, opts}`. A backend never parses a manifest.

## See also

- [Auto-restore on boot](auto_restore_on_boot.md) — the local layers this tier backs
- [Reset](reset.md) — a local reset with the tier on rehydrates unless the backend is cleared too
- [ProcessHub internals](../internals/process_hub.md) — the declared list
- [Observability](observability.md) — where the `:flezha_durability` source lands

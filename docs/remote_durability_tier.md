# Remote durability tier

Every durable byte flezha owns lives on the owning controller's local disk under
`app_data/` — Entity WALs and snapshots, workflow proc-journals, durable
inboxes, the effectively-once input log, and the ProcessHub registry cspec that
records *which operatives exist at all*. `Flezha.State.WalTransfer` moves a
subset once, at migration time, controller to controller; ongoing replication is
a non-goal. So a controller that dies **with its disk** has always taken its
operatives' state, and the record of their existence, with it.

The remote durability tier closes that. It ships immutable artifacts to a
config-selected remote backend at boundaries that already exist, and hydrates
them back when a controller binds an operative whose local state is absent.

**A production controller must choose.** A prod release boot with
`FLEZHA_DURABILITY_BACKEND` unset refuses to start and names the two valid
moves. With `backend: none` — set explicitly — nothing is started, no dependency
is loaded, and every acknowledgement path gates on local fsync exactly as it did
before the tier existed; the local-only posture then shows on `/healthz` and
`flz controller status`. Dev and test still default to `none` with no refusal.

## What it is not

It is **not** a pluggable WAL. The local write path is untouched: every ack
still gates on local fsync, and the local files stay authoritative. Putting a
remote store on the per-change path is not viable at flezha's rates (a stream
operative runs to ~100K changes/s; an S3 PUT is 20–100 ms), and a backend that
had to re-implement append, truncate, rewind, torn-tail repair and correction
baselines would multiply the crash matrix by the number of backends.

This is Flink's shape — local state plus checkpoint upload — except that the
workflow kind can also have Temporal's guarantee, per class, via synchronous
mode below.

## Configuration

```elixir
config :flezha, :durability_tier,
  backend: :postgres,          # :none | :postgres | :s3 | :local_path
  ship_interval_ms: 30_000,    # backstop sweep for entities that hit no boundary
  rpo_target_ms: 120_000,      # unshipped-progress age at which the alarm fires
  gc_interval_ms: 3_600_000,   # orphan-blob collection period
  gc_grace_ms: 3_600_000,      # nothing younger is ever collected
  gc_abandoned_after_ms: 604_800_000,  # reclaim outside this node's scope
  backend_opts: [...]
```

### Choosing a posture

`FLEZHA_DURABILITY_BACKEND` has **no production default**. Unset, a prod release
refuses to boot with a message naming both moves:

```
FLEZHA_DURABILITY_BACKEND=postgres   # or s3, or local_path
FLEZHA_DURABILITY_BACKEND=none       # explicit opt-out, local disk only
```

The refusal is deliberate rather than a warning: the tier exists because a
controller that dies with its disk loses everything, and a warning would be
ignored by exactly the deployments that ship the default. Both moves are one
line of environment.

**Upgrading an existing deployment.** Add one of the two lines before rolling the
new image. `none` reproduces the previous behaviour exactly — the tier is not
started, nothing is shipped, and no ack path changes — so it is a safe
placeholder while a backend is provisioned.

**Finding local-only controllers.** With `none`, `/healthz` reports
`"durability":"local_only"` on every response and `flz controller status` prints
`durability_posture: local_only`. Neither is an alarm; both are how a fleet audit
finds the nodes whose durable state has one copy.

**Provisioning a backend** — the per-backend options are below. Postgres is the
default recommendation (real compare-and-swap for the fencing write, server-side
clock for collection's grace); S3 suits deployments that already have object
storage; `local_path` needs a root that outlives the cluster, such as an NFS
mount.

**Rollback is lossless.** Set `none` and restart: local state was authoritative
throughout, so nothing on disk changes and the remote artifacts merely go stale.

Environment variables (`config/runtime.exs`): `FLEZHA_DURABILITY_BACKEND`,
`FLEZHA_DURABILITY_SHIP_INTERVAL_MS`, `FLEZHA_DURABILITY_RPO_TARGET_MS`,
`FLEZHA_DURABILITY_GC_INTERVAL_MS`, `FLEZHA_DURABILITY_GC_GRACE_MS`,
`FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS`, `FLEZHA_DURABILITY_WRITER_ID`, plus
the per-backend options below.

**`FLEZHA_DURABILITY_WRITER_ID`** names this controller as a writer of remote
lineages. It defaults to `node()`, which is correct under a StatefulSet or a
fixed host. Set it explicitly under a Deployment, where the pod name changes on
every reschedule: without it a rescheduled controller does not recognise its own
lineages and takes each one over again, one claim at a time. It must be stable
across restarts of the same controller and must *not* be carried to a
replacement that starts with an empty disk — that one genuinely is a new
claimant.

### Postgres backend

One table of `bytea` blobs keyed by artifact key with an epoch column.
`postgrex` is already a tree dependency (the Postgres output sink), so enabling
this adds nothing to the build.

| Option | Env | Default |
| --- | --- | --- |
| `:hostname` / `:port` | `FLEZHA_DURABILITY_PG_HOST` / `_PORT` | `localhost` / `5432` |
| `:database` / `:username` | `FLEZHA_DURABILITY_PG_DATABASE` / `_USERNAME` | `flezha` / `flezha` |
| `:password_env` | `FLEZHA_DURABILITY_PG_PASSWORD_ENV` | — |
| `:ssl` | `FLEZHA_DURABILITY_PG_SSL` | `false` |
| `:table` | `FLEZHA_DURABILITY_PG_TABLE` | `flezha_durability_artifacts` |
| `:auto_create` | `FLEZHA_DURABILITY_PG_AUTO_CREATE` | `true` |

The password travels only inside the connection's `:configure` closure, so it
never lands in child args, process state, or a log line — the same hygiene the
Postgres sink plugin uses.

### S3-compatible backend

Hand-rolled AWS Signature V4 over `:mint`, path-style addressing, following the
OTLP exporter precedent. Works against S3, MinIO, and anything speaking the same
four verbs.

| Option | Env | Default |
| --- | --- | --- |
| `:endpoint` | `FLEZHA_DURABILITY_S3_ENDPOINT` | — |
| `:bucket` | `FLEZHA_DURABILITY_S3_BUCKET` | — |
| `:region` | `FLEZHA_DURABILITY_S3_REGION` | `us-east-1` |
| `:access_key_id` | `FLEZHA_DURABILITY_S3_ACCESS_KEY_ID` | — |
| `:secret_access_key_env` | `FLEZHA_DURABILITY_S3_SECRET_ENV` | — |
| `:prefix` | `FLEZHA_DURABILITY_S3_PREFIX` | `flezha` |

`Flezha.State.DurabilityTier.Backend.S3.create_bucket/1` provisions the bucket;
it is a one-shot operator call, not part of any ship path.

### LocalPath backend

Dependency-free filesystem storage: one file per artifact key under the
configured root, written atomically (fsynced temp file + rename), the claim
counter in a `.claim` sidecar.

| Option | Env | Default |
| --- | --- | --- |
| `:path` | `FLEZHA_DURABILITY_PATH` | — |

**The path must outlive the cluster.** The tier exists so a controller that
dies WITH its disk is recoverable — a root on a cluster node's own disk shares
that disk's fate. Point it at an NFS mount or an externally provisioned
volume. A plain filesystem also offers no compare-and-swap, so the fencing
write (`put_if/4`) is read-then-write: correct under the tier's normal
one-writer-per-lineage operation, but a deployment that needs hard multi-writer
fencing should prefer the Postgres or S3 backend.

### The DLQ and the control-plane stores

Parked failures ship as one whole-payload artifact per `<operative_class>/<procedure_id>`
directory, claim-fenced like the operative that produced them and hydrated with
it — so `flz dlq` on a replacement controller sees the inherited parks the moment
the operative binds. The unit is the whole directory on purpose: a resolved entry
has to *disappear* from the backend, and a per-entry artifact would leave the
remote copy resurrecting it.

The control-plane stores — the authz registries and the schedule definitions —
are node-local with no cross-node replication, so like the cspec each controller
ships its own replica and recovery takes the newest. They hydrate in the boot
child, **before** any hub starts and before the transport accepts a connection:
a resurrected controller enforces from its first accepted verb, and the one-shot
bootstrap claim reads already-claimed rather than being re-runnable.

### The declared list rides the tier too

With any backend configured, each namespace hub's **declared list** (the
declared-operator-intent record — what should be running) ships through the
same backend via `Flezha.State.DurabilityTier.DeclaredManifest`, under the key
`declared/<hub_id>`, outside the orphan collector's `blob/` scope. Nothing to
configure: `HubManager` wires the hub's `remote_manifest` automatically
whenever the tier is on, and boot adopts whichever copy — local or remote —
carries the higher version. See
`docs/resilience/process_hub_recovery_model.md`.

### Adding an artifact class

An artifact class is a row in `Flezha.State.DurabilityTier.Manifest`'s
`@classes` table — its key tag and how many segments its identity takes, which
is what makes the key encoder and its inverse two readings of one fact — plus a
clause in `Artifact.payload_files/1` (whole-payload) or a WAL path
(seq-addressed). Shipping, manifest-last atomicity, fencing, hydration and
orphan collection are shared and need no change.

### Adding a backend

A backend implements `Flezha.State.DurabilityTier.Backend` — `put/3`, `get/2`,
`list/2`, `delete/2`, and the claim-conditional `put_if/4` — and nothing else.
Every artifact, manifest, supersession and recovery rule lives in the tier and
is shared. Select a module directly with `backend: {MyBackend, opts}`.

Two of those carry more than their names suggest:

- **`list/2` returns `{key, age_ms}`**, and the age should come from the store's
  own clock where it has one (Postgres computes it in the query; S3's is derived
  from `LastModified`). Collection's grace then never depends on the controller
  and the store agreeing about the time.
- **`put_if/4` takes `{:extend, n} | {:claim, n}`** and answers
  `{:error, {:claim_held, stored}}` when refused. `:extend` applies unless the
  stored counter is strictly higher; `:claim` applies only if it is strictly
  lower. Both modes are required: `<=` alone lets simultaneous claimants
  overwrite each other, `<` alone refuses an owner's ordinary repeat writes. A
  backend never parses a manifest — it reports the counter, and the tier reads
  the owner from the entry.

## What ships, and when

| Artifact | Shape | Trigger |
| --- | --- | --- |
| Entity WAL records | seq-addressed record bundle | checkpoint close, compaction, backstop |
| Entity snapshots | the `*.snap.<seq>` file as-is | compaction completion |
| Proc-journal / durable inbox | seq-addressed bundle (same substrate) | ProcCommit / IterCommit |
| Input log | seq-addressed bundle | checkpoint close, backstop |
| Registry cspec | whole payload, content-addressed | cspec mutation, backstop |
| DLQ parked records | whole payload (JSONL index + payload blobs) | every park, drop, replay-mark, retention drop |
| Control-plane stores | whole payload (authz + schedule DETS) | backstop |

A shipped WAL artifact is a **seq range read at ship time**, never a file byte
range. The entity WAL is one live file that compaction (`rewrite_keep_seq_gt`)
and correction (`rewind_to`) rewrite in place, so a byte offset recorded
yesterday addresses different records today. A seq range does not move.

The only new trigger is the backstop sweep: everything else is a boundary the
system already had. There is no barrier alignment and no cross-entity
coordination — an effect record already binds `(state change, consumed offset)`
atomically per Entity, so every Entity's shipped cut is independently
consistent.

## Manifest-last atomicity

Blobs upload first, under content-addressed names. The per-reference manifest
entry naming them is written last, through the backend's claim-conditional write.
Recovery reads **only** the manifest, so a crash between the two leaves the
previous entry authoritative and the new blobs invisible — orphans, collected
later. Garbage collection never deletes a manifest-referenced key, so a GC bug
can leak storage but never lose state.

**Collection must survive a peer mid-ship.** "Uploaded but not yet referenced" is
a state every ship passes through, and from another controller it is
indistinguishable from an orphan. So an artifact must fail two independent tests
before it is removed:

- **Age.** Nothing younger than `gc_grace_ms` is collected, by anyone, in any
  scope. This is the safety property, and it needs no knowledge of whose artifact
  it is: an upload still in flight is young by definition. The default (1 h) is
  bounded by the slowest plausible ship rather than tuned — both backends time
  out at 30 s and the retry ladder caps at 30 s, and a failed manifest write
  re-uploads its blobs, refreshing their age.
- **Scope.** A node collects the references it holds local state for. This bounds
  what one node's mistake can reach and makes the ordinary pass far cheaper; it
  is *not* a safety argument on its own, because a migration, a rescale and
  hydration all leave two nodes holding one reference.

Scoping alone would strand the artifacts of a reference no node holds any more —
a stopped operative, or one whose state moved — so those are still reclaimed,
past the much longer `gc_abandoned_after_ms` (default 7 d). The two thresholds
must differ: collapsed to one number, the scoping would contribute nothing.

Manifest entries are epoch-stamped and mirror local truth rather than
accumulating:

- **Compaction** — a snapshot covers everything below the WAL's new floor, so
  the ranges reaching at or below it are dropped and the truncated tail is
  re-covered from the local WAL.
- **Correction** — `rewind_to(seq, epoch + 1)` dropped the local records above
  `seq`, so the ranges covering them stop being referenced; the next ship writes
  at the higher epoch. Hydration follows the highest-epoch chain and can never
  reconstruct the superseded past.

## RPO by kind

| Kind | Mode | Loss on losing the controller's disk |
| --- | --- | --- |
| Workflow | `SYNC_DURABILITY = true` | **none** — the barrier is remote before it is acked |
| Workflow | default (async) | at most the work since the last shipped barrier |
| Stream | checkpoint-ack | at most the current, unclosed interval |
| Stream | per-input | at most one `ship_interval_ms` |

### Synchronous mode

A workflow class may declare it:

```rust
impl WorkflowOperative for Payment {
    const SYNC_DURABILITY: bool = true;
    // ...
}
```

```java
public boolean syncDurability() { return true; }
```

The controller then completes the remote ship of the barrier before replying to
its `ProcCommit`/`IterCommit`, so the acknowledgement the driver blocks on
implies remote durability. Cost is one remote round trip per barrier — on a LAN
Postgres, 1–5 ms at Hz rates, inside the SDK's existing 30 s ack budget.

A crash between the local append and the remote ship is the ordinary
uncommitted-proc case: the SDK never saw the ack, so it re-runs the procedure on
resume, and the existing idempotence machinery covers it.

**The guarantee starts at the second barrier.** The gate is armed by the class
declaration, which the controller learns from the `AddOperative` response — and
a fast first procedure can reach its barrier before that response has been
decoded. That first barrier therefore ships asynchronously. Nothing is lost by
it: an unacked barrier is re-run on resume like any other, which is the same
path a crash mid-ship takes. Every barrier after it is gated.

**Stream classes cannot declare it.** They acknowledge at input rates, where a
remote round trip per acknowledgement is not a guarantee anyone can keep;
declaring it is refused at registration with an error naming the class.

An operator can force either way per class without an SDK change:

```elixir
config :flezha, :sync_durability, %{"payment-flow" => false}
```

## Recovery

When a controller binds an operative whose local durable state is absent, the
bind hydrates the manifest-referenced artifacts into the local layout and then
runs the **existing** restore-and-replay path unchanged. There is no second
restore implementation.

Nothing is ever hydrated over local state: a reference with local durable files
is left exactly as it is. Local is authoritative, always.

Two orderings matter:

- **The cspec is hydrated at boot, before any hub starts.** ProcessHub opens
  `registry.dets` at hub start, so a cspec landing afterwards would be invisible
  to the orphan reconcile and the operatives it names would never be started.
  It runs as a one-shot supervision child ahead of `Flezha.Service.HubManager`.
- **The input log is hydrated with the Entities.** It is class-level, so it sits
  outside the per-Entity manifest prefix and is fetched by name. Without it a
  hydrated Entity comes back holding a consumed floor with no log to replay
  from, and the next input — landing at offset 0, below the floor — is silently
  skipped as already consumed.

Hydration also supports a narrowed request: the manifest is per-Entity, so
asking about one entity key fetches one entity key's artifacts. `flz state at`
and its siblings use this to answer about an operative the controller holds only
remotely.

### Node-loss walkthrough

1. A controller dies with its disk. Its operatives are unreachable.
2. A fresh controller boots with the same `:durability_tier` configuration.
3. Before its first hub starts, it hydrates every shipped registry cspec — it
   now knows which operatives exist and how they are configured.
4. The orphan reconcile starts them. Each bind hydrates that operative's
   Entities, proc-journal, durable inbox and input log.
5. The ordinary restore-and-replay runs: sync-mode workflow classes resume with
   zero loss, stream classes from their last shipped cut.

## Who owns a lineage

Two or more controllers sharing one backend is the **intended** configuration —
a fresh node adopting another's operatives is what the tier exists for — so
every write has to be correct with a peer writing at the same time.

Each per-reference manifest entry carries an **owner** and a monotone **claim
counter**, and every manifest write is conditional on it:

- the owner **extends** its own lineage at the claim it already holds, as many
  times as it ships;
- anyone else **bids** one above it, and the backend applies that only if the
  stored counter is strictly lower. That strictness is a compare-and-set: two
  nodes bidding the same number cannot both win.

**The WAL epoch is not the fencing token, and could never have been.** It lives
in the `.outbox.dedup` side-file, which `WalTransfer` copies with the rest of the
state and `Hydrator` restores verbatim — so after a migration both nodes hold the
same epoch and it cannot tell them apart. It remains in the entry as the
supersession fact the manifest algebra reads. The claim counter exists only in
the remote store, which is exactly why it separates writers: nothing local can
carry it.

**A bid requires an adoption.** Shipping alone must never claim a lineage.
`WalTransfer.pull_operative/3` copies and nothing deletes the source, so after a
migration the node that just lost the operative still holds every file; if having
the state were enough, its next backstop sweep would take the lineage straight
back. `Lifecycle.complete_init/4` is what says "this node runs it now".

**Losing a claim is ordinary, not an alarm.** Every migration produces a loser.
The superseded node stops shipping that reference, reports `superseded_by`, and
clears its pending-progress mark — without that last part every handover would
leave a permanent RPO breach and a permanently `degraded` `/healthz`. Nothing
local is touched: the tier never deletes local files, and the superseded node is
often the only other place the state exists.

**Blob names are content-addressed** (`r-<epoch>-<from>-<to>-<sha256>`,
`s-<seq>-<sha256>`, `f-<sha256>`). This is the other half, and it is not
redundant with the claim: blobs go up *before* the manifest write, so a writer
that has already lost a lineage uploads before it can discover that. Position-
addressed names would have let it overwrite the winner's records inside the
winner's own live manifest — corruption strictly worse than a missing blob,
because absence is at least detectable. Different bytes now mean a different key,
and identical bytes mean the same key, so a re-ship stays idempotent.

**The registry cspec is keyed per writer** (`c/<hub_id>/<writer>`), not claimed.
Every controller in a namespace holds its own replica of one gossiped ProcessHub
registry — N writers is correct there, and a claim would make correct nodes fence
each other out of shipping it. Hydration picks the newest replica by
`shipped_at_ms`. That is a per-node wall clock, so skew could pick a slightly
older replica; the replicas are equivalent by design and the orphan reconcile
heals the difference, and there is no cluster-wide monotonic source at this layer
to do better.

## Failure posture and alarms

Async shipping never touches an ack path. A slow or failing backend delays no
append, fsync, ack or watermark advance; failed ships retry on the shared
`ProviderBehaviour.retry_backoff_ms/3` ladder while local operation continues at
full speed.

What degrades is the *guarantee*, and that is loud:

- `[:flezha, :state, :rpo_exceeded]` — alarm-grade, raised by
  `Flezha.State.Watchdog` when an entity's local progress has gone unshipped
  past `rpo_target_ms`. Latched per reference and cleared when it catches up.
- `[:flezha, :state, :artifact_missing]` — alarm-grade: a manifest references an
  artifact the backend does not hold, so that lineage is corrupt. Deliberately
  distinct from `:hydration_failed` (an unreachable backend), because the two
  call for opposite responses — one waits, the other must be acted on while some
  node still has a local copy to re-ship from. **Detection only**: the tier does
  not repair a corrupt lineage, since the next ship computes
  `from = shipped_through + 1` and never re-uploads what is missing.
- `[:flezha, :state, :claim_superseded]` — another writer took a lineage over.
  Routine, and emitted at info: a handover produces one every time.
- `[:flezha, :state, :ship_failed]` / `:artifact_shipped` / `:hydrated` /
  `:hydration_failed` / `:durability_gc`.

All of these reach the observability bus through `TelemetryBridge` under the
`:flezha_durability` source.

`flz controller status` reports `durability_backend`, `durability_rpo_breaches`
and `durability_superseded`; `/healthz` reports `degraded` with reason
`durability_rpo_exceeded` on a breach (never `503` — local durability is intact
and a restart cannot reach a backend that is not answering).

Sync-mode failures stall the opted-in workflow at its barrier: visible, bounded
by the existing ack budget, and correct — the class asked for RPO 0.

## Drills

The kill-the-disk drill is in
`test/system/remote_durability_e2e_test.exs`: a sync workflow class and a
checkpoint stream class run against a real Postgres, their entire local state
directory is destroyed mid-run, and both must come back — the workflow with its
committed procedure skipped rather than re-run, the stream with its
checkpointed state intact.

```
docker compose -f docker/dev/docker-compose.yml up -d postgres-sink
POSTGRES_INTEGRATION=1 mix test --include system --include postgres_integration \
  test/system/remote_durability_e2e_test.exs
```

To verify alarming in a live environment, pause the backend and watch
`rpo_exceeded` fire and `flz controller status` report the breach.

## Rollback

Set `backend: none` and restart. Local state is authoritative throughout, so
disabling the tier alters nothing on disk — the remote artifacts merely go
stale.

## Scope, and the accepted exclusions

Shipped — everything a replacement controller needs to come back as itself:

| Store | On disk | Artifact |
| --- | --- | --- |
| Entity state, proc-journals, durable inboxes | `app_data/state/` | seq-addressed |
| Input log | `app_data/state/` | seq-addressed |
| Registry cspec | `app_data/process_hub/` | whole payload, per writer |
| Declared operative intent | *(remote only)* | `declared/<hub_id>`, version-guarded |
| DLQ parked records | `app_data/dlq/` | whole payload, claim-fenced |
| Authz state (roles, tokens, namespaces, quota counters, the durable enabled flag) | `app_data/auth/` | whole payload, per writer |
| Schedule definitions | `app_data/schedules/` | whole payload, per writer |

**Accepted exclusions.** Anything durable under `app_data/` that is not shipped
appears here with its alternative durability story. A store missing from both
lists is a defect, not an accepted loss.

| Excluded | Why | Alternative |
| --- | --- | --- |
| Audit log (`app_data/audit/`) | Append-only, monotonic, with its own retention story; shipping it would make the tier a log-shipping system | External log shipping — the JSONL records are designed to be tailed off the node |
| `app_data/auth/bootstrap.token` | A one-shot plaintext secret whose only job is local; a resurrected controller reads *already claimed* out of `roles.dets` and never needs the token back | Re-issued by the ordinary bootstrap flow if no admin binding exists |
| `app_data/auth/REBOOTSTRAP` | Break-glass marker an operator writes by hand, with the controller stopped | Re-created by hand where it is needed |
| TLS material (`app_data/certs/`) | Provisioned key material, not runtime state; private keys must not enter a shared backend | Provisioned with the deployment |
| Installed plugin artifacts (`app_data/plugins/`) | Content-addressed build output, re-installable from its source | Plugin installer re-fetches on demand |
| Sockets (`app_data/sockets/`) | Not durable — recreated at boot | — |

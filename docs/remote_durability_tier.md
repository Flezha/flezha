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

**It is off by default.** With `backend: none` — the default — nothing is
started, no dependency is loaded, and every acknowledgement path gates on local
fsync exactly as it did before the tier existed.

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
  backend: :postgres,          # :none (default) | :postgres | :s3
  ship_interval_ms: 30_000,    # backstop sweep for entities that hit no boundary
  rpo_target_ms: 120_000,      # unshipped-progress age at which the alarm fires
  gc_interval_ms: 3_600_000,   # orphan-blob collection
  backend_opts: [...]
```

Environment variables (`config/runtime.exs`): `FLEZHA_DURABILITY_BACKEND`,
`FLEZHA_DURABILITY_SHIP_INTERVAL_MS`, `FLEZHA_DURABILITY_RPO_TARGET_MS`,
`FLEZHA_DURABILITY_GC_INTERVAL_MS`, plus the per-backend options below.

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

### Adding a backend

A backend implements `Flezha.State.DurabilityTier.Backend` — `put/3`, `get/2`,
`list/2`, `delete/2`, and the epoch-conditional `put_if/4` — and nothing else.
Every artifact, manifest, supersession and recovery rule lives in the tier and
is shared. Select a module directly with `backend: {MyBackend, opts}`.

## What ships, and when

| Artifact | Shape | Trigger |
| --- | --- | --- |
| Entity WAL records | seq-addressed record bundle | checkpoint close, compaction, backstop |
| Entity snapshots | the `*.snap.<seq>` file as-is | compaction completion |
| Proc-journal / durable inbox | seq-addressed bundle (same substrate) | ProcCommit / IterCommit |
| Input log | seq-addressed bundle | checkpoint close, backstop |
| Registry cspec | whole file, content-addressed | cspec mutation, backstop |

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

Blobs upload first. The per-reference manifest entry naming them is written
last, through the backend's epoch-conditional write. Recovery reads **only** the
manifest, so a crash between the two leaves the previous entry authoritative and
the new blobs invisible — orphans, collected later by comparing `list/2` against
the live manifests. Garbage collection never deletes a manifest-referenced key,
so a GC bug can leak storage but never lose state.

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

## Fencing

Every manifest write carries the writer's WAL-owner epoch and is applied
conditionally. A write from a stale epoch is refused, and the refused controller
**stops shipping that reference** and raises an alarm rather than retrying —
overwriting a claimant that legitimately superseded it is the one outcome worse
than not shipping. This makes the remote store an external arbiter and is
strictly stronger than the local-only fencing it extends.

## Failure posture and alarms

Async shipping never touches an ack path. A slow or failing backend delays no
append, fsync, ack or watermark advance; failed ships retry on the shared
`ProviderBehaviour.retry_backoff_ms/3` ladder while local operation continues at
full speed.

What degrades is the *guarantee*, and that is loud:

- `[:flezha, :state, :rpo_exceeded]` — alarm-grade, raised by
  `Flezha.State.Watchdog` when an entity's local progress has gone unshipped
  past `rpo_target_ms`. Latched per reference and cleared when it catches up.
- `[:flezha, :state, :ship_fenced]` — a stale-epoch refusal.
- `[:flezha, :state, :ship_failed]` / `:artifact_shipped` / `:hydrated` /
  `:hydration_failed` / `:durability_gc`.

`flz controller status` reports `durability_backend`, `durability_rpo_breaches`
and `durability_fenced`; `/healthz` reports `degraded` with reason
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

## Scope

Shipped: Entity state, proc-journals, durable inboxes, the input log, and the
registry cspec — everything needed for an operative to resurrect elsewhere with
full fidelity.

Deliberately excluded: DLQ entries, the audit log, and auth DETS. Their loss
degrades operability, not the correctness of live dataflow, and each has its own
retention and ownership model. The manifest namespace is generic, so they can
ride the same tier later with no design change.

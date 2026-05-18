# Durable audit log (`platform-audit-log`)

The audit-log subsystem layers a durable, append-only sink on top of
the observability foundation. It subscribes to a configurable
allow-list of `(source, name)` pairs from the foundation bus and
writes one JSON Lines record per accepted signal to disk under a
configured directory. Operators read it with the `flz audit` CLI
verbs.

The subsystem is **off by default**. Turning it on changes the
running host's disk-space behaviour, so operators must opt in
explicitly via `:audit_log, :enabled: true` (or the
`FLEZHA_AUDIT_ENABLED=true` env var).

## Default allow-list

The sink writes a record for every signal whose `(source, name)`
matches the allow-list. The compiled-in defaults are:

```elixir
[
  {:observability_service, :any},      # service-API audit events
  {:flezha_shutdown, :any},             # graceful-shutdown phases
  {:flezha_plugin_installer, :any},     # plugin install / load activity
  {:platform_authz, :any},              # authn/authz mutations and denials
  {:audit_log, :any}                    # audit-of-audit (reads, rotations)
]
```

The `:platform_authz` entry ships with the
[`platform-authn-authz`](platform_authn_authz.md) foundation — every
bootstrap / token / namespace / role mutation, plus every
authorization denial, lands here.

Extend it via `:audit_log, :extra_subscriptions` in
`config/runtime.exs`:

```elixir
config :flezha, :audit_log,
  enabled: true,
  extra_subscriptions: [
    {:flezha_relay, :start},
    {:flezha_relay, :stop},
    {:observability_export, :otlp_batch_dropped}
  ]
```

Operators reasonably expect "audit everything mutating" — the
default list is intentionally narrow so an unwary operator does not
fill disk; add the relay-lifecycle events listed above when you
enable the subsystem.

## On-disk layout

Under `:dir` (default `app_data/audit`):

```
app_data/audit/
├── INDEX                             # binary catalog (rotated files only)
├── audit-current.jsonl               # active file (rotated on size/age)
├── audit-20260518T071455Z-1.jsonl    # rotated files (sorted by name)
├── audit-20260518T071455Z-2.jsonl    # -N suffix disambiguates same-second
└── audit-20260518T130000Z-1.jsonl.gz # operators may gzip rotated files out-of-band
```

`INDEX` is `:erlang.term_to_binary/1` of a list of
`{filename, first_ts_us, last_ts_us, record_count, size_bytes, sha256}`
entries. It is rewritten atomically (`.tmp + rename`) on every
rotation and on every retention sweep that deleted entries. If the
file is missing or fails to decode, the writer rebuilds it on boot
by streaming every `audit-*.jsonl[.gz]` file in the directory.

## JSON Lines record schema

Each line is one UTF-8 JSON object terminated by `\n`. Field order
is stable: `ts_us, ts_iso, source, name, namespace, measurements,
metadata, caller`. `caller` is denormalised from the metadata
convention so filters like `jq 'select(.caller.source == "cli")'`
work without knowing the underlying field names.

```json
{
  "ts_us": -576460748,
  "ts_iso": "2026-05-18T07:14:55.451Z",
  "source": "observability_service",
  "name": "disable_source",
  "namespace": "_system",
  "measurements": {"count": 1},
  "metadata": {"source":"flezha_state","caller_source":"cli","request_id":"req-abc"},
  "caller": {"source":"cli","principal":"alice@example.com","request_id":"req-abc"}
}
```

`caller.principal` is the verified principal whenever
`:authn_authz, :enabled` is `true` (resolved from the CLI / SDK
handshake or the bound client cert); it falls back to the calling
source atom (`"cli"`, `"internal"`, …) while auth is disabled,
preserving pre-`platform-authn-authz` behaviour. The top-level
`namespace` field is set from the caller context; the reader
tolerates absent fields on pre-upgrade records and defaults them
to `"_system"`.

## Fsync modes

`fsync_mode: :per_record` (default) calls `:file.sync/1` after
every write before the writer replies. Producer latency is
`O(disk-sync)` (typically 1-10 ms on local SSD, more on network
storage). At audit-event volumes (a handful per minute) this is
fine.

`fsync_mode: :batched` calls `:file.sync/1` periodically every
`:fsync_interval_ms` (default `100`). Throughput is roughly
`1000 / fsync_interval_ms` syncs/sec. The loss window for an
unclean crash is `fsync_interval_ms` (records buffered in the OS
page cache but not yet flushed). Use this mode only when audit
volume is high and you have an off-host backstop (e.g. OTLP
exporter from `platform-observability-export`).

## Rotation policy

The active file rotates when EITHER:

  * `size_bytes >= :max_file_bytes` (default `64 MiB`), OR
  * `age_seconds >= :max_file_age_seconds` (default `86_400`, 1 day)

whichever fires first. Checked on a periodic timer
(`:rotation_check_interval_ms`, default `60_000`) and on every
write. The rotated file is renamed
`audit-YYYYMMDDTHHMMSSZ-N.jsonl` where `-N` is a monotonic
within-second counter (1, 2, …). An `:audit_rotated` event is
emitted on the `:audit_log` source carrying the rotated filename
and size.

`flz audit rotate` forces an immediate rotation regardless of size
or age — useful before a compliance export.

## Retention policy

Every `:retention_check_interval_ms` (default `3_600_000`, 1 hour)
the retention sweep:

  1. Deletes rotated files older than `:retention_seconds` (default
     `2_592_000`, 30 days).
  2. If the total directory size still exceeds `:max_total_bytes`
     (default `4 GiB`), deletes the oldest remaining rotated files
     until under the cap.

The active file is NEVER deleted by retention — its lifecycle is
owned by rotation. After deletions the `INDEX` is rewritten
atomically and an `:audit_retention_sweep` event is emitted with
`deleted_count`, `deleted_bytes`, `remaining_count`, `total_bytes`.

## CLI verbs

```
flz audit list [--json]
    List the on-disk catalog (filename, first_ts, last_ts, count, size).

flz audit read <FILE> [--source S] [--name N] [--since US] [--until US]
              [--limit N] [--json]
    Read records from a specific file with filter pushdown. Default
    limit is 1000. Use `audit-current.jsonl` for the active file.

flz audit tail [--json] [--duration-ms N]
    Stream live audit records until the duration elapses (default
    30s). Each new record arrives within a scheduler tick.

flz audit stats [--json]
    Operational snapshot — current file path, current file bytes,
    total bytes, total files, oldest ts, record count, drop count.

flz audit rotate [--json]
    Force an immediate rotation of the active file.
```

When audit is disabled every verb exits non-zero with a stderr line
naming the disabled flag and how to enable it
(`FLEZHA_AUDIT_ENABLED=true` or `:audit_log, :enabled: true`).

## Recommended file-system posture

The audit directory inherits the running user's umask. For
on-host confidentiality the recommended posture is `chmod 0640`
(owner-read+write, group-read, others-none), with the audit dir
owned by the running user and a dedicated `audit` group for
log readers. Two deployment-flavour examples:

systemd unit:
```ini
[Service]
RuntimeDirectory=flezha
RuntimeDirectoryMode=0750
StateDirectory=flezha
StateDirectoryMode=0750
UMask=0027
```

Kubernetes pod spec (audit on a persistent volume):
```yaml
volumes:
  - name: audit
    persistentVolumeClaim:
      claimName: flezha-audit
volumeMounts:
  - name: audit
    mountPath: /var/lib/flezha/audit
securityContext:
  runAsUser: 1000
  fsGroup: 1000
  umask: 0027
```

[`platform-authn-authz`](platform_authn_authz.md) adds a policy
gate at the `Flezha.Service.AuditLog` boundary: `read/2` / `tail/2`
results are filtered to the caller's namespace, except for
`admin@_system` and internal callers, who see every namespace.

## Relationship to remote shipping

This change ships only the local durable file plus the read CLI. To
ship audit data off-host run the OTLP exporter from
`platform-observability-export` alongside this — the OTLP sink
batches and POSTs every signal that flows through the bus, so the
audit allow-list does not need to be re-replicated remotely. Add
`:observability_export, :otlp_batch_dropped` to the audit
allow-list so on-host operators are notified when remote shipping
loses a batch.

## Non-goals

  * **No remote shipping.** Use the OTLP exporter to ship off-host.
  * **No tamper-evident chain across files.** The per-file SHA-256
    in the INDEX defeats casual editing but not a privileged
    attacker with disk access.
  * **No tamper-evident chain across the audit subsystem and the
    auth subsystem.** Auth mutations land here as ordinary records;
    a privileged attacker with disk access can rewrite both.
  * **No compression of rotated files by the writer.** Operators can
    `gzip` rotated files out-of-band; the Reader transparently
    handles `.jsonl.gz` files via streaming `:zlib` decompression.

## Configuration knobs

```elixir
config :flezha, :audit_log,
  enabled: false,
  dir: "app_data/audit",
  max_file_bytes: 67_108_864,         # 64 MiB
  max_file_age_seconds: 86_400,        # 1 day
  retention_seconds: 30 * 86_400,      # 30 days
  max_total_bytes: 4_294_967_296,      # 4 GiB
  fsync_mode: :per_record,             # or :batched
  fsync_interval_ms: 100,              # batched-mode flush cadence
  rotation_check_interval_ms: 60_000,
  retention_check_interval_ms: 3_600_000,
  extra_subscriptions: []
```

Env-var overrides:

  * `FLEZHA_AUDIT_ENABLED` — `true|1|yes|on` flips the master flag.
  * `FLEZHA_AUDIT_DIR` — redirects the on-disk path.
  * `FLEZHA_AUDIT_RETENTION_DAYS` — overrides `:retention_seconds`
    in whole-day units.

## Shutdown integration

The graceful-shutdown coordinator (`Flezha.Shutdown.Coordinator`)
calls `Flezha.Observability.AuditLog.Writer.close/0` during its
finalise phase, BEFORE the foundation's `FsyncBatcher` flush. The
close drains any in-flight writes, fsyncs the active file, and
shuts the file descriptor. Signals that arrive at the sink after
close are silently discarded with a `:post_close_dropped` counter
increment — no exception propagates to the producer.

`flz controller stop` therefore guarantees that any audit record
emitted before the shutdown call is durable on disk. Set
`:graceful_shutdown_timeout_ms` to a value that accommodates
audit-fsync time under your storage profile.

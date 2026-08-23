# Audit log

For operators who need a durable record of who did what on a Controller. The
audit log subscribes to an allow-list of `(source, name)` pairs on the
observability bus, appends one JSON Lines record per accepted signal under a
configured directory, and `flz audit` reads it back. Off by default (it changes
the host's disk-space behaviour): `:audit_log, :enabled: true` or `FLEZHA_AUDIT_ENABLED=true`.

## Allow-list

The sink subscribes to the bus with overflow policy `:block`, so an audit emit
is never silently dropped; the `(source, name | :any)` filter is applied inside
the sink. Compiled-in defaults:

```elixir
[
  {:observability_service, :any},    # service-API audit events
  {:flezha_shutdown, :any},          # graceful-shutdown phases
  {:flezha_plugin_installer, :any},  # plugin install / load activity
  {:audit_log, :any},                # audit-of-audit: reads, rotations, sweeps
  {:platform_authz, :any},           # auth mutations and authorization denials
  {:flezha_dlq, :operator_drop},     # flz dlq drop
  {:flezha_dlq, :operator_replay}    # flz dlq replay
]
```

The list is deliberately narrow so an unwary operator does not fill the disk;
extend it with `:extra_subscriptions`, e.g. `[{:observability_export, :otlp_batch_dropped}]`.

## On-disk layout

Under `:dir` (default `app_data/audit`):

```text
app_data/audit/
├── INDEX                              # binary catalog of rotated files
├── audit-current.jsonl                # active file
├── audit-20260518T071455Z-1.jsonl     # rotated; -N disambiguates the same second
└── audit-20260518T130000Z-1.jsonl.gz  # gzipped out-of-band by the operator
```

`INDEX` is `:erlang.term_to_binary/1` of sorted
`{filename, first_ts_us, last_ts_us, record_count, size_bytes, sha256}` entries,
rewritten atomically (`INDEX.tmp` + rename) on every rotation and deleting sweep,
and rebuilt at boot from every `audit-*.jsonl[.gz]` when missing or undecodable.

Files inherit the process umask (the writer sets no mode). Posture: `UMask=0027`
(systemd: `StateDirectory=flezha`, `StateDirectoryMode=0750`), directory owned
by the running user, a dedicated group for log readers; on Kubernetes
`FLEZHA_AUDIT_DIR=/var/lib/flezha/audit` — the default path is on the read-only rootfs.

## Record schema

One UTF-8 JSON object per line, terminated by `\n`. Field order is stable:
`ts_us, ts_iso, source, name, measurements, metadata, caller, namespace`.

```json
{"ts_us": -576460748, "ts_iso": "2026-05-18T07:14:55.451Z",
 "source": "platform_authz", "name": "token_issued", "measurements": {"count": 1},
 "metadata": {"principal": "bob", "namespace": "team-orders", "role": "operator",
              "created_by_principal": "alice", "token_hash_prefix": "4f2a9b1c2e7d"},
 "caller": {"source": "cli", "principal": "alice", "request_id": null},
 "namespace": "team-orders"}
```

`caller` is denormalised from `metadata.caller_source`, `metadata.principal`
and `metadata.request_id`, so `jq 'select(.caller.source == "cli")'` needs no
knowledge of the metadata convention. `caller.principal` is the verified
principal while authorization is enabled (`null` when the signal carried none)
and the caller source (`"cli"`, `"internal"`, ...) while it is disabled.
`namespace` is `"_system"` when the signal has none; the reader treats an absent
field the same way. `ts_us` is the bus's monotonic timestamp, `ts_iso` its
wall-clock translation; the `--since`/`--until` filters take `ts_us`.

## Fsync modes

| Mode | Behaviour | Use |
| --- | --- | --- |
| `:per_record` (default) | `:file.sync/1` after every write, before the writer replies; producer latency is one disk sync (1–10 ms on local SSD) | the default audit volume, a handful of events per minute |
| `:batched` | a timer fsyncs at most `:fsync_interval_ms` (default `100`) after the first unsynced write; an unclean crash loses at most that window | high volume with an off-host backstop such as the OTLP exporter |

## Rotation

The active file rotates when `size_bytes >= :max_file_bytes` (default 64 MiB)
or its age reaches `:max_file_age_seconds` (default 1 day), checked every
`:rotation_check_interval_ms` (default `60_000`); the timer never rotates an
empty file. It is renamed `audit-YYYYMMDDTHHMMSSZ-N.jsonl` and `:audit_rotated`
is emitted on `:audit_log` (measurements `size_bytes`, `record_count`; metadata
`rotated_to`, `sha256`). `flz audit rotate` forces one regardless of size or age.

## Retention

Every `:retention_check_interval_ms` (default 1 hour) the sweep deletes rotated
files older than `:retention_seconds` (default 30 days), then, while the
directory still exceeds `:max_total_bytes` (default 4 GiB), the oldest
remaining rotated files. The active file is never deleted by retention. After a
deletion `INDEX` is rewritten and `:audit_retention_sweep` is emitted with
`deleted_count`, `deleted_bytes`, `remaining_count`, `total_bytes`.

## CLI

```text
flz audit list [--json]                    catalog of rotated files
flz audit read <FILE> [--source S] [--name N] [--since TS_US] [--until TS_US]
               [--limit N] [--json]        one file (audit-current.jsonl = active), limit 1000
flz audit tail [--json] [--duration-ms N]  live records, default 30000 ms
flz audit stats [--json]                   current file, bytes, totals, oldest ts, drop count
flz audit rotate [--json]                  force a rotation of the active file
```

`--json` emits NDJSON for `read` and `tail`. When the subsystem is disabled
every verb exits non-zero with `error: audit log is disabled; set
FLEZHA_AUDIT_ENABLED=true or :audit_log, :enabled: true in config`.

`Flezha.Service.AuditLog` authorizes every call and filters `read` results to
the caller's namespace — `admin@_system` and internal callers see everything.
The `flz audit` verbs pass no `--token`, so while authorization is enforced they
refuse with `principal_not_bound`; read the files directly in that posture.

## Configuration knobs

```elixir
config :flezha, :audit_log,
  enabled: false,                      # FLEZHA_AUDIT_ENABLED=1|true|yes|on
  dir: "app_data/audit",               # FLEZHA_AUDIT_DIR
  max_file_bytes: 67_108_864,          # 64 MiB
  max_file_age_seconds: 86_400,        # 1 day
  retention_seconds: 30 * 86_400,      # FLEZHA_AUDIT_RETENTION_DAYS (whole days)
  max_total_bytes: 4_294_967_296,      # 4 GiB
  fsync_mode: :per_record,             # or :batched
  fsync_interval_ms: 100,
  rotation_check_interval_ms: 60_000,
  retention_check_interval_ms: 3_600_000,
  extra_subscriptions: []
```

## Shutdown

`Flezha.Shutdown.Coordinator` calls `Flezha.Observability.AuditLog.Writer.close/1`
in its finalise phase (5 s cap), before the `FsyncBatcher` flush: pending writes
drain, the active file is fsynced and closed. A later signal is discarded and
counted (`:post_close_dropped`, the `drop count` in `flz audit stats`). So
`flz controller stop` leaves every record emitted before the stop durable; size
`:graceful_shutdown_timeout_ms` (default `60_000`) for your storage's fsync time.

## Limits

- No remote shipping: run the OTLP exporter alongside; it batches every signal
  on the bus, so the allow-list needs no replication.
- No tamper-evident chain across files or subsystems: the per-file SHA-256 in
  `INDEX` detects casual edits, but anyone with write access to the disk can
  rewrite the audit files and the auth stores alike.
- No compression by the writer: gzip rotated files out-of-band; the reader
  streams `.jsonl.gz` through `:zlib`.

## See also

- [authn_authz.md](authn_authz.md) — the `:platform_authz` events and the token the CLI carries
- [observability.md](observability.md) — the bus the sink subscribes to and the OTLP exporter
- [graceful_shutdown.md](graceful_shutdown.md) — the finalise phase

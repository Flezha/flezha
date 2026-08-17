# Durable audit log (`platform-audit-log`)

- **Modules**: `lib/flezha/observability/audit_log/` — `Codec`,
  `Index`, `Reader`, `Writer`, `Rotation`, `Retention`, `Sink`,
  `Supervisor`. Service surface at
  `lib/flezha/service/audit_log.ex`.
- **Supervision tree**: `Flezha.Observability.AuditLog.Supervisor`
  is the LAST child of `Flezha.Observability.Supervisor` (gated on
  `:audit_log, :enabled`, default `false`). Children, in start
  order: `Writer` → `Rotation` → `Retention` → `Sink`. The Sink is
  last so subscriptions go live only after the Writer is ready to
  accept records.
- **Default allow-list**: `:observability_service`, `:flezha_shutdown`,
  `:flezha_plugin_installer`, `:audit_log` (audit-of-audit). Extend
  via `:audit_log, :extra_subscriptions`.
- **On-disk layout**: `app_data/audit/INDEX` (binary catalog),
  `app_data/audit/audit-current.jsonl` (active file),
  `app_data/audit/audit-YYYYMMDDTHHMMSSZ-N.jsonl[.gz]` (rotated).
  `INDEX` is rebuildable on boot if missing or corrupt.
- **JSON Lines record**: stable field order `ts_us, ts_iso, source,
  name, measurements, metadata, caller`; `caller` is denormalised
  from `metadata.caller_source / :principal / :request_id`.
- **Fsync**: `:per_record` (default, ~1-10 ms/record on local SSD)
  or `:batched` (every `:fsync_interval_ms`, bounded loss window).
  Overflow policy `:block` so audit emits are never silently
  dropped.
- **Rotation**: size (`:max_file_bytes`, 64 MiB) OR age
  (`:max_file_age_seconds`, 1 day), checked every
  `:rotation_check_interval_ms` (60 s) and on every write.
- **Retention**: age (`:retention_seconds`, 30 d) AND total size
  (`:max_total_bytes`, 4 GiB) — more restrictive wins. Active file
  is never deleted by retention.
- **Service API**: `Flezha.Service.AuditLog` —
  `list_files/1`, `read/2`, `tail/2`, `unsubscribe/2`, `stats/1`,
  `rotate/1`. Every public call emits an audit-of-audit event on
  `:audit_log` source. Master flag check returns
  `{:error, :observability_disabled}`; subsystem flag check returns
  `{:error, :audit_log_disabled}`.
- **CLI**: `flz audit list`, `flz audit read <file>`,
  `flz audit tail`, `flz audit stats`, `flz audit rotate` —
  implemented in `cli/src/audit.rs` and
  `Flezha.Service.CliManager.audit_log_render_*`.
- **Shutdown integration**: `Flezha.Shutdown.Coordinator.do_finalise`
  calls `Writer.close/0` BEFORE the `FsyncBatcher` flush, workflow
  by a 5 s timeout. Post-close signals are dropped with a per-sink
  counter increment.
- **Runtime config knobs**: `:audit_log` map (see
  `docs/audit_log.md` for the full list). Env-var overrides:
  `FLEZHA_AUDIT_ENABLED`, `FLEZHA_AUDIT_DIR`,
  `FLEZHA_AUDIT_RETENTION_DAYS`.
- **Detailed notes**: `docs/audit_log.md`.


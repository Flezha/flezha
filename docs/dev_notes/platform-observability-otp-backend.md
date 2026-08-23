# OTP backend sources (`platform-observability-otp-backend`)

- **Modules**: `lib/flezha/observability/otp_backend/` —
  `Supervisor`, `Source` (the shared `use` macro), `BeamVmSource`,
  `HostSource`, `ProcParser`, `ProcessHubSource`, `TransportSource`,
  `BoundedMailboxSource`.
- **Supervision tree**: `Flezha.Observability.OtpBackend.Supervisor`
  is inserted in `Flezha.Observability.Supervisor` AFTER
  `DefaultSinks` and BEFORE `Export.Supervisor`. `:one_for_one` so a
  per-source crash leaves peers running.
- **Shipped sources** — `:beam_vm` (memory / schedulers / GC /
  process+port counts, 1s default), `:host` (Linux `/proc` RSS /
  CPU / FD / loadavg, 5s default; one-shot `:host_source_degraded`
  event on non-Linux), `:processhub` (per-hub registered processes +
  pending recoveries, 1s), `:flezha_transport` (per-transport
  pending-stream + driver-loop/queue/write-buffer metrics and global
  channel/credit aggregates via `Flezha.Transport.Server.get_stats/1`,
  1s), `:flezha_bounded_mailbox` (registered, one-shot
  `:bounded_mailbox_unavailable` event until the Rust→Elixir mailbox
  surface lands).
- **Runtime config knobs**: `:observability, :otp_backend_enabled`
  (default `true` in prod, `false` in test); `:observability,
  :otp_backend, :poll_intervals` map per-source override.
- **CLI**: `flz observe sources`, `flz observe sinks`,
  `flz observe tail <source>`, `flz observe enable <source>`,
  `flz observe disable <source>` — implemented in
  `cli/src/observe.rs` and
  `Flezha.Service.CliManager.observability_render_*`.
- **Detailed notes**: `docs/operations/observability.md`.


# Postgres sink plugin (`effectively-once-output-postgres-sink`)

- **Modules**: `lib/flezha/plugin/provider/postgres.ex`
  (`Flezha.Plugin.Provider.Postgres`, `:io_binding`, registered as
  `operative_postgres`) + `postgres/{sink_sup,sink_worker}.ex`. `SinkSup`
  and Kafka's `ProducerSup` are thin wrappers over the shared
  `Flezha.Plugin.Provider.WorkerSup` core (one Registry + DynamicSupervisor
  instance per provider family; ensure/reap/stop semantics, reuse policy
  injectable — Kafka passes `ProducerWorker.reusable?/1`). A re-prepared
  plugin reuses its live worker so buffered records survive Overwatch
  restarts. New top-level dep `postgrex ~> 0.22` (active only when the
  plugin is configured, like `:kafka_ex`).
- **Semantics**: per flush batch ONE `Postgrex.transaction` — bulk upsert
  into `flezha_records` (key = `flz.key`, fallback `<entity>/<seq>` for
  keyless tier outputs, unique `_anon/N` for unstamped records) + one
  `flezha_sink_progress` watermark upsert per entity. Both guarded by
  `(epoch, string_to_array(seq)::bigint[])` row comparison so re-drives
  and stale epochs are byte-exact no-ops. Same-key records within a batch
  are collapsed to the newest identity first (`ON CONFLICT` double-touch).
- **Failure posture**: unreachable DB → retry with exponential backoff
  (`retry_backoff_ms` 250 → `retry_max_backoff_ms` 5000), records kept;
  enqueue callers park past `queue_max` (10_000) — delay, never loss.
  `auto_create` default false: missing tables fail `health/1` with
  `{:missing_tables, [...]}` and no DDL runs. `health/1` also reports
  `{:commit_stalled, reason}` when pending + failing past
  `unhealthy_after_ms` (30 s) — feeds the plugin-health → convergence path.
- **Credential hygiene**: password rides a closure into DBConnection's
  `:configure` (never in worker state/child args); `init_provider` returns
  opts with `:password` redacted; failure logs carry `host:port/db` only.
- **Tests**: `test/plugin/provider/postgres_sink_test.exs`
  (`POSTGRES_INTEGRATION=1 mix test --include postgres_integration`, needs
  `docker compose -f docker/dev/docker-compose.yml up -d postgres-sink` —
  service is named `postgres-sink` on host port **5433** because the
  flezha_market dev stack shares compose project "dev" and owns `postgres`
  on 5432); e2e crash matrix `test/system/effectively_once_postgres_e2e_test.exs`
  (also needs `--include system`). Shared kill/rebind harness for both
  tier e2e suites: `test/helpers/tier_e2e_harness.ex`.
- **Docs**: `docs/resilience/data_durability.md` "Delivery semantics".


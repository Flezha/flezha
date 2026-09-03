# `:system` is excluded by convention — those tests boot extra OS processes;
# `mix test --include system` runs them against the SDK runtime named by
# `FLEZHA_E2E_RUNTIME` (default `rust`), and `--include sdk_smoke` runs the
# smoke tier alone. What that runtime carries comes from its manifest
# (`test/apps/<runtime>_runner/conformance.json`) — see
# `docs/sdk/conformance.md`.
# `:testnet` is excluded because the WS testnet daemon must be running on
# port 4500 — run `npm --prefix test/apps/testnet start` first, then
# `mix test --include testnet`.
# `:memory_drift` is the long-running load harness in
# `test/load/memory_drift_test.exs`; it also requires
# `MIX_RUN_MEMORY_DRIFT=1`.
# `:s3_integration` needs the `minio` compose service (localhost:9100) and
# `S3_INTEGRATION=1`.
# `:interop` is the one scenario that places TWO SDK runtimes in one run: the
# tiers are one-runtime-per-run by contract, and its namespace must not be alive
# while another tier's cleanup tick walks the fleet. An ExUnit include BEATS an
# exclude, so `--include system` would run it however it is excluded here — the
# module skips itself unless `--include interop` is passed too.
# `:multi_node` boots a peer BEAM and stops it mid-suite, which disturbs the
# shared cluster state the boot-time runners and the concurrently-running
# namespace tests depend on. Run with `mix test --include multi_node`.
alias FlezhaTest.Helper.NodeTestHelper

# What the selected runtime carries is not a filter: `--include system` is how
# this suite runs, and an ExUnit include overrides an exclude. A deferred
# capability is skipped by name in `FlezhaTest.SystemCase`, and a transport the
# manifest omits is never enumerated into a test.
runtime = NodeTestHelper.runtime()

if Enum.any?([:system, :sdk_smoke], &(&1 in (ExUnit.configuration()[:include] || []))) do
  IO.puts(
    "[conformance] runtime=#{runtime} manifest=#{NodeTestHelper.manifest_path(runtime)} " <>
      "deferred=#{inspect(NodeTestHelper.deferred(runtime))} " <>
      "transports=#{inspect(NodeTestHelper.transports(runtime))}"
  )
end

ExUnit.start(
  exclude: [
    :system,
    :interop,
    :kafka_integration,
    :postgres_integration,
    :s3_integration,
    :testnet,
    :memory_drift,
    :credit_soak,
    :multi_node
  ]
)

# ProcessHub's `LoggerService.error/3` logs every `StartChildrenRequest`
# failure at error level — useful in production, but multiple tests
# intentionally drive failure paths (cross-node module-not-loaded during
# peer redistribution after the prior test's operatives leak into the
# global registry, `source_operative_not_found` in Sibling integration
# tests). Cap that single module's log level to `:none` in test env so
# the console stays focused on actual ExUnit assertions.
:ok = :logger.set_module_level(ProcessHub.Service.LoggerService, :none)

Mimic.copy(KafkaEx)
Mimic.copy(Flezha.Service.OperativeManager)
Mimic.copy(Flezha.Service.PluginManager)
Mimic.copy(Flezha.Service.HubManager)
Mimic.copy(Flezha.Runner.StreamChannel)
Mimic.copy(Flezha.Runner.StreamPool)
Mimic.copy(Flezha.Runner.ControlChannel)
Mimic.copy(Flezha.AuthZ.Quotas)
Mimic.copy(Flezha.Plugin.Provider.Kafka.WorkerRegistry)
Mimic.copy(Flezha.State.FsyncBatcher)
Mimic.copy(Flezha.Transport.Server)
Mimic.copy(ProcessHub)
Mimic.copy(ProcessHub.Future)
Mimic.copy(ProcessHub.Service.ProcessRegistry)
Mimic.copy(ProcessHub.Service.Migration)
Mimic.copy(Flezha.Cluster.Directory)
Mimic.copy(Flezha.State.Supervisor)
Mimic.copy(Flezha.State.WriterRegistry)
Mimic.copy(Flezha.State.WalWriter)
Mimic.copy(Flezha.Runner.ControlUnit)
Mimic.copy(Flezha.Runner.FleetManager)
Mimic.copy(Flezha.State.Watchdog)
Mimic.copy(Flezha.State.Retention)
Mimic.copy(Flezha.State.Paths)
Mimic.copy(Flezha.State.ProcInbox)
Mimic.copy(Flezha.Schedule.Hub)
Mimic.copy(Flezha.Dlq.Store)
Mimic.copy(Flezha.State.InputLog)
Mimic.copy(Flezha.State.WalTransfer)
Mimic.copy(Flezha.State.Commands)
Mimic.copy(Flezha.Operative.PartitionGroup)
Mimic.copy(Flezha.Operative.ClassRegistry)

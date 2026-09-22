# Which tags plain `mix test` skips is `FlezhaTest.Tiers`, one row per tier: the
# tag, the workflow that runs it or the reason a person has to, what a host
# needs first, and the anchor in `docs/sdk/conformance.md` that describes it.
# That table is the one place a tier is declared; this file only reads it.
#
# One rule the table cannot express and every reader needs: an ExUnit include
# BEATS an exclude, so `--include system` runs an excluded tier too. A tier that
# must not ride another one's include says so with `self_skip`, and its module
# skips itself unless its own tag is included.
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

ExUnit.start(exclude: FlezhaTest.Tiers.excluded())

# The run's own state root and socket directory are derived from its token and
# nothing else removes them. Only a path this run derived and still uses goes: a
# path a variable named is not the suite's, nor is anything under `app_data/`.
# They go when the VM exits, not after the suite: `--repeat-until-failure` runs
# the suite again against the same live controller.
run_dirs =
  for {key, path} <-
        Map.take(Flezha.Config.Environment.per_run(Flezha.Config.get(:run_token)), [
          :state_root,
          :socket_files_path
        ]),
      Application.get_env(:flezha, key) == path,
      not String.starts_with?(path, Path.expand("app_data") <> "/"),
      do: path

System.at_exit(fn _status -> Enum.each(run_dirs, &File.rm_rf/1) end)

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
Mimic.copy(Flezha.Service.OperativeManager.Placement)
Mimic.copy(Flezha.Service.PluginManager)
Mimic.copy(Flezha.Service.HubManager)
Mimic.copy(Flezha.Runner.StreamChannel)
Mimic.copy(Flezha.Runner.StreamPool)
Mimic.copy(Flezha.Runner.ControlChannel)
Mimic.copy(Flezha.AuthZ.Quotas)
Mimic.copy(Flezha.Plugin.Provider.Kafka.WorkerRegistry)
Mimic.copy(Flezha.Http.S3)
Mimic.copy(Flezha.State.FsyncBatcher)
Mimic.copy(Flezha.Transport.Server)
Mimic.copy(ProcessHub)
Mimic.copy(ProcessHub.Future)
Mimic.copy(ProcessHub.Service.ProcessRegistry)
Mimic.copy(ProcessHub.Service.Migration)
Mimic.copy(Flezha.Cluster.Directory)
Mimic.copy(Flezha.Cluster.Orchestrator)
Mimic.copy(Flezha.State.Supervisor)
Mimic.copy(Flezha.State.WriterRegistry)
Mimic.copy(Flezha.State.WalWriter)
Mimic.copy(Flezha.Runner.ControlUnit)
Mimic.copy(Flezha.Runner.FleetManager)
Mimic.copy(Flezha.AuthZ.RunnerProof)
Mimic.copy(Flezha.State.Watchdog)
Mimic.copy(Flezha.State.Retention)
Mimic.copy(Flezha.State.Paths)
Mimic.copy(Flezha.Schedule.Hub)
Mimic.copy(Flezha.Dlq.Store)
Mimic.copy(Flezha.State.InputLog)
Mimic.copy(Flezha.State.WalTransfer)
Mimic.copy(Flezha.State.Commands)
Mimic.copy(Flezha.Operative.PartitionGroup)
Mimic.copy(Flezha.Operative.ClassRegistry)
Mimic.copy(Flezha.Convergence.Heal)
Mimic.copy(Flezha.Convergence.Intent)
Mimic.copy(Flezha.Convergence.Probe)
Mimic.copy(Flezha.Convergence.RecoveryGate)

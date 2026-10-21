# Which tags plain `mix test` skips is `FlezhaTest.Tiers`, one row per tier: the
# tag, the steps relay that runs it or the reason a person has to, what a host
# needs first, and the anchor in `docs/internal/sdk/conformance.md` that describes it.
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

# A reason that reaches a caller with no catalogue entry is a defect the run
# must name, wherever it happened: a handler records it in the emitting process,
# and the suite fails once it has finished. A test that provokes one on purpose
# says so in its own process first (`Process.put(:flezha_uncatalogued_expected, true)`).
:ets.new(:flezha_uncatalogued, [:named_table, :public, :bag])

:telemetry.attach(
  "flezha-test-uncatalogued",
  [:flezha, :error, :uncatalogued],
  fn _event, _measurements, metadata, _config ->
    unless Process.get(:flezha_uncatalogued_expected),
      do: :ets.insert(:flezha_uncatalogued, {metadata.site, metadata.reason})
  end,
  nil
)

ExUnit.after_suite(fn _result ->
  case :ets.tab2list(:flezha_uncatalogued) do
    [] ->
      :ok

    missed ->
      IO.puts(:stderr, "\nuncatalogued errors reached a caller (flz_transport/errors.json):")

      for {site, reason} <- Enum.uniq(missed),
          do: IO.puts(:stderr, "  #{site}: #{inspect(reason)}")

      System.at_exit(fn _status -> exit({:shutdown, 1}) end)
  end
end)

# The run's own state root and socket directory are derived from its token and
# nothing else removes them. Only a path this run derived and still uses goes: a
# path a variable named is not the suite's, nor is anything under `storage/app/`.
# They go when the VM exits, not after the suite: `--repeat-until-failure` runs
# the suite again against the same live controller.
run_dirs =
  for {key, path} <-
        Map.take(Flezha.Config.Environment.per_run(Flezha.Config.get(:run_token)), [
          :state_root,
          :socket_files_path
        ]),
      Application.get_env(:flezha, key) == path,
      not String.starts_with?(path, Path.expand("storage") <> "/"),
      do: path

System.at_exit(fn _status -> Enum.each(run_dirs, &File.rm_rf/1) end)

# ProcessHub's `LoggerService.error/3` logs every `StartChildrenRequest`
# failure at error level — useful in production, but multiple tests
# intentionally drive failure paths (cross-node module-not-loaded during
# peer redistribution after the prior test's relays leak into the
# global registry, `source_relay_not_found` in Sibling integration
# tests). Cap that single module's log level to `:none` in test env so
# the console stays focused on actual ExUnit assertions.
:ok = :logger.set_module_level(ProcessHub.Service.LoggerService, :none)

Mimic.copy(Flezha.Service.RelayApi)
Mimic.copy(Flezha.Service.RelayManager)
Mimic.copy(Flezha.Service.RelayManager.Placement)
Mimic.copy(Flezha.Service.RelayManager.Teardown)
Mimic.copy(Flezha.Service.RelayManager.Init)
Mimic.copy(Flezha.Relay.Overwatch)
Mimic.copy(Flezha.Service.PluginManager)
Mimic.copy(Flezha.Service.HubManager)
Mimic.copy(Flezha.Service.HubManager.Lifecycle)
Mimic.copy(Flezha.State.Recovery)
Mimic.copy(Flezha.Runner.StreamChannel)
Mimic.copy(Flezha.Runner.StreamPool)
Mimic.copy(Flezha.Runner.ControlChannel)
Mimic.copy(Flezha.AuthZ.Quotas)
Mimic.copy(Flezha.Http.S3)
Mimic.copy(Flezha.State.FsyncBatcher)
Mimic.copy(Flezha.Transport.Server)
Mimic.copy(Flezha.Observability.AuditLog.Writer)
Mimic.copy(ProcessHub)
Mimic.copy(ProcessHub.Future)
Mimic.copy(ProcessHub.Service.ProcessRegistry)
Mimic.copy(ProcessHub.Service.Migration)
Mimic.copy(Flezha.Cluster.Directory)
Mimic.copy(Flezha.Cluster.GroupedHashing)
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
Mimic.copy(Flezha.State.Completion)
Mimic.copy(Flezha.State.InputLog)
Mimic.copy(Flezha.State.WalTransfer)
Mimic.copy(Flezha.State.Adoption)
Mimic.copy(Flezha.State.DurabilityTier.Artifact)
Mimic.copy(Flezha.State.TaskJournal)
Mimic.copy(Flezha.State.TaskJournal.Turns)
Mimic.copy(Flezha.State.Commands)
Mimic.copy(Flezha.Relay.PartitionGroup)
Mimic.copy(Flezha.Relay.ClassRegistry)
Mimic.copy(Flezha.Relay.Plugins)
Mimic.copy(Flezha.Relay.Replay)
Mimic.copy(Flezha.Service.Validator)
Mimic.copy(Flezha.Process.CallDelivery)
Mimic.copy(Flezha.Plugin.Extension)
Mimic.copy(Flezha.Convergence.Heal)
Mimic.copy(Flezha.Convergence.Intent)
Mimic.copy(Flezha.Convergence.Probe)
Mimic.copy(Flezha.Convergence.RecoveryGate)
Mimic.copy(Flezha.Shutdown.Coordinator)

# Registered after every `Mimic.copy/1` above, so it runs before their own
# after-suite resets: a reset purges a copied module's original code, which kills
# any process still inside it, and an input log's sync companion is one. A log a
# test left open is closed first, as a shutdown would. A repeated run keeps the
# copies, and its logs.
ExUnit.after_suite(fn _result ->
  if ExUnit.configuration()[:repeat_until_failure] in [0, nil] do
    for {class, :input_log} <- Flezha.State.Supervisor.registered_ids(),
        do: Flezha.State.TestHelpers.close_input_log(class)
  end
end)

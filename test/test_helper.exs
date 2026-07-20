# `:java_sdk` is excluded from default `mix test` because it requires the
# Java runner fat jar to be on disk. Run those tests via the
# `mix test.java_sdk` alias, which builds the jar first.
# `:system` is excluded by convention — those tests boot extra OS processes.
# `:testnet` is excluded because the WS testnet daemon must be running on
# port 4500 — run `npm --prefix test/apps/testnet start` first, then
# `mix test --include testnet`.
# `:memory_drift` is the long-running load harness in
# `test/load/memory_drift_test.exs`; it also requires
# `MIX_RUN_MEMORY_DRIFT=1`.
ExUnit.start(
  exclude: [
    :system,
    :java_sdk,
    :kafka_integration,
    :postgres_integration,
    :testnet,
    :memory_drift,
    :credit_soak
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
Mimic.copy(Flezha.Runner.ControlChannel)
Mimic.copy(Flezha.AuthZ.Quotas)
Mimic.copy(Flezha.Plugin.Provider.Kafka.WorkerRegistry)
Mimic.copy(Flezha.State.FsyncBatcher)
Mimic.copy(Flezha.Transport.Server)
Mimic.copy(ProcessHub)
Mimic.copy(ProcessHub.Service.ProcessRegistry)
Mimic.copy(ProcessHub.Service.Migration)
Mimic.copy(Flezha.State.Supervisor)
Mimic.copy(Flezha.State.WalWriter)
Mimic.copy(Flezha.Runner.ControlUnit)
Mimic.copy(Flezha.Runner.FleetManager)
Mimic.copy(Flezha.State.Watchdog)
Mimic.copy(Flezha.State.Retention)
Mimic.copy(Flezha.State.Registry)
Mimic.copy(Flezha.State.OpInbox)

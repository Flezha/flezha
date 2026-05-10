# `:java_sdk` is excluded from default `mix test` because it requires the
# Java executor fat jar to be on disk. Run those tests via the
# `mix test.java_sdk` alias, which builds the jar first.
# `:system` is excluded by convention — those tests boot extra OS processes.
# `:testnet` is excluded because the WS testnet daemon must be running on
# port 4500 — run `npm --prefix test/apps/testnet start` first, then
# `mix test --include testnet`.
ExUnit.start(exclude: [:system, :java_sdk, :kafka_integration, :testnet])

# ProcessHub's `LoggerService.error/3` logs every `StartChildrenRequest`
# failure at error level — useful in production, but multiple tests
# intentionally drive failure paths (cross-node module-not-loaded during
# peer redistribution after the prior test's relays leak into the
# global registry, `source_relay_not_found` in Sibling integration
# tests). Cap that single module's log level to `:none` in test env so
# the console stays focused on actual ExUnit assertions.
:ok = :logger.set_module_level(ProcessHub.Service.LoggerService, :none)

Mimic.copy(KafkaEx)
Mimic.copy(Flezha.Service.RelayManager)
Mimic.copy(Flezha.Service.PluginManager)
Mimic.copy(Flezha.Service.HubManager)
Mimic.copy(Flezha.Plugin.Provider.Kafka.WorkerRegistry)
Mimic.copy(Flezha.State.FsyncBatcher)
Mimic.copy(Flezha.Transport.Server)
Mimic.copy(ProcessHub)
Mimic.copy(ProcessHub.Service.ProcessRegistry)

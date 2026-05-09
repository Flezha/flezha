defmodule Flezha do
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.ExecutorManager
  alias Flezha.Service.PluginManager

  @moduledoc """
  Flezha is a compute engine platform with an Elixir controller and SDK runtimes
  in Rust, Java, and others. It supports clustered relays with hot-swap
  migration and a single auto-restore-on-boot mechanism — ProcessHub's
  durable registry persists relay specs, and per-Partition WAL/snapshot
  files persist progress (see `docs/auto_restore_on_boot.md` and
  `docs/controller_state_persistence.md`).
  """

  defdelegate start_relay(namespace, relay), to: RelayManager

  defdelegate stop_relay(namespace, relay), to: RelayManager

  defdelegate list_namespaces(), to: HubManager, as: :get_running_namespaces

  defdelegate list_executors(namespace),
    to: ExecutorManager,
    as: :get_global_executors

  defdelegate list_relays(namespace), to: RelayManager, as: :get_relay_ids

  defdelegate list_factories(namespace), to: Flezha.Executor.FleetManager

  defdelegate enable_plugin(plugin_id, plugin), to: PluginManager

  defdelegate disable_plugin(plugin_id), to: PluginManager

  defdelegate partition_state(namespace, relay),
    to: RelayManager,
    as: :get_partition_state
end

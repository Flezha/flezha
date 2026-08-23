defmodule Flezha do
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager

  @moduledoc """
  Flezha is a compute engine platform with an Elixir controller and SDK runtimes
  in Rust, Java, and others. It supports clustered operatives with hot-swap
  migration and a single auto-restore-on-boot mechanism — ProcessHub's
  durable registry persists operative specs, and per-Entity WAL/snapshot
  files persist progress (see `docs/operations/auto_restore_on_boot.md` and
  `docs/internals/state_persistence.md`).
  """

  defdelegate start_operative(namespace, operative), to: OperativeManager

  defdelegate stop_operative(namespace, operative), to: OperativeManager

  defdelegate list_namespaces(), to: HubManager, as: :get_running_namespaces

  defdelegate list_runners(namespace),
    to: Flezha.Runner.FleetManager,
    as: :global_runners

  defdelegate list_operatives(namespace), to: OperativeManager, as: :get_operative_ids

  defdelegate list_factories(namespace), to: Flezha.Runner.FleetManager

  defdelegate enable_plugin(plugin_id, plugin), to: PluginManager

  defdelegate disable_plugin(plugin_id), to: PluginManager

  defdelegate entity_state(namespace, operative),
    to: OperativeManager,
    as: :get_entity_state
end

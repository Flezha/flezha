defmodule Flezha do
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager

  @moduledoc """
  Flezha is a compute engine platform with an Elixir controller and SDK runtimes
  in Rust, Java, and others. It supports clustered operators with hot-swap
  migration and a single auto-restore-on-boot mechanism — ProcessHub's
  durable registry persists operator specs, and per-Cell WAL/snapshot
  files persist progress (see `docs/operations/auto_restore_on_boot.md` and
  `docs/internals/state_persistence.md`).
  """

  defdelegate start_operator(namespace, operator), to: OperatorManager

  defdelegate stop_operator(namespace, operator), to: OperatorManager

  # Answers every running namespace, because it carries no caller context to
  # narrow by. `Flezha.Service.Operator.list_namespaces/1` is the spelling
  # that takes one and is narrowed to the caller; retiring this delegate is a
  # published-surface retirement and lands with the consumer that calls it.
  defdelegate list_namespaces(), to: HubManager, as: :get_running_namespaces

  defdelegate list_runners(namespace),
    to: Flezha.Runner.FleetManager,
    as: :global_runners

  defdelegate list_operators(namespace), to: OperatorManager, as: :get_operator_ids

  defdelegate list_factories(namespace), to: Flezha.Runner.FleetManager

  defdelegate enable_plugin(plugin_id, plugin), to: PluginManager

  defdelegate disable_plugin(plugin_id), to: PluginManager, as: :disable_plugin_group

  defdelegate cell_state(namespace, operator),
    to: OperatorManager,
    as: :get_cell_state
end

defmodule Flezha do
  alias Flezha.Service.HubManager
  alias Flezha.Service.ComputationManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager

  @moduledoc """
  Documentation for `Flezha`.
  """

  defdelegate start_computation(namespace, computation), to: ComputationManager

  defdelegate stop_computation(namespace, computation), to: ComputationManager

  defdelegate list_namespaces(), to: HubManager, as: :get_running_namespaces

  defdelegate list_operators(namespace), to: OperatorManager, as: :get_global_operators

  defdelegate list_computations(namespace), to: ComputationManager, as: :get_computation_ids

  defdelegate enable_plugin(plugin_id, plugin), to: PluginManager

  defdelegate disable_plugin(plugin_id), to: PluginManager

  defdelegate computation_state(namespace, computation),
    to: ComputationManager,
    as: :get_computation_state
end

defmodule Flezha do
  alias Flezha.Service.HubManager
  alias Flezha.Service.ComputationManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager

  @moduledoc """
  Documentation for `Flezha`.
  """

  defdelegate start_computation(group_id, computation), to: ComputationManager

  defdelegate stop_computation(group_id, computation), to: ComputationManager

  defdelegate list_groups(), to: HubManager, as: :get_running_group_ids

  defdelegate list_operators(group_id), to: OperatorManager, as: :get_global_operators

  defdelegate list_computations(group_id), to: ComputationManager, as: :get_computation_ids

  defdelegate enable_plugin(plugin_settings), to: PluginManager

  defdelegate disable_plugin(plugin_settings), to: PluginManager

  defdelegate computation_state(group_id, computation),
    to: ComputationManager,
    as: :get_computation_state
end

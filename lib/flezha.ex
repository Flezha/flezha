defmodule Flezha do
  alias Flezha.Service.ComputationManager
  alias Flezha.Service.PluginManager

  @moduledoc """
  Documentation for `Flezha`.
  """

  defdelegate start_computation(group_id, computation), to: ComputationManager

  defdelegate stop_computation(group_id, computation), to: ComputationManager

  defdelegate enable_plugin(plugin_settings), to: PluginManager

  defdelegate computation_state(group_id, computation),
    to: ComputationManager,
    as: :get_computation_state

  # TODO: Disable plugin
end

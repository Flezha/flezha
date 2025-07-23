defmodule Flezha do
  alias Flezha.Service.ComputationManager
  alias Flezha.Service.PluginManager

  @moduledoc """
  Documentation for `Flezha`.
  """

  defdelegate start_pipeline(group_id, pipeline), to: ComputationManager

  defdelegate stop_pipeline(group_id, pipeline), to: ComputationManager

  defdelegate enable_plugin(plugin_settings), to: PluginManager

  defdelegate computation_state(group_id, pipeline), to: ComputationManager, as: :get_computation_state

  # TODO: Disable plugin
end

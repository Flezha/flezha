defmodule Flezha do
  alias Flezha.Service.PipelineManager
  alias Flezha.Service.PluginManager

  @moduledoc """
  Documentation for `Flezha`.
  """

  defdelegate start_pipeline(group_id, pipeline), to: PipelineManager

  defdelegate stop_pipeline(group_id, pipeline), to: PipelineManager

  defdelegate enable_plugin(plugin_settings), to: PluginManager

  defdelegate pipeline_state(group_id, pipeline), to: PipelineManager, as: :get_pipeline_state

  # TODO: Disable plugin
end

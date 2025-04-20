defmodule Aflecha do
  alias Aflecha.Service.PipelineManager
  alias Aflecha.Service.PluginManager

  @moduledoc """
  Documentation for `Aflecha`.
  """

  defdelegate start_pipeline(group_id, pipeline), to: PipelineManager

  defdelegate stop_pipeline(group_id, pipeline), to: PipelineManager

  defdelegate enable_plugin(plugin_settings), to: PluginManager

  defdelegate pipeline_state(group_id, pipeline), to: PipelineManager, as: :get_pipeline_state

  # TODO: Disable plugin
end

defmodule Aflecha do
  alias Aflecha.Service.PipelineManager
  alias Aflecha.Service.PluginManager

  @moduledoc """
  Documentation for `Aflecha`.
  """

  defdelegate create_pipeline(group_id, pipeline), to: PipelineManager

  defdelegate rem_pipeline(group_id, pipeline), to: PipelineManager

  defdelegate enable_plugin(plugin_settings), to: PluginManager

  # TODO: Disable plugin

  # TODO: get pipeline state
end

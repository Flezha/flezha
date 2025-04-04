defmodule Aflecha do
  alias Aflecha.Service.PipelineManager
  alias Aflecha.IO.IOManager

  @moduledoc """
  Documentation for `Aflecha`.
  """

  defdelegate create_pipeline(group_id, pipeline), to: PipelineManager

  defdelegate rem_pipeline(group_id, pipeline), to: PipelineManager

  # TODO: fix this.
  defdelegate enable_plugin(group_id, pipeline_id, io_struct, io_mode, opts), to: IOManager

  # TODO: Disable plugin

  # TODO: get pipeline state
end

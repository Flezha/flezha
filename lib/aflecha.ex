defmodule Aflecha do
  alias Aflecha.Service.WorkerManager
  alias Aflecha.IO.IOManager

  @moduledoc """
  Documentation for `Aflecha`.
  """

  # TODO:
  # def pipeline_state(group_id, pipeline_id) do
  #   GenServer.call(Controller, {:get_pipeline_state, group_id, pipeline_id})
  # end

  defdelegate start_pipeline(group_id, pipeline), to: WorkerManager

  defdelegate stop_pipeline(group_id, pipeline), to: WorkerManager

  defdelegate add_io(group_id, pipeline_id, io_struct, io_mode, opts), to: IOManager
end

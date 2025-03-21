defmodule Aflecha do
  alias Aflecha.Service.WorkerManager
  alias Aflecha.IO.IOManager

  @moduledoc """
  Documentation for `Aflecha`.
  """

  # TODO:
  # def worker_state(group_id, pipeline_id) do
  #   GenServer.call(Controller, {:get_worker_state, group_id, pipeline_id})
  # end

  defdelegate start_worker(group_id, worker), to: WorkerManager

  defdelegate stop_worker(group_id, worker), to: WorkerManager

  defdelegate add_io(group_id, pipeline_id, io_struct, io_mode, opts), to: IOManager
end

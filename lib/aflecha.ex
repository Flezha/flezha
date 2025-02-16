defmodule Aflecha do
  alias Aflecha.Worker.WorkerManager
  alias Aflecha.IO.IOManager

  @moduledoc """
  Documentation for `Aflecha`.
  """

  # TODO:
  # def worker_state(group_id, worker_id) do
  #   GenServer.call(Controller, {:get_worker_state, group_id, worker_id})
  # end

  defdelegate start_worker(group_id, worker), to: WorkerManager

  defdelegate stop_worker(group_id, worker), to: WorkerManager

  defdelegate add_io(group_id, worker_id, io_struct, io_mode, opts), to: IOManager
end

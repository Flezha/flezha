defmodule Aflecha do
  alias Aflecha.Computation.ComputeManager
  alias Aflecha.IO.IOManager

  @moduledoc """
  Documentation for `Aflecha`.
  """

  # TODO:
  # def computation_state(group_id, computation_id) do
  #   GenServer.call(Controller, {:get_computation_state, group_id, computation_id})
  # end

  defdelegate start_computation(group_id, computation), to: ComputeManager

  defdelegate stop_computation(group_id, computation), to: ComputeManager

  defdelegate add_io(group_id, computation_id, io_struct, io_mode, opts), to: IOManager
end

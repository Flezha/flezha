defmodule Aflecha do
  alias Aflecha.Computation.ComputeManager
  alias Aflecha.IO.IOManager

  @moduledoc """
  Documentation for `Aflecha`.
  """

  # TODO:
  # def computation_state(operator_id, computation_id) do
  #   GenServer.call(Controller, {:get_computation_state, operator_id, computation_id})
  # end

  defdelegate start_computation(operator_id, computation), to: ComputeManager

  defdelegate stop_computation(operator_id, computation), to: ComputeManager

  defdelegate add_io(operator_id, computation_id, io_struct, io_mode, opts), to: IOManager
end

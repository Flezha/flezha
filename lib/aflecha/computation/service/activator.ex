defmodule Aflecha.Computation.Service.Activator do
  @moduledoc """
  Manages the computation start and stop.
  """

  alias Aflecha.Operator.Controller

  @doc """
  Adds a new computation to the operator.
  """
  @spec start_computation(String.t(), Aflecha.Computation.Builder.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def start_computation(operator_id, computation) do
    GenServer.call(Controller, {:start_computation, operator_id, computation})
  end

  def stop_computation(operator_id, computation_id) do
    GenServer.call(Controller, {:rem_computation, operator_id, computation_id})
  end
end

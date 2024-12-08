defmodule Aflecha.Computation.ActivatorService do
  @moduledoc """
  Manages the computation startings and stoppings.
  """

  alias Aflecha.Operator.Controller

  @doc """
  Starts a new computation to the operator.
  """
  @spec start_computation(String.t(), Aflecha.Computation.Builder.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def start_computation(operator_id, computation) do
    GenServer.call(Controller, {:start_computation, operator_id, computation})
  end

  @doc """
  Stops a computation.
  """
  @spec stop_computation(String.t(), String.t()) :: :ok | {:error, String.t()}
  def stop_computation(operator_id, computation_id) do
    GenServer.call(Controller, {:stop_computation, operator_id, computation_id})
  end
end

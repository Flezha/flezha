defmodule Aflecha.Service.RpcCookbook do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Aflecha.Computation.ComputeManager
  alias Aflecha.IO.IOManager

  @doc "Starts a computation."
  @spec computation_start(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def computation_start(operator_id, computation_id, factory_id, variables) do
    computation = %Aflecha.Computation.Builder{
      id: computation_id,
      factory_id: factory_id,
      variables: Aflecha.Service.Payloader.json_to_map(variables)
    }

    ComputeManager.start_computation(operator_id(operator_id), computation)
  end

  @doc "Stops a computation."
  @spec computation_stop(String.t(), String.t()) :: :ok | {:error, String.t()}
  def computation_stop(operator_id, computation_id) do
    ComputeManager.stop_computation(operator_id(operator_id), computation_id)
  end

  @doc "Adds an IO."
  def add_io(operator_id, computation_id, io_params, io_mode, opts) do
    io_struct = Aflecha.Service.Payloader.extract_io_settings(io_params)

    IOManager.add_io(operator_id(operator_id), computation_id, io_struct, io_mode, opts)
  end

  defp operator_id(operator_id) do
    case operator_id do
      "" -> Aflecha.Operator.FleetSupervisor.default_operator()
      _ -> operator_id
    end
  end
end

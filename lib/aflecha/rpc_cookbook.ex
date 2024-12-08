defmodule Aflecha.Service.RpcCookbook do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Aflecha.Computation.Service.Activator

  @doc "Starts a computation."
  @spec computation_start(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def computation_start(operator_id, computation_id, factory_id, variables) do
    computation = %Aflecha.Computation.Builder{
      id: computation_id,
      factory_id: factory_id,
      variables: Aflecha.Service.Payloader.json_to_map(variables)
    }

    Activator.start_computation(operator_id(operator_id), computation)
  end

  @doc "Stops a computation."
  @spec computation_stop(String.t(), String.t()) :: :ok | {:error, String.t()}
  def computation_stop(operator_id, computation_id) do
    Activator.stop_computation(operator_id(operator_id), computation_id)
  end

  defp operator_id(operator_id) do
    case operator_id do
      "" -> Aflecha.Operator.OperatorsSup.default_operator()
      _ -> operator_id
    end
  end

  # TODO: Add IO cli command recipes.
end

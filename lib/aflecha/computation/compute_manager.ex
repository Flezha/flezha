defmodule Aflecha.Computation.ComputeManager do
  @moduledoc """
  Manages the computation startings and stoppings.
  """

  alias Aflecha.Worker.Controller
  alias Aflecha.Service.Identificator
  alias Aflecha.Computation.Builder, as: ComputationBuilder

  @doc """
  Starts a new computation to the operator.
  """
  @spec start_computation(String.t(), Aflecha.Computation.Builder.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def start_computation(group_id, computation) do
    case ComputationBuilder.validate_computation(group_id, computation) do
      {:error, reason} ->
        {:error, reason}

      :ok ->
        # TODO: we should validate the group_id also.
        add_computation(group_id, computation)
    end
  end

  @doc """
  Stops a computation.
  """
  @spec stop_computation(String.t(), String.t()) :: :ok | {:error, String.t()}
  def stop_computation(group_id, computation_id) do
    GenServer.call(Controller, {:stop_computation, group_id, computation_id})
  end

  defp add_computation(group_id, computation) do
    child_spec = %{
      id: computation.id,
      start: {Aflecha.Computation.GroupManager, :start_link, [{group_id, computation}]}
    }

    result =
      Identificator.hub(group_id)
      |> ProcessHub.start_child(child_spec, async_wait: true)
      |> ProcessHub.await()

    case result do
      {:ok, pid} -> {:ok, pid}
      {:error, reason} -> {:error, reason}
    end
  end
end

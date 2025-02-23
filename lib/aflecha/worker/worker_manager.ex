defmodule Aflecha.Worker.WorkerManager do
  @moduledoc """
  Manages the worker startings and stoppings.
  """

  alias Aflecha.HubManager
  alias Aflecha.Operator.Controller
  alias Aflecha.Service.Identificator
  alias Aflecha.Worker.Builder, as: WorkerBuilder

  @doc """
  Starts a new worker to the operator.
  """
  @spec start_worker(String.t(), Aflecha.Worker.Builder.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def start_worker(group_id, worker) do
    case WorkerBuilder.validate_worker(group_id, worker) do
      {:error, reason} ->
        {:error, reason}

      :ok ->
        # TODO: we should validate the group_id also.
        add_worker(group_id, worker)
    end
  end

  @doc """
  Stops a worker.
  """
  @spec stop_worker(String.t(), String.t()) :: :ok | {:error, String.t()}
  def stop_worker(group_id, worker_id) do
    GenServer.call(Controller, {:stop_worker, group_id, worker_id})
  end

  defp add_worker(group_id, worker) do
    operator_ids = Aflecha.Operator.FleetManager.get_operators(group_id)
    operator_id = select_operator(operator_ids)

    child_spec = %{
      id: worker.id,
      start:
        {
          Aflecha.Worker.Overwatch,
          :start_link,
          [{group_id, operator_id, worker}]
        }
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

  defp select_operator(operator_ids) do
    # TODO: add load balancing here.
    Enum.random(operator_ids)
  end
end

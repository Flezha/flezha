defmodule Aflecha.Service.WorkerManager do
  @moduledoc """
  Manages the worker startings and stoppings.
  """

  alias Aflecha.HubManager
  alias Aflecha.Operator.Controller
  alias Aflecha.Service.Identificator
  alias Aflecha.Pipeline.Builder, as: PipelineBuilder

  @doc """
  Starts a new worker to the operator.
  """
  @spec start_worker(String.t(), Aflecha.Pipeline.Builder.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def start_worker(group_id, worker) do
    case PipelineBuilder.validate_pipeline(group_id, worker) do
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
  def stop_worker(group_id, pipeline_id) do
    GenServer.call(Controller, {:stop_worker, group_id, pipeline_id})
  end

  defp add_worker(group_id, worker) do
    operator_ids = Aflecha.Operator.FleetManager.get_operators(group_id)
    operator_id = select_operator(operator_ids)

    child_spec = %{
      id: worker.id,
      start: {
        Aflecha.Pipeline.Overwatch,
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

  def add_binding(group_id, pipeline_id, binding, mode, opts \\ []) do
    # TODO: Move worker ID extraction to separate service.
    {:ok, worker_pid} = HubManager.get_pid(group_id, pipeline_id)

    GenServer.call(worker_pid, {:add_binding, binding, mode, opts})
  end

  defp select_operator(operator_ids) do
    # TODO: add load balancing here.
    Enum.random(operator_ids)
  end
end

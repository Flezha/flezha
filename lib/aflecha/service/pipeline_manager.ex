defmodule Aflecha.Service.PipelineManager do
  @moduledoc """
  Manages the pipeline startings and stoppings.
  """

  alias Aflecha.Service.HubManager
  alias Aflecha.Service.Identificator
  alias Aflecha.Operator.FleetManager
  alias Aflecha.Pipeline.Builder, as: PipelineBuilder

  def pipeline_registry(group_id) do
    case HubManager.get_registry(group_id) do
      {:ok, registry} ->
        registry

      {:error, reason} ->
        {:error, reason}
    end
  end

  def get_pipeline_pid(group_id, pipeline_id) do
    case HubManager.get_pid(group_id, pipeline_id) do
      {:ok, pid} -> {:ok, pid}
      {:error, "no pid found"} -> {:error, "pipeline not found"}
      err -> err
    end
  end

  def get_overwatch_state(group_id, pipeline_id) do
    case get_pipeline_pid(group_id, pipeline_id) do
      {:ok, pipeline_pid} ->
        {:ok, GenServer.call(pipeline_pid, :get_overwatch_state)}

      err ->
        err
    end
  end

  @doc """
  Starts a new pipeline to the operator.
  """
  @spec start_pipeline(String.t(), PipelineBuilder.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def start_pipeline(group_id, pipeline) do
    case PipelineBuilder.validate_pipeline(group_id, pipeline) do
      {:error, reason} ->
        {:error, reason}

      :ok ->
        # TODO: we should validate the group_id also.
        init_pipeline(group_id, pipeline)
    end
  end

  @doc """
  Removes a pipeline.
  """
  @spec stop_pipeline(String.t(), String.t()) :: :ok | {:error, String.t()}
  def stop_pipeline(group_id, pipeline_id) do
    case get_pipeline_pid(group_id, pipeline_id) do
      {:ok, pipeline_pid} ->
        case GenServer.call(pipeline_pid, :stop_pipeline) do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      err ->
        err
    end
  end

  def stop_all_pipelines(group_id) do
    pipelines = HubManager.get_all_pipelines(group_id)

    Enum.map(pipelines, fn {_pipeline_id, pipeline_pid} ->
      if Process.alive?(pipeline_pid) do
        GenServer.call(pipeline_pid, :stop_pipeline)
      else
        {:error, "pipeline not alive"}
      end
    end)
  end

  def send_io(pipeline_id, group_id, message) do
    case get_pipeline_pid(group_id, pipeline_id) do
      {:ok, pipeline_pid} ->
        GenServer.cast(pipeline_pid, {:io_in, message})

      err ->
        err
    end
  end

  def create_snapshot(group_id, pipeline_id) do
    case get_pipeline_pid(group_id, pipeline_id) do
      {:ok, pipeline_pid} ->
        GenServer.call(pipeline_pid, :create_snapshot)

      err ->
        err
    end
  end

  defp init_pipeline(group_id, pipeline) do
    child_spec = %{
      id: pipeline.id,
      start: {
        Aflecha.Pipeline.Overwatch,
        :start_link,
        [{group_id, pipeline}]
      },
      restart: :transient
    }

    result =
      Identificator.hub(group_id)
      |> ProcessHub.start_child(child_spec,
        async_wait: true,
        # We will get the errors from the start result.
        disable_logging: true
      )
      |> ProcessHub.await()

    case result do
      {:ok, {_pipeline_id, [{node, pid}]}} ->
        {:ok, operator_id} = GenServer.call(pid, :registry_operator_set)

        {:ok,
         %{
           pipeline_id: pipeline.id,
           operator_id: operator_id,
           node: node,
           pid: pid
         }}

      {:error, reason} ->
        {{pipeline_id, node, {error_msg, _}}, _partial_success} = reason

        {
          :error,
          %{
            pipeline_id: pipeline_id,
            node: node,
            reason: error_msg
          }
        }
    end
  end

  def exist?(group_id, pipeline_id) do
    case get_pipeline_pid(group_id, pipeline_id) do
      {:ok, _pid} -> true
      {:error, _reason} -> false
    end
  end

  def get_pipeline_state(group_id, pipeline_id) do
    case get_pipeline_pid(group_id, pipeline_id) do
      {:ok, pipeline_pid} -> GenServer.call(pipeline_pid, :get_pipeline_state)
      err -> err
    end
  end

  def assign_operator(group_id) do
    select_operator(FleetManager.active_operators(group_id))
  end

  defp select_operator(operator_ids) do
    # TODO: add load balancing here.
    Enum.random(operator_ids)
  end
end

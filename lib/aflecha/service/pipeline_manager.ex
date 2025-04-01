defmodule Aflecha.Service.PipelineManager do
  @moduledoc """
  Manages the pipeline startings and stoppings.
  """

  alias Aflecha.HubManager
  alias Aflecha.Service.Identificator
  alias Aflecha.Pipeline.Builder, as: PipelineBuilder

  def get_pipeline_pid(group_id, pipeline_id) do
    case HubManager.get_pid(group_id, pipeline_id) do
      {:ok, pid} -> {:ok, pid}
      {:error, "no pid found"} -> {:error, "no pipeline found"}
      err -> err
    end
  end

  @doc """
  Starts a new pipeline to the operator.
  """
  @spec create_pipeline(String.t(), PipelineBuilder.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def create_pipeline(group_id, pipeline) do
    case PipelineBuilder.validate_pipeline(group_id, pipeline) do
      {:error, reason} ->
        {:error, reason}

      :ok ->
        # TODO: we should validate the group_id also.
        add_pipeline(group_id, pipeline)
    end
  end

  @doc """
  Removes a pipeline.
  """
  @spec rem_pipeline(String.t(), String.t()) :: :ok | {:error, String.t()}
  def rem_pipeline(group_id, pipeline_id) do
    case get_pipeline_pid(group_id, pipeline_id) do
      {:ok, pipeline_pid} ->
        case GenServer.call(pipeline_pid, :rem_pipeline) do
          :ok -> {:ok, :pipeline_removed}
          {:error, reason} -> {:error, reason}
        end

      err ->
        err
    end
  end

  defp add_pipeline(group_id, pipeline) do
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
        operator_id = GenServer.call(pid, :get_operator)

        {:ok,
         %{
           pipeline_id: pipeline.id,
           operator_id: operator_id,
           node: node,
           pid: pid
         }}

      {:error, reason} ->
        {{pipeline_id, node, {error_msg, _}}, _partial_success} = reason

        {:error,
         %{
           pipeline_id: pipeline_id,
           node: node,
           reason: error_msg
         }}
    end
  end

  def exist?(group_id, pipeline_id) do
    case get_pipeline_pid(group_id, pipeline_id) do
      {:ok, _pid} -> true
      {:error, _reason} -> false
    end
  end

  def add_binding(group_id, pipeline_id, binding, mode, opts \\ []) do
    case get_pipeline_pid(group_id, pipeline_id) do
      {:ok, pipeline_pid} -> GenServer.call(pipeline_pid, {:add_binding, binding, mode, opts})
      err -> err
    end
  end

  def get_pipeline_state(group_id, pipeline_id) do
    case get_pipeline_pid(group_id, pipeline_id) do
      {:ok, pipeline_pid} -> GenServer.call(pipeline_pid, :get_pipeline_state)
      err -> err
    end
  end
end

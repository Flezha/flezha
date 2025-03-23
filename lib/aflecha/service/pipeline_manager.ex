defmodule Aflecha.Service.PipelineManager do
  @moduledoc """
  Manages the pipeline startings and stoppings.
  """

  alias Aflecha.HubManager
  alias Aflecha.Operator.Controller
  alias Aflecha.Service.Identificator
  alias Aflecha.Pipeline.Builder, as: PipelineBuilder

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
  Stops a pipeline.
  """
  @spec stop_pipeline(String.t(), String.t()) :: :ok | {:error, String.t()}
  def stop_pipeline(group_id, pipeline_id) do
    GenServer.call(Controller, {:stop_pipeline, group_id, pipeline_id})
  end

  defp add_pipeline(group_id, pipeline) do
    operator_ids = Aflecha.Operator.FleetManager.get_operators(group_id)
    operator_id = select_operator(operator_ids)

    child_spec = %{
      id: pipeline.id,
      start: {
        Aflecha.Pipeline.Overwatch,
        :start_link,
        [{group_id, operator_id, pipeline}]
      }
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
      {:ok, pid} ->
        {:ok,
         %{
           pipeline_id: pipeline.id,
           operator_id: operator_id,
           node: node(),
           pid: pid
         }}

      {:error, reason} ->
        {[{pipeline_id, node, {error_msg, _}}], _partial_success} = reason

        {:error,
         %{
           pipeline_id: pipeline_id,
           operator_id: operator_id,
           node: node,
           reason: error_msg
         }}
    end
  end

  def add_binding(group_id, pipeline_id, binding, mode, opts \\ []) do
    # TODO: Move pipeline ID extraction to separate service.
    {:ok, pipeline_pid} = HubManager.get_pid(group_id, pipeline_id)

    GenServer.call(pipeline_pid, {:add_binding, binding, mode, opts})
  end

  defp select_operator(operator_ids) do
    # TODO: add load balancing here.
    Enum.random(operator_ids)
  end
end

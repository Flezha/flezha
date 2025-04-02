defmodule Aflecha.Service.RpcCookbook do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Aflecha.Service.PipelineManager
  alias Aflecha.IO.IOManager

  @doc "Starts a pipeline."
  @spec pipeline_start(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def pipeline_start(group_id, pipeline_id, factory_id, variables) do
    pipeline = %Aflecha.Pipeline.Builder{
      id: pipeline_id,
      factory_id: factory_id,
      variables: Aflecha.Service.Payloader.json_to_map(variables)
    }

    PipelineManager.create_pipeline(group_id(group_id), pipeline)
  end

  @doc "Stops a pipelinene."
  @spec pipeline_stop(String.t(), String.t()) :: :ok | {:error, String.t()}
  def pipeline_stop(group_id, pipeline_id) do
    PipelineManager.rem_pipeline(group_id(group_id), pipeline_id)
  end

  @doc "Adds an IO."
  def add_io(group_id, pipeline_id, io_params, io_mode, opts) do
    io_struct = Aflecha.Service.Payloader.extract_io_settings(io_params)

    IOManager.add_io(group_id(group_id), pipeline_id, io_struct, io_mode, opts)
  end

  defp group_id(group_id) do
    case group_id do
      "" -> "default"
      _ -> group_id
    end
  end
end

defmodule Aflecha.Service.RpcCookbook do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Aflecha.Service.PipelineManager
  alias Aflecha.Service.Payloader

  @doc "Starts a pipeline."
  @spec pipeline_start(String.t(), String.t(), String.t(), String.t()) ::
          {:ok, pid()} | {:error, String.t()}
  def pipeline_start(group_id, pipeline_id, factory_id, variables) do
    pipeline = %Aflecha.Pipeline.Builder{
      id: pipeline_id,
      factory_id: factory_id,
      variables: Payloader.json_to_map(variables)
    }

    PipelineManager.create_pipeline(group_id(group_id), pipeline)
  end

  @doc "Stops a pipelinene."
  @spec pipeline_stop(String.t(), String.t()) :: :ok | {:error, String.t()}
  def pipeline_stop(group_id, pipeline_id) do
    PipelineManager.rem_pipeline(group_id(group_id), pipeline_id)
  end

  @doc "Enalbes plugin."
  def enable_plugin(group_id, plugin_id, plugin_settings) do
    result = try do
      module_name = String.to_existing_atom(plugin_id)

      # TODO: check if the module is under the enabled plugins and implements the PluginBase behaviour.

      {:ok, module_name}
    rescue
      ArgumentError -> {:error, "Module does not exist"}
    end

    case result do
      {:ok, module_name} ->
        plugin_settings = case Jason.decode(plugin_settings, [keys: :atoms!]) do
          {:ok, map} -> map
          {:error, reason} -> raise reason
        end

        # TODO: test with wrong module and wrong settings.
        plugin_struct = struct!(module_name, plugin_settings)

      {:error, reason} -> raise reason
    end
  end

  defp group_id(group_id) do
    case group_id do
      "" -> "default"
      _ -> group_id
    end
  end
end

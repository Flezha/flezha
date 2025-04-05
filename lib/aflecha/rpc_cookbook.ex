defmodule Aflecha.Service.RpcCookbook do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Aflecha.Service.PipelineManager
  alias Aflecha.Service.Payloader
  alias Aflecha.Service.PluginManager
  alias Aflecha.Service.ConfigManager

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
  def enable_plugin(plugin_id, plugin_settings) do
    enabled_plugins = ConfigManager.get_enabled_plugins()

    plugin_mappings =
      Enum.map(enabled_plugins, fn plugin_module ->
        {
          apply(plugin_module, :plugin_id, []),
          {:mod, plugin_module}
        }
      end)
      |> Map.new()

    with {:mod, module} <- Map.get(plugin_mappings, plugin_id, {:error, "plugin not found"}),
         {:ok, msg} <- handle_plugin_action(module, plugin_settings) do
      # TODO: implement success
      nil
    else
      {:error, reason} ->
        nil
        # TODO: implement error
    end

    # TODO: handle errors and successes.
  end

  defp handle_plugin_action(module, plugin_settings) do
    plugin_settings =
      case Jason.decode(plugin_settings, keys: :atoms!) do
        {:ok, map} -> map
        {:error, reason} -> raise reason
      end

    plugin_settings = Map.put_new(plugin_settings, :group, "default")

    # Convert any custom keys to atoms
    plugin_settings = atom_converter(plugin_settings)

    # TODO: test with wrong module and wrong settings.
    plugin_struct = struct!(module, plugin_settings)

    PluginManager.enable_plugin(plugin_struct)
  end

  defp atom_converter(orig_map) do
    mappings = %{
      "on_input" => :on_input,
      "on_output" => :on_output,
      "interval" => :interval,
      "proactive" => :proactive
    }

    Enum.map(orig_map, fn {key, value} ->
      case key do
        :trigger ->
          if Map.has_key?(mappings, key) do
            {mappings[key], value}
          else
            {key, value}
          end

        any ->
          any
      end
    end)
    |> Map.new()
  end

  # TODO: disable_plugin

  defp group_id(group_id) do
    case group_id do
      "" -> "default"
      _ -> group_id
    end
  end
end

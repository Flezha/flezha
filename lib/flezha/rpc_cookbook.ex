defmodule Flezha.Service.RpcCookbook do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Flezha.Service.ComputationManager
  alias Flezha.Service.Payloader
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ConfigManager

  @doc "Starts a pipeline."
  def pipeline_start(group_id, computation_id, factory_id, init_values, opts \\ []) do
    opts =
      Payloader.json_to_map(opts)
      |> atom_converter()
      |> Keyword.new()

    pipeline = %Flezha.Computation.Builder{
      id: computation_id,
      factory_id: factory_id,
      init_values: Payloader.json_to_map(init_values),
      opts: opts
    }

    case ComputationManager.start_pipeline(group_id(group_id), pipeline) do
      {:ok, _} -> IO.puts("Computation has been started")
      {:error, reason} -> IO.puts("Error starting pipeline: #{inspect(reason)}")
    end
  end

  @doc "Stops a pipelinene."
  def pipeline_stop(group_id, computation_id) do
    case ComputationManager.stop_pipeline(group_id(group_id), computation_id) do
      :ok -> IO.puts("Computation has been stopped")
      {:error, reason} -> IO.puts("Error stopping pipeline: #{inspect(reason)}")
    end
  end

  @doc "Get pipeline state."
  def computation_state(group_id, computation_id) do
    result = ComputationManager.get_computation_state(group_id(group_id), computation_id)

    message =
      case result do
        {:ok, json_state} -> json_state
        {:error, reason} -> reason
      end

    IO.puts(message)
  end

  @doc "Enalbes plugin."
  def enable_plugin(plugin_id, plugin_settings) do
    with {:ok, module} <- get_plugin_mod(plugin_id),
         {:ok, _msg} <- handle_plugin_action(module, plugin_settings) do
      IO.puts("Plugin #{plugin_id} enabled successfully.")
    else
      {:error, reason} ->
        IO.puts("Error enabling plugin #{inspect(reason)}.")
    end
  end

  defp get_plugin_mod(plugin_id) do
    enabled_plugins = ConfigManager.get_enabled_plugins()

    plugin_mappings =
      Enum.map(enabled_plugins, fn plugin_module ->
        {
          apply(plugin_module, :plugin_id, []),
          {:mod, plugin_module}
        }
      end)
      |> Map.new()

    case Map.get(plugin_mappings, plugin_id, nil) do
      {:mod, module} -> {:ok, module}
      nil -> {:error, "plugin not found, make sure the plugin is enabled."}
    end
  end

  defp handle_plugin_action(module, plugin_settings) do
    plugin_settings =
      case Jason.decode(plugin_settings, keys: :atoms!) do
        {:ok, map} -> map
        # TODO: we should return error.
        {:error, reason} -> raise reason
      end

    plugin_settings = Map.put_new(plugin_settings, :group, "default")

    # Convert any custom keys to atoms.
    plugin_settings = atom_converter(plugin_settings)

    # TODO: test with wrong module and wrong settings.
    module
    |> struct!(plugin_settings)
    |> PluginManager.enable_plugin()
  end

  defp atom_converter(orig_map) do
    mappings = %{
      "on_input" => :on_input,
      "on_output" => :on_output,
      "interval" => :interval,
      "proactive" => :proactive,
      "snapshot_interval" => :snapshot_interval
    }

    Enum.map(orig_map, fn {key, value} ->
      case key do
        :trigger ->
          if Map.has_key?(mappings, value) do
            {key, mappings[value]}
          else
            {key, value}
          end

        any ->
          case Map.has_key?(mappings, any) do
            false ->
              {key, value}

            true ->
              {mappings[any], value}
          end
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

defmodule Flezha.Service.CliManager do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Flezha.Service.ComputationManager
  alias Flezha.Service.Payloader
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ConfigManager

  @doc "Starts a computation."
  def computation_start(group_id, computation_id, factory_id, init_values, opts \\ []) do
    opts =
      Payloader.json_to_map(opts)
      |> atom_converter()
      |> Keyword.new()

    computation = %Flezha.Computation.Builder{
      id: computation_id,
      factory_id: factory_id,
      init_values: Payloader.json_to_map(init_values),
      opts: opts
    }

    case ComputationManager.start_computation(group_id(group_id), computation) do
      {:ok, _} -> IO.puts("Computation has been started")
      {:error, reason} -> IO.puts("Error starting computation: #{inspect(reason)}")
    end
  end

  @doc "Stops a computationne."
  def computation_stop(group_id, computation_id) do
    case ComputationManager.stop_computation(group_id(group_id), computation_id) do
      :ok -> IO.puts("Computation has been stopped")
      {:error, reason} -> IO.puts("Error stopping computation: #{inspect(reason)}")
    end
  end

  @doc "Get computation state."
  def computation_state(group_id, computation_id) do
    group_display = if group_id == "", do: "default", else: group_id
    
    case ComputationManager.get_computation_state(group_id(group_id), computation_id) do
      {:ok, json_state} when is_binary(json_state) ->
        IO.puts("State for computation '#{computation_id}' in group '#{group_display}':")
        IO.puts(json_state)
      
      {:ok, state} ->
        IO.puts("State for computation '#{computation_id}' in group '#{group_display}':")
        IO.puts(inspect(state))
      
      {:error, :not_found} ->
        IO.puts("Computation '#{computation_id}' not found in group '#{group_display}'.")
      
      {:error, reason} when is_binary(reason) ->
        IO.puts("Error getting computation state: #{reason}")
      
      {:error, reason} ->
        IO.puts("Error getting computation state: #{inspect(reason)}")
      
      other ->
        IO.puts("Unexpected result: #{inspect(other)}")
    end
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

  @doc "Lists all running groups."
  def list_groups() do
    case Flezha.list_groups() do
      {:ok, groups} when is_list(groups) ->
        if Enum.empty?(groups) do
          IO.puts("No running groups found.")
        else
          IO.puts("Running groups:")
          Enum.each(groups, &IO.puts("  - #{&1}"))
        end

      groups when is_list(groups) ->
        if Enum.empty?(groups) do
          IO.puts("No running groups found.")
        else
          IO.puts("Running groups:")
          Enum.each(groups, &IO.puts("  - #{&1}"))
        end

      {:error, reason} ->
        IO.puts("Error listing groups: #{inspect(reason)}")

      other ->
        IO.puts("Unexpected result: #{inspect(other)}")
    end
  end

  @doc "Lists operators for a specific group."
  def list_operators(group_id) do
    group_display = if group_id == "", do: "default", else: group_id

    case Flezha.list_operators(group_display) do
      {:ok, operators} when is_list(operators) ->
        if Enum.empty?(operators) do
          IO.puts("No operators found for group '#{group_display}'.")
        else
          IO.puts("Operators in group '#{group_display}':")
          Enum.each(operators, &IO.puts("  - #{&1}"))
        end

      operators when is_list(operators) ->
        if Enum.empty?(operators) do
          IO.puts("No operators found for group '#{group_display}'.")
        else
          IO.puts("Operators in group '#{group_display}':")
          Enum.each(operators, &IO.puts("  - #{&1}"))
        end

      {:error, reason} ->
        IO.puts("Error listing operators: #{inspect(reason)}")

      other ->
        IO.puts("Unexpected result: #{inspect(other)}")
    end
  end

  @doc "Lists computations for a specific group."
  def list_computations(group_id) do
    group_display = if group_id == "", do: "default", else: group_id

    case Flezha.list_computations(group_display) do
      {:ok, computations} when is_list(computations) ->
        if Enum.empty?(computations) do
          IO.puts("No computations found for group '#{group_display}'.")
        else
          IO.puts("Computations in group '#{group_display}':")
          Enum.each(computations, &IO.puts("  - #{&1}"))
        end

      computations when is_list(computations) ->
        if Enum.empty?(computations) do
          IO.puts("No computations found for group '#{group_display}'.")
        else
          IO.puts("Computations in group '#{group_display}':")
          Enum.each(computations, &IO.puts("  - #{&1}"))
        end

      {:error, reason} ->
        IO.puts("Error listing computations: #{inspect(reason)}")

      other ->
        IO.puts("Unexpected result: #{inspect(other)}")
    end
  end

  # TODO: disable_plugin

  defp group_id(group_id) do
    case group_id do
      "" -> "default"
      _ -> group_id
    end
  end
end

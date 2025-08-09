defmodule Flezha.Service.CliManager do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Flezha.Service.ComputationManager
  alias Flezha.Service.Payloader
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ConfigManager
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Plugin.Driver.IOBindingPlugin

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

  @doc "Listens to a computation IO."
  def computation_listen(group_id, computation_id, io_type) do
    group_display = if group_id == "", do: "default", else: group_id

    io_type =
      case io_type do
        "input" -> :on_input
        "output" -> :on_output
        _ -> raise "Invalid IO type. Use 'input' or 'output'."
      end

    provider = %MonitoredIOForward{
      recv_pid: self(),
      plugin_id: "monitored_io_forward_dynamic",
      computation_id: computation_id,
      group: group_display
    }

    PluginManager.enable_plugin("monitored_io_forward_dynamic", %IOBindingPlugin{
      provider: provider,
      group: group_display,
      computation_id: computation_id,
      trigger: io_type
    })

    listen_loop()
  end

  @doc "Enables plugin."
  def enable_plugin(plugin_id, provider_id, plugin_flags_json, opts_json) do
    with {:ok, provider_module} <- get_plugin_provider_mod(provider_id),
         {:ok, plugin_flags} <- Jason.decode(plugin_flags_json),
         {:ok, opts} <- Jason.decode(opts_json),
         {:ok, plugin_struct} <-
           build_plugin_struct_dynamic(provider_module, opts, plugin_flags) do
      case PluginManager.enable_plugin(plugin_id, plugin_struct) do
        {:ok, _msg} -> IO.puts("Plugin #{plugin_id} enabled successfully.")
        {:error, reason} -> IO.puts("Error enabling plugin: #{inspect(reason)}.")
      end
    else
      {:error, reason} ->
        IO.puts("Error enabling plugin: #{inspect(reason)}.")
    end
  end

  defp build_plugin_struct("io_binding", provider_module, opts, plugin_flags) do
    # Use the provider's build function to create the provider struct
    case provider_module.build(plugin_flags) do
      {:ok, provider_struct} ->
        # Use IOBindingPlugin's build function to create the wrapper
        case IOBindingPlugin.build(opts, provider_struct) do
          {:ok, plugin_struct} -> {:ok, plugin_struct}
          {:error, reason} -> {:error, "Failed to create IOBindingPlugin: #{inspect(reason)}"}
        end

      {:error, reason} ->
        {:error, "Failed to create provider struct: #{inspect(reason)}"}
    end
  end

  defp build_plugin_struct(plugin_type, _module, _opts, _plugin_flags) do
    {:error, "Unsupported plugin type: #{plugin_type}"}
  end

  defp listen_loop() do
    receive do
      {:message_forward, {message, _metadata}} ->
        IO.puts(message)

      {:heartbeat_keepalive, pid} ->
        Process.send(pid, {:heartbeat_keepalive, :ok}, [])
    end

    listen_loop()
  end

  defp get_plugin_provider_mod(provider_id) do
    enabled_plugins = ConfigManager.get_enabled_plugins()

    case Map.get(enabled_plugins, provider_id, nil) do
      nil -> {:error, "plugin provider not found, make sure the plugin is enabled."}
      module -> {:ok, module}
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

  defp handle_plugin_action(module, plugin_settings, weight) do
    plugin_settings =
      case Jason.decode(plugin_settings, keys: :atoms!) do
        {:ok, map} -> map
        # TODO: we should return error.
        {:error, reason} -> raise reason
      end

    plugin_settings =
      plugin_settings
      |> Map.put_new(:group, "default")
      |> Map.put(:weight, weight)

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
  def list_groups(output_format \\ "text") do
    case Flezha.list_groups() do
      {:ok, groups} when is_list(groups) ->
        handle_list_groups_output(groups, output_format)

      groups when is_list(groups) ->
        handle_list_groups_output(groups, output_format)

      {:error, reason} ->
        handle_list_groups_error(reason, output_format)

      other ->
        handle_list_groups_error(other, output_format)
    end
  end

  defp handle_list_groups_output(groups, "json") do
    result = %{
      groups: groups
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_list_groups_output(groups, _) do
    if Enum.empty?(groups) do
      IO.puts("No running groups found.")
    else
      IO.puts("Running groups:")
      Enum.each(groups, &IO.puts("  - #{&1}"))
    end
  end

  defp handle_list_groups_error(reason, "json") do
    result = %{
      error: inspect(reason)
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_list_groups_error(reason, _) do
    IO.puts("Error listing groups: #{inspect(reason)}")
  end

  @doc "Lists operators for a specific group."
  def list_operators(group_id, output_format \\ "text") do
    group_display = if group_id == "", do: "default", else: group_id

    case Flezha.list_operators(group_display) do
      {:ok, operators} when is_list(operators) ->
        handle_list_operators_output(operators, group_display, output_format)

      operators when is_list(operators) ->
        handle_list_operators_output(operators, group_display, output_format)

      {:error, reason} ->
        handle_list_operators_error(reason, output_format)

      other ->
        handle_list_operators_error(other, output_format)
    end
  end

  defp handle_list_operators_output(operators, group_display, "json") do
    result = %{
      group: group_display,
      operators: operators
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_list_operators_output(operators, group_display, _) do
    if Enum.empty?(operators) do
      IO.puts("No operators found for group '#{group_display}'.")
    else
      IO.puts("Operators in group '#{group_display}':")
      Enum.each(operators, &IO.puts("  - #{&1}"))
    end
  end

  defp handle_list_operators_error(reason, "json") do
    result = %{
      error: inspect(reason)
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_list_operators_error(reason, _) do
    IO.puts("Error listing operators: #{inspect(reason)}")
  end

  @doc "Lists computations for a specific group."
  def list_computations(group_id, output_format \\ "text") do
    group_display = if group_id == "", do: "default", else: group_id

    case Flezha.list_computations(group_display) do
      {:ok, computations} when is_list(computations) ->
        handle_list_computations_output(computations, group_display, output_format)

      computations when is_list(computations) ->
        handle_list_computations_output(computations, group_display, output_format)

      {:error, reason} ->
        handle_list_computations_error(reason, output_format)

      other ->
        handle_list_computations_error(other, output_format)
    end
  end

  defp handle_list_computations_output(computations, group_display, "json") do
    result = %{
      group: group_display,
      computations: computations
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_list_computations_output(computations, group_display, _) do
    if Enum.empty?(computations) do
      IO.puts("No computations found for group '#{group_display}'.")
    else
      IO.puts("Computations in group '#{group_display}':")
      Enum.each(computations, &IO.puts("  - #{&1}"))
    end
  end

  defp handle_list_computations_error(reason, "json") do
    result = %{
      error: inspect(reason)
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_list_computations_error(reason, _) do
    IO.puts("Error listing computations: #{inspect(reason)}")
  end

  @doc "Shows detailed information about computations and their overwatch plugins."
  def computation_info(group_id, output_format \\ "text") do
    group_display = if group_id == "", do: "default", else: group_id

    case Flezha.list_computations(group_display) do
      {:ok, computations} when is_list(computations) ->
        if Enum.empty?(computations) do
          handle_empty_computations(group_display, output_format)
        else
          handle_computation_info_output(computations, group_display, output_format)
        end

      computations when is_list(computations) ->
        if Enum.empty?(computations) do
          handle_empty_computations(group_display, output_format)
        else
          handle_computation_info_output(computations, group_display, output_format)
        end

      {:error, reason} ->
        handle_computation_info_error(reason, output_format)

      other ->
        handle_computation_info_error(other, output_format)
    end
  end

  defp handle_empty_computations(group_display, "json") do
    result = %{
      group: group_display,
      computations: [],
      message: "No computations found"
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_empty_computations(group_display, _) do
    IO.puts("No computations found for group '#{group_display}'.")
  end

  defp handle_computation_info_output(computations, group_display, "json") do
    computation_data =
      Enum.map(computations, fn computation_id ->
        case ComputationManager.get_overwatch_state(group_display, computation_id) do
          {:ok, state} ->
            %{
              id: computation_id,
              operator_id: state.operator_id,
              group_id: state.group_id,
              default_input_type: state.default_input_type,
              bindings: format_bindings_json(state.bindings)
            }

          {:error, reason} ->
            %{
              id: computation_id,
              error: inspect(reason)
            }
        end
      end)

    result = %{
      group: group_display,
      computations: computation_data
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_computation_info_output(computations, group_display, _) do
    IO.puts("Computation information for group '#{group_display}':")
    IO.puts("")

    Enum.each(computations, fn computation_id ->
      case ComputationManager.get_overwatch_state(group_display, computation_id) do
        {:ok, state} ->
          display_computation_info(computation_id, state)

        {:error, reason} ->
          IO.puts("Computation: #{computation_id}")
          IO.puts("  Error getting overwatch state: #{inspect(reason)}")
          IO.puts("")
      end
    end)
  end

  defp handle_computation_info_error(reason, "json") do
    result = %{
      error: inspect(reason)
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_computation_info_error(reason, _) do
    IO.puts("Error listing computations: #{inspect(reason)}")
  end

  defp format_bindings_json(bindings) do
    Enum.map(bindings, fn {trigger, bindings_list} ->
      formatted_bindings =
        Enum.map(bindings_list, fn binding ->
          %{
            id: Map.get(binding, :id, "unknown"),
            plugin_group: Map.get(binding, :plugin_group, "0"),
            module:
              case binding do
                %{__struct__: module} -> module |> to_string() |> String.split(".") |> List.last()
                _ -> "unknown"
              end
          }
        end)

      %{
        trigger: trigger,
        bindings: formatted_bindings
      }
    end)
  end

  defp display_computation_info(computation_id, state) do
    IO.puts("Computation: #{computation_id}")
    IO.puts("  Operator ID: #{state.operator_id}")
    IO.puts("  Group ID: #{state.group_id}")
    IO.puts("  Default Input Type: #{state.default_input_type}")
    IO.puts("  Bindings:")

    # Display each binding type and its plugins
    Enum.each(state.bindings, fn {trigger, bindings_list} ->
      if not Enum.empty?(bindings_list) do
        IO.puts("    #{trigger}:")

        Enum.each(bindings_list, fn binding ->
          # Extract available binding information
          instance_id = Map.get(binding, :instance_id, "unknown")
          plugin_group = Map.get(binding, :plugin_group, "0")

          # Try to get the module name which might indicate plugin type
          module_name =
            case binding do
              %{provider: %{__struct__: module}} ->
                module |> to_string() |> String.split(".") |> List.last()

              _ ->
                "unknown"
            end

          IO.puts("      - Instance ID: #{instance_id}")
          IO.puts("        Plugin Group: #{plugin_group}")
          IO.puts("        Module: #{module_name}")
        end)
      end
    end)

    IO.puts("")
  end

  @doc "Disables plugin."
  def disable_plugin(group_id, computation_id, plugin_id) do
    group_display = if group_id == "", do: "default", else: group_id

    case ComputationManager.get_computation_pid(group_display, computation_id) do
      {:ok, computation_pid} ->
        case GenServer.call(computation_pid, {:disable_plugin, plugin_id}) do
          :ok ->
            IO.puts(
              "Plugin '#{plugin_id}' disabled successfully from computation '#{computation_id}'."
            )

          {:error, reason} ->
            IO.puts("Error disabling plugin: #{inspect(reason)}")
        end

      {:error, reason} ->
        IO.puts("Error finding computation '#{computation_id}': #{inspect(reason)}")
    end
  end

  defp group_id(group_id) do
    case group_id do
      "" -> "default"
      _ -> group_id
    end
  end
end

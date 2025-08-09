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

    case ComputationManager.start_computation(normalize_group_id(group_id), computation) do
      {:ok, _} -> IO.puts("Computation has been started")
      {:error, reason} -> IO.puts("Error starting computation: #{inspect(reason)}")
    end
  end

  @doc "Stops a computation."
  def computation_stop(group_id, computation_id) do
    case ComputationManager.stop_computation(normalize_group_id(group_id), computation_id) do
      :ok -> IO.puts("Computation has been stopped")
      {:error, reason} -> IO.puts("Error stopping computation: #{inspect(reason)}")
    end
  end

  @doc "Get computation state."
  def computation_state(group_id, computation_id) do
    group_display = normalize_group_id(group_id)

    case ComputationManager.get_computation_state(normalize_group_id(group_id), computation_id) do
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
    group_display = normalize_group_id(group_id)

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
    with {:ok, provider_mod} <- get_plugin_provider_mod(provider_id),
         {:ok, plugin_flags} <- Jason.decode(plugin_flags_json),
         {:ok, opts} <- Jason.decode(opts_json),
         {:ok, plugin_struct} <- apply(provider_mod, :build, [plugin_flags]),
         {:ok, plugin} <- build_plugin(provider_mod, plugin_struct, plugin_flags, opts) do
      case PluginManager.enable_plugin(plugin_id, plugin) do
        {:ok, _msg} -> IO.puts("Plugin #{plugin_id} enabled successfully.")
        {:error, reason} -> IO.puts("Error enabling plugin: #{inspect(reason)}.")
      end
    else
      {:error, reason} ->
        IO.puts("Error enabling plugin: #{inspect(reason)}.")
    end
  end

  defp build_plugin(provider_mod, provider, plugin_flags, opts) do
    case apply(provider_mod, :plugin_type, []) do
      :io_binding ->
        plugin = %IOBindingPlugin{
          provider: provider,
          group: Map.get(opts, "group", "default"),
          computation_id: Map.get(plugin_flags, "computation_id"),
          trigger: Map.get(plugin_flags, "trigger"),
          weight: Map.get(plugin_flags, "weight", 0),
          plugin_group: Map.get(plugin_flags, "plugin_group", "0")
        }

        {:ok, plugin}

      _ ->
        {:error, "unknown plugin type"}
    end
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

  defp atom_converter(orig_map) do
    string_to_atom = %{
      "on_input" => :on_input,
      "on_output" => :on_output,
      "interval" => :interval,
      "proactive" => :proactive,
      "snapshot_interval" => :snapshot_interval
    }

    Enum.reduce(orig_map, %{}, fn {key, value}, acc ->
      new_key = Map.get(string_to_atom, key, key)
      new_value = if key == :trigger, do: Map.get(string_to_atom, value, value), else: value
      Map.put(acc, new_key, new_value)
    end)
  end

  @doc "Lists all running groups."
  def list_groups(output_format \\ "text") do
    case Flezha.list_groups() do
      {:ok, groups} when is_list(groups) ->
        handle_list_output(groups, "running groups", output_format)

      groups when is_list(groups) ->
        handle_list_output(groups, "running groups", output_format)

      {:error, reason} ->
        handle_list_error(reason, "listing groups", output_format)

      other ->
        handle_list_error(other, "listing groups", output_format)
    end
  end

  # Generic output handlers that work for all list operations
  defp handle_list_output(items, item_name, "json", context \\ nil) do
    result =
      case context do
        nil -> %{String.to_atom(item_name) => items}
        %{key: key, value: value} -> %{String.to_atom(item_name) => items, key => value}
      end

    IO.puts(Jason.encode!(result))
  end

  defp handle_list_output([], item_name, _, context) do
    message =
      case context do
        nil -> "No #{item_name} found."
        %{display: display, value: value} -> "No #{item_name} found for #{display} '#{value}'."
      end

    IO.puts(message)
  end

  defp handle_list_output(items, item_name, _, context) do
    header =
      case context do
        nil ->
          "#{String.capitalize(item_name)}:"

        %{display: display, value: value} ->
          "#{String.capitalize(item_name)} in #{display} '#{value}':"
      end

    IO.puts(header)
    Enum.each(items, &IO.puts("  - #{&1}"))
  end

  defp handle_list_error(reason, _operation, "json") do
    result = %{error: inspect(reason)}
    IO.puts(Jason.encode!(result))
  end

  defp handle_list_error(reason, operation, _) do
    IO.puts("Error #{operation}: #{inspect(reason)}")
  end

  @doc "Lists operators for a specific group."
  def list_operators(group_id, output_format \\ "text") do
    group_display = normalize_group_id(group_id)

    case Flezha.list_operators(group_display) do
      {:ok, operators} when is_list(operators) ->
        context = %{key: :group, value: group_display, display: "group"}
        handle_list_output(operators, "operators", output_format, context)

      operators when is_list(operators) ->
        context = %{key: :group, value: group_display, display: "group"}
        handle_list_output(operators, "operators", output_format, context)

      {:error, reason} ->
        handle_list_error(reason, "listing operators", output_format)

      other ->
        handle_list_error(other, "listing operators", output_format)
    end
  end

  @doc "Lists computations for a specific group."
  def list_computations(group_id, output_format \\ "text") do
    group_display = normalize_group_id(group_id)

    case Flezha.list_computations(group_display) do
      {:ok, computations} when is_list(computations) ->
        context = %{key: :group, value: group_display, display: "group"}
        handle_list_output(computations, "computations", output_format, context)

      computations when is_list(computations) ->
        context = %{key: :group, value: group_display, display: "group"}
        handle_list_output(computations, "computations", output_format, context)

      {:error, reason} ->
        handle_list_error(reason, "listing computations", output_format)

      other ->
        handle_list_error(other, "listing computations", output_format)
    end
  end

  @doc "Shows detailed information about computations and their overwatch plugins."
  def computation_info(group_id, output_format \\ "text") do
    group_display = normalize_group_id(group_id)

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
        handle_list_error(reason, "listing computations", output_format)

      other ->
        handle_list_error(other, "listing computations", output_format)
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

        Enum.each(bindings_list, fn {plugin_id, binding} ->
          # Extract available binding information
          plugin_group = Map.get(binding, :plugin_group, "0")

          # Try to get the module name which might indicate plugin type
          module_name =
            case binding do
              %{provider: %{__struct__: module}} ->
                module |> to_string() |> String.split(".") |> List.last()

              _ ->
                "unknown"
            end

          IO.puts("      - Plugin ID: #{plugin_id}")
          IO.puts("        Plugin Group: #{plugin_group}")
          IO.puts("        Module: #{module_name}")
        end)
      end
    end)

    IO.puts("")
  end

  @doc "Disables plugin."
  def disable_plugin(plugin_id) do
    case PluginManager.disable_plugin(plugin_id) do
      {:ok, _msg} -> IO.puts("Plugin #{plugin_id} disabled successfully.")
      {:error, reason} -> IO.puts("Error disabling plugin: #{inspect(reason)}.")
    end
  end

  defp normalize_group_id(""), do: "default"
  defp normalize_group_id(group_id), do: group_id
end

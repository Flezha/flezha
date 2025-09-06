defmodule Flezha.Service.CliManager do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Flezha.Service.ProcessorManager
  alias Flezha.Service.Payloader
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ConfigManager
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Plugin.Driver.IOBindingPlugin

  @doc "Starts a processor."
  def processor_start(namespace, processor_id, factory_id, init_values, opts \\ []) do
    opts =
      Payloader.json_to_map(opts)
      |> atom_converter()
      |> Keyword.new()

    processor = %Flezha.Processor.Builder{
      id: processor_id,
      factory_id: factory_id,
      init_values: Payloader.json_to_map(init_values),
      opts: opts
    }

    case ProcessorManager.start_processor(normalize_namespace(namespace), processor) do
      {:ok, _} -> IO.puts("Processor has been started")
      {:error, reason} -> IO.puts("Error starting processor: #{inspect(reason)}")
    end
  end

  @doc "Stops a processor."
  def processor_stop(namespace, processor_id) do
    case ProcessorManager.stop_processor(normalize_namespace(namespace), processor_id) do
      :ok -> IO.puts("Processor has been stopped")
      {:error, reason} -> IO.puts("Error stopping processor: #{inspect(reason)}")
    end
  end

  @doc "Get processor state."
  def processor_state(namespace, processor_id) do
    namespace_display = normalize_namespace(namespace)

    case ProcessorManager.get_processor_state(
           normalize_namespace(namespace),
           processor_id
         ) do
      {:ok, json_state} when is_binary(json_state) ->
        IO.puts("State for processor '#{processor_id}' in namespace '#{namespace_display}':")
        IO.puts(json_state)

      {:ok, state} ->
        IO.puts("State for processor '#{processor_id}' in namespace '#{namespace_display}':")
        IO.puts(inspect(state))

      {:error, :not_found} ->
        IO.puts("Processor '#{processor_id}' not found in namespace '#{namespace_display}'.")

      {:error, reason} when is_binary(reason) ->
        IO.puts("Error getting processor state: #{reason}")

      {:error, reason} ->
        IO.puts("Error getting processor state: #{inspect(reason)}")

      other ->
        IO.puts("Unexpected result: #{inspect(other)}")
    end
  end

  @doc "Listens to a processor IO."
  def processor_listen(namespace, processor_id, io_type) do
    namespace_display = normalize_namespace(namespace)

    io_type =
      case io_type do
        "input" -> :on_input
        "output" -> :on_output
        _ -> raise "Invalid IO type. Use 'input' or 'output'."
      end

    provider = %MonitoredIOForward{
      recv_pid: self(),
      plugin_id: "monitored_io_forward_dynamic",
      processor_id: processor_id,
      namespace: namespace_display
    }

    PluginManager.enable_plugin("monitored_io_forward_dynamic", %IOBindingPlugin{
      provider: provider,
      namespace: namespace_display,
      processor_id: processor_id,
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
        # Prepare plugin_flags with opts merged in
        build_params =
          plugin_flags
          |> Map.put("namespace", Map.get(opts, "namespace", "default"))

        case IOBindingPlugin.build(build_params, provider) do
          {:ok, plugin} -> {:ok, plugin}
          {:error, reason} -> {:error, reason}
        end

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

  @doc "Lists all running namespaces."
  def list_namespaces(output_format \\ "text") do
    namespaces = Flezha.list_namespaces()
    handle_list_output(namespaces, "running namespaces", output_format)
  end

  # Generic output handler that works for all list operations
  defp handle_list_output(items, item_name, output_format, context \\ nil) do
    case {items, output_format} do
      {items, "json"} ->
        result =
          case context do
            nil -> %{String.to_atom(item_name) => items}
            %{key: key, value: value} -> %{String.to_atom(item_name) => items, key => value}
          end

        IO.puts(Jason.encode!(result))

      {[], _} ->
        message =
          case context do
            nil ->
              "No #{item_name} found."

            %{display: display, value: value} ->
              "No #{item_name} found for #{display} '#{value}'."
          end

        IO.puts(message)

      {items, _} ->
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
  end

  @doc "Lists compute_engines for a specific namespace."
  def list_compute_engines(namespace, output_format \\ "text") do
    namespace_display = normalize_namespace(namespace)

    case Flezha.list_compute_engines(namespace_display) do
      {:ok, compute_engines} when is_list(compute_engines) ->
        context = %{key: :namespace, value: namespace_display, display: "namespace"}
        handle_list_output(compute_engines, "compute_engines", output_format, context)
    end
  end

  @doc "Lists processors for a specific namespace."
  def list_processors(namespace, output_format \\ "text") do
    namespace_display = normalize_namespace(namespace)

    case Flezha.list_processors(namespace_display) do
      {:ok, processors} ->
        context = %{key: :namespace, value: namespace_display, display: "namespace"}
        handle_list_output(processors, "processors", output_format, context)
    end
  end

  @doc "Shows detailed information about processors and their overwatch plugins."
  def processor_info(namespace, output_format \\ "text") do
    namespace_display = normalize_namespace(namespace)

    case Flezha.list_processors(namespace_display) do
      {:ok, processors} ->
        if Enum.empty?(processors) do
          handle_empty_processors(namespace_display, output_format)
        else
          handle_processor_info_output(processors, namespace_display, output_format)
        end
    end
  end

  defp handle_empty_processors(namespace_display, "json") do
    result = %{
      namespace: namespace_display,
      processors: [],
      message: "No processors found"
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_empty_processors(namespace_display, _) do
    IO.puts("No processors found for namespace '#{namespace_display}'.")
  end

  defp handle_processor_info_output(processors, namespace_display, "json") do
    processor_data =
      Enum.map(processors, fn processor_id ->
        case ProcessorManager.get_overwatch_state(namespace_display, processor_id) do
          {:ok, state} ->
            %{
              id: processor_id,
              compute_engine_id: state.compute_engine_id,
              namespace: state.namespace,
              default_input_type: state.default_input_type,
              bindings: format_bindings_json(state.bindings)
            }

          {:error, reason} ->
            %{
              id: processor_id,
              error: inspect(reason)
            }
        end
      end)

    result = %{
      namespace: namespace_display,
      processors: processor_data
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_processor_info_output(processors, namespace_display, _) do
    IO.puts("Processor information for namespace '#{namespace_display}':")
    IO.puts("")

    Enum.each(processors, fn processor_id ->
      case ProcessorManager.get_overwatch_state(namespace_display, processor_id) do
        {:ok, state} ->
          display_processor_info(processor_id, state)

        {:error, reason} ->
          IO.puts("Processor: #{processor_id}")
          IO.puts("  Error getting overwatch state: #{inspect(reason)}")
          IO.puts("")
      end
    end)
  end

  defp format_bindings_json(bindings) do
    Enum.map(bindings, fn {trigger, bindings_list} ->
      formatted_bindings =
        Enum.map(bindings_list, fn {plugin_id, binding} ->
          %{
            id: plugin_id,
            plugin_group: Map.get(binding, :plugin_group, "0"),
            position: Map.get(binding, :position, nil),
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

  defp display_processor_info(processor_id, state) do
    IO.puts("Processor: #{processor_id}")
    IO.puts("  ComputeEngine ID: #{state.compute_engine_id}")
    IO.puts("  Group ID: #{state.namespace}")
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
          IO.puts("        Trigger: #{inspect(binding.trigger)}")
          IO.puts("        Position: #{binding.position}")
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

  @doc "Show help for a specific plugin provider."
  def plugin_help(provider_id) do
    case get_plugin_provider_mod(provider_id) do
      {:ok, provider_mod} ->
        try do
          help_text = apply(provider_mod, :help, [])
          IO.puts(help_text)
        rescue
          UndefinedFunctionError ->
            IO.puts(
              "Help not available for plugin '#{provider_id}'. The plugin provider does not implement help/0."
            )

          e ->
            IO.puts("Error getting help for plugin '#{provider_id}': #{Exception.message(e)}")
        end

      {:error, reason} ->
        IO.puts("Error: #{reason}")
    end
  end

  @doc "List all available plugin providers."
  def list_plugins() do
    enabled_plugins = ConfigManager.get_enabled_plugins()

    IO.puts("Available Plugin Providers:")
    IO.puts("==========================")

    Enum.each(enabled_plugins, fn {provider_id, module} ->
      IO.puts("  #{provider_id}")
      IO.puts("    Module: #{inspect(module)}")

      # Try to get plugin type
      try do
        plugin_type = apply(module, :plugin_type, [])
        IO.puts("    Type: #{plugin_type}")
      rescue
        _ -> nil
      end

      IO.puts("")
    end)

    IO.puts("Use 'flz plugin help <provider_id>' for detailed help on a specific plugin.")
  end

  defp normalize_namespace(""), do: "default"
  defp normalize_namespace(namespace), do: namespace
end

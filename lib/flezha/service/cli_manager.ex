defmodule Flezha.Service.CliManager do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Service.ConfigManager
  alias Flezha.Service.Payloader
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ProcessorManager

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

    extension =
      case io_type do
        "input" -> :on_input
        "output" -> :on_output
        _ -> raise "Invalid IO type. Use 'input' or 'output'."
      end

    plugin_id =
      "io_listener_#{:rand.uniform(999_999) |> Integer.to_string() |> String.pad_leading(6, "0")}"

    plugin = %Plugin{
      id: plugin_id,
      provider: MonitoredIOForward,
      namespace: namespace_display,
      processor_id: processor_id,
      opts: %{
        recv_pid: self(),
        plugin_id: plugin_id,
        processor_id: processor_id,
        namespace: namespace_display
      },
      subscriptions: [{extension, []}]
    }

    PluginManager.enable_plugin(plugin_id, plugin)

    Process.flag(:trap_exit, true)

    listen_loop(plugin_id)
  end

  @doc "Enables plugin."
  def enable_plugin(plugin_id, provider_id, plugin_flags_json, opts_json) do
    with {:ok, provider_mod} <- get_plugin_provider_mod(provider_id),
         {:ok, plugin_flags} <- Jason.decode(plugin_flags_json),
         {:ok, opts} <- Jason.decode(opts_json) do
      subscriptions = extract_subscriptions(plugin_flags, opts)
      namespace = Map.get(opts, "namespace", "default")
      processor_id = Map.get(plugin_flags, "processor_id") || Map.get(opts, "processor_id")

      plugin = %Plugin{
        id: plugin_id,
        provider: provider_mod,
        namespace: namespace,
        processor_id: processor_id,
        opts: atomize_keys(plugin_flags),
        subscriptions: subscriptions
      }

      case PluginManager.enable_plugin(plugin_id, plugin) do
        {:ok, _msg} -> IO.puts("Plugin #{plugin_id} enabled successfully.")
        {:error, reason} -> IO.puts("Error enabling plugin: #{inspect(reason)}.")
      end
    else
      {:error, reason} -> IO.puts("Error enabling plugin: #{inspect(reason)}.")
    end
  end

  # Build subscriptions from the legacy `--trigger=on_input` shape (single
  # trigger) OR the new `--extension=on_input` repeatable shape. The legacy
  # trigger values `:proactive` and `:interval` both map onto `:on_interval`.
  defp extract_subscriptions(plugin_flags, opts) do
    extensions =
      plugin_flags
      |> Map.get("extensions")
      |> List.wrap()

    extensions =
      if extensions == [],
        do: Map.get(opts, "extensions") |> List.wrap(),
        else: extensions

    if extensions != [] do
      Enum.map(extensions, fn e ->
        {String.to_atom(Map.get(e, "name")), atomize_keyword(Map.get(e, "opts", %{}))}
      end)
    else
      trigger = Map.get(opts, "trigger") || Map.get(plugin_flags, "trigger")
      trigger_to_subscription(trigger)
    end
  end

  defp trigger_to_subscription("on_input"), do: [{:on_input, []}]
  defp trigger_to_subscription("on_output"), do: [{:on_output, []}]
  defp trigger_to_subscription("interval"), do: [{:on_interval, [interval_ms: 5_000]}]
  defp trigger_to_subscription("proactive"), do: [{:on_terminate, []}]
  defp trigger_to_subscription(_), do: [{:on_input, []}]

  defp atomize_keys(map) when is_map(map) do
    Map.new(map, fn {k, v} ->
      {(is_binary(k) && String.to_atom(k)) || k, v}
    end)
  end

  defp atomize_keys(other), do: other

  defp atomize_keyword(map) when is_map(map) do
    Enum.map(map, fn {k, v} -> {(is_binary(k) && String.to_atom(k)) || k, v} end)
  end

  defp atomize_keyword(_), do: []

  defp listen_loop(plugin_id) do
    receive do
      {:message_forward, {message, _metadata}} ->
        IO.puts(message)

      {:heartbeat_keepalive, pid} ->
        Process.send(pid, {:heartbeat_keepalive, :ok}, [])

      {:EXIT, _pid, reason} ->
        cleanup_io_listen(plugin_id)
        exit(reason)
    end

    listen_loop(plugin_id)
  end

  defp cleanup_io_listen(plugin_id) do
    IO.puts("\nShutting down IO listener...")
    PluginManager.disable_plugin(plugin_id)
    :timer.sleep(100)
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
      "on_interval" => :on_interval,
      "on_processor_init" => :on_processor_init,
      "on_message" => :on_message,
      "on_snapshot" => :on_snapshot,
      "on_terminate" => :on_terminate,
      "snapshot_interval" => :snapshot_interval,
      "plugins" => :plugins
    }

    Enum.reduce(orig_map, %{}, fn {key, value}, acc ->
      new_key = Map.get(string_to_atom, key, key)
      Map.put(acc, new_key, value)
    end)
  end

  @doc "Lists all running namespaces."
  def list_namespaces(output_format \\ "text") do
    namespaces = Flezha.list_namespaces()
    handle_list_output(namespaces, "running namespaces", output_format)
  end

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
            nil -> "No #{item_name} found."
            %{display: display, value: value} -> "No #{item_name} found for #{display} '#{value}'."
          end

        IO.puts(message)

      {items, _} ->
        header =
          case context do
            nil -> "#{String.capitalize(item_name)}:"
            %{display: display, value: value} -> "#{String.capitalize(item_name)} in #{display} '#{value}':"
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

  @doc "Shows detailed information about processors and their enabled plugins."
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
              plugins: format_plugins_json(state.extensions)
            }

          {:error, reason} ->
            %{id: processor_id, error: inspect(reason)}
        end
      end)

    result = %{namespace: namespace_display, processors: processor_data}
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

  defp format_plugins_json(%{plugins: plugins}) do
    Enum.map(plugins, fn {plugin_id, plugin} ->
      %{
        id: plugin_id,
        provider: inspect(plugin.provider),
        subscriptions:
          Enum.map(plugin.subscriptions, fn {name, opts} ->
            %{extension: Atom.to_string(name), opts: Map.new(opts)}
          end)
      }
    end)
  end

  defp format_plugins_json(_), do: []

  defp display_processor_info(processor_id, state) do
    IO.puts("Processor: #{processor_id}")
    IO.puts("  ComputeEngine ID: #{state.compute_engine_id}")
    IO.puts("  Group ID: #{state.namespace}")
    IO.puts("  Default Input Type: #{state.default_input_type}")
    IO.puts("  Plugins:")

    case state.extensions do
      %{plugins: plugins} when map_size(plugins) > 0 ->
        Enum.each(plugins, fn {plugin_id, plugin} ->
          IO.puts("    - #{plugin_id}: #{inspect(plugin.provider)}")

          Enum.each(plugin.subscriptions, fn {name, opts} ->
            IO.puts("        subscription: #{name} #{inspect(opts)}")
          end)
        end)

      _ ->
        IO.puts("    (none)")
    end

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
          IO.puts("")
          print_supported_extensions(provider_mod)
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

  defp print_supported_extensions(provider_mod) do
    extensions =
      for ext <- [
            :on_processor_init,
            :on_input,
            :on_output,
            :on_interval,
            :on_message,
            :on_snapshot,
            :on_terminate
          ],
          function_exported?(provider_mod, ext, 2),
          do: ext

    IO.puts("SUPPORTED EXTENSIONS:")
    Enum.each(extensions, &IO.puts("    --extension=#{&1}"))
  end

  @doc "List all available plugin providers."
  def list_plugins() do
    enabled_plugins = ConfigManager.get_enabled_plugins()

    IO.puts("Available Plugin Providers:")
    IO.puts("==========================")

    Enum.each(enabled_plugins, fn {provider_id, module} ->
      IO.puts("  #{provider_id}")
      IO.puts("    Module: #{inspect(module)}")

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

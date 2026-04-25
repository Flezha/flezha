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
  def processor_start(
        namespace,
        processor_id,
        factory_id,
        init_values,
        opts \\ [],
        plugins_json \\ "[]"
      ) do
    opts_kw =
      Payloader.json_to_map(opts)
      |> atom_converter()
      |> Keyword.new()

    case decode_plugins(plugins_json, normalize_namespace(namespace), processor_id) do
      {:ok, []} ->
        start_processor(namespace, processor_id, factory_id, init_values, opts_kw)

      {:ok, plugins} ->
        start_processor(
          namespace,
          processor_id,
          factory_id,
          init_values,
          Keyword.put(opts_kw, :plugins, plugins)
        )

      {:error, reason} ->
        IO.puts("Error starting processor: #{inspect(reason)}")
    end
  end

  defp start_processor(namespace, processor_id, factory_id, init_values, opts_kw) do
    processor = %Flezha.Processor.Builder{
      id: processor_id,
      factory_id: factory_id,
      init_values: Payloader.json_to_map(init_values),
      opts: opts_kw
    }

    case ProcessorManager.start_processor(normalize_namespace(namespace), processor) do
      {:ok, _} -> IO.puts("Processor has been started")
      {:error, reason} -> IO.puts("Error starting processor: #{inspect(reason)}")
    end
  end

  ## CLI --plugins decoder ----------------------------------------------------
  ##
  ## Turns the JSON array shipped by `flz processor start --plugins` into a list
  ## of `%Flezha.Plugin{}` structs ready for `Builder.opts = [plugins: ...]`.
  ## Returns errors matching design.md §D2b's vocabulary:
  ##   * `{:invalid_plugins_json, reason}`
  ##   * `{:unknown_provider, provider_id}`
  ##   * `{:invalid_subscription, details}`
  ##   * `{:invalid_opts, details}`

  defp decode_plugins(plugins_json, namespace, processor_id) do
    case decode_plugins_json(plugins_json) do
      {:ok, []} ->
        {:ok, []}

      {:ok, specs} ->
        providers = ConfigManager.get_enabled_plugins()

        Enum.reduce_while(specs, {:ok, []}, fn spec, {:ok, acc} ->
          case build_plugin(spec, providers, namespace, processor_id) do
            {:ok, plugin} -> {:cont, {:ok, [plugin | acc]}}
            {:error, _} = err -> {:halt, err}
          end
        end)
        |> case do
          {:ok, plugins} -> {:ok, Enum.reverse(plugins)}
          err -> err
        end

      {:error, reason} ->
        {:error, {:invalid_plugins_json, reason}}
    end
  end

  defp decode_plugins_json(nil), do: {:ok, []}
  defp decode_plugins_json(""), do: {:ok, []}
  defp decode_plugins_json("[]"), do: {:ok, []}
  defp decode_plugins_json("{}"), do: {:ok, []}

  defp decode_plugins_json(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, list} when is_list(list) -> {:ok, list}
      {:ok, _} -> {:error, :not_a_json_array}
      {:error, %Jason.DecodeError{} = err} -> {:error, Exception.message(err)}
    end
  end

  defp decode_plugins_json(_), do: {:ok, []}

  defp build_plugin(spec, providers, namespace, processor_id) when is_map(spec) do
    with {:ok, plugin_id} <- fetch_string(spec, "id"),
         {:ok, provider_id} <- fetch_string(spec, "provider"),
         {:ok, provider_mod} <- resolve_provider(providers, provider_id),
         {:ok, raw_opts} <- fetch_map(spec, "opts"),
         {:ok, subscriptions} <- build_subscriptions(Map.get(spec, "subscriptions", [])) do
      {:ok,
       %Plugin{
         id: plugin_id,
         provider: provider_mod,
         opts: atomize_map(raw_opts),
         subscriptions: subscriptions,
         namespace: namespace,
         processor_id: processor_id
       }}
    end
  end

  defp build_plugin(_, _, _, _), do: {:error, {:invalid_opts, :plugin_spec_not_a_map}}

  defp fetch_string(spec, key) do
    case Map.get(spec, key) do
      s when is_binary(s) and byte_size(s) > 0 -> {:ok, s}
      _ -> {:error, {:invalid_opts, {:missing_field, key}}}
    end
  end

  defp fetch_map(spec, key) do
    case Map.get(spec, key, %{}) do
      m when is_map(m) -> {:ok, m}
      _ -> {:error, {:invalid_opts, {:not_a_map, key}}}
    end
  end

  defp resolve_provider(providers, provider_id) do
    case Map.get(providers, provider_id) do
      nil -> {:error, {:unknown_provider, provider_id}}
      mod -> {:ok, mod}
    end
  end

  defp build_subscriptions(subs) when is_list(subs) and subs != [] do
    Enum.reduce_while(subs, {:ok, []}, fn sub, {:ok, acc} ->
      case build_subscription(sub) do
        {:ok, tuple} -> {:cont, {:ok, [tuple | acc]}}
        err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, acc} -> {:ok, Enum.reverse(acc)}
      err -> err
    end
  end

  defp build_subscriptions([]), do: {:error, {:invalid_subscription, :empty}}
  defp build_subscriptions(_), do: {:error, {:invalid_subscription, :not_a_list}}

  defp build_subscription([name, opts]) when is_binary(name) and is_map(opts) do
    {:ok, {String.to_atom(name), atomize_sub_opts(opts)}}
  end

  defp build_subscription([name]) when is_binary(name) do
    {:ok, {String.to_atom(name), []}}
  end

  defp build_subscription(name) when is_binary(name) do
    {:ok, {String.to_atom(name), []}}
  end

  defp build_subscription(%{"name" => name} = sub) when is_binary(name) do
    opts = Map.get(sub, "opts", %{})

    if is_map(opts) do
      {:ok, {String.to_atom(name), atomize_sub_opts(opts)}}
    else
      {:error, {:invalid_subscription, {:opts_not_a_map, name}}}
    end
  end

  defp build_subscription(other),
    do: {:error, {:invalid_subscription, {:unrecognised_shape, other}}}

  # Subscription opts carry both atom-keyed keys and atom-valued flags
  # (e.g. `on_error: :empty`). The JSON wire format loses both — keys and
  # values arrive as strings. Atomise both so the downstream
  # `Table.validate_on_error/1` sees the shape it expects.
  defp atomize_sub_opts(map) when is_map(map) do
    Enum.map(map, fn {k, v} ->
      {atomize_binary(k), atomize_sub_opt_value(v)}
    end)
  end

  defp atomize_binary(s) when is_binary(s), do: String.to_atom(s)
  defp atomize_binary(other), do: other

  defp atomize_sub_opt_value(v) when is_binary(v), do: String.to_atom(v)
  defp atomize_sub_opt_value(v), do: v

  # Atomise top-level keys of a plugin-opts map. Values (including nested maps
  # like `request_payload`) stay verbatim — plugins that need atoms in their
  # nested opts decode them themselves.
  defp atomize_map(map) when is_map(map) do
    Map.new(map, fn {k, v} ->
      key = if is_binary(k), do: String.to_atom(k), else: k
      {key, v}
    end)
  end

  defp atomize_map(other), do: other

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

  ## Installable-plugin subsystem bridges -------------------------------------

  @doc "Install a plugin from a `.fpz` archive."
  def install_plugin(path, _opts_json) do
    case Flezha.Plugin.Installer.install(path) do
      {:ok, installed} ->
        IO.puts(
          "Installed plugin #{installed.id}@#{installed.version} (status: #{installed.status})"
        )

      {:error, reason} ->
        IO.puts("Error installing plugin: #{inspect(reason)}.")
    end
  end

  @doc "Upgrade an installed plugin to a new version in `path`."
  def upgrade_plugin(plugin_id, path, _opts_json) do
    case Flezha.Plugin.Installer.upgrade(plugin_id, path) do
      {:ok, installed} ->
        IO.puts("Upgraded plugin #{installed.id} to #{installed.version}")

      {:error, reason} ->
        IO.puts("Error upgrading plugin: #{inspect(reason)}.")
    end
  end

  @doc "Uninstall an installed plugin by id."
  def uninstall_plugin(plugin_id, _opts_json) do
    case Flezha.Plugin.Installer.uninstall(plugin_id) do
      :ok -> IO.puts("Uninstalled plugin #{plugin_id}.")
      {:error, reason} -> IO.puts("Error uninstalling plugin: #{inspect(reason)}.")
    end
  end

  @doc "Enable an installed plugin (routes through the Installer)."
  def installer_enable_plugin(plugin_id, _opts_json) do
    case Flezha.Plugin.Installer.enable(plugin_id) do
      :ok -> IO.puts("Plugin #{plugin_id} enabled successfully.")
      {:error, reason} -> IO.puts("Error enabling plugin: #{inspect(reason)}.")
    end
  end

  @doc "Disable an installed plugin (routes through the Installer)."
  def installer_disable_plugin(plugin_id, _opts_json) do
    case Flezha.Plugin.Installer.disable(plugin_id) do
      :ok -> IO.puts("Plugin #{plugin_id} disabled successfully.")
      {:error, reason} -> IO.puts("Error disabling plugin: #{inspect(reason)}.")
    end
  end

  @doc """
  List every installed plugin. `format` is `"text" | "json"`.
  """
  def list_installed_plugins(format \\ "text") do
    plugins = Flezha.Plugin.Installer.list()

    case format do
      "json" ->
        plugins
        |> Enum.map(&installed_plugin_to_json/1)
        |> Jason.encode!()
        |> IO.puts()

      _ ->
        if plugins == [] do
          IO.puts("No plugins installed.")
        else
          IO.puts("Installed plugins:")

          Enum.each(plugins, fn p ->
            IO.puts("  #{p.id}@#{p.version} (#{p.status}) — #{p.description || ""}")
          end)
        end
    end
  end

  @doc "Return detailed information about an installed plugin."
  def installed_plugin_info(plugin_id) do
    case Flezha.Plugin.Installer.info(plugin_id) do
      {:ok, p} ->
        IO.puts("#{p.id}@#{p.version}")
        IO.puts("  Status: #{p.status}")
        IO.puts("  Description: #{p.description || "(none)"}")
        IO.puts("  Author: #{p.author || "(none)"}")
        IO.puts("  Modules: #{inspect(p.modules)}")
        IO.puts("  Provides:")

        Enum.each(p.provides, fn prov ->
          IO.puts("    - #{Map.get(prov, :id)} -> #{inspect(Map.get(prov, :provider))}")
        end)

      {:error, :not_found} ->
        IO.puts("Plugin '#{plugin_id}' not installed.")
    end
  end

  @doc """
  Returns `true` when `plugin_id` is tracked by `Flezha.Plugin.Installer`,
  so the CLI's `enable`/`disable` commands know which code path to use.
  """
  def installed_plugin?(plugin_id) do
    case Flezha.Plugin.Installer.info(plugin_id) do
      {:ok, _} -> true
      _ -> false
    end
  end

  defp installed_plugin_to_json(%Flezha.Plugin.Installer.InstalledPlugin{} = p) do
    %{
      id: p.id,
      version: p.version,
      status: p.status,
      description: p.description,
      author: p.author,
      modules: Enum.map(p.modules, &inspect/1),
      provides:
        Enum.map(p.provides, fn prov ->
          %{
            id: Map.get(prov, :id),
            provider: inspect(Map.get(prov, :provider)),
            scope: Map.get(prov, :scope)
          }
        end),
      installed_at: p.installed_at
    }
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

  ## Cluster snapshot/restore -------------------------------------------------

  @doc """
  Capture a cluster snapshot to `path`. Wired by `flz cluster snapshot`.
  Prints a one-line operator summary and exits with a non-zero status when
  the coordinator returns `{:error, _}`.
  """
  def cluster_snapshot(path, opts_json \\ "{}") do
    opts = decode_cluster_opts(opts_json)
    coord_opts = build_snapshot_opts(opts)

    case Flezha.Cluster.Snapshot.Coordinator.snapshot(path, coord_opts) do
      {:ok, %{processors: p, plugins: pl, bytes: b}} ->
        IO.puts("snapshot: #{p} processors, #{pl} plugins, #{b} bytes written to #{path}")

      {:error, reason} ->
        IO.puts(:stderr, "snapshot failed: #{inspect(reason)}")
        System.halt(1)
    end
  end

  @doc """
  Restore a cluster snapshot from `path`. Wired by `flz cluster restore`.
  Prints a one-line operator summary and exits with a non-zero status when
  the coordinator returns `{:error, _}`.
  """
  def cluster_restore(path, opts_json \\ "{}") do
    opts = decode_cluster_opts(opts_json)
    coord_opts = build_restore_opts(opts)

    case Flezha.Cluster.Snapshot.Coordinator.restore(path, coord_opts) do
      {:ok, %{processors: p, plugins: pl}} ->
        IO.puts("restore: #{p} processors, #{pl} plugins applied from #{path}")

      {:error, reason} ->
        IO.puts(:stderr, "restore failed: #{inspect(reason)}")
        System.halt(1)
    end
  end

  defp decode_cluster_opts(""), do: %{}
  defp decode_cluster_opts("{}"), do: %{}

  defp decode_cluster_opts(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, map} when is_map(map) -> map
      _ -> %{}
    end
  end

  defp build_snapshot_opts(map) do
    [
      peer_timeout_ms: parse_pos_int(Map.get(map, "peer_timeout_ms"), 5_000),
      strict?: parse_bool(Map.get(map, "strict"), false)
    ]
  end

  defp build_restore_opts(map) do
    namespace = Map.get(map, "namespace")
    namespace = if is_binary(namespace) and namespace != "", do: namespace, else: nil

    [
      peer_timeout_ms: parse_pos_int(Map.get(map, "peer_timeout_ms"), 5_000),
      warn_if_older_than_ms: parse_duration(Map.get(map, "warn_if_older_than")),
      namespace: namespace
    ]
  end

  defp parse_pos_int(nil, default), do: default
  defp parse_pos_int(n, _default) when is_integer(n) and n > 0, do: n

  defp parse_pos_int(n, default) when is_binary(n) do
    case Integer.parse(n) do
      {parsed, ""} when parsed > 0 -> parsed
      _ -> default
    end
  end

  defp parse_pos_int(_, default), do: default

  defp parse_bool(true, _), do: true
  defp parse_bool("true", _), do: true
  defp parse_bool(_, default), do: default

  # Accepts integers (raw ms), binary durations like "30s", "10m", "12h", "7d",
  # or `nil`/empty (treated as `:infinity`).
  defp parse_duration(nil), do: :infinity
  defp parse_duration(""), do: :infinity
  defp parse_duration("infinity"), do: :infinity
  defp parse_duration(n) when is_integer(n) and n > 0, do: n

  defp parse_duration(s) when is_binary(s) do
    case Regex.run(~r/^(\d+)([smhd]?)$/, s) do
      [_, num, ""] -> String.to_integer(num)
      [_, num, "s"] -> String.to_integer(num) * 1_000
      [_, num, "m"] -> String.to_integer(num) * 60 * 1_000
      [_, num, "h"] -> String.to_integer(num) * 60 * 60 * 1_000
      [_, num, "d"] -> String.to_integer(num) * 24 * 60 * 60 * 1_000
      _ -> :infinity
    end
  end

  defp parse_duration(_), do: :infinity
end

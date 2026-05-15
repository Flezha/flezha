defmodule Flezha.Service.CliManager do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Service.ConfigManager
  alias Flezha.Service.Payloader
  alias Flezha.Service.PluginManager
  alias Flezha.Service.RelayManager

  @doc "Starts a relay."
  def relay_start(
        namespace,
        relay_id,
        factory_id,
        init_values,
        opts \\ [],
        plugins_json \\ "[]"
      ) do
    opts_kw =
      Payloader.json_to_map(opts)
      |> atom_converter()
      |> Keyword.new()

    case decode_plugins(plugins_json, normalize_namespace(namespace), relay_id) do
      {:ok, []} ->
        start_relay(namespace, relay_id, factory_id, init_values, opts_kw)

      {:ok, plugins} ->
        start_relay(
          namespace,
          relay_id,
          factory_id,
          init_values,
          Keyword.put(opts_kw, :plugins, plugins)
        )

      {:error, reason} ->
        IO.puts("Error starting relay: #{inspect(reason)}")
    end
  end

  defp start_relay(namespace, relay_id, factory_id, init_values, opts_kw) do
    relay = %Flezha.Relay.Builder{
      id: relay_id,
      factory_id: factory_id,
      init_values: Payloader.json_to_map(init_values),
      opts: opts_kw
    }

    case RelayManager.start_relay(normalize_namespace(namespace), relay) do
      {:ok, _} -> IO.puts("Relay has been started")
      {:error, reason} -> IO.puts("Error starting relay: #{inspect(reason)}")
    end
  end

  ## CLI --plugins decoder ----------------------------------------------------
  ##
  ## Turns the JSON array shipped by `flz relay start --plugins` into a list
  ## of `%Flezha.Plugin{}` structs ready for `Builder.opts = [plugins: ...]`.
  ## Returns errors matching design.md §D2b's vocabulary:
  ##   * `{:invalid_plugins_json, reason}`
  ##   * `{:unknown_provider, provider_id}`
  ##   * `{:invalid_subscription, details}`
  ##   * `{:invalid_opts, details}`

  defp decode_plugins(plugins_json, namespace, relay_id) do
    case decode_plugins_json(plugins_json) do
      {:ok, []} ->
        {:ok, []}

      {:ok, specs} ->
        providers = ConfigManager.get_enabled_plugins()

        Enum.reduce_while(specs, {:ok, []}, fn spec, {:ok, acc} ->
          case build_plugin(spec, providers, namespace, relay_id) do
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

  defp build_plugin(spec, providers, namespace, relay_id) when is_map(spec) do
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
         relay_id: relay_id
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

  @doc "Stops a relay."
  def relay_stop(namespace, relay_id) do
    case RelayManager.stop_relay(normalize_namespace(namespace), relay_id) do
      :ok -> IO.puts("Relay has been stopped")
      {:error, reason} -> IO.puts("Error stopping relay: #{inspect(reason)}")
    end
  end

  @doc "Get relay state."
  def partition_state(namespace, relay_id) do
    namespace_display = normalize_namespace(namespace)

    case RelayManager.get_partition_state(
           normalize_namespace(namespace),
           relay_id
         ) do
      {:ok, json_state} when is_binary(json_state) ->
        IO.puts("State for relay '#{relay_id}' in namespace '#{namespace_display}':")
        IO.puts(json_state)

      {:ok, state} ->
        IO.puts("State for relay '#{relay_id}' in namespace '#{namespace_display}':")
        IO.puts(inspect(state))

      {:error, :not_found} ->
        IO.puts("Relay '#{relay_id}' not found in namespace '#{namespace_display}'.")

      {:error, reason} when is_binary(reason) ->
        IO.puts("Error getting relay state: #{reason}")

      {:error, reason} ->
        IO.puts("Error getting relay state: #{inspect(reason)}")

      other ->
        IO.puts("Unexpected result: #{inspect(other)}")
    end
  end

  @doc "Listens to a relay IO."
  def relay_listen(namespace, relay_id, io_type) do
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
      relay_id: relay_id,
      opts: %{
        recv_pid: self(),
        plugin_id: plugin_id,
        relay_id: relay_id,
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
      relay_id = Map.get(plugin_flags, "relay_id") || Map.get(opts, "relay_id")

      plugin = %Plugin{
        id: plugin_id,
        provider: provider_mod,
        namespace: namespace,
        relay_id: relay_id,
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
      "on_relay_init" => :on_relay_init,
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

  @doc "Lists executors for a specific namespace."
  def list_executors(namespace, output_format \\ "text") do
    namespace_display = normalize_namespace(namespace)

    case Flezha.list_executors(namespace_display) do
      {:ok, executors} when is_list(executors) ->
        context = %{key: :namespace, value: namespace_display, display: "namespace"}
        handle_list_output(executors, "executors", output_format, context)
    end
  end

  @doc "Lists relays for a specific namespace."
  def list_relays(namespace, output_format \\ "text") do
    namespace_display = normalize_namespace(namespace)

    case Flezha.list_relays(namespace_display) do
      {:ok, relays} ->
        context = %{key: :namespace, value: namespace_display, display: "namespace"}
        handle_list_output(relays, "relays", output_format, context)
    end
  end

  @doc "Shows detailed information about relays and their enabled plugins."
  def relay_info(namespace, output_format \\ "text") do
    namespace_display = normalize_namespace(namespace)

    case Flezha.list_relays(namespace_display) do
      {:ok, relays} ->
        if Enum.empty?(relays) do
          handle_empty_relays(namespace_display, output_format)
        else
          handle_relay_info_output(relays, namespace_display, output_format)
        end
    end
  end

  defp handle_empty_relays(namespace_display, "json") do
    result = %{
      namespace: namespace_display,
      relays: [],
      message: "No relays found"
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_empty_relays(namespace_display, _) do
    IO.puts("No relays found for namespace '#{namespace_display}'.")
  end

  defp handle_relay_info_output(relays, namespace_display, "json") do
    relay_data =
      Enum.map(relays, fn relay_id ->
        case RelayManager.get_overwatch_state(namespace_display, relay_id) do
          {:ok, state} ->
            %{
              id: relay_id,
              executor_id: state.executor_id,
              namespace: state.namespace,
              default_input_type: state.default_input_type,
              plugins: format_plugins_json(state.extensions)
            }

          {:error, reason} ->
            %{id: relay_id, error: inspect(reason)}
        end
      end)

    result = %{namespace: namespace_display, relays: relay_data}
    IO.puts(Jason.encode!(result))
  end

  defp handle_relay_info_output(relays, namespace_display, _) do
    IO.puts("Relay information for namespace '#{namespace_display}':")
    IO.puts("")

    Enum.each(relays, fn relay_id ->
      case RelayManager.get_overwatch_state(namespace_display, relay_id) do
        {:ok, state} ->
          display_relay_info(relay_id, state)

        {:error, reason} ->
          IO.puts("Relay: #{relay_id}")
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

  defp display_relay_info(relay_id, state) do
    IO.puts("Relay: #{relay_id}")
    IO.puts("  Executor ID: #{state.executor_id}")
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
            :on_relay_init,
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

  ## Boot status / await / legacy snapshot import ----------------------------

  @doc """
  `flz boot status` — per-namespace recovery state and persistent-registry
  row count. Returns immediately; for blocking semantics use `boot_await/1`.
  """
  def boot_status() do
    rows =
      Flezha.Service.HubManager.get_running_namespaces()
      |> Enum.map(fn namespace ->
        hub_id = Flezha.Service.Identificator.hub(namespace)
        recovery_state = safe_recovery_state(hub_id)
        registry_count = safe_registry_count(hub_id)

        %{namespace: namespace, recovery_state: recovery_state, registry_rows: registry_count}
      end)

    IO.puts("namespace                          recovery_state    registry_rows")
    IO.puts("----------------------------------------------------------------")

    Enum.each(rows, fn row ->
      IO.puts(
        String.pad_trailing(row.namespace, 35) <>
          String.pad_trailing(Atom.to_string(row.recovery_state), 17) <>
          Integer.to_string(row.registry_rows)
      )
    end)

    :ok
  end

  defp safe_recovery_state(hub_id) do
    try do
      ProcessHub.recovery_state(hub_id)
    catch
      _, _ -> :unknown
    end
  end

  defp safe_registry_count(hub_id) do
    try do
      hub_id |> ProcessHub.registry_dump() |> Enum.count()
    catch
      _, _ -> 0
    end
  end

  @doc """
  `flz boot await --timeout SECS` — block until every namespace hub
  reports `:normal`, or the per-hub timeout elapses.
  """
  def boot_await(timeout_ms \\ 60_000) when is_integer(timeout_ms) do
    namespaces = Flezha.Service.HubManager.get_running_namespaces()

    failures =
      Enum.flat_map(namespaces, fn namespace ->
        hub_id = Flezha.Service.Identificator.hub(namespace)

        try do
          case ProcessHub.await_normal(hub_id, timeout_ms) do
            :ok -> []
            {:error, reason} -> [{namespace, reason}]
            other -> [{namespace, other}]
          end
        catch
          kind, reason -> [{namespace, {kind, reason}}]
        end
      end)

    case failures do
      [] ->
        IO.puts("boot await: all namespaces reached :normal")
        :ok

      _ ->
        Enum.each(failures, fn {ns, reason} ->
          IO.puts(:stderr, "boot await: #{ns}: #{inspect(reason)}")
        end)

        System.halt(1)
    end
  end

  @doc """
  `flz boot import-legacy-snapshot <file> [--force]` — read a legacy `.flz`
  ETF snapshot and write each relay's child spec into ProcessHub's
  persistent registry via `start_children/3`. Refuses to import on a
  non-empty hub unless `force?` is `true`.
  """
  def boot_import_legacy_snapshot(path, force? \\ false) do
    with {:ok, blob} <- File.read(path),
         {:ok, decoded} <- safe_binary_to_term(blob),
         {:ok, payload} <- validate_legacy_payload(decoded),
         :ok <- ensure_force_or_empty(payload, force?) do
      run_legacy_import(payload)
    else
      {:error, reason} ->
        IO.puts(:stderr, "boot import-legacy-snapshot: #{inspect(reason)}")
        System.halt(1)
    end
  end

  defp safe_binary_to_term(blob) do
    try do
      {:ok, :erlang.binary_to_term(blob, [:safe])}
    rescue
      _ -> {:error, :corrupt_snapshot}
    end
  end

  defp validate_legacy_payload({:flezha_snapshot, 1, payload}) when is_map(payload),
    do: {:ok, payload}

  defp validate_legacy_payload(_), do: {:error, :unsupported_snapshot_format}

  defp ensure_force_or_empty(_payload, true), do: :ok

  defp ensure_force_or_empty(payload, false) do
    namespaces = Map.get(payload, :namespaces, [])

    Enum.find_value(namespaces, :ok, fn ns ->
      hub_id = Flezha.Service.Identificator.hub(ns)

      if safe_registry_count(hub_id) > 0 do
        {:error, {:hub_not_empty, ns}}
      else
        nil
      end
    end)
  end

  defp run_legacy_import(payload) do
    relays = Map.get(payload, :relays, [])

    Enum.each(relays, fn entry ->
      namespace = Map.fetch!(entry, :namespace)
      cspec = Map.fetch!(entry, :cspec)
      hub_id = Flezha.Service.Identificator.hub(namespace)
      _ = ProcessHub.start_children(hub_id, [cspec], async_wait: false)
    end)

    IO.puts("boot import-legacy-snapshot: imported #{length(relays)} relays")
    :ok
  end

  # ---------- `flz state` subcommand handlers ----------

  @doc """
  `flz state list <relay_class>` — print one row per Partition under
  `<relay_class>` with `wal_seq`, `snapshot_seq`, `wal_bytes`.
  """
  def state_list(relay_class, output_format \\ "text") do
    rows =
      for {{rc, sh, pk} = pid, counters} <- Flezha.State.Metrics.snapshot_all(),
          rc == relay_class do
        wal_bytes =
          case :file.read_file_info(Flezha.State.Registry.wal_path(pid)) do
            {:ok, info} -> elem(info, 1)
            _ -> 0
          end

        %{
          relay_class: rc,
          shard_id: sh,
          partition_key: pk,
          wal_seq: counters.wal_seq,
          snapshot_seq: counters.snapshot_seq,
          wal_bytes: wal_bytes,
          flush_latency_ms_p99: counters.flush_latency_ms_p99,
          fsync_durability_lag_ms: counters.fsync_durability_lag_ms,
          dedup_drops_total: counters.dedup_drops_total
        }
      end

    case output_format do
      "json" ->
        IO.puts(Jason.encode!(%{relay_class: relay_class, partitions: rows}))

      _ ->
        IO.puts("Partitions under relay #{relay_class}:\n")
        Enum.each(rows, &print_state_row/1)
    end
  end

  defp print_state_row(row) do
    IO.puts("  #{row.shard_id}/#{row.partition_key}")
    IO.puts("    wal_seq:                 #{row.wal_seq}")
    IO.puts("    snapshot_seq:            #{row.snapshot_seq}")
    IO.puts("    wal_bytes:               #{row.wal_bytes}")
    IO.puts("    flush_latency_ms_p99:    #{row.flush_latency_ms_p99}")
    IO.puts("    fsync_durability_lag_ms: #{row.fsync_durability_lag_ms}")
    IO.puts("    dedup_drops_total:       #{row.dedup_drops_total}")
    IO.puts("")
  end

  @doc """
  `flz state at <relay/shard/partition> <seq | timestamp>` — point-in-time
  query. Resolves the path string into a `(executor_id, relay,
  shard, partition_key)` by scanning the on-disk state tree (one match
  expected; ambiguous paths print all matches and exit non-zero).
  """
  def state_at(path_string, target_string, output_format \\ "text") do
    case parse_state_path(path_string) do
      {:ok, partition_id} ->
        target = parse_state_target(target_string)

        case Flezha.State.TimeTravel.query(partition_id, target) do
          {:ok, result} ->
            print_state_at(partition_id, target, result, output_format)

          {:error, %{kind: kind, message: msg, earliest_available: earliest}} ->
            IO.puts(
              :stderr,
              "state at: #{kind}: #{msg} (earliest_available=#{inspect(earliest)})"
            )

            System.halt(1)
        end

      {:error, reason} ->
        IO.puts(:stderr, "state at: #{reason}")
        System.halt(1)
    end
  end

  defp print_state_at(_partition_id, _target, result, "json") do
    IO.puts(
      Jason.encode!(%{
        terminal_seq: result.terminal_seq,
        snapshot_size: byte_size(result.snapshot_bytes),
        change_log_size: byte_size(result.change_log_bytes)
      })
    )
  end

  defp print_state_at(partition_id, target, result, _) do
    {rc, sh, pk} = partition_id

    IO.puts("state at #{rc}/#{sh}/#{pk} target=#{inspect(target)}:")
    IO.puts("  terminal_seq:    #{result.terminal_seq}")
    IO.puts("  snapshot bytes:  #{byte_size(result.snapshot_bytes)}")
    IO.puts("  change_log bytes:#{byte_size(result.change_log_bytes)}")
  end

  @doc """
  `flz state rotate <relay/shard/partition>` — force a compaction by
  asking Overwatch to dispatch a `CompactPartition` to the SDK.
  """
  def state_rotate(path_string) do
    case parse_state_path(path_string) do
      {:ok, {rc, sh, pk}} ->
        case lookup_executor_for_partition(rc, sh, pk) do
          {:ok, executor_id} ->
            case Flezha.Relay.Overwatch.dispatch_compact_partition(executor_id, {rc, sh, pk}) do
              {:ok, _ref} ->
                IO.puts("compaction queued for #{rc}/#{sh}/#{pk}")

              {:error, reason} ->
                IO.puts(:stderr, "state rotate: #{inspect(reason)}")
                System.halt(1)
            end

          {:error, reason} ->
            IO.puts(:stderr, "state rotate: #{reason}")
            System.halt(1)
        end

      {:error, reason} ->
        IO.puts(:stderr, "state rotate: #{reason}")
        System.halt(1)
    end
  end

  defp lookup_executor_for_partition(rc, _sh, _pk) do
    namespace = "default"

    case Flezha.Service.HubManager.get_registry(namespace) do
      {:ok, registry} ->
        match =
          Enum.find_value(registry, fn
            {_relay_id, %{metadata: %{tag: tag, relay: %{relay_class: ^rc}}}} when is_binary(tag) ->
              tag

            _ ->
              nil
          end)

        case match do
          nil -> {:error, "no active executor owns relay_class=#{rc}"}
          eid -> {:ok, eid}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  `flz state retention <relay_class> <hours>` — set runtime retention
  for `<relay_class>`. Persisted across the next Application boot only
  if the executor separately writes it to runtime.exs.
  """
  def state_retention(relay_class, hours_string) do
    case Integer.parse(hours_string) do
      {hours, ""} when hours >= 0 ->
        current = Application.get_env(:flezha, :time_travel_retention_hours, %{})
        updated = Map.put(current, relay_class, hours)
        Application.put_env(:flezha, :time_travel_retention_hours, updated, persistent: true)
        IO.puts("retention[#{relay_class}] = #{hours} hours (in-memory; persist via runtime.exs)")

      _ ->
        IO.puts(:stderr, "state retention: hours must be a non-negative integer")
        System.halt(1)
    end
  end

  defp parse_state_path(path_string) do
    case String.split(path_string, "/", parts: 3) do
      [rc, sh, pk] ->
        if has_persisted_state?(rc, sh, pk) do
          {:ok, {rc, sh, pk}}
        else
          {:error, "no persisted state at #{path_string}"}
        end

      _ ->
        {:error, "expected <relay>/<shard>/<partition_key>; got #{path_string}"}
    end
  end

  defp has_persisted_state?(rc, sh, pk) do
    Enum.any?(Flezha.State.Registry.all_partitions(), fn {r, s, p} ->
      r == rc and s == sh and p == pk
    end)
  end

  defp parse_state_target(s) do
    case Integer.parse(s) do
      {n, ""} ->
        {:seq, n}

      _ ->
        # Try ISO 8601 timestamp.
        case DateTime.from_iso8601(s) do
          {:ok, dt, _} -> {:timestamp, DateTime.to_unix(dt)}
          _ -> {:seq, 0}
        end
    end
  end

  # ---------------------------------------------------------------------------
  # Controller management RPCs (graceful shutdown / status)
  # ---------------------------------------------------------------------------

  @doc """
  `flz controller stop` RPC. Drives `Flezha.Shutdown.Coordinator.shutdown/2`
  and prints structured progress lines. The final line is a
  machine-readable marker the Rust CLI scans to derive its exit code:

      flz-controller-stop-result: clean | partial | forced | already_in_progress

  Options (keys may be atoms or binaries):
    * `:timeout_ms` — overrides the configured timeout
    * `:no_wait` — fire-and-forget; print marker `clean` and return immediately
    * `:force` — skip quiesce + drain, finalise only
  """
  def controller_stop(opts \\ []) do
    opts_map = normalize_controller_stop_opts(opts)

    timeout_ms = Map.get(opts_map, :timeout_ms)
    no_wait = Map.get(opts_map, :no_wait, false)
    force = Map.get(opts_map, :force, false)

    shutdown_opts =
      []
      |> maybe_put(:timeout_ms, timeout_ms)
      |> maybe_put(:force, force || nil)

    if no_wait do
      _ = Task.start(fn -> Flezha.Shutdown.Coordinator.shutdown(:cli, shutdown_opts) end)
      IO.puts("[shutdown] dispatched (no-wait)")
      IO.puts("flz-controller-stop-result: clean")
      :ok
    else
      handler_id = {__MODULE__, :controller_stop_progress, make_ref()}

      :ok =
        :telemetry.attach_many(
          handler_id,
          [
            [:flezha, :shutdown, :phase_started],
            [:flezha, :shutdown, :phase_completed],
            [:flezha, :shutdown, :drain_progress],
            [:flezha, :shutdown, :timeout]
          ],
          &__MODULE__.controller_stop_progress_handler/4,
          %{caller: self()}
        )

      start_ms = System.monotonic_time(:millisecond)

      try do
        case Flezha.Shutdown.Coordinator.shutdown(:cli, shutdown_opts) do
          :ok ->
            duration = System.monotonic_time(:millisecond) - start_ms
            outcome = if force, do: :forced, else: :clean

            IO.puts(
              "[shutdown] complete duration_ms=#{duration} outcome=#{outcome}"
            )

            IO.puts("flz-controller-stop-result: #{outcome}")
            :ok

          {:partial, still_local} ->
            duration = System.monotonic_time(:millisecond) - start_ms

            IO.puts(
              "[shutdown] complete duration_ms=#{duration} outcome=partial still_local=#{length(still_local)}"
            )

            IO.puts("flz-controller-stop-result: partial")
            :ok

          other ->
            IO.puts("[shutdown] unexpected coordinator return: #{inspect(other)}")
            IO.puts("flz-controller-stop-result: partial")
            :ok
        end
      catch
        :exit, {:noproc, _} ->
          IO.puts("[shutdown] coordinator not running")
          IO.puts("flz-controller-stop-result: already_in_progress")
          :ok

        :exit, reason ->
          IO.puts("[shutdown] coordinator exit: #{inspect(reason)}")
          IO.puts("flz-controller-stop-result: already_in_progress")
          :ok
      after
        :telemetry.detach(handler_id)
      end
    end
  end

  @doc false
  def controller_stop_progress_handler(event, measurements, metadata, _config) do
    case event do
      [:flezha, :shutdown, :phase_started] ->
        IO.puts(:stderr, "[shutdown] phase=#{metadata.phase} started")

      [:flezha, :shutdown, :phase_completed] ->
        IO.puts(
          :stderr,
          "[shutdown] phase=#{metadata.phase} completed duration_ms=#{measurements[:duration_ms] || 0}"
        )

      [:flezha, :shutdown, :drain_progress] ->
        IO.puts(
          :stderr,
          "drain: migrated=#{measurements.migrated} remaining=#{measurements.remaining} elapsed_ms=#{measurements.elapsed_ms}"
        )

      [:flezha, :shutdown, :timeout] ->
        IO.puts(
          :stderr,
          "[shutdown] timeout phase=#{metadata.phase} still_local=#{length(metadata.still_local)}"
        )

      _ ->
        :ok
    end
  rescue
    _ -> :ok
  end

  defp normalize_controller_stop_opts(opts) when is_list(opts) do
    Enum.into(opts, %{}, fn
      {k, v} when is_atom(k) -> {k, v}
      {k, v} when is_binary(k) -> {String.to_atom(k), v}
    end)
  end

  defp normalize_controller_stop_opts(opts) when is_map(opts) do
    Enum.into(opts, %{}, fn
      {k, v} when is_atom(k) -> {k, v}
      {k, v} when is_binary(k) -> {String.to_atom(k), v}
    end)
  end

  defp maybe_put(list, _k, nil), do: list
  defp maybe_put(list, _k, false), do: list
  defp maybe_put(list, k, v), do: Keyword.put(list, k, v)

  @doc """
  `flz controller status` RPC. Prints a human-readable status block
  including graceful-shutdown state and configuration knobs.
  """
  def controller_status(output_format \\ "text") do
    status = safe_shutdown_status()

    case output_format do
      "json" ->
        json =
          status
          |> Map.put(:node, Atom.to_string(node()))
          |> jsonable()
          |> Jason.encode!(pretty: true)

        IO.puts(json)

      _ ->
        IO.puts("node:                          #{node()}")
        IO.puts("shutdown_in_progress:          #{status.shutdown_in_progress}")
        IO.puts("shutdown_phase:                #{inspect(status.shutdown_phase)}")
        IO.puts("signal_handler_status:         #{status.signal_handler_status}")
        IO.puts("graceful_shutdown_enabled:     #{status.graceful_shutdown_enabled}")
        IO.puts("graceful_shutdown_timeout_ms:  #{status.graceful_shutdown_timeout_ms}")
    end

    :ok
  end

  defp safe_shutdown_status() do
    try do
      Flezha.Shutdown.Coordinator.status()
    catch
      :exit, _ ->
        %{
          shutdown_in_progress: false,
          shutdown_phase: nil,
          signal_handler_status: :unknown,
          graceful_shutdown_enabled:
            Application.get_env(:flezha, :graceful_shutdown_enabled, true),
          graceful_shutdown_timeout_ms:
            Application.get_env(:flezha, :graceful_shutdown_timeout_ms, 60_000)
        }
    end
  end

  defp jsonable(map) when is_map(map) do
    Enum.into(map, %{}, fn
      {k, v} when is_atom(v) and not is_boolean(v) and not is_nil(v) -> {k, Atom.to_string(v)}
      {k, v} -> {k, v}
    end)
  end

  # ---------------------------------------------------------------------------
  # `flz controller mem-top` — operator-invoked memory inspector
  # ---------------------------------------------------------------------------

  @top_proc_count 20

  @doc """
  `flz controller mem-top` RPC. Captures a single snapshot of the
  Controller's memory hot spots: top-20 processes by `:memory`, ETS
  tables by `:memory`, DETS files under `app_data/process_hub/`, and
  the `:recon_alloc.memory(:allocated_types)` allocator breakdown.

  `output_format` is `"text"` or `"json"`.
  """
  def controller_mem_top(output_format \\ "text") do
    snapshot = build_mem_top_snapshot()

    case output_format do
      "json" -> IO.puts(Jason.encode!(snapshot))
      _ -> print_mem_top_text(snapshot)
    end

    :ok
  end

  @doc false
  def build_mem_top_snapshot do
    %{
      captured_at: DateTime.utc_now() |> DateTime.to_iso8601(),
      node: Atom.to_string(node()),
      erlang_memory: erlang_memory_map(),
      top_processes: top_processes(@top_proc_count),
      ets_tables: top_ets_tables(),
      dets_files: dets_files_under_process_hub(),
      recon_alloc_allocated_types: recon_alloc_allocated_types()
    }
  end

  defp erlang_memory_map do
    :erlang.memory()
    |> Enum.into(%{})
    |> Map.take([:total, :binary, :processes, :ets, :atom, :code, :system])
  end

  defp top_processes(limit) do
    Process.list()
    |> Enum.map(&process_info_row/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.sort_by(& &1.memory, :desc)
    |> Enum.take(limit)
  end

  defp process_info_row(pid) do
    case Process.info(pid, [
           :memory,
           :registered_name,
           :initial_call,
           :current_function,
           :message_queue_len,
           :heap_size,
           :total_heap_size
         ]) do
      nil ->
        nil

      info ->
        %{
          pid: :erlang.pid_to_list(pid) |> List.to_string(),
          memory: Keyword.get(info, :memory, 0),
          registered_name: format_registered_name(Keyword.get(info, :registered_name)),
          initial_call: inspect(Keyword.get(info, :initial_call)),
          current_function: inspect(Keyword.get(info, :current_function)),
          message_queue_len: Keyword.get(info, :message_queue_len, 0),
          heap_size: Keyword.get(info, :heap_size, 0),
          total_heap_size: Keyword.get(info, :total_heap_size, 0)
        }
    end
  catch
    # process_info/2 raises `badarg` if the pid dies between Process.list/0
    # and the introspection call. Treat as "gone" rather than crashing the
    # whole snapshot.
    :error, :badarg -> nil
  end

  defp format_registered_name([]), do: nil
  defp format_registered_name(name) when is_atom(name), do: Atom.to_string(name)
  defp format_registered_name(other), do: inspect(other)

  defp top_ets_tables do
    :ets.all()
    |> Enum.map(&ets_table_row/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.sort_by(& &1.memory_bytes, :desc)
  end

  defp ets_table_row(tab) do
    memory_words = :ets.info(tab, :memory)
    size = :ets.info(tab, :size)
    name = :ets.info(tab, :name)

    if is_integer(memory_words) do
      %{
        name: inspect(name),
        size: size || 0,
        memory_bytes: memory_words * :erlang.system_info(:wordsize)
      }
    end
  catch
    :error, :badarg -> nil
  end

  defp dets_files_under_process_hub do
    root = Path.join([File.cwd!(), "app_data", "process_hub"])

    if File.exists?(root) do
      root
      |> list_files_recursive()
      |> Enum.filter(&String.ends_with?(&1, ".dets"))
      |> Enum.map(fn path ->
        size =
          case File.stat(path) do
            {:ok, %File.Stat{size: s}} -> s
            _ -> 0
          end

        %{path: Path.relative_to_cwd(path), size_bytes: size}
      end)
      |> Enum.sort_by(& &1.size_bytes, :desc)
    else
      []
    end
  end

  defp list_files_recursive(dir) do
    case File.ls(dir) do
      {:ok, entries} ->
        Enum.flat_map(entries, fn entry ->
          path = Path.join(dir, entry)

          cond do
            File.dir?(path) -> list_files_recursive(path)
            File.regular?(path) -> [path]
            true -> []
          end
        end)

      _ ->
        []
    end
  end

  defp recon_alloc_allocated_types do
    :recon_alloc.memory(:allocated_types)
    |> Enum.into(%{}, fn {k, v} -> {Atom.to_string(k), v} end)
  end

  defp print_mem_top_text(snapshot) do
    IO.puts("Controller memory snapshot — #{snapshot.captured_at} (#{snapshot.node})")
    IO.puts("")

    IO.puts("erlang_memory (bytes):")

    Enum.each(snapshot.erlang_memory, fn {k, v} ->
      IO.puts("  #{String.pad_trailing(Atom.to_string(k), 12)} #{v}")
    end)

    IO.puts("")
    IO.puts("Top #{length(snapshot.top_processes)} processes by :memory:")

    IO.puts(
      "  " <>
        String.pad_trailing("memory", 12) <>
        String.pad_trailing("msgq", 8) <>
        String.pad_trailing("name / initial_call", 60)
    )

    Enum.each(snapshot.top_processes, fn p ->
      label = p.registered_name || p.initial_call

      IO.puts(
        "  " <>
          String.pad_trailing(Integer.to_string(p.memory), 12) <>
          String.pad_trailing(Integer.to_string(p.message_queue_len), 8) <>
          to_string(label)
      )
    end)

    IO.puts("")
    IO.puts("ETS tables by :memory (top 20):")

    snapshot.ets_tables
    |> Enum.take(20)
    |> Enum.each(fn t ->
      IO.puts(
        "  " <>
          String.pad_trailing(Integer.to_string(t.memory_bytes), 12) <>
          String.pad_trailing("size=#{t.size}", 14) <>
          t.name
      )
    end)

    IO.puts("")
    IO.puts("DETS files under app_data/process_hub:")

    if snapshot.dets_files == [] do
      IO.puts("  (none)")
    else
      Enum.each(snapshot.dets_files, fn d ->
        IO.puts("  " <> String.pad_trailing(Integer.to_string(d.size_bytes), 12) <> d.path)
      end)
    end

    IO.puts("")
    IO.puts(":recon_alloc.memory(:allocated_types):")

    snapshot.recon_alloc_allocated_types
    |> Enum.sort_by(fn {_, v} -> v end, :desc)
    |> Enum.each(fn {k, v} ->
      IO.puts("  #{String.pad_trailing(k, 24)} #{v}")
    end)
  end
end

defmodule Flezha.Service.CliManager do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Service.ConfigManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.Payloader
  alias Flezha.Service.PluginManager
  alias Flezha.Service.OperativeManager

  @doc "Starts a operative."
  def operative_start(
        namespace,
        operative_id,
        factory_id,
        init_values,
        opts \\ [],
        plugins_json \\ "[]",
        kind \\ "continuous"
      ) do
    opts_kw =
      Payloader.json_to_map(opts)
      |> atom_converter()
      |> Keyword.new()

    case decode_plugins(plugins_json, normalize_namespace(namespace), operative_id) do
      {:ok, []} ->
        start_operative(namespace, operative_id, factory_id, init_values, opts_kw, kind)

      {:ok, plugins} ->
        start_operative(
          namespace,
          operative_id,
          factory_id,
          init_values,
          Keyword.put(opts_kw, :plugins, plugins),
          kind
        )

      {:error, reason} ->
        IO.puts("Error starting operative: #{inspect(reason)}")
    end
  end

  defp start_operative(namespace, operative_id, factory_id, init_values, opts_kw, kind) do
    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: factory_id,
      kind: parse_kind(kind),
      init_values: Payloader.json_to_map(init_values),
      opts: opts_kw
    }

    case OperativeManager.start_operative(normalize_namespace(namespace), operative) do
      {:ok, _} -> IO.puts("Operative has been started")
      {:error, reason} -> IO.puts("Error starting operative: #{inspect(reason)}")
    end
  end

  # Resolve the wire kind string to its atom without minting atoms from
  # untrusted input: only already-known kinds convert; anything else stays a
  # string, which `Validator.valid_kind/1` rejects at start.
  defp parse_kind(kind) when kind in [nil, "", "continuous"], do: :continuous

  defp parse_kind(kind) when is_binary(kind) do
    String.to_existing_atom(kind)
  rescue
    ArgumentError -> kind
  end

  defp parse_kind(kind), do: kind

  ## CLI --plugins decoder ----------------------------------------------------
  ##
  ## Turns the JSON array shipped by `flz operative start --plugins` into a list
  ## of `%Flezha.Plugin{}` structs ready for `Builder.opts = [plugins: ...]`.
  ## Returns errors matching design.md §D2b's vocabulary:
  ##   * `{:invalid_plugins_json, reason}`
  ##   * `{:unknown_provider, provider_id}`
  ##   * `{:invalid_subscription, details}`
  ##   * `{:invalid_opts, details}`

  defp decode_plugins(plugins_json, namespace, operative_id) do
    case decode_plugins_json(plugins_json) do
      {:ok, []} ->
        {:ok, []}

      {:ok, specs} ->
        providers = ConfigManager.get_enabled_plugins()

        Enum.reduce_while(specs, {:ok, []}, fn spec, {:ok, acc} ->
          case build_plugin(spec, providers, namespace, operative_id) do
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

  defp decode_plugins_json(j) when j in [nil, "", "[]", "{}"], do: {:ok, []}

  defp decode_plugins_json(json) when is_binary(json) do
    case Jason.decode(json) do
      {:ok, list} when is_list(list) -> {:ok, list}
      {:ok, _} -> {:error, :not_a_json_array}
      {:error, %Jason.DecodeError{} = err} -> {:error, Exception.message(err)}
    end
  end

  defp decode_plugins_json(_), do: {:ok, []}

  defp build_plugin(spec, providers, namespace, operative_id) when is_map(spec) do
    with {:ok, plugin_id} <- fetch_string(spec, "id"),
         {:ok, provider_id} <- fetch_string(spec, "provider"),
         {:ok, provider_mod} <- resolve_provider(providers, provider_id),
         {:ok, raw_opts} <- fetch_map(spec, "opts"),
         {:ok, subscriptions} <- build_subscriptions(Map.get(spec, "subscriptions", [])) do
      {:ok,
       %Plugin{
         id: plugin_id,
         provider: provider_mod,
         opts: atomize_keys(raw_opts),
         subscriptions: subscriptions,
         namespace: namespace,
         operative_id: operative_id
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

  @doc "Stops a operative."
  def operative_stop(namespace, operative_id) do
    case OperativeManager.stop_operative(normalize_namespace(namespace), operative_id) do
      :ok -> IO.puts("Operative has been stopped")
      {:error, reason} -> IO.puts("Error stopping operative: #{inspect(reason)}")
    end
  end

  @doc "Get operative state."
  def segment_state(namespace, operative_id) do
    namespace_display = normalize_namespace(namespace)

    case OperativeManager.get_segment_state(
           normalize_namespace(namespace),
           operative_id
         ) do
      {:ok, json_state} when is_binary(json_state) ->
        IO.puts("State for operative '#{operative_id}' in namespace '#{namespace_display}':")
        IO.puts(json_state)

      {:ok, state} ->
        IO.puts("State for operative '#{operative_id}' in namespace '#{namespace_display}':")
        IO.puts(inspect(state))

      {:error, :not_found} ->
        IO.puts("Operative '#{operative_id}' not found in namespace '#{namespace_display}'.")

      {:error, {:unsupported_for_kind, :persistent_state}} ->
        IO.puts(
          "Operative '#{operative_id}' in namespace '#{namespace_display}' has no per-segment state: " <>
            "its kind does not support state introspection."
        )

        System.halt(1)

      {:error, reason} when is_binary(reason) ->
        IO.puts("Error getting operative state: #{reason}")

      {:error, reason} ->
        IO.puts("Error getting operative state: #{inspect(reason)}")

      other ->
        IO.puts("Unexpected result: #{inspect(other)}")
    end
  end

  @doc "Listens to a operative IO."
  def operative_listen(namespace, operative_id, io_type) do
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
      operative_id: operative_id,
      opts: %{
        recv_pid: self(),
        plugin_id: plugin_id,
        operative_id: operative_id,
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
      operative_id = Map.get(plugin_flags, "operative_id") || Map.get(opts, "operative_id")

      plugin = %Plugin{
        id: plugin_id,
        provider: provider_mod,
        namespace: namespace,
        operative_id: operative_id,
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

  @doc false
  def trigger_to_subscription("on_input"), do: [{:on_input, []}]
  def trigger_to_subscription("on_output"), do: [{:on_output, []}]
  def trigger_to_subscription("interval"), do: [{:on_interval, [interval_ms: 5_000]}]
  def trigger_to_subscription("proactive"), do: [{:on_interval, [interval_ms: 5_000]}]
  def trigger_to_subscription(_), do: [{:on_input, []}]

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
      "on_operative_init" => :on_operative_init,
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

  @doc """
  Lists a namespace's operatives with the reconciler's per-operative health, assigned
  executor, and live node. Reads the durable registry, so it surfaces FAILED
  operatives (whose only placement is a dead node) and is multi-node-safe — it no
  longer crashes on a operative registered across nodes.
  """
  def list_operatives(namespace, output_format \\ "text") do
    namespace_display = normalize_namespace(namespace)
    rows = HubManager.operative_rows(namespace_display)
    render_operative_list(rows, namespace_display, output_format)
  end

  defp render_operative_list(rows, namespace, "json") do
    result = %{
      namespace: namespace,
      operatives:
        Enum.map(rows, fn r ->
          %{
            id: r.id,
            health: r.health,
            executor: r.executor,
            node: r.node && to_string(r.node)
          }
        end)
    }

    IO.puts(Jason.encode!(result))
  end

  defp render_operative_list([], namespace, _) do
    IO.puts("No operatives found for namespace '#{namespace}'.")
  end

  defp render_operative_list(rows, namespace, _) do
    IO.puts("Operatives in namespace '#{namespace}':")
    IO.puts("")

    IO.puts([
      "  ",
      String.pad_trailing("OPERATIVE", 34),
      String.pad_trailing("HEALTH", 10),
      String.pad_trailing("NODE", 22),
      "EXECUTOR"
    ])

    Enum.each(rows, fn r ->
      IO.puts([
        "  ",
        String.pad_trailing(to_string(r.id), 34),
        String.pad_trailing(to_string(r.health), 10),
        String.pad_trailing(label_or(r.node, "(unplaced)"), 22),
        label_or(r.executor, "(none)")
      ])
    end)
  end

  @doc """
  Cluster topology for operators: the elected leader, whether this node is the
  leader, each node's live operative count, any unplaced (FAILED) operatives, and a
  health rollup. Composes `HubManager` registry access with the reconciler's
  `Leader` so a split view (one node sees operatives, its peer sees none) is
  visible at a glance.
  """
  def cluster_status(namespace, output_format \\ "text") do
    namespace_display = normalize_namespace(namespace)
    nodes = HubManager.get_hub_nodes(namespace_display, true)
    rows = HubManager.operative_rows(namespace_display)
    counts = Enum.frequencies_by(rows, & &1.node)
    health = Enum.frequencies_by(rows, & &1.health)
    {recovery_per_node, marker_per_node} = cluster_status_recovery(namespace_display, nodes)

    render_cluster_status(
      %{
        namespace: namespace_display,
        nodes: nodes,
        leader: safe_leader(),
        is_leader: safe_is_leader(),
        counts: counts,
        health: health,
        recovery_per_node: recovery_per_node,
        recovery_marker_per_node: marker_per_node
      },
      output_format
    )
  end

  @doc """
  RPC fan-out: ask every node in the cluster (including the local one)
  for its `HubManager.recovery_state/1` and whether the marker file
  exists. Returns `{state_map, marker_present_map}` keyed by node().
  Used to populate the `recovery_state` / `recovery_armed` columns on
  `flz cluster status`.
  """
  @spec cluster_status_recovery(binary(), [node()]) :: {map(), map()}
  def cluster_status_recovery(namespace, nodes) do
    hub_id = Flezha.Service.Identificator.hub(namespace)

    {state_map, marker_map} =
      Enum.reduce(nodes, {%{}, %{}}, fn n, {sa, ma} ->
        {state, marker_present?} =
          if n == node() do
            local_recovery_snapshot(hub_id)
          else
            try do
              :rpc.call(n, __MODULE__, :local_recovery_snapshot, [hub_id], 5_000)
              |> case do
                {:badrpc, _} -> {:unreachable, false}
                {s, m} -> {s, m}
              end
            catch
              _, _ -> {:unreachable, false}
            end
          end

        {Map.put(sa, n, state), Map.put(ma, n, marker_present?)}
      end)

    {state_map, marker_map}
  end

  @doc false
  @spec local_recovery_snapshot(atom()) :: {atom(), boolean()}
  def local_recovery_snapshot(hub_id) when is_atom(hub_id) do
    state =
      case HubManager.recovery_state(hub_id) do
        {:ok, s} -> s
        {:error, :unknown_hub} -> :unknown
      end

    marker_present? = File.exists?(HubManager.resolved_marker_path(hub_id))
    {state, marker_present?}
  end

  @doc """
  Operator-facing CLI implementation for `flz cluster prepare-recovery`.
  Fans `ProcessHub.prepare_recovery_cluster/1` out via the upstream
  library and prints one row per node. Exit code derived from the
  upstream return shape: 0 on full success, 1 on partial, 2 on error.
  """
  @spec cluster_prepare_recovery(binary()) :: :ok
  def cluster_prepare_recovery(namespace \\ "default") do
    namespace_display = normalize_namespace(namespace)
    hub_id = Flezha.Service.Identificator.hub(namespace_display)
    marker_path = HubManager.resolved_marker_path(hub_id)
    nodes = HubManager.get_hub_nodes(namespace_display, true)

    result = ProcessHub.prepare_recovery_cluster(hub_id)
    print_prepare_recovery_table(hub_id, marker_path, nodes, result)
    set_prepare_recovery_exit_code(result)
    :ok
  end

  defp print_prepare_recovery_table(hub_id, marker_path, nodes, result) do
    IO.puts(
      "  " <>
        String.pad_trailing("node", 30) <>
        String.pad_trailing("hub_id", 18) <>
        String.pad_trailing("marker_path", 60) <>
        "result"
    )

    {ok_nodes, unreachable_nodes} =
      case result do
        {:ok, acked} -> {acked, []}
        {:partial, acked, unreachable} -> {acked, unreachable}
        {:error, _reason} -> {[], nodes}
      end

    Enum.each(nodes, fn n ->
      label =
        cond do
          n in ok_nodes -> "ok"
          n in unreachable_nodes -> "unreachable"
          true -> "skipped"
        end

      IO.puts([
        "  ",
        String.pad_trailing(to_string(n), 30),
        String.pad_trailing(to_string(hub_id), 18),
        String.pad_trailing(marker_path, 60),
        label
      ])
    end)

    case result do
      {:error, reason} ->
        IO.puts("  error: #{inspect(reason)}")

      _ ->
        :ok
    end
  end

  defp set_prepare_recovery_exit_code(result) do
    # Matches the System.halt/1 exit-code convention used by `boot_await/0`,
    # `boot_import_legacy_snapshot/2`, and the controller verbs. See
    # `Flezha.Service.CliManager` for the codebase-wide pattern.
    case result do
      {:ok, _} -> :ok
      {:partial, _, _} -> System.halt(1)
      {:error, _} -> System.halt(2)
    end
  end

  defp render_cluster_status(s, "json") do
    result = %{
      namespace: s.namespace,
      leader: s.leader && to_string(s.leader),
      local_node: to_string(node()),
      is_leader: s.is_leader,
      nodes:
        Enum.map(s.nodes, fn n ->
          %{
            node: to_string(n),
            operative_count: Map.get(s.counts, n, 0),
            leader: n == s.leader,
            recovery_state: Map.get(s.recovery_per_node, n, :unknown),
            recovery_armed: not Map.get(s.recovery_marker_per_node, n, false)
          }
        end),
      unplaced_operatives: Map.get(s.counts, nil, 0),
      operative_health: s.health
    }

    IO.puts(Jason.encode!(result))
  end

  defp render_cluster_status(s, _) do
    IO.puts("Cluster status for namespace '#{s.namespace}':")
    IO.puts("")
    IO.puts("  Leader:     #{label_or(s.leader, "(none)")}")
    IO.puts("  Local node: #{node()}#{if s.is_leader, do: " (leader)", else: ""}")
    IO.puts("")
    IO.puts("  Nodes:")

    if s.nodes == [] do
      IO.puts("    (none)")
    else
      Enum.each(s.nodes, fn n ->
        marker = if n == s.leader, do: " [leader]", else: ""
        rs = Map.get(s.recovery_per_node, n, :unknown)
        armed = not Map.get(s.recovery_marker_per_node, n, false)

        IO.puts(
          "    #{n}: #{Map.get(s.counts, n, 0)} operative(s) recovery_state=#{rs} " <>
            "recovery_armed=#{armed}#{marker}"
        )
      end)
    end

    case Map.get(s.counts, nil, 0) do
      0 -> :ok
      unplaced -> IO.puts("    (unplaced): #{unplaced} operative(s)")
    end

    IO.puts("")
    IO.puts("  Operative health: #{health_summary(s.health)}")
  end

  defp health_summary(health) when map_size(health) == 0, do: "no operatives"

  defp health_summary(health) do
    health
    |> Enum.map(fn {state, count} -> "#{state}=#{count}" end)
    |> Enum.join(", ")
  end

  defp label_or(nil, default), do: default
  defp label_or(value, _default), do: to_string(value)

  # There is no leader-election process: the convergence Loop uses local
  # reads + process_hub's own coordinator for cluster-wide work. The CLI
  # cluster_status surface keeps the column and surfaces the local node
  # as the de-facto leader.
  defp safe_leader do
    node()
  end

  defp safe_is_leader do
    true
  end

  @doc "Shows detailed information about operatives and their enabled plugins."
  def operative_info(namespace, output_format \\ "text") do
    namespace_display = normalize_namespace(namespace)

    case Flezha.list_operatives(namespace_display) do
      {:ok, operatives} ->
        if Enum.empty?(operatives) do
          handle_empty_operatives(namespace_display, output_format)
        else
          handle_operative_info_output(operatives, namespace_display, output_format)
        end
    end
  end

  defp handle_empty_operatives(namespace_display, "json") do
    result = %{
      namespace: namespace_display,
      operatives: [],
      message: "No operatives found"
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_empty_operatives(namespace_display, _) do
    IO.puts("No operatives found for namespace '#{namespace_display}'.")
  end

  defp handle_operative_info_output(operatives, namespace_display, "json") do
    operative_data =
      Enum.map(operatives, fn operative_id ->
        case OperativeManager.get_overwatch_state(namespace_display, operative_id) do
          {:ok, state} ->
            %{
              id: operative_id,
              executor_id: state.executor_id,
              namespace: state.namespace,
              default_input_type: state.default_input_type,
              plugins: format_plugins_json(state.extensions)
            }

          {:error, reason} ->
            %{id: operative_id, error: inspect(reason)}
        end
      end)

    result = %{namespace: namespace_display, operatives: operative_data}
    IO.puts(Jason.encode!(result))
  end

  defp handle_operative_info_output(operatives, namespace_display, _) do
    IO.puts("Operative information for namespace '#{namespace_display}':")
    IO.puts("")

    Enum.each(operatives, fn operative_id ->
      case OperativeManager.get_overwatch_state(namespace_display, operative_id) do
        {:ok, state} ->
          display_operative_info(operative_id, state)

        {:error, reason} ->
          IO.puts("Operative: #{operative_id}")
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

  defp display_operative_info(operative_id, state) do
    IO.puts("Operative: #{operative_id}")
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
            :on_operative_init,
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
  ETF snapshot and write each operative's child spec into ProcessHub's
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
    operatives = Map.get(payload, :operatives, [])

    Enum.each(operatives, fn entry ->
      namespace = Map.fetch!(entry, :namespace)
      cspec = Map.fetch!(entry, :cspec)
      hub_id = Flezha.Service.Identificator.hub(namespace)
      _ = ProcessHub.start_children(hub_id, [cspec], async_wait: false)
    end)

    IO.puts("boot import-legacy-snapshot: imported #{length(operatives)} operatives")
    :ok
  end

  # ---------- `flz state` subcommand handlers ----------

  @doc """
  `flz state list <operative_class>` — print one row per Segment under
  `<operative_class>` with `wal_seq`, `snapshot_seq`, `wal_bytes`.
  """
  def state_list(operative_class, output_format \\ "text") do
    rows =
      for {{rc, sh, pk} = pid, counters} <- Flezha.State.Metrics.snapshot_all(),
          rc == operative_class do
        wal_bytes =
          case :file.read_file_info(Flezha.State.Registry.wal_path(pid)) do
            {:ok, info} -> elem(info, 1)
            _ -> 0
          end

        %{
          operative_class: rc,
          operation_id: sh,
          segment_key: pk,
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
        IO.puts(Jason.encode!(%{operative_class: operative_class, segments: rows}))

      _ ->
        IO.puts("Segments under operative #{operative_class}:\n")
        Enum.each(rows, &print_state_row/1)
    end
  end

  defp print_state_row(row) do
    IO.puts("  #{row.operation_id}/#{row.segment_key}")
    IO.puts("    wal_seq:                 #{row.wal_seq}")
    IO.puts("    snapshot_seq:            #{row.snapshot_seq}")
    IO.puts("    wal_bytes:               #{row.wal_bytes}")
    IO.puts("    flush_latency_ms_p99:    #{row.flush_latency_ms_p99}")
    IO.puts("    fsync_durability_lag_ms: #{row.fsync_durability_lag_ms}")
    IO.puts("    dedup_drops_total:       #{row.dedup_drops_total}")
    IO.puts("")
  end

  @doc """
  `flz state at <operative/operation/segment> <seq | timestamp>` — point-in-time
  query. Resolves the path string into a `(executor_id, operative,
  operation, segment_key)` by scanning the on-disk state tree (one match
  expected; ambiguous paths print all matches and exit non-zero).
  """
  def state_at(path_string, target_string, output_format \\ "text") do
    case parse_state_path(path_string) do
      {:ok, segment_id} ->
        target = parse_state_target(target_string)

        case Flezha.State.TimeTravel.query(segment_id, target) do
          {:ok, result} ->
            print_state_at(segment_id, target, result, output_format)

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

  defp print_state_at(_segment_id, _target, result, "json") do
    IO.puts(
      Jason.encode!(%{
        terminal_seq: result.terminal_seq,
        snapshot_size: byte_size(result.snapshot_bytes),
        change_log_size: byte_size(result.change_log_bytes)
      })
    )
  end

  defp print_state_at(segment_id, target, result, _) do
    {rc, sh, pk} = segment_id

    IO.puts("state at #{rc}/#{sh}/#{pk} target=#{inspect(target)}:")
    IO.puts("  terminal_seq:    #{result.terminal_seq}")
    IO.puts("  snapshot bytes:  #{byte_size(result.snapshot_bytes)}")
    IO.puts("  change_log bytes:#{byte_size(result.change_log_bytes)}")
  end

  @doc """
  `flz state rotate <operative/operation/segment>` — force a compaction by
  asking Overwatch to dispatch a `CompactSegment` to the SDK.
  """
  def state_rotate(path_string) do
    case parse_state_path(path_string) do
      {:ok, {rc, sh, pk}} ->
        case lookup_executor_for_segment(rc, sh, pk) do
          {:ok, executor_id} ->
            case Flezha.Operative.Overwatch.dispatch_compact_segment(executor_id, {rc, sh, pk}) do
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

  defp lookup_executor_for_segment(rc, sh, pk) do
    Flezha.State.Recovery.resolve_executor({rc, sh, pk})
  end

  @doc """
  `flz state delete <operative/operation/key>` — hard-delete one Segment's
  persisted state (WAL + snapshots + outbox-dedup). Idempotent: deleting
  a segment with no on-disk state still succeeds. This is the
  operator-driven, confirmation-returning counterpart to the SDK reaper's
  fire-and-forget `delete_segment_state` send (Decision 8).
  """
  def state_delete(path_string) do
    case String.split(path_string, "/", parts: 3) do
      [rc, sh, pk] ->
        {:ok, _} = Flezha.State.Supervisor.delete_segment({rc, sh, pk})
        IO.puts("deleted persisted state for #{rc}/#{sh}/#{pk}")

      _ ->
        IO.puts(
          :stderr,
          "state delete: expected <operative>/<operation>/<segment_key>; got #{path_string}"
        )

        System.halt(1)
    end
  end

  @doc """
  `flz state retention <operative_class> <hours>` — set runtime retention
  for `<operative_class>`. Persisted across the next Application boot only
  if the executor separately writes it to runtime.exs.
  """
  def state_retention(operative_class, hours_string) do
    case Integer.parse(hours_string) do
      {hours, ""} when hours >= 0 ->
        current = Application.get_env(:flezha, :time_travel_retention_hours, %{})
        updated = Map.put(current, operative_class, hours)
        Application.put_env(:flezha, :time_travel_retention_hours, updated, persistent: true)
        IO.puts("retention[#{operative_class}] = #{hours} hours (in-memory; persist via runtime.exs)")

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
        {:error, "expected <operative>/<operation>/<segment_key>; got #{path_string}"}
    end
  end

  defp has_persisted_state?(rc, sh, pk) do
    Enum.any?(Flezha.State.Registry.all_segments(), fn {r, s, p} ->
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

            IO.puts("[shutdown] complete duration_ms=#{duration} outcome=#{outcome}")

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

  # ============================================================================
  # Observability service wrappers
  # ============================================================================
  #
  # Thin shims that build a `caller_context` with `source: :cli` and
  # forward to `Flezha.Service.Observability`. Future `flz observe ...`
  # subcommands call these.

  alias Flezha.Service.Observability, as: ObsSvc

  def observability_list_sources(opts \\ []) do
    ObsSvc.list_sources(cli_context(opts))
  end

  def observability_list_sinks(opts \\ []) do
    ObsSvc.list_sinks(cli_context(opts))
  end

  def observability_query(params, opts \\ []) when is_map(params) do
    ObsSvc.query(params, cli_context(opts))
  end

  def observability_subscribe(source, opts \\ []) when is_atom(source) do
    ObsSvc.subscribe(source, cli_context(opts))
  end

  def observability_enable_source(source, opts \\ []) when is_atom(source) do
    ObsSvc.enable_source(source, cli_context(opts))
  end

  def observability_disable_source(source, opts \\ []) when is_atom(source) do
    ObsSvc.disable_source(source, cli_context(opts))
  end

  @doc """
  CLI entry point for `flz observe sources`. Renders text or JSON.
  """
  def observability_render_sources(format) when format in ["text", "json"] do
    case ObsSvc.list_sources(cli_context([])) do
      {:ok, sources} -> render_sources_output(sources, format)
      {:error, reason} -> render_error(reason)
    end
  end

  def observability_render_sinks(format) when format in ["text", "json"] do
    case ObsSvc.list_sinks(cli_context([])) do
      {:ok, sinks} -> render_sinks_output(sinks, format)
      {:error, reason} -> render_error(reason)
    end
  end

  @doc """
  CLI entry point for `flz observe tail <source>`. Subscribes and
  streams signals as NDJSON (one signal per line) until the calling
  RPC connection closes.
  """
  def observability_render_tail(source_str, format, opts \\ []) when format in ["text", "json"] do
    source = String.to_atom(source_str)
    duration_ms = Keyword.get(opts, :duration_ms, 30_000)

    case ObsSvc.tail(source, cli_context(opts)) do
      :ok ->
        tail_loop(source, format, duration_ms)

      {:error, reason} ->
        render_error(reason)
    end
  end

  def observability_render_enable(source_str) do
    source = String.to_atom(source_str)

    case ObsSvc.enable_source(source, cli_context([])) do
      :ok ->
        IO.puts("source #{source} enabled")
        :ok

      {:error, reason} ->
        render_error(reason)
    end
  end

  def observability_render_disable(source_str) do
    source = String.to_atom(source_str)

    case ObsSvc.disable_source(source, cli_context([])) do
      :ok ->
        IO.puts("source #{source} disabled")
        :ok

      {:error, reason} ->
        render_error(reason)
    end
  end

  defp render_sources_output(sources, "json") do
    sources
    |> Enum.map(&source_to_map/1)
    |> Jason.encode!()
    |> IO.puts()
  end

  defp render_sources_output(sources, "text") do
    header = ["name", "status", "kinds", "budget"]
    IO.puts(Enum.map_join(header, "\t", & &1))

    Enum.each(sources, fn source ->
      IO.puts(
        Enum.map_join(
          [
            source.name,
            source.status,
            Enum.map_join(source.kinds, ",", &Atom.to_string/1),
            source.cardinality_budget
          ],
          "\t",
          &to_string/1
        )
      )
    end)
  end

  defp render_sinks_output(sinks, "json") do
    sinks
    |> Enum.map(&sink_to_map/1)
    |> Jason.encode!()
    |> IO.puts()
  end

  defp render_sinks_output(sinks, "text") do
    header = ["name", "module", "overflow", "watermark"]
    IO.puts(Enum.map_join(header, "\t", & &1))

    Enum.each(sinks, fn sink ->
      IO.puts(
        Enum.map_join(
          [
            sink.name,
            inspect(sink.module),
            sink.overflow,
            sink.mailbox_high_watermark
          ],
          "\t",
          &to_string/1
        )
      )
    end)
  end

  defp source_to_map(source) do
    %{
      name: source.name,
      status: source.status,
      flavour: source.flavour,
      kinds: source.kinds,
      cardinality_budget: source.cardinality_budget
    }
  end

  defp sink_to_map(sink) do
    %{
      name: sink.name,
      module: inspect(sink.module),
      overflow: sink.overflow,
      mailbox_high_watermark: sink.mailbox_high_watermark
    }
  end

  defp tail_loop(source, format, duration_ms) do
    deadline = System.monotonic_time(:millisecond) + duration_ms
    do_tail_loop(source, format, deadline)
  end

  defp do_tail_loop(source, format, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      :ok
    else
      receive do
        {:flezha_signal, signal} ->
          render_tail_signal(signal, format)
          do_tail_loop(source, format, deadline)
      after
        min(remaining, 1_000) ->
          if System.monotonic_time(:millisecond) >= deadline,
            do: :ok,
            else: do_tail_loop(source, format, deadline)
      end
    end
  end

  defp render_tail_signal(signal, "json") do
    %{
      kind: signal.kind,
      source: signal.source,
      name: signal.name,
      ts: signal.ts,
      measurements: stringify_keys(signal.measurements),
      metadata: stringify_keys(signal.metadata)
    }
    |> Jason.encode!()
    |> IO.puts()
  end

  defp render_tail_signal(signal, "text") do
    IO.puts(
      "[#{signal.ts}] #{signal.source}.#{signal.name} kind=#{signal.kind} " <>
        "measurements=#{inspect(signal.measurements)} metadata=#{inspect(signal.metadata)}"
    )
  end

  defp stringify_keys(map) when is_map(map) do
    Enum.into(map, %{}, fn {k, v} -> {to_string(k), inspect_or_pass(v)} end)
  end

  defp inspect_or_pass(v) when is_binary(v) or is_number(v) or is_boolean(v), do: v
  defp inspect_or_pass(v) when is_atom(v), do: Atom.to_string(v)
  defp inspect_or_pass(v), do: inspect(v)

  defp render_error(:pubsub_sink_disabled) do
    IO.puts(
      :stderr,
      "error: pubsub sink is disabled; enable :observability, :pubsub_sink_enabled to use `flz observe tail`"
    )

    System.halt(2)
  end

  defp render_error(:observability_disabled) do
    IO.puts(:stderr, "error: observability is disabled")
    System.halt(3)
  end

  defp render_error(reason) do
    IO.puts(:stderr, "error: #{inspect(reason)}")
    System.halt(1)
  end

  defp cli_context(opts) do
    base = %{source: :cli}

    base =
      case Keyword.get(opts, :request_id) do
        nil -> base
        id when is_binary(id) -> Map.put(base, :request_id, id)
      end

    # AuthN-bound fields from the CLI socket handshake. When the
    # handshake hasn't run (auth disabled or pre-Section 10 path)
    # they remain absent; downstream `Flezha.AuthZ` returns `:ok`
    # while `:enabled` is `false`.
    base
    |> maybe_put_field(:principal, Keyword.get(opts, :principal))
    |> maybe_put_field(:namespace, Keyword.get(opts, :namespace))
    |> maybe_put_field(:role, Keyword.get(opts, :role))
  end

  defp maybe_put_field(map, _key, nil), do: map
  defp maybe_put_field(map, key, value), do: Map.put(map, key, value)

  # ============================================================================
  # AuthN / AuthZ service wrappers
  # ============================================================================
  #
  # Thin shims for the new `flz auth`, `flz ns`, `flz role` verb
  # families. Every wrapper builds a `caller_context` via
  # `cli_context/1` and routes through `Flezha.AuthZ.*`.

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{Bootstrap, NamespaceStore, RoleStore, Scope, TokenStore}

  @authz_source :platform_authz

  def authz_bootstrap(token, opts \\ []) when is_binary(token) do
    case Bootstrap.claim(token, opts) do
      {:ok, %{principal: principal, minted_token: minted}} ->
        emit_cli_audit(:bootstrap_completed, principal, %{
          namespace: "_system",
          minted_token?: not is_nil(minted)
        })

        {:ok, %{principal: principal, minted_token: minted}}

      {:error, _} = err ->
        err
    end
  end

  def authz_issue(principal, namespace, role, ttl_seconds, opts \\ []) do
    cli_ctx = cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.token_issue", Scope.build(namespace)) do
      TokenStore.issue(principal, namespace, role, ttl_seconds,
        created_by_principal: Map.get(cli_ctx, :principal),
        label: Keyword.get(opts, :label)
      )
    end
  end

  def authz_revoke(token_id, opts \\ []) when is_binary(token_id) do
    cli_ctx = cli_context(opts)

    # We must check the token's namespace to authorize cross-namespace
    # revocations. For simplicity we look it up first.
    case find_token_by_id(token_id) do
      {:ok, entry} ->
        with :ok <- AuthZ.require_bound_principal(cli_ctx),
             :ok <-
               AuthZ.authorize(
                 cli_ctx,
                 :"authz.token_revoke",
                 Scope.build(entry.namespace)
               ) do
          TokenStore.revoke(token_id, revoked_by_principal: Map.get(cli_ctx, :principal))
        end

      :error ->
        {:error, :not_found}
    end
  end

  def authz_list(opts \\ []) do
    cli_ctx = cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <-
           AuthZ.authorize(cli_ctx, :"authz.token_list", Scope.build(scope_namespace(cli_ctx))) do
      ns = filter_namespace_for_list(cli_ctx)
      {:ok, TokenStore.list(ns)}
    end
  end

  def authz_whoami(opts \\ []) do
    cli_ctx = cli_context(opts)

    {:ok,
     %{
       principal: Map.get(cli_ctx, :principal),
       namespace: Map.get(cli_ctx, :namespace),
       role: Map.get(cli_ctx, :role)
     }}
  end

  def namespace_create(name, quotas, opts \\ []) when is_binary(name) and is_map(quotas) do
    cli_ctx = cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.namespace_create", Scope.build(name)) do
      result =
        NamespaceStore.create(name,
          quotas: quotas,
          created_by_principal: Map.get(cli_ctx, :principal)
        )

      case result do
        {:ok, entry} ->
          emit_cli_audit(:namespace_created, Map.get(cli_ctx, :principal), %{namespace: name})
          {:ok, entry}

        other ->
          other
      end
    end
  end

  def namespace_delete(name, opts \\ []) when is_binary(name) do
    cli_ctx = cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.namespace_delete", Scope.build(name)) do
      case NamespaceStore.delete(name) do
        :ok ->
          _ = Flezha.AuthZ.QuotaCounters.drop_namespace(name)
          emit_cli_audit(:namespace_deleted, Map.get(cli_ctx, :principal), %{namespace: name})
          :ok

        other ->
          other
      end
    end
  end

  def namespace_list(opts \\ []) do
    cli_ctx = cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <-
           AuthZ.authorize(
             cli_ctx,
             :"authz.namespace_list",
             Scope.build(scope_namespace(cli_ctx))
           ) do
      {:ok, NamespaceStore.list()}
    end
  end

  def namespace_show(name, opts \\ []) when is_binary(name) do
    cli_ctx = cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.namespace_show", Scope.build(name)) do
      case NamespaceStore.get(name) do
        {:ok, entry} -> {:ok, entry}
        :error -> {:error, :not_found}
      end
    end
  end

  def namespace_set_quota(name, overrides, opts \\ [])
      when is_binary(name) and is_map(overrides) do
    cli_ctx = cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.namespace_set_quota", Scope.build(name)) do
      case NamespaceStore.set_quotas(name, overrides) do
        {:ok, entry} ->
          emit_cli_audit(:namespace_quota_updated, Map.get(cli_ctx, :principal), %{
            namespace: name,
            quotas: overrides
          })

          {:ok, entry}

        other ->
          other
      end
    end
  end

  def role_bind(principal, namespace, role, opts \\ [])
      when is_binary(principal) and is_binary(namespace) and is_atom(role) do
    cli_ctx = cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.role_bind", Scope.build(namespace)) do
      case RoleStore.bind(principal, namespace, role, bound_by: Map.get(cli_ctx, :principal)) do
        :ok ->
          emit_cli_audit(:role_bound, Map.get(cli_ctx, :principal), %{
            principal: principal,
            namespace: namespace,
            role: role
          })

          :ok

        other ->
          other
      end
    end
  end

  def role_unbind(principal, opts \\ []) when is_binary(principal) do
    cli_ctx = cli_context(opts)

    bound_ns =
      case RoleStore.lookup(principal) do
        {:ok, %{namespace: ns}} -> ns
        :error -> "_system"
      end

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <- AuthZ.authorize(cli_ctx, :"authz.role_unbind", Scope.build(bound_ns)) do
      :ok = RoleStore.unbind(principal)
      emit_cli_audit(:role_unbound, Map.get(cli_ctx, :principal), %{principal: principal})
      :ok
    end
  end

  def role_list(opts \\ []) do
    cli_ctx = cli_context(opts)

    with :ok <- AuthZ.require_bound_principal(cli_ctx),
         :ok <-
           AuthZ.authorize(
             cli_ctx,
             :"authz.role_list",
             Scope.build(scope_namespace(cli_ctx))
           ) do
      {:ok, RoleStore.list()}
    end
  end

  # ----- private helpers for the authz wrappers -----

  defp scope_namespace(ctx), do: Map.get(ctx, :namespace) || "_system"

  defp filter_namespace_for_list(%{role: :admin, namespace: "_system"}), do: :all
  defp filter_namespace_for_list(ctx), do: scope_namespace(ctx)

  defp find_token_by_id(id) do
    TokenStore.list(:all)
    |> Enum.find(fn entry -> String.starts_with?(entry.token_hash, id) end)
    |> case do
      nil -> :error
      entry -> {:ok, entry}
    end
  end

  defp emit_cli_audit(name, principal, extra) do
    metadata = Map.merge(extra, %{principal: principal})

    Flezha.Observability.Bus.emit(
      Flezha.Observability.Signal.new(%{
        kind: :event,
        source: @authz_source,
        name: name,
        metadata: metadata
      })
    )

    :ok
  rescue
    _ -> :ok
  end

  # ============================================================================
  # Audit-log service wrappers
  # ============================================================================

  alias Flezha.Service.AuditLog, as: AuditSvc

  def audit_log_list_files(opts \\ []) do
    AuditSvc.list_files(cli_context(opts))
  end

  def audit_log_read(params, opts \\ []) when is_map(params) do
    AuditSvc.read(params, cli_context(opts))
  end

  def audit_log_tail(_params, opts \\ []) do
    AuditSvc.tail(%{}, cli_context(opts))
  end

  def audit_log_stats(opts \\ []) do
    AuditSvc.stats(cli_context(opts))
  end

  def audit_log_rotate(opts \\ []) do
    AuditSvc.rotate(cli_context(opts))
  end

  @doc """
  CLI entry: `flz audit list`. Renders the catalog as text or JSON.
  """
  def audit_log_render_list(format) when format in ["text", "json"] do
    case AuditSvc.list_files(cli_context([])) do
      {:ok, entries} -> render_audit_list(entries, format)
      {:error, reason} -> render_audit_error(reason)
    end
  end

  @doc """
  CLI entry: `flz audit read <file> [filters]`. Streams matching records as
  one-record-per-line (text) or NDJSON.
  """
  def audit_log_render_read(file, format, opts \\ []) when format in ["text", "json"] do
    params =
      %{file: file}
      |> maybe_put_param(:source, Keyword.get(opts, :source))
      |> maybe_put_param(:name, Keyword.get(opts, :name))
      |> maybe_put_param(:since_us, Keyword.get(opts, :since_us))
      |> maybe_put_param(:until_us, Keyword.get(opts, :until_us))
      |> maybe_put_param(:limit, Keyword.get(opts, :limit))

    case AuditSvc.read(params, cli_context(opts)) do
      {:ok, records} -> render_audit_records(records, format)
      {:error, reason} -> render_audit_error(reason)
    end
  end

  @doc """
  CLI entry: `flz audit tail`. Subscribes for live records and streams them
  to stdout until the duration elapses or the writer closes.
  """
  def audit_log_render_tail(format, opts \\ []) when format in ["text", "json"] do
    duration_ms = Keyword.get(opts, :duration_ms, 30_000)

    case AuditSvc.tail(%{}, cli_context(opts)) do
      {:ok, ref} ->
        audit_tail_loop(ref, format, duration_ms)

      {:error, reason} ->
        render_audit_error(reason)
    end
  end

  @doc """
  CLI entry: `flz audit stats`. Prints the operational snapshot.
  """
  def audit_log_render_stats(format) when format in ["text", "json"] do
    case AuditSvc.stats(cli_context([])) do
      {:ok, snap} -> render_audit_stats(snap, format)
      {:error, reason} -> render_audit_error(reason)
    end
  end

  @doc """
  CLI entry: `flz audit rotate`. Forces an immediate rotation.
  """
  def audit_log_render_rotate(format) when format in ["text", "json"] do
    case AuditSvc.rotate(cli_context([])) do
      {:ok, info} -> render_audit_rotate(info, format)
      {:error, reason} -> render_audit_error(reason)
    end
  end

  defp maybe_put_param(map, _k, nil), do: map
  defp maybe_put_param(map, k, v), do: Map.put(map, k, v)

  defp render_audit_list(entries, "json") do
    entries
    |> Enum.map(&audit_entry_to_json/1)
    |> Jason.encode!()
    |> IO.puts()
  end

  defp render_audit_list(entries, "text") do
    header = ["filename", "first_ts", "last_ts", "count", "size"]
    IO.puts(Enum.join(header, "\t"))

    Enum.each(entries, fn e ->
      IO.puts(
        Enum.join(
          [
            e.filename,
            to_string(e.first_ts_us),
            to_string(e.last_ts_us),
            to_string(e.record_count),
            to_string(e.size_bytes)
          ],
          "\t"
        )
      )
    end)
  end

  defp audit_entry_to_json(e) do
    %{
      filename: e.filename,
      first_ts_us: e.first_ts_us,
      last_ts_us: e.last_ts_us,
      record_count: e.record_count,
      size_bytes: e.size_bytes,
      sha256: e.sha256
    }
  end

  defp render_audit_records(records, "json") do
    Enum.each(records, fn r -> IO.puts(Jason.encode!(r)) end)
  end

  defp render_audit_records(records, "text") do
    Enum.each(records, fn r ->
      caller = Map.get(r, "caller") || %{}

      IO.puts(
        "[#{Map.get(r, "ts_iso")}] #{Map.get(r, "source")}.#{Map.get(r, "name")} " <>
          "caller=#{Map.get(caller, "source")} request_id=#{Map.get(caller, "request_id")}"
      )
    end)
  end

  defp render_audit_stats(snap, "json") do
    IO.puts(Jason.encode!(snap))
  end

  defp render_audit_stats(snap, "text") do
    IO.puts("current_file:         #{snap.current_file}")
    IO.puts("current_file_bytes:   #{snap.current_file_bytes}")
    IO.puts("total_bytes:          #{snap.total_bytes}")
    IO.puts("total_files:          #{snap.total_files}")
    IO.puts("oldest_ts_us:         #{inspect(snap.oldest_ts_us)}")
    IO.puts("record_count:         #{snap.record_count}")
    IO.puts("drop_count:           #{snap.drop_count}")
    IO.puts("fsync_mode:           #{snap.fsync_mode}")
    IO.puts("closed:               #{snap.closed}")
  end

  defp render_audit_rotate(info, "json") do
    IO.puts(Jason.encode!(info))
  end

  defp render_audit_rotate(info, "text") do
    IO.puts("rotated to: #{info.rotated_to}")
  end

  defp audit_tail_loop(ref, format, duration_ms) do
    deadline = System.monotonic_time(:millisecond) + duration_ms
    do_audit_tail_loop(ref, format, deadline)
  end

  defp do_audit_tail_loop(ref, format, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      _ = Flezha.Observability.AuditLog.Writer.unsubscribe_tail(ref)
      :ok
    else
      receive do
        {:audit_record, ^ref, record} ->
          case format do
            "json" -> IO.puts(Jason.encode!(record))
            _ -> render_audit_records([record], "text")
          end

          do_audit_tail_loop(ref, format, deadline)

        {:audit_writer_closed, ^ref} ->
          :ok
      after
        min(remaining, 1_000) ->
          if System.monotonic_time(:millisecond) >= deadline do
            _ = Flezha.Observability.AuditLog.Writer.unsubscribe_tail(ref)
            :ok
          else
            do_audit_tail_loop(ref, format, deadline)
          end
      end
    end
  end

  defp render_audit_error(:audit_log_disabled) do
    IO.puts(
      :stderr,
      "error: audit log is disabled; set FLEZHA_AUDIT_ENABLED=true or :audit_log, :enabled: true in config"
    )

    System.halt(2)
  end

  defp render_audit_error(:observability_disabled) do
    IO.puts(:stderr, "error: observability is disabled")
    System.halt(3)
  end

  defp render_audit_error(:invalid_caller_context) do
    IO.puts(:stderr, "error: invalid caller context")
    System.halt(1)
  end

  defp render_audit_error(:file_not_found) do
    IO.puts(:stderr, "error: file not found")
    System.halt(1)
  end

  defp render_audit_error(reason) do
    IO.puts(:stderr, "error: #{inspect(reason)}")
    System.halt(1)
  end
end

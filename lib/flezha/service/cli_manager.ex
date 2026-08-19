defmodule Flezha.Service.CliManager do
  @moduledoc """
  Provides recipes for the RPC calls.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Plugin.SpecDecoder
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
        kind \\ "stream"
      ) do
    opts_kw =
      Payloader.json_to_map(opts)
      |> atom_converter()
      |> Keyword.new()

    {partitions, opts_kw} = pop_partitions(opts_kw)

    if partitions >= 2 do
      start_partitioned(
        namespace,
        operative_id,
        factory_id,
        init_values,
        opts_kw,
        kind,
        partitions,
        plugins_json
      )
    else
      case SpecDecoder.decode(plugins_json, normalize_namespace(namespace), operative_id) do
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
          cli_error!("Error starting operative: #{inspect(reason)}")
      end
    end
  end

  # `--partitions` arrives as a decimal string like every CLI opt. Absent or
  # `1` takes the ordinary single-instance path, byte-identical to before
  # partitioning existed.
  defp pop_partitions(opts_kw) do
    case Keyword.pop(opts_kw, :partitions) do
      {nil, rest} ->
        {1, rest}

      {raw, rest} ->
        case Integer.parse(to_string(raw)) do
          {n, ""} when n >= 1 -> {n, rest}
          _ -> cli_error!("Error starting operative: --partitions must be a positive integer")
        end
    end
  end

  defp start_partitioned(
         namespace,
         operative_id,
         factory_id,
         init_values,
         opts_kw,
         kind,
         partitions,
         plugins_json
       ) do
    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: factory_id,
      kind: parse_kind(kind),
      init_values: Payloader.json_to_map(init_values),
      opts: opts_kw
    }

    case PartitionGroup.start(
           normalize_namespace(namespace),
           operative,
           partitions,
           plugins_json
         ) do
      {:ok, %{members: members, warnings: warnings} = result} ->
        Enum.each(warnings, &IO.puts("WARNING: #{&1}"))
        report_group_start(operative_id, members, Map.get(result, :already_running, []))

      {:error, reason} ->
        cli_error!("Error starting operative: #{format_reason(reason)}")
    end
  end

  defp report_group_start(operative_id, members, []) do
    IO.puts("Operative #{operative_id} started with #{length(members)} partitions:")
    Enum.each(members, &IO.puts("  #{&1}"))
  end

  defp report_group_start(operative_id, [], already) do
    IO.puts(
      "Operative #{operative_id} is already running with #{length(already)} partitions; " <>
        "the existing members were left untouched."
    )
  end

  defp report_group_start(operative_id, members, already) do
    IO.puts(
      "Operative #{operative_id} converged: started #{length(members)} missing member(s), " <>
        "#{length(already)} already running:"
    )

    Enum.each(members, &IO.puts("  started #{&1}"))
    Enum.each(already, &IO.puts("  already running #{&1}"))
  end

  defp format_reason(reason) when is_binary(reason), do: reason
  defp format_reason(reason), do: inspect(reason)

  defp start_operative(namespace, operative_id, factory_id, init_values, opts_kw, kind) do
    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: factory_id,
      kind: parse_kind(kind),
      init_values: Payloader.json_to_map(init_values),
      opts: opts_kw
    }

    case OperativeManager.start_operative(normalize_namespace(namespace), operative) do
      {:ok, _} ->
        IO.puts("Operative has been started")

      # A live instance holds the id: the re-run is a no-op, not a failure.
      # A claim refused with no live winner stays in the error clause below.
      :duplicate ->
        IO.puts(
          "Operative #{operative.id} is already running; the existing instance was left " <>
            "untouched (options were not compared — see `flz operative info #{operative.id}`)."
        )

      {:error, reason} ->
        cli_error!("Error starting operative: #{inspect(reason)}")
    end
  end

  # Resolve the wire kind string to its atom without minting atoms from
  # untrusted input: only already-known kinds convert; anything else stays a
  # string, which `Validator.valid_kind/1` rejects at start.
  defp parse_kind(kind) when kind in [nil, "", "stream"], do: :stream

  defp parse_kind(kind) when is_binary(kind) do
    String.to_existing_atom(kind)
  rescue
    ArgumentError -> kind
  end

  defp parse_kind(kind), do: kind

  ## Plugin-spec decoding lives in `Flezha.Plugin.SpecDecoder` — one decoder,
  ## shared with the schedule firing path.

  @doc "Stops a operative (cooperatively for a workflow operative, hard otherwise)."
  def operative_stop(namespace, operative_id, opts \\ []) do
    namespace_display = normalize_namespace(namespace)

    with_cli_authorization(
      opts,
      :"operative.stop",
      namespace_display,
      %{operative_id: operative_id},
      fn ->
        case PartitionGroup.members(namespace_display, operative_id) do
          [] -> stop_one(namespace_display, operative_id)
          members -> stop_group(namespace_display, operative_id, members)
        end
      end
    )
  end

  defp stop_group(namespace, group_id, members) do
    results = Enum.map(members, &{&1.id, OperativeManager.request_stop(namespace, &1.id)})

    case Enum.reject(results, fn {_id, result} -> result == :ok or match?({:ok, _}, result) end) do
      [] ->
        IO.puts("Operative group #{group_id} stopped (#{length(results)} partitions)")

      failed ->
        cli_error!(
          "Error stopping operative group #{group_id}: " <>
            Enum.map_join(failed, "; ", fn {id, err} -> "#{id}: #{inspect(err)}" end)
        )
    end
  end

  defp stop_one(namespace, operative_id) do
    case OperativeManager.request_stop(namespace, operative_id) do
      :ok ->
        IO.puts("Operative has been stopped")

      {:ok, :cooperative} ->
        IO.puts(
          "Cooperative stop requested for workflow operative '#{operative_id}'. " <>
            "It will finish in-flight work, then complete (cancelled) and be removed."
        )

      {:error, reason} ->
        cli_error!("Error stopping operative: #{inspect(reason)}")
    end
  end

  @doc """
  `flz operative scale <group> <M>` — rescale a partition group to M members.

  Quiesces the group (sources off, input logs consumed, DLQs empty), moves
  every Entity whose key group changes owner, then starts M members with the
  recomputed ranges. Refuses — changing nothing — when the quiesce cannot be
  reached or the new member set fails preflight.
  """
  def operative_scale(namespace, group_id, target, opts_json \\ "{}") do
    target =
      case Integer.parse(to_string(target)) do
        {n, ""} when n >= 1 -> n
        _ -> cli_error!("Error scaling operative: <partitions> must be a positive integer")
      end

    opts =
      case Payloader.json_to_map(opts_json) do
        %{"timeout_ms" => raw} -> [quiesce_timeout_ms: String.to_integer(to_string(raw))]
        _ -> []
      end

    case PartitionGroup.rescale(normalize_namespace(namespace), group_id, target, opts) do
      {:ok, result} ->
        Enum.each(result.warnings, &IO.puts("WARNING: #{&1}"))

        IO.puts(
          "Operative #{group_id} rescaled #{result.from} -> #{result.to} " <>
            "(#{result.entities_moved} entities moved, #{length(result.vacated)} trees removed, " <>
            "#{result.forwards_refreshed} forwarding plugin(s) re-pointed):"
        )

        Enum.each(result.members, &IO.puts("  #{&1}"))

      {:error, reason} ->
        cli_error!("Error scaling operative: #{format_reason(reason)}")
    end
  end

  @doc """
  Rewind a workflow loop to the committed boundary of iteration `to_iter`: the
  iterations above it are compensated in reverse, state and handoff return to
  that boundary, the epoch is bumped so re-run output supersedes the unwound
  run's at a guarded sink, and the inbox items those iterations consumed are
  re-delivered.

  Refused, changing nothing, when the target has not committed or when an unwound
  procedure emitted plugin output it cannot compensate.
  """
  def operative_rewind(namespace, operative_id, to_iter) do
    case OperativeManager.get_operative_pid(normalize_namespace(namespace), operative_id) do
      {:ok, pid} ->
        print_rewind(operative_id, to_iter, Flezha.Operative.Lifecycle.rewind(pid, to_iter))

      _ ->
        cli_error!("Error: operative '#{operative_id}' not found")
    end
  end

  defp print_rewind(operative_id, to_iter, :ok) do
    IO.puts(
      "Operative '#{operative_id}' rewound to iteration #{to_iter}. " <>
        "Iterations above it were compensated; the loop resumes at #{to_iter + 1}."
    )
  end

  defp print_rewind(_operative_id, _to_iter, {:error, {:uncommitted_iteration, []}}) do
    cli_error!("Error: this operative has no committed iterations to rewind to")
  end

  defp print_rewind(_operative_id, to_iter, {:error, {:uncommitted_iteration, committed}}) do
    cli_error!(
      "Error: iteration #{to_iter} has not committed. " <>
        "Committed iterations: #{Enum.min(committed)}..#{Enum.max(committed)}"
    )
  end

  defp print_rewind(_operative_id, _to_iter, {:error, {:uncovered, channels}}) do
    cli_error!(
      "Error: refusing to rewind — these channels were written by an unwound " <>
        "procedure that declares no `compensate` hook, so the output cannot be " <>
        "undone: #{Enum.join(channels, ", ")}"
    )
  end

  defp print_rewind(_operative_id, _to_iter, {:error, reason}) do
    cli_error!("Error rewinding operative: #{inspect(reason)}")
  end

  @doc """
  Inject a payload into an operative's input stream (the SDK-facing `send_io`
  route), blocking for the confirming verdict so a parked or absent target is
  reported rather than silently dropped. `channel` is optional (`""` = none).
  """
  def operative_send(namespace, operative_id, message, channel) do
    metadata = if channel in [nil, ""], do: %{}, else: %{"channel" => channel}

    case OperativeManager.send_io_confirming(
           operative_id,
           normalize_namespace(namespace),
           message,
           metadata
         ) do
      :ok ->
        IO.puts("Delivered to '#{operative_id}'.")

      {:ok, :absorbed} ->
        IO.puts("Absorbed (duplicate) by '#{operative_id}'.")

      {:ok, :suspended} ->
        IO.puts("Journaled for suspended '#{operative_id}' — delivers on wake.")

      {:error, :not_ready} ->
        cli_error!("Error: operative '#{operative_id}' is not ready to accept input.")

      {:error, :not_found} ->
        cli_error!("Error: operative '#{operative_id}' not found.")

      {:error, :refused} ->
        cli_error!("Error: input refused — durable append failed for '#{operative_id}'.")
    end
  end

  @doc "Get operative state."
  def entity_state(namespace, operative_id) do
    namespace_display = normalize_namespace(namespace)

    case OperativeManager.get_entity_state(
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
        cli_error!(
          "Operative '#{operative_id}' in namespace '#{namespace_display}' has no per-entity state: " <>
            "its kind does not support state introspection."
        )

      {:error, reason} when is_binary(reason) ->
        cli_error!("Error getting operative state: #{reason}")

      {:error, reason} ->
        cli_error!("Error getting operative state: #{inspect(reason)}")

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

  # Subscription directives, not provider options: they are consumed by
  # `extract_subscriptions/2` and must not reach `init_provider/1`, which for a
  # provider with a strict allow-list refuses every key it does not own.
  @subscription_keys ["extensions", "trigger"]

  @doc "Enables plugin."
  def enable_plugin(plugin_id, provider_id, plugin_flags_json, opts_json) do
    with {:ok, provider_mod} <- get_plugin_provider_mod(provider_id),
         {:ok, plugin_flags} <- Jason.decode(plugin_flags_json),
         {:ok, opts} <- Jason.decode(opts_json) do
      subscriptions = extract_subscriptions(plugin_flags, opts)
      namespace = Map.get(opts, "namespace", "default")
      operative_id = Map.get(plugin_flags, "operative_id") || Map.get(opts, "operative_id")

      with_cli_authorization(
        json_token_opts(opts_json),
        :"plugin.enable",
        namespace,
        %{plugin_id: plugin_id, provider: provider_id},
        fn ->
          plugin = %Plugin{
            id: plugin_id,
            provider: provider_mod,
            namespace: namespace,
            operative_id: operative_id,
            opts: provider_opts(plugin_flags),
            subscriptions: subscriptions
          }

          case PluginManager.enable_plugin(plugin_id, plugin) do
            {:ok, :already_enabled} ->
              IO.puts(
                "Plugin #{plugin_id} is already enabled; the existing instance was left " <>
                  "untouched (options were not compared)."
              )

            {:ok, _msg} ->
              IO.puts("Plugin #{plugin_id} enabled successfully.")

            {:error, reason} ->
              cli_error!("Error enabling plugin: #{inspect(reason)}.")
          end
        end
      )
    else
      {:error, reason} -> cli_error!("Error enabling plugin: #{inspect(reason)}.")
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

  @doc """
  The plugin flags a provider actually receives: everything the operator wrote
  as `--X…` minus the subscription directives, which `extract_subscriptions/2`
  has already consumed.
  """
  def provider_opts(plugin_flags) do
    plugin_flags
    |> Map.drop(@subscription_keys)
    |> SpecDecoder.atomize_keys()
  end

  @doc false
  def trigger_to_subscription("on_input"), do: [{:on_input, []}]
  def trigger_to_subscription("on_output"), do: [{:on_output, []}]
  def trigger_to_subscription("interval"), do: [{:on_interval, [interval_ms: 5_000]}]
  def trigger_to_subscription("proactive"), do: [{:on_interval, [interval_ms: 5_000]}]
  def trigger_to_subscription(_), do: [{:on_input, []}]

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
      "plugins" => :plugins,
      "partitions" => :partitions,
      "group" => :group,
      "partition" => :partition,
      "key_group_from" => :key_group_from,
      "key_group_to" => :key_group_to
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

  @doc "Lists runners for a specific namespace."
  def list_runners(namespace, output_format \\ "text") do
    namespace_display = normalize_namespace(namespace)

    case Flezha.list_runners(namespace_display) do
      {:ok, runners} when is_list(runners) ->
        context = %{key: :namespace, value: namespace_display, display: "namespace"}
        handle_list_output(runners, "runners", output_format, context)
    end
  end

  @doc """
  Lists a namespace's operatives with the reconciler's per-operative health, assigned
  runner, and live node. Reads the durable registry, so it surfaces FAILED
  operatives (whose only placement is a dead node) and is multi-node-safe — it no
  longer crashes on a operative registered across nodes.
  """
  def list_operatives(namespace, output_format \\ "text", opts \\ []) do
    namespace_display = normalize_namespace(namespace)

    # Listings are namespace-scoped: a principal bound to another namespace is
    # refused here (namespace_mismatch), so no verb leaks another tenant's rows.
    _ = authorize_cli!(opts, :"operative.list", namespace_display)

    rows =
      namespace_display
      |> HubManager.operative_rows()
      |> group_partition_rows()

    render_operative_list(rows, namespace_display, output_format)
  end

  @doc """
  Collapse a partition group's member rows into one logical row.

  A pure function between `HubManager.operative_rows/1` and the renderers, so
  the group view is recomputed from the rows on every read — there is no stored
  group record that could disagree with them. Ungrouped operatives pass
  through untouched, which is why `cluster_status` (whose per-node counts are
  per-process facts) simply does not call this.
  """
  @spec group_partition_rows([map()]) :: [map()]
  def group_partition_rows(rows) do
    plain = Enum.reject(rows, &(&1[:partition] != nil))
    Enum.sort_by(plain ++ Enum.map(partition_groups(rows), &group_row/1), &inspect(&1.id))
  end

  defp group_row(report) do
    members = report.members

    %{
      id: report.group,
      health: worst_health(members),
      runner: Enum.map_join(members, ", ", &"p#{&1.partition.index}→#{label_or(&1.runner, "-")}"),
      factory: members |> hd() |> Map.get(:factory),
      partitions: "#{length(members)}/#{report.declared}",
      node: members |> hd() |> Map.get(:node),
      members: members
    }
  end

  # A group is exactly as healthy as its sickest member: an aggregate that
  # rounded up would hide the one partition that stopped serving its keys.
  @health_order [:healthy, :degraded, :unbound, :failed]
  defp worst_health(rows) do
    rows
    |> Enum.map(& &1.health)
    |> Enum.max_by(fn health ->
      case Enum.find_index(@health_order, &(&1 == health)) do
        nil -> length(@health_order)
        index -> index
      end
    end)
  end

  defp render_operative_list(rows, namespace, "json") do
    result = %{
      namespace: namespace,
      operatives:
        Enum.map(rows, fn r ->
          %{
            id: r.id,
            health: r.health,
            runner: r.runner,
            node: r.node && to_string(r.node)
          }
          |> put_unless_nil(:partitions, r[:partitions])
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
      String.pad_trailing("PARTS", 8),
      String.pad_trailing("NODE", 22),
      "RUNNER"
    ])

    Enum.each(rows, fn r ->
      IO.puts([
        "  ",
        String.pad_trailing(to_string(r.id), 34),
        String.pad_trailing(to_string(r.health), 10),
        String.pad_trailing(label_or(r[:partitions], "-"), 8),
        String.pad_trailing(label_or(r.node, "(unplaced)"), 22),
        label_or(r.runner, "(none)")
      ])
    end)
  end

  defp put_unless_nil(map, _key, nil), do: map
  defp put_unless_nil(map, key, value), do: Map.put(map, key, value)

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
    recovery_per_node = cluster_status_recovery(namespace_display, nodes)

    render_cluster_status(
      %{
        namespace: namespace_display,
        nodes: nodes,
        leader: safe_leader(),
        is_leader: safe_is_leader(),
        counts: counts,
        health: health,
        recovery_per_node: recovery_per_node
      },
      output_format
    )
  end

  @doc """
  RPC fan-out: ask every node in the cluster (including the local one)
  for its `HubManager.recovery_state/1`. Returns a map keyed by node().
  Used to populate the `recovery_state` column on `flz cluster status`.
  """
  @spec cluster_status_recovery(binary(), [node()]) :: map()
  def cluster_status_recovery(namespace, nodes) do
    hub_id = Flezha.Service.Identificator.hub(namespace)

    Map.new(nodes, fn n ->
      state =
        if n == node() do
          local_recovery_snapshot(hub_id)
        else
          try do
            case :rpc.call(n, __MODULE__, :local_recovery_snapshot, [hub_id], 5_000) do
              {:badrpc, _} -> :unreachable
              s when is_atom(s) -> s
            end
          catch
            _, _ -> :unreachable
          end
        end

      {n, state}
    end)
  end

  @doc false
  @spec local_recovery_snapshot(atom()) :: atom()
  def local_recovery_snapshot(hub_id) when is_atom(hub_id) do
    case HubManager.recovery_state(hub_id) do
      {:ok, s} -> s
      {:error, :unknown_hub} -> :unknown
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
            recovery_state: Map.get(s.recovery_per_node, n, :unknown)
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

        IO.puts("    #{n}: #{Map.get(s.counts, n, 0)} operative(s) recovery_state=#{rs}#{marker}")
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

    live =
      case Flezha.list_operatives(namespace_display) do
        {:ok, operatives} -> operatives
        _ -> []
      end

    # A finished workflow operative is gone from the live registry but its
    # completion record is retained within the window — surface it too.
    finished = finished_operatives(namespace_display, live)

    if live == [] and finished == [] do
      handle_empty_operatives(namespace_display, output_format)
    else
      handle_operative_info_output(live, finished, namespace_display, output_format)
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

  defp handle_operative_info_output(live, finished, namespace_display, "json") do
    live_data =
      Enum.map(live, fn operative_id ->
        case OperativeManager.get_overwatch_state(namespace_display, operative_id) do
          {:ok, state} ->
            %{
              id: operative_id,
              runner_id: state.runner_id,
              namespace: state.namespace,
              kind: operative_kind(state),
              phase: operative_phase(state),
              uptime_ms: operative_uptime_ms(state),
              default_input_type: state.default_input_type,
              plugins: format_plugins_json(state.extensions)
            }

          {:error, reason} ->
            %{id: operative_id, error: inspect(reason)}
        end
      end)

    finished_data = Enum.map(finished, fn {id, rec} -> finished_operative_json(id, rec) end)

    result = %{
      namespace: namespace_display,
      operatives: live_data ++ finished_data,
      groups: Enum.map(partition_groups(namespace_display), &group_report_json/1)
    }

    IO.puts(Jason.encode!(result))
  end

  defp handle_operative_info_output(live, finished, namespace_display, _) do
    IO.puts("Operative information for namespace '#{namespace_display}':")
    IO.puts("")

    Enum.each(partition_groups(namespace_display), &display_group_report/1)

    Enum.each(live, fn operative_id ->
      case OperativeManager.get_overwatch_state(namespace_display, operative_id) do
        {:ok, state} ->
          display_operative_info(operative_id, state)

        {:error, reason} ->
          IO.puts("Operative: #{operative_id}")
          IO.puts("  Error getting overwatch state: #{inspect(reason)}")
          IO.puts("")
      end
    end)

    Enum.each(finished, fn {id, rec} -> display_finished_operative(id, rec) end)
  end

  defp group_report_json(report) do
    %{
      group: report.group,
      partitions: report.declared,
      members:
        Enum.map(report.members, fn m ->
          %{
            id: m.id,
            partition: m.partition.index,
            key_groups: Tuple.to_list(m.partition.range),
            runner: m.runner,
            health: m.health
          }
        end),
      coverage: coverage_json(report.coverage)
    }
  end

  defp coverage_json(:ok), do: %{complete: true}

  defp coverage_json({:error, %{gaps: gaps, overlaps: overlaps}}) do
    %{
      complete: false,
      gaps: Enum.map(gaps, &Tuple.to_list/1),
      overlaps: Enum.map(overlaps, fn {group, ids} -> %{key_group: group, owners: ids} end)
    }
  end

  # Every partition group among `rows`, with its members in partition order and
  # the read-time coverage check. There is no stored group record, so the check
  # is what turns a missing member into a visible fault rather than a quietly
  # short list. The one grouping both group views read.
  defp partition_groups(rows) when is_list(rows) do
    rows
    |> Enum.filter(&(&1[:partition] != nil))
    |> Enum.group_by(& &1.partition.group)
    |> Enum.map(fn {group, group_rows} ->
      members = Enum.sort_by(group_rows, & &1.partition.index)

      %{
        group: group,
        declared: members |> Enum.map(& &1.partition.count) |> Enum.max(),
        members: members,
        coverage: Partition.coverage(Enum.map(members, &{&1.id, &1.partition.range}))
      }
    end)
    |> Enum.sort_by(& &1.group)
  end

  defp partition_groups(namespace) when is_binary(namespace),
    do: namespace |> HubManager.operative_rows() |> partition_groups()

  defp display_group_report(report) do
    IO.puts("Partition group: #{report.group} (#{length(report.members)}/#{report.declared})")

    Enum.each(report.members, fn m ->
      {from, to} = m.partition.range

      IO.puts(
        "  p#{m.partition.index}  #{String.pad_trailing(m.id, 30)} " <>
          "key groups #{from}-#{to}  #{label_or(m.runner, "(none)")}  #{m.health}"
      )
    end)

    case report.coverage do
      :ok ->
        IO.puts("  coverage: all #{Partition.key_groups()} key groups owned exactly once")

      {:error, %{gaps: gaps, overlaps: overlaps}} ->
        IO.puts("  WARNING: key-group coverage is broken — traffic for the named groups is lost")

        Enum.each(gaps, fn {from, to} ->
          IO.puts("    gap: key groups #{from}-#{to} are owned by no member")
        end)

        Enum.each(overlaps, fn {group, owners} ->
          IO.puts("    overlap: key group #{group} is claimed by #{Enum.join(owners, ", ")}")
        end)
    end

    IO.puts("")
  end

  # Finished workflow operatives with a completion record still inside the
  # retention window, keyed by the operative_class dir (== operative_id).
  defp finished_operatives(namespace, live_ids) do
    now = System.system_time(:second)

    Flezha.State.Registry.operative_classes()
    |> Enum.reject(&(&1 in live_ids))
    |> Enum.flat_map(fn operative_id ->
      case Flezha.State.Completion.read(operative_id) do
        {:ok, %{namespace: ns, finished_at: fa} = rec}
        when ns == namespace and is_integer(fa) ->
          window =
            Flezha.State.PersistenceResolver.workflow_completed_retention_hours(operative_id)

          if fa + window * 3600 >= now, do: [{operative_id, rec}], else: []

        _ ->
          []
      end
    end)
  end

  defp finished_operative_json(operative_id, rec) do
    %{
      id: operative_id,
      namespace: Map.get(rec, :namespace),
      kind: "workflow",
      phase: "finished",
      status: Map.get(rec, :status),
      error: nil_if_empty(Map.get(rec, :error)),
      result: encode_result(Map.get(rec, :result)),
      finished_at: Map.get(rec, :finished_at)
    }
  end

  defp display_finished_operative(operative_id, rec) do
    IO.puts("Operative: #{operative_id}")
    IO.puts("  Kind: workflow")
    IO.puts("  Phase: finished")
    IO.puts("  Status: #{Map.get(rec, :status)}")

    case nil_if_empty(Map.get(rec, :error)) do
      nil -> :ok
      err -> IO.puts("  Error: #{err}")
    end

    IO.puts("  Finished At (unix s): #{Map.get(rec, :finished_at)}")
    IO.puts("")
  end

  defp encode_result(r) when is_binary(r) and byte_size(r) > 0, do: Base.encode64(r)
  defp encode_result(_), do: nil

  defp nil_if_empty(s) when is_binary(s) and byte_size(s) > 0, do: s
  defp nil_if_empty(_), do: nil

  defp operative_kind(%{builder: %{kind: kind}}) when is_atom(kind), do: Atom.to_string(kind)
  defp operative_kind(_), do: "stream"

  # A live operative is "running", unless its loop is parked at a committed
  # iteration boundary — then it is "suspended": still registered, still
  # addressable, holding zero runner resources until an input (or its wake
  # deadline) rehydrates it (`workflow-durable-suspension`).
  defp operative_phase(%{suspended: true}), do: "suspended"
  defp operative_phase(_state), do: "running"

  defp operative_uptime_ms(%{started_at: started}) when is_integer(started),
    do: max(0, System.system_time(:millisecond) - started)

  defp operative_uptime_ms(_), do: nil

  defp format_plugins_json(%{plugins: plugins} = extensions) do
    Enum.map(plugins, fn {plugin_id, plugin} ->
      %{
        id: plugin_id,
        provider: inspect(plugin.provider),
        subscriptions:
          Enum.map(plugin.subscriptions, fn {name, opts} ->
            %{extension: Atom.to_string(name), opts: Map.new(opts)}
          end),
        state: plugin_state(extensions, plugin_id, plugin)
      }
    end)
  end

  defp format_plugins_json(_), do: []

  # A provider's own operator-facing detail, when it exports one. Same shape as
  # the health contract: the stored opts with the tracked pid injected, and a
  # provider that raises or has nothing to say reports nothing rather than
  # failing the whole listing.
  defp plugin_state(extensions, plugin_id, %{provider: provider, opts: opts}) do
    if Code.ensure_loaded?(provider) and function_exported?(provider, :inspect_state, 1) do
      owned_pid =
        case Map.get(Map.get(extensions, :owned_pids, %{}), plugin_id) do
          {pid, _ref} -> pid
          _ -> nil
        end

      provider.inspect_state(Map.put(opts || %{}, :owned_pid, owned_pid))
    end
  catch
    _, _ -> nil
  end

  defp display_plugin_state(nil), do: :ok

  defp display_plugin_state(state) when is_map(state) do
    Enum.each(state, fn {key, value} ->
      IO.puts("        #{key}: #{format_plugin_state_value(value)}")
    end)
  end

  defp display_plugin_state(_), do: :ok

  defp format_plugin_state_value(value) when is_list(value) do
    if value == [], do: "(none)", else: Enum.join(value, ", ")
  end

  defp format_plugin_state_value(nil), do: "(none)"
  defp format_plugin_state_value(value) when is_binary(value), do: value
  defp format_plugin_state_value(value), do: inspect(value)

  defp display_operative_info(operative_id, state) do
    IO.puts("Operative: #{operative_id}")
    IO.puts("  Kind: #{operative_kind(state)}")
    IO.puts("  Phase: #{operative_phase(state)}")
    IO.puts("  Runner ID: #{state.runner_id}")
    IO.puts("  Group ID: #{state.namespace}")
    IO.puts("  Uptime (ms): #{operative_uptime_ms(state)}")
    IO.puts("  Default Input Type: #{state.default_input_type}")
    IO.puts("  Plugins:")

    case state.extensions do
      %{plugins: plugins} = extensions when map_size(plugins) > 0 ->
        Enum.each(plugins, fn {plugin_id, plugin} ->
          IO.puts("    - #{plugin_id}: #{inspect(plugin.provider)}")

          Enum.each(plugin.subscriptions, fn {name, opts} ->
            IO.puts("        subscription: #{name} #{inspect(opts)}")
          end)

          display_plugin_state(plugin_state(extensions, plugin_id, plugin))
        end)

      _ ->
        IO.puts("    (none)")
    end

    IO.puts("")
  end

  @doc """
  Disables a plugin — or, when the id was fanned out over a partition group,
  every `#pK` instance of it.
  """
  def disable_plugin(plugin_id, opts \\ []) do
    with_cli_authorization(
      opts,
      :"plugin.disable",
      plugin_target_namespace(plugin_id),
      %{plugin_id: plugin_id},
      fn ->
        case PluginManager.disable_plugin_group(plugin_id) do
          {:ok, msg} -> IO.puts("Plugin #{plugin_id} disabled successfully (#{msg}).")
          {:error, reason} -> cli_error!("Error disabling plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  # A running plugin's namespace comes from its own registration; an unknown
  # id authorizes against `_system` (and then fails on its own terms).
  defp plugin_target_namespace(plugin_id) do
    case Map.get(PluginManager.get_plugins(), plugin_id) do
      {namespace, _operative_id} when is_binary(namespace) -> namespace
      _ -> "_system"
    end
  end

  ## Installable-plugin subsystem bridges -------------------------------------

  # The installer surface is node-level (a `.fpz` lands on the controller's
  # disk for every namespace), so every installer verb authorizes on `_system`.
  defp json_token_opts(opts_json) when is_binary(opts_json) do
    case Jason.decode(opts_json) do
      {:ok, %{"token" => token}} when is_binary(token) -> [token: token]
      _ -> []
    end
  end

  defp json_token_opts(_), do: []

  @doc "Install a plugin from a `.fpz` archive."
  def install_plugin(path, opts_json) do
    with_cli_authorization(
      json_token_opts(opts_json),
      :"plugin.install",
      "_system",
      %{path: path},
      fn ->
        case Flezha.Plugin.Installer.install(path) do
          {:ok, installed} ->
            IO.puts(
              "Installed plugin #{installed.id}@#{installed.version} (status: #{installed.status})"
            )

          {:error, reason} ->
            cli_error!("Error installing plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  @doc "Upgrade an installed plugin to a new version in `path`."
  def upgrade_plugin(plugin_id, path, opts_json) do
    with_cli_authorization(
      json_token_opts(opts_json),
      :"plugin.upgrade",
      "_system",
      %{plugin_id: plugin_id},
      fn ->
        case Flezha.Plugin.Installer.upgrade(plugin_id, path) do
          {:ok, installed} ->
            IO.puts("Upgraded plugin #{installed.id} to #{installed.version}")

          {:error, reason} ->
            cli_error!("Error upgrading plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  @doc "Uninstall an installed plugin by id."
  def uninstall_plugin(plugin_id, opts_json) do
    with_cli_authorization(
      json_token_opts(opts_json),
      :"plugin.uninstall",
      "_system",
      %{plugin_id: plugin_id},
      fn ->
        case Flezha.Plugin.Installer.uninstall(plugin_id) do
          :ok -> IO.puts("Uninstalled plugin #{plugin_id}.")
          {:error, reason} -> cli_error!("Error uninstalling plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  @doc "Enable an installed plugin (routes through the Installer)."
  def installer_enable_plugin(plugin_id, opts_json) do
    with_cli_authorization(
      json_token_opts(opts_json),
      :"plugin.enable",
      "_system",
      %{plugin_id: plugin_id},
      fn ->
        case Flezha.Plugin.Installer.enable(plugin_id) do
          :ok -> IO.puts("Plugin #{plugin_id} enabled successfully.")
          {:error, reason} -> cli_error!("Error enabling plugin: #{inspect(reason)}.")
        end
      end
    )
  end

  @doc "Disable an installed plugin (routes through the Installer)."
  def installer_disable_plugin(plugin_id, opts_json) do
    with_cli_authorization(
      json_token_opts(opts_json),
      :"plugin.disable",
      "_system",
      %{plugin_id: plugin_id},
      fn ->
        case Flezha.Plugin.Installer.disable(plugin_id) do
          :ok -> IO.puts("Plugin #{plugin_id} disabled successfully.")
          {:error, reason} -> cli_error!("Error disabling plugin: #{inspect(reason)}.")
        end
      end
    )
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
            cli_error!("Error getting help for plugin '#{provider_id}': #{Exception.message(e)}")
        end

      {:error, reason} ->
        cli_error!("Error: #{reason}")
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
      ProcessHub.Service.Recovery.recovery_state(hub_id)
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
          case ProcessHub.Service.Recovery.await_normal(hub_id, timeout_ms) do
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

        exit(:cli_error)
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
        cli_error!("boot import-legacy-snapshot: #{inspect(reason)}", :stderr)
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

      # Declared like any operator start: an imported operative the declared
      # set never learns about would be invisible to the reconcile — imported
      # once, restored never.
      _ =
        ProcessHub.start_children(hub_id, [cspec],
          async_wait: false,
          durable: HubManager.durable_starts?()
        )
    end)

    IO.puts("boot import-legacy-snapshot: imported #{length(operatives)} operatives")
    :ok
  end

  # ---------- `flz state` subcommand handlers ----------

  @doc """
  `flz state get <operative/procedure[/entity_key]>` — the CURRENT in-memory
  state of one live Entity (or of one Procedure's Entities), as JSON, with the
  `change_seq` it was read at.

  The value is the freshest one there is: it includes changes applied but not
  yet flushed, so it is at or ahead of the durable state, and the seq is how a
  caller tells versions apart. An Entity that is not held in memory is refused
  naming why — never served from the durable store, which is what
  `flz state at` is for.
  """
  def state_get(path_string, output_format \\ "text", opts \\ []) do
    with {:ok, {operative_id, procedure, entity_key}} <- parse_live_path(path_string),
         {:ok, namespace, targets} <- resolve_state_targets(operative_id, entity_key),
         :ok <-
           Flezha.AuthZ.authorize(
             cli_context(opts),
             :"state.get",
             Flezha.AuthZ.Scope.build(namespace)
           ),
         {:ok, json, change_seq} <-
           read_entity_state(namespace, targets, procedure, entity_key),
         :ok <- within_read_ceiling(json) do
      print_state_get(path_string, json, change_seq, output_format)
    else
      {:error, reason} -> cli_error!("state get: #{render_state_get_error(reason)}", :stderr)
    end
  end

  # The read authorizes and resolves against the namespace that actually holds
  # the Operative rather than assuming `default`.
  #
  # A partition group id names no operative — it names N — so a keyed read
  # resolves the one member owning the key's group, and a procedure-level read
  # fans out over every member and merges: each holds only its own share.
  defp resolve_state_targets(operative_id, entity_key) do
    case OperativeManager.namespace_of(operative_id) do
      {:ok, namespace} ->
        {:ok, namespace, [operative_id]}

      :error ->
        with {:ok, namespace} <- group_namespace(operative_id),
             members = PartitionGroup.members(namespace, operative_id) do
          {:ok, namespace, owning_members(members, entity_key)}
        end
    end
  end

  defp group_namespace(operative_id) do
    case OperativeManager.namespace_of_group(operative_id) do
      {:ok, namespace} -> {:ok, namespace}
      :error -> {:error, {:no_operative, operative_id}}
    end
  end

  defp owning_members(members, nil), do: Enum.map(members, & &1.id)

  defp owning_members(members, entity_key) do
    case Enum.find(members, &Partition.owns?(&1.partition.range, entity_key)) do
      nil -> Enum.map(members, & &1.id)
      member -> [member.id]
    end
  end

  defp read_entity_state(namespace, [operative_id], procedure, entity_key) do
    OperativeManager.get_entity_state(namespace, operative_id, %{
      procedure: procedure,
      entity_key: entity_key
    })
  end

  defp read_entity_state(namespace, targets, procedure, entity_key) do
    targets
    |> Enum.map(&read_entity_state(namespace, [&1], procedure, entity_key))
    |> Enum.filter(&match?({:ok, _json, _seq}, &1))
    |> case do
      [] ->
        {:error, {:no_operative, hd(targets)}}

      reads ->
        merged =
          Enum.reduce(reads, %{}, fn {:ok, json, _seq}, acc ->
            case Jason.decode(json) do
              {:ok, decoded} when is_map(decoded) -> deep_merge(acc, decoded)
              _ -> acc
            end
          end)

        {:ok, Jason.encode!(merged), Enum.map(reads, fn {:ok, _, seq} -> seq end) |> Enum.max()}
    end
  end

  # Two members answer the same procedure with disjoint entity keys, so the
  # merge is per-procedure, per-key — never a whole-value overwrite.
  defp deep_merge(left, right) when is_map(left) and is_map(right),
    do: Map.merge(left, right, fn _key, l, r -> deep_merge(l, r) end)

  defp deep_merge(_left, right), do: right

  # `<operative>/<procedure>[/<entity_key>]` for a LIVE read. No on-disk check,
  # unlike `parse_state_path/1`: a resident Entity may have no WAL yet, and a
  # persistence-disabled one never will.
  defp parse_live_path(path_string) do
    case String.split(path_string, "/", parts: 3) do
      [operative, procedure, entity_key] when operative != "" and procedure != "" ->
        {:ok, {operative, procedure, entity_key}}

      [operative, procedure] when operative != "" and procedure != "" ->
        {:ok, {operative, procedure, nil}}

      _ ->
        {:error,
         {:bad_path, "expected <operative>/<procedure>[/<entity_key>]; got #{path_string}"}}
    end
  end

  # Same ceiling the workflow handoff uses — a whole-Procedure read of a big
  # Operative is the way to blow it, and naming the narrowing selector is the
  # way out.
  defp within_read_ceiling(json) do
    limit = Flezha.State.PersistenceResolver.wal_high_watermark_bytes()

    case byte_size(json) do
      bytes when bytes > limit -> {:error, {:oversized_state, bytes}}
      _ -> :ok
    end
  end

  defp print_state_get(path_string, json, change_seq, "json") do
    IO.puts(
      Jason.encode!(%{
        path: path_string,
        change_seq: change_seq,
        state: Jason.decode!(json)
      })
    )
  end

  defp print_state_get(path_string, json, change_seq, _) do
    IO.puts("state get #{path_string}:")
    IO.puts("  change_seq: #{change_seq || "-"}")
    IO.puts("  state:      #{json}")
  end

  defp render_state_get_error({:bad_path, message}), do: message

  defp render_state_get_error({:no_operative, operative_id}),
    do: "no running operative `#{operative_id}` in any namespace"

  defp render_state_get_error({:not_resident, :suspended}),
    do:
      "the instance is suspended and holds no live Entity — " <>
        "read its durable state with `flz state at`"

  defp render_state_get_error({:not_resident, :unbound}),
    do:
      "the Operative is not bound to a runner, so nothing holds its state — " <>
        "read its durable state with `flz state at`"

  defp render_state_get_error({:unsupported_for_kind, :persistent_state}),
    do: "this Operative kind holds no per-Entity state, so there is nothing to read"

  defp render_state_get_error({:oversized_state, bytes}),
    do:
      "the selected state is #{bytes} bytes, over the :wal_high_watermark_bytes " <>
        "ceiling — narrow the read with <operative>/<procedure>/<entity_key>"

  defp render_state_get_error(reason) when is_binary(reason) do
    # SDK refusals lead with a canonical token; the not-resident family points
    # at the durable-read path, which the SDK cannot know about.
    if String.starts_with?(reason, ["entity_evicted", "entity_not_found"]) do
      reason <> " — read its durable state with `flz state at`"
    else
      reason
    end
  end

  defp render_state_get_error({:forbidden, reason}), do: render_forbidden(reason)
  defp render_state_get_error(reason), do: inspect(reason)

  @doc """
  `flz state list <operative_class>` — print one row per Entity under
  `<operative_class>` with `wal_seq`, `snapshot_seq`, `wal_bytes`.
  """
  def state_list(operative_class, output_format \\ "text", opts \\ []) do
    _ = authorize_cli!(opts, :"state.list", state_target_namespace(operative_class))
    do_state_list(operative_class, output_format)
  end

  defp do_state_list(operative_class, output_format) do
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
          procedure_id: sh,
          entity_key: pk,
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
        IO.puts(Jason.encode!(%{operative_class: operative_class, entities: rows}))

      _ ->
        IO.puts("Entities under operative #{operative_class}:\n")
        Enum.each(rows, &print_state_row/1)
    end
  end

  defp print_state_row(row) do
    IO.puts("  #{row.procedure_id}/#{row.entity_key}")
    IO.puts("    wal_seq:                 #{row.wal_seq}")
    IO.puts("    snapshot_seq:            #{row.snapshot_seq}")
    IO.puts("    wal_bytes:               #{row.wal_bytes}")
    IO.puts("    flush_latency_ms_p99:    #{row.flush_latency_ms_p99}")
    IO.puts("    fsync_durability_lag_ms: #{row.fsync_durability_lag_ms}")
    IO.puts("    dedup_drops_total:       #{row.dedup_drops_total}")
    IO.puts("")
  end

  @doc """
  `flz state at <operative/procedure/entity> <seq | timestamp>` — point-in-time
  query. Resolves the path string into a `(runner_id, operative,
  procedure, entity_key)` by scanning the on-disk state tree (one match
  expected; ambiguous paths print all matches and exit non-zero).
  """
  def state_at(path_string, target_string, output_format \\ "text") do
    case parse_state_path(path_string) do
      {:ok, entity_id} ->
        target = parse_state_target(target_string)

        case Flezha.State.TimeTravel.query(entity_id, target) do
          {:ok, result} ->
            print_state_at(entity_id, target, result, output_format)

          {:error, %{kind: kind, message: msg, earliest_available: earliest}} ->
            cli_error!(
              "state at: #{kind}: #{msg} (earliest_available=#{inspect(earliest)})",
              :stderr
            )
        end

      {:error, reason} ->
        cli_error!("state at: #{reason}", :stderr)
    end
  end

  defp print_state_at(_entity_id, _target, result, "json") do
    IO.puts(
      Jason.encode!(%{
        terminal_seq: result.terminal_seq,
        snapshot_size: byte_size(result.snapshot_bytes),
        change_log_size: byte_size(result.change_log_bytes)
      })
    )
  end

  defp print_state_at(entity_id, target, result, _) do
    {rc, sh, pk} = entity_id

    IO.puts("state at #{rc}/#{sh}/#{pk} target=#{inspect(target)}:")
    IO.puts("  terminal_seq:    #{result.terminal_seq}")
    IO.puts("  snapshot bytes:  #{byte_size(result.snapshot_bytes)}")
    IO.puts("  change_log bytes:#{byte_size(result.change_log_bytes)}")
  end

  @doc """
  `flz state rotate <operative/procedure/entity>` — force a compaction by
  dispatching a `CompactEntity` to the SDK.
  """
  def state_rotate(path_string, opts \\ []) do
    case parse_state_path(path_string) do
      {:ok, {rc, sh, pk}} ->
        with_cli_authorization(
          opts,
          :"state.rotate",
          state_target_namespace(rc),
          %{entity: path_string},
          fn ->
            case lookup_runner_for_entity(rc, sh, pk) do
              {:ok, runner_id} ->
                case Flezha.State.Commands.dispatch_compact_entity(runner_id, {rc, sh, pk}) do
                  {:ok, _ref} ->
                    IO.puts("compaction queued for #{rc}/#{sh}/#{pk}")

                  {:error, reason} ->
                    cli_error!("state rotate: #{inspect(reason)}", :stderr)
                end

              {:error, reason} ->
                cli_error!("state rotate: #{reason}", :stderr)
            end
          end
        )

      {:error, reason} ->
        cli_error!("state rotate: #{reason}", :stderr)
    end
  end

  # The namespace a state verb is authorized against: the registry's answer
  # for a live Operative, else the on-disk claim (`Flezha.State.Registry`),
  # so a stopped Operative's durable state is still owned by its namespace.
  defp state_target_namespace(operative_class) do
    case OperativeManager.namespace_of(operative_class) do
      {:ok, namespace} -> namespace
      :error -> Flezha.State.Registry.namespace_of_class(operative_class)
    end
  end

  defp lookup_runner_for_entity(rc, sh, pk) do
    Flezha.State.Recovery.resolve_runner({rc, sh, pk})
  end

  @doc """
  `flz state delete <operative/procedure/key>` — hard-delete one Entity's
  persisted state (WAL + snapshots + outbox-dedup). Idempotent: deleting
  an entity with no on-disk state still succeeds. This is the
  operator-driven, confirmation-returning counterpart to the SDK reaper's
  fire-and-forget `delete_entity_state` send (Decision 8).
  """
  def state_delete(path_string, opts \\ []) do
    case String.split(path_string, "/", parts: 3) do
      [rc, sh, pk] ->
        with_cli_authorization(
          opts,
          :"state.delete",
          state_target_namespace(rc),
          %{entity: path_string},
          fn ->
            {:ok, _} = Flezha.State.Supervisor.delete_entity({rc, sh, pk})
            IO.puts("deleted persisted state for #{rc}/#{sh}/#{pk}")
          end
        )

      _ ->
        cli_error!(
          "state delete: expected <operative>/<procedure>/<entity_key>; got #{path_string}",
          :stderr
        )
    end
  end

  @doc """
  `flz state outbox <operative/procedure/key>` — effectively-once outbox
  status for one Entity: durable seq, flush cursor, epoch, undispatched
  lag, and the operative's input-log min-watermark.
  """
  def state_outbox(path_string, _opts \\ []) do
    case parse_state_path(path_string) do
      {:ok, {rc, sh, pk} = entity_id} ->
        status =
          case Flezha.State.Supervisor.lookup(entity_id) do
            {:ok, pid} ->
              Flezha.State.WalWriter.outbox_status(pid)

            :error ->
              dedup = Flezha.State.Outbox.read(entity_id)

              %{
                last_persisted_seq: dedup.last_persisted_seq,
                outbox_flushed_seq: dedup.outbox_flushed_seq,
                epoch: dedup.epoch,
                pending: nil
              }
          end

        watermark = Flezha.State.InputLog.watermark(rc)

        IO.puts("entity            #{rc}/#{sh}/#{pk}")
        IO.puts("last_persisted_seq #{status.last_persisted_seq}")
        IO.puts("outbox_flushed_seq #{status.outbox_flushed_seq}")
        IO.puts("epoch              #{status.epoch}")
        IO.puts("outbox_lag         #{status.pending || "unknown (writer not running)"}")
        IO.puts("input_watermark    #{watermark || "none"}")

      {:error, reason} ->
        cli_error!("state outbox: #{reason}", :stderr)
    end
  end

  @doc """
  `flz operative replay <operative/procedure/key> --from <seq|ts>` — re-run a
  Entity's retained inputs from a point in the past through the current code.

  The same transaction a late input triggers automatically, with the operator
  choosing the baseline instead of a late event: rewind the Entity there, bump
  the epoch, and replay the input log forward. A conforming keyed sink overwrites
  the stale rows by natural key, which is what makes "deploy a fix, heal the
  recent past" a single verb rather than a migration.
  """
  def operative_replay(namespace, path_string, from_string) do
    with {:ok, {operative_class, _, _} = entity_id} <- parse_state_path(path_string),
         {:ok, overwatch} <- resolve_replay_overwatch(namespace, operative_class),
         {:ok, target_seq} <- resolve_replay_seq(entity_id, from_string) do
      Flezha.Operative.Replay.replay_from(overwatch, entity_id, target_seq)

      IO.puts(
        "Replaying #{path_string} from seq #{target_seq}. Outputs republish under a " <>
          "bumped epoch; a keyed sink overwrites the stale rows by natural key."
      )
    else
      {:error, %{kind: :outside_retention, earliest_available: earliest}} ->
        cli_error!(
          "replay: outside_retention — the earliest replayable point for " <>
            "#{path_string} is seq #{inspect(earliest)}",
          :stderr
        )

      {:error, reason} ->
        cli_error!("replay: #{inspect(reason)}", :stderr)
    end
  end

  defp resolve_replay_overwatch(namespace, operative_class) do
    _ = normalize_namespace(namespace)

    case Flezha.Operative.ClassRegistry.overwatch_pid(operative_class) do
      {:ok, pid} -> {:ok, pid}
      :error -> {:error, {:not_running, operative_class}}
    end
  end

  # A `--from` is a WAL seq or a point in time; the same `TimeTravel` resolution
  # `flz state at` uses, which is now exact rather than an mtime approximation.
  defp resolve_replay_seq(entity_id, from_string) do
    case Flezha.State.TimeTravel.query(entity_id, parse_state_target(from_string)) do
      {:ok, %{terminal_seq: seq}} -> {:ok, seq}
      {:error, err} -> {:error, err}
    end
  end

  @doc """
  `flz state journal <operative/instance>` — render a workflow instance's
  proc-journal entries (`workflow-proc-journal`) in append order: seq, type,
  proc index, iteration, the entities that ran, and the handoff batch size.
  """
  def state_journal(instance_id, output_format \\ "text", _opts \\ []) do
    {:ok, entries} = Flezha.State.ProcJournal.read(instance_id)

    rows = Enum.map(entries, &journal_row/1)
    leaf_procs = Flezha.Operative.ClassRegistry.leaf_procs(instance_id)
    complete = Flezha.State.ProcJournal.complete?(instance_id, leaf_procs)

    case output_format do
      "json" ->
        IO.puts(
          Jason.encode!(%{
            instance: instance_id,
            entries: rows,
            leaf_procs: leaf_procs,
            complete: complete
          })
        )

      _ ->
        if rows == [] do
          IO.puts("no proc-journal for instance #{instance_id}")
        else
          IO.puts("proc-journal for instance #{instance_id}:\n")
          Enum.each(rows, &print_journal_row/1)
          print_journal_completion(leaf_procs, complete)
        end
    end
  end

  # The leaf set is what "done" means for a tree-shaped chain: the operative is
  # complete when the in-flight iteration has committed every leaf. A class that
  # declared none (run-sugar, stream, an older SDK) leaves it underivable.
  defp print_journal_completion(nil, _complete),
    do: IO.puts("\n  complete: unknown (class declares no leaf procedures)")

  defp print_journal_completion(leaf_procs, complete),
    do: IO.puts("\n  leaves: #{Enum.join(leaf_procs, ",")}  complete: #{complete}")

  defp journal_row(%{type: :proc_committed, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.merge(journal_handoff(payload.handoff))
    |> Map.merge(%{
      entities_ran: Enum.map(payload.entities_ran, &Flezha.State.Wire.entity_label/1),
      outbox: journal_outbox(payload.outbox)
    })
  end

  defp journal_row(%{type: :compensated, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:outbox, journal_outbox(payload.outbox))
  end

  defp journal_row(%{type: :iter_committed, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.merge(journal_handoff(payload.handoff))
    |> Map.put(:consumed_offset, payload.consumed_offset)
  end

  defp journal_row(%{type: :parked, payload: payload} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:wake_deadline_ms, payload.wake_deadline_ms)
  end

  # `for_type` names which entry's outbox this marks delivered — a procedure
  # can have both a forward `proc_committed` outbox and a `compensated` undo one.
  defp journal_row(%{type: :outbox_flushed, payload: %{for_type: for_type}} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:flushes, to_string(for_type))
  end

  # A `rewound` entry's `iter` IS the iteration it rewound to; spell that out
  # rather than leaving the reader to infer it from a bare `iter=` column.
  defp journal_row(%{type: :rewound} = entry) do
    entry
    |> base_journal_row()
    |> Map.put(:rewound_to, entry.iter)
  end

  defp journal_row(entry), do: base_journal_row(entry)

  defp base_journal_row(entry) do
    %{
      seq: entry.seq,
      type: Atom.to_string(entry.type),
      proc_idx: entry.proc_idx,
      iter: entry.iter,
      epoch: entry.epoch
    }
  end

  # A handoff is per-edge: each item carries the channel it was released on,
  # which is what routes it to the branch consuming that channel. The totals are
  # the edges summed, so one grouping answers both.
  defp journal_handoff(handoff) do
    edges =
      handoff
      |> Enum.group_by(fn {cid, _} -> cid end, fn {_, bytes} -> byte_size(bytes) end)
      |> Enum.sort()
      |> Enum.map(fn {cid, sizes} ->
        %{channel_id: cid, items: length(sizes), bytes: Enum.sum(sizes)}
      end)

    %{
      handoff_items: edges |> Enum.map(& &1.items) |> Enum.sum(),
      handoff_bytes: edges |> Enum.map(& &1.bytes) |> Enum.sum(),
      handoff_edges: edges
    }
  end

  defp journal_outbox(outbox) do
    Enum.map(outbox, fn item ->
      %{
        entity: Flezha.State.Wire.entity_label(item.entity_id),
        seq_in_proc: item.seq_in_proc,
        channel: item.channel,
        bytes: byte_size(item.payload)
      }
    end)
  end

  defp print_journal_row(row) do
    IO.puts(
      "  seq=#{row.seq} #{row.type} proc_idx=#{row.proc_idx} iter=#{row.iter} epoch=#{row.epoch}"
    )

    Enum.each(Map.get(row, :entities_ran, []), fn seg -> IO.puts("    ran: #{seg}") end)

    if Map.has_key?(row, :handoff_items) do
      IO.puts("    handoff: #{row.handoff_items} item(s), #{row.handoff_bytes} byte(s)")
    end

    Enum.each(Map.get(row, :handoff_edges, []), fn edge ->
      IO.puts(
        "      edge: channel_id=#{edge.channel_id} #{edge.items} item(s), #{edge.bytes} byte(s)"
      )
    end)

    Enum.each(Map.get(row, :outbox, []), fn item ->
      IO.puts(
        "    outbox: #{item.entity} seq_in_proc=#{item.seq_in_proc} " <>
          "channel=#{item.channel} #{item.bytes} byte(s)"
      )
    end)

    if flushes = Map.get(row, :flushes) do
      IO.puts("    flushes: #{flushes}")
    end

    if Map.has_key?(row, :consumed_offset) do
      IO.puts("    consumed inbox offset: #{row.consumed_offset}")
    end

    if Map.has_key?(row, :wake_deadline_ms) do
      IO.puts("    wakes at: #{row.wake_deadline_ms || "next input"}")
    end

    if Map.has_key?(row, :rewound_to) do
      IO.puts("    rewound to iteration: #{row.rewound_to}")
    end

    IO.puts("")
  end

  @doc """
  `flz state inbox <instance>` — the durable inbox of a workflow instance
  (`workflow-durable-suspension`): every appended input with its offset, marking
  which are still undelivered (above the consumed offset the last committed
  iteration recorded).
  """
  def state_inbox(instance_id, output_format \\ "text", _opts \\ []) do
    entries = Flezha.State.ProcInbox.read(instance_id)
    consumed = Flezha.State.ProcJournal.consumed_offset(instance_id)

    rows =
      Enum.map(entries, fn entry ->
        %{
          offset: entry.offset,
          consumed: entry.offset <= consumed,
          bytes: byte_size(entry.message),
          channel: Map.get(entry.metadata, "channel"),
          wake: Map.get(entry.metadata, "flz.wake")
        }
      end)

    case output_format do
      "json" ->
        IO.puts(Jason.encode!(%{instance: instance_id, consumed_offset: consumed, entries: rows}))

      _ ->
        if rows == [] do
          IO.puts("no durable inbox for instance #{instance_id}")
        else
          IO.puts("durable inbox for instance #{instance_id} (consumed through #{consumed}):\n")

          Enum.each(rows, fn row ->
            IO.puts(
              "  offset=#{row.offset} #{if row.consumed, do: "consumed", else: "PENDING"} " <>
                "#{row.bytes} byte(s)" <>
                if(row.channel, do: " channel=#{row.channel}", else: "") <>
                if(row.wake, do: " wake=#{row.wake}", else: "")
            )
          end)

          IO.puts("")
        end
    end
  end

  @doc """
  `flz state retention <operative_class> <hours>` — set runtime retention
  for `<operative_class>`. Persisted across the next Application boot only
  if the runner separately writes it to runtime.exs.
  """
  def state_retention(operative_class, hours_string) do
    case Integer.parse(hours_string) do
      {hours, ""} when hours >= 0 ->
        current = Application.get_env(:flezha, :time_travel_retention_hours, %{})
        updated = Map.put(current, operative_class, hours)
        Application.put_env(:flezha, :time_travel_retention_hours, updated, persistent: true)

        IO.puts(
          "retention[#{operative_class}] = #{hours} hours (in-memory; persist via runtime.exs)"
        )

      _ ->
        cli_error!("state retention: hours must be a non-negative integer", :stderr)
    end
  end

  defp parse_state_path(path_string) do
    case String.split(path_string, "/", parts: 3) do
      [rc, sh, pk] ->
        # A controller that adopted this operative may hold it only remotely.
        # The manifest is per-Entity, so answering about one key fetches one
        # key's artifacts — never the whole Procedure. Inert with no backend.
        if has_persisted_state?(rc, sh, pk) or hydrated_state?(rc, sh, pk) do
          {:ok, {rc, sh, pk}}
        else
          {:error, "no persisted state at #{path_string}"}
        end

      _ ->
        {:error, "expected <operative>/<procedure>/<entity_key>; got #{path_string}"}
    end
  end

  defp has_persisted_state?(rc, sh, pk) do
    Enum.any?(Flezha.State.Registry.all_entities(), fn {r, s, p} ->
      r == rc and s == sh and p == pk
    end)
  end

  defp hydrated_state?(rc, sh, pk) do
    Flezha.State.DurabilityTier.hydrate_entities(rc, sh, [pk])
    has_persisted_state?(rc, sh, pk)
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

    with_cli_authorization(
      [token: Map.get(opts_map, :token)],
      :"controller.stop",
      "_system",
      %{},
      fn -> do_controller_stop(opts_map) end
    )
  end

  defp do_controller_stop(opts_map) do
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
    status = Map.merge(safe_shutdown_status(), durability_status())

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
        IO.puts("durability_backend:            #{status.durability_backend}")
        IO.puts("durability_rpo_breaches:       #{status.durability_rpo_breaches}")
        IO.puts("durability_superseded:         #{status.durability_superseded}")
    end

    :ok
  end

  # Remote durability tier, as an operator reads it: which backend is
  # configured, how many references are past their RPO target, and how many are
  # now shipped by another controller. A stalled shipper must be visible here
  # rather than only by inspecting the backend.
  #
  # A superseded count is not an alarm on its own — a handover produces one —
  # but a count that keeps climbing on a node that should still own its
  # operatives is how a placement problem shows up here.
  defp durability_status() do
    backend =
      Application.get_env(:flezha, :durability_tier, []) |> Keyword.get(:backend, :none)

    %{
      durability_backend: backend,
      durability_rpo_breaches: length(Flezha.State.DurabilityTier.breaches()),
      durability_superseded:
        Enum.count(Flezha.State.DurabilityTier.status(), &(&1.superseded_by != nil))
    }
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
    cli_error!(
      "error: pubsub sink is disabled; enable :observability, :pubsub_sink_enabled to use `flz observe tail`",
      :stderr
    )
  end

  defp render_error(:observability_disabled) do
    cli_error!("error: observability is disabled", :stderr)
  end

  defp render_error(reason) do
    cli_error!("error: #{inspect(reason)}", :stderr)
  end

  defp cli_context(opts) do
    base = %{source: :cli}

    base =
      case Keyword.get(opts, :request_id) do
        nil -> base
        id when is_binary(id) -> Map.put(base, :request_id, id)
      end

    # Identity comes ONLY from a verified token (`flz --token` /
    # `FLEZHA_TOKEN`, threaded through as `opts[:token]`). A caller-supplied
    # principal/namespace/role is never trusted — without a resolvable token
    # the context stays unbound and gated verbs refuse naming the credential.
    case Keyword.get(opts, :token) do
      token when is_binary(token) and token != "" -> bind_token_identity(base, token)
      _ -> base
    end
  end

  defp bind_token_identity(base, token) do
    case Flezha.AuthZ.TokenStore.verify(token) do
      {:ok, %{principal: principal, namespace: namespace, role: role}} ->
        base
        |> Map.put(:principal, principal)
        |> Map.put(:namespace, namespace)
        |> Map.put(:role, role)

      {:error, _reason} ->
        base
    end
  catch
    # The token store is not running while auth is disabled; an unbound
    # context is the correct degraded shape either way.
    _, _ -> base
  end

  # The ONE authorize-or-halt seam for CLI verbs: authorize against the verb's
  # RESOLVED target namespace, rendering a refusal as the CLI's non-zero exit.
  # Returns the caller context so gated writers can audit the actor. The
  # denial itself is audited by `AuthZ.authorize/3`.
  defp authorize_cli!(opts, action, namespace) do
    ctx = cli_context(opts)

    case Flezha.AuthZ.authorize(ctx, action, Flezha.AuthZ.Scope.build(namespace)) do
      :ok ->
        ctx

      {:error, {:forbidden, reason}} ->
        cli_error!("#{action}: #{render_forbidden(reason)}", :stderr)
    end
  end

  # Destructive-verb shape on top of `authorize_cli!/3`: run the body, then
  # audit actor + target on success.
  defp with_cli_authorization(opts, action, namespace, audit_target, fun) do
    ctx = authorize_cli!(opts, action, namespace)
    result = fun.()

    emit_cli_audit(
      action,
      Map.get(ctx, :principal),
      Map.merge(audit_target, %{namespace: namespace})
    )

    result
  end

  defp render_forbidden(:bootstrap_pending) do
    "refused: authorization is enforced but the bootstrap has not been claimed — " <>
      "run `flz auth bootstrap <token>` (token path is logged at controller boot)"
  end

  defp render_forbidden(:principal_not_bound) do
    "refused: no verifiable identity — supply `--token <token>` or set FLEZHA_TOKEN"
  end

  defp render_forbidden(:no_role_binding) do
    "refused: the presented identity has no role binding — ask an admin for `flz role bind`"
  end

  defp render_forbidden(reason), do: "refused: #{reason}"

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

  # ----- CLI renderers for the `flz auth|token|role|namespace` verbs -----
  #
  # Thin printing shells over the wrappers above — the wrappers own
  # authorization and auditing; these own string parsing and output.

  @doc "CLI entry: `flz auth bootstrap <token>`."
  def authz_render_bootstrap(token, opts \\ []) do
    case authz_bootstrap(token, opts) do
      {:ok, %{principal: principal, minted_token: nil}} ->
        IO.puts("bootstrap claimed; admin@_system bound to #{principal}")

      {:ok, %{principal: principal, minted_token: minted}} ->
        IO.puts("bootstrap claimed; admin@_system bound to #{principal}")
        IO.puts("admin token (shown once, store it now): #{minted}")

      {:error, reason} ->
        cli_error!("auth bootstrap: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz auth whoami`."
  def authz_render_whoami(opts \\ []) do
    {:ok, identity} = authz_whoami(opts)
    IO.puts("principal: #{identity.principal || "(unbound — supply --token / FLEZHA_TOKEN)"}")
    IO.puts("namespace: #{identity.namespace || "-"}")
    IO.puts("role:      #{identity.role || "-"}")
  end

  @doc "CLI entry: `flz token issue <principal> <namespace> <role> [--ttl <seconds>]`."
  def authz_render_token_issue(principal, namespace, role_string, ttl_seconds, opts \\ []) do
    with {:ok, role} <- parse_role(role_string),
         {:ok, raw, entry} <- authz_issue(principal, namespace, role, ttl_seconds, opts) do
      IO.puts("token issued for #{principal} (#{role}@#{namespace})")
      IO.puts("id:    #{String.slice(entry.token_hash, 0, 12)}")
      IO.puts("token (shown once, store it now): #{raw}")
    else
      {:error, reason} -> cli_error!("token issue: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz token revoke <id>`."
  def authz_render_token_revoke(token_id, opts \\ []) do
    case authz_revoke(token_id, opts) do
      {:ok, entry} -> IO.puts("revoked token #{String.slice(entry.token_hash, 0, 12)}")
      {:error, reason} -> cli_error!("token revoke: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz token list`."
  def authz_render_token_list(opts \\ []) do
    case authz_list(opts) do
      {:ok, []} ->
        IO.puts("no tokens")

      {:ok, entries} ->
        Enum.each(entries, fn entry ->
          status =
            cond do
              is_integer(entry.revoked_at) ->
                "revoked"

              entry.expires_at != :never and entry.expires_at <= System.system_time(:second) ->
                "expired"

              true ->
                "active"
            end

          IO.puts(
            "#{String.slice(entry.token_hash, 0, 12)}  #{entry.principal}  " <>
              "#{entry.role}@#{entry.namespace}  #{status}#{token_label_suffix(entry)}"
          )
        end)

      {:error, reason} ->
        cli_error!("token list: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  defp token_label_suffix(%{label: label}) when is_binary(label) and label != "",
    do: "  (#{label})"

  defp token_label_suffix(_), do: ""

  @doc "CLI entry: `flz role bind <principal> <role> <namespace>`."
  def authz_render_role_bind(principal, role_string, namespace, opts \\ []) do
    with {:ok, role} <- parse_role(role_string),
         :ok <- role_bind(principal, namespace, role, opts) do
      IO.puts("bound #{principal} as #{role}@#{namespace}")
    else
      {:error, reason} -> cli_error!("role bind: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz role unbind <principal>`."
  def authz_render_role_unbind(principal, opts \\ []) do
    case role_unbind(principal, opts) do
      :ok -> IO.puts("unbound #{principal}")
      {:error, reason} -> cli_error!("role unbind: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz role list`."
  def authz_render_role_list(opts \\ []) do
    case role_list(opts) do
      {:ok, []} ->
        IO.puts("no role bindings")

      {:ok, bindings} ->
        Enum.each(bindings, fn binding ->
          IO.puts("#{binding.principal}  #{binding.role}@#{binding.namespace}")
        end)

      {:error, reason} ->
        cli_error!("role list: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz namespace create <name> [--Xquota=value ...]`."
  def authz_render_namespace_create(name, quotas_json, opts \\ []) do
    with {:ok, quotas} <- parse_quota_overrides(quotas_json),
         {:ok, entry} <- namespace_create(name, quotas, opts) do
      IO.puts("namespace #{name} created")
      print_namespace_entry(entry)
    else
      {:error, reason} -> cli_error!("namespace create: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz namespace delete <name>`."
  def authz_render_namespace_delete(name, opts \\ []) do
    case namespace_delete(name, opts) do
      :ok -> IO.puts("namespace #{name} deleted")
      {:error, reason} -> cli_error!("namespace delete: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz namespace show <name>`."
  def authz_render_namespace_show(name, opts \\ []) do
    case namespace_show(name, opts) do
      {:ok, entry} -> print_namespace_entry(entry)
      {:error, reason} -> cli_error!("namespace show: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  @doc "CLI entry: `flz namespace set-quota <name> --X<quota>=<value> ...`."
  def authz_render_namespace_set_quota(name, quotas_json, opts \\ []) do
    with {:ok, overrides} <- parse_quota_overrides(quotas_json),
         {:ok, entry} <- namespace_set_quota(name, overrides, opts) do
      IO.puts("namespace #{name} quotas updated")
      print_namespace_entry(entry)
    else
      {:error, reason} ->
        cli_error!("namespace set-quota: #{render_authz_verdict(reason)}", :stderr)
    end
  end

  defp print_namespace_entry(entry) do
    IO.puts("namespace: #{entry.name}")
    IO.puts("quotas:")

    entry.quotas
    |> Enum.sort()
    |> Enum.each(fn {k, v} -> IO.puts("  #{k}: #{v}") end)
  end

  defp parse_role(role_string) when is_binary(role_string) do
    case role_string do
      "viewer" -> {:ok, :viewer}
      "operator" -> {:ok, :operator}
      "admin" -> {:ok, :admin}
      "runner" -> {:ok, :runner}
      other -> {:error, {:unknown_role, other}}
    end
  end

  defp parse_role(role) when is_atom(role), do: {:ok, role}

  # Quota overrides arrive as the CLI's `--X` JSON object; keys must name
  # documented quota fields, values must be non-negative integers (validated
  # again by `Namespace.build_quotas/1`).
  defp parse_quota_overrides(quotas_json) when is_binary(quotas_json) do
    with {:ok, decoded} <- Jason.decode(quotas_json) do
      {:ok,
       Map.new(decoded, fn {k, v} ->
         {String.to_existing_atom(k), parse_quota_value(v)}
       end)}
    end
  rescue
    ArgumentError -> {:error, :unknown_quota_field}
  end

  defp parse_quota_overrides(%{} = quotas), do: {:ok, quotas}

  defp parse_quota_value(v) when is_integer(v), do: v
  defp parse_quota_value(v) when is_binary(v), do: String.to_integer(v)

  defp render_authz_verdict({:forbidden, reason}), do: render_forbidden(reason)
  defp render_authz_verdict(reason), do: inspect(reason)

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
    cli_error!(
      "error: audit log is disabled; set FLEZHA_AUDIT_ENABLED=true or :audit_log, :enabled: true in config",
      :stderr
    )
  end

  defp render_audit_error(:observability_disabled) do
    cli_error!("error: observability is disabled", :stderr)
  end

  defp render_audit_error(:invalid_caller_context) do
    cli_error!("error: invalid caller context", :stderr)
  end

  defp render_audit_error(:file_not_found) do
    cli_error!("error: file not found", :stderr)
  end

  defp render_audit_error(reason) do
    cli_error!("error: #{inspect(reason)}", :stderr)
  end

  # ---------- Dead-letter queue (dead-letter-queue) ----------

  # How long `dlq replay` waits for a re-park before declaring the replay
  # successful and removing the entry. A re-park after the grace window
  # simply creates a fresh entry (same content identity, generation 1).
  defp dlq_replay_grace_ms() do
    Application.get_env(:flezha, :dlq, [])[:replay_grace_ms] || 5_000
  end

  @doc """
  `flz dlq list [operative]` — parked entries, nearest-to-expiry (oldest)
  first.
  """
  def dlq_list(operative_class \\ "") do
    filter = if operative_class == "", do: nil, else: operative_class
    entries = Flezha.Dlq.Store.list(filter)

    if entries == [] do
      IO.puts("dead-letter queue is empty")
    else
      IO.puts(
        String.pad_trailing("ENTRY", 60) <>
          String.pad_trailing("ENTITY", 16) <>
          String.pad_trailing("CHANNEL", 12) <>
          String.pad_trailing("GEN", 5) <>
          String.pad_trailing("AGE", 10) <> "ERROR"
      )

      Enum.each(entries, fn e ->
        ref = "#{e["operative_class"]}/#{e["procedure_id"]}/#{e["id"]}"
        age_s = div(System.system_time(:millisecond) - (e["ts_ms"] || 0), 1000)

        IO.puts(
          String.pad_trailing(ref, 60) <>
            String.pad_trailing(e["entity_key"] || "", 16) <>
            String.pad_trailing(e["channel"] || "", 12) <>
            String.pad_trailing(Integer.to_string(e["generation"] || 1), 5) <>
            String.pad_trailing("#{age_s}s", 10) <> String.slice(e["error"] || "", 0, 60)
        )
      end)
    end
  end

  @doc """
  `flz dlq show <operative/procedure/id>` — entry metadata + payload preview.
  """
  def dlq_show(entry_ref) do
    case Flezha.Dlq.Store.get(entry_ref) do
      {:ok, entry, payload} ->
        Enum.each(
          ~w(id operative_class procedure_id entity_key channel origin_offset error attempts generation status ts_ms payload_bytes),
          fn key ->
            IO.puts(String.pad_trailing(key, 18) <> to_string(inspect(entry[key])))
          end
        )

        case payload do
          nil ->
            IO.puts("payload            <not byte-representable; entry is not replayable>")

          bytes ->
            preview = binary_part(bytes, 0, min(byte_size(bytes), 256))

            rendered =
              if String.printable?(preview),
                do: preview,
                else: Base.encode16(preview, case: :lower)

            IO.puts("payload            #{rendered}")
        end

      {:error, reason} ->
        render_dlq_error(reason)
    end
  end

  @doc """
  `flz dlq replay <operative/procedure/id>` — re-dispatch a parked payload
  to its operative through the ordinary input path. A successful replay
  removes the entry; a failing one re-parks it with `generation`
  incremented (the store correlates on content identity). Replay identity
  is fresh by construction: the re-drive enters as a NEW input, so a
  tier-on invocation records fresh WAL seqs — downstream sinks see new
  `flz.seq` under the current epoch.
  """
  def dlq_replay(entry_ref, opts \\ []) do
    with {:ok, entry, payload} <- Flezha.Dlq.Store.get(entry_ref),
         :ok <- replayable(payload),
         {:ok, overwatch} <- resolve_overwatch(entry["operative_class"]) do
      with_cli_authorization(
        opts,
        :"dlq.replay",
        state_target_namespace(entry["operative_class"]),
        %{entry: entry_ref},
        fn ->
          generation_before = entry["generation"] || 1
          :ok = Flezha.Dlq.Store.mark_replaying(entry_ref)

          :telemetry.execute([:flezha, :dlq, :operator_replay], %{count: 1}, %{
            entry: entry_ref,
            channel: entry["channel"],
            generation: generation_before,
            caller_source: :cli
          })

          GenServer.cast(overwatch, {:io_in, payload, %{"channel" => entry["channel"]}})
          await_replay_outcome(entry_ref, generation_before, dlq_replay_grace_ms())
        end
      )
    else
      {:error, reason} -> render_dlq_error(reason)
    end
  end

  @doc """
  `flz dlq drop <operative/procedure/id>` — discard a parked entry and its
  payload. Audited: the drop is deliberate data loss.
  """
  def dlq_drop(entry_ref, opts \\ []) do
    case Flezha.Dlq.Store.get(entry_ref) do
      {:ok, entry, _payload} ->
        with_cli_authorization(
          opts,
          :"dlq.drop",
          state_target_namespace(entry["operative_class"]),
          %{entry: entry_ref},
          fn ->
            :ok = Flezha.Dlq.Store.drop(entry_ref)

            :telemetry.execute([:flezha, :dlq, :operator_drop], %{count: 1}, %{
              entry: entry_ref,
              channel: entry["channel"],
              entity_key: entry["entity_key"],
              error: entry["error"],
              caller_source: :cli
            })

            IO.puts("dropped #{entry_ref}")
          end
        )

      {:error, reason} ->
        render_dlq_error(reason)
    end
  end

  defp replayable(nil), do: {:error, :payload_not_replayable}
  defp replayable(_payload), do: :ok

  defp resolve_overwatch(operative_class) do
    case Flezha.Operative.ClassRegistry.overwatch_pid(operative_class) do
      {:ok, pid} -> {:ok, pid}
      :error -> resolve_overwatch_cluster(operative_class)
    end
  end

  # The class registry is node-local; a target hosted on a peer resolves
  # through the routed pid lookup and the re-injection cast rides distribution.
  defp resolve_overwatch_cluster(operative_class) do
    with {:ok, namespace} <- OperativeManager.namespace_of(operative_class),
         {:ok, pid} <- OperativeManager.get_operative_pid(namespace, operative_class) do
      {:ok, pid}
    else
      _ -> {:error, :operative_not_running}
    end
  end

  # Poll for the re-park (generation bump) until the grace window closes;
  # no re-park within it means the replay processed cleanly.
  defp await_replay_outcome(entry_ref, _generation_before, remaining_ms)
       when remaining_ms <= 0 do
    :ok = Flezha.Dlq.Store.resolve(entry_ref)
    IO.puts("replay succeeded; #{entry_ref} removed")
  end

  defp await_replay_outcome(entry_ref, generation_before, remaining_ms) do
    Process.sleep(100)

    case Flezha.Dlq.Store.get(entry_ref) do
      {:ok, %{"status" => "parked", "generation" => gen}, _payload}
      when gen > generation_before ->
        IO.puts("replay failed; #{entry_ref} re-parked with generation #{gen}")

      {:ok, _entry, _payload} ->
        await_replay_outcome(entry_ref, generation_before, remaining_ms - 100)

      {:error, :not_found} ->
        IO.puts("replay succeeded; #{entry_ref} removed")
    end
  end

  defp render_dlq_error(:not_found) do
    cli_error!("error: DLQ entry not found", :stderr)
  end

  defp render_dlq_error(:bad_entry_ref) do
    cli_error!("error: expected <operative>/<procedure>/<entry-id>", :stderr)
  end

  defp render_dlq_error(:payload_not_replayable) do
    cli_error!(
      "error: entry carries no payload bytes (inter-procedure input); inspect or drop it",
      :stderr
    )
  end

  defp render_dlq_error(:operative_not_running) do
    cli_error!("error: the entry's operative is not running; start it before replaying", :stderr)
  end

  defp render_dlq_error(reason) do
    cli_error!("error: #{inspect(reason)}", :stderr)
  end

  # ---------- Scheduled spawns (add-scheduled-operative-spawns) ----------

  def schedule_add(
        name,
        cron_expr,
        timezone,
        factory_id,
        namespace \\ "",
        init_values \\ "",
        kind \\ "workflow",
        plugins_json \\ ""
      ) do
    ns = normalize_namespace(namespace)

    with :ok <- schedule_workflow_only(kind),
         {:ok, init_map} <- schedule_decode_init(init_values),
         :ok <- schedule_check_plugins(plugins_json, ns, name),
         {:ok, entry} <-
           Flezha.Schedule.Store.add(%{
             name: name,
             cron: cron_expr,
             timezone: timezone,
             factory_id: factory_id,
             namespace: ns,
             init_values: init_map,
             plugins: plugins_json
           }) do
      schedule_warn_absent_factory(ns, factory_id)
      _ = Flezha.Schedule.Hub.assert(entry)

      IO.puts(
        "Schedule #{name} added (#{cron_expr} #{timezone}); next fire #{schedule_next_fire(entry)}"
      )
    else
      {:error, reason} -> render_schedule_error(reason)
    end
  end

  def schedule_list() do
    case Flezha.Schedule.Store.list() do
      [] ->
        IO.puts("no schedules")

      entries ->
        statuses = Flezha.Schedule.Hub.statuses()

        IO.puts(
          schedule_row([
            "NAME",
            "CRON",
            "TZ",
            "FACTORY",
            "NAMESPACE",
            "NODE",
            "NEXT FIRE (UTC)",
            "LAST FIRE (UTC)",
            "LAST RESULT"
          ])
        )

        Enum.each(entries, fn e ->
          status = Map.get(statuses, e.name, %{})
          last_fire = Map.get(status, :last_fire)

          IO.puts(
            schedule_row([
              e.name,
              e.cron,
              e.timezone,
              e.factory_id,
              e.namespace,
              label_or(Map.get(status, :node), "-"),
              schedule_next_fire(e),
              schedule_last_fire_at(last_fire),
              schedule_last_result(last_fire)
            ])
          )
        end)
    end
  end

  defp schedule_last_fire_at(%{at: at}),
    do: at |> DateTime.truncate(:second) |> DateTime.to_iso8601()

  defp schedule_last_fire_at(_), do: "-"

  # Error reasons truncate to the column; the full reason stays in
  # log/telemetry.
  @schedule_result_width 48
  defp schedule_last_result(%{outcome: :ok}), do: "ok"
  defp schedule_last_result(%{outcome: :duplicate}), do: "duplicate"

  defp schedule_last_result(%{outcome: {:retrying, reason}}),
    do: String.slice("retrying: #{reason}", 0, @schedule_result_width)

  defp schedule_last_result(_), do: "-"

  def schedule_remove(name) do
    # Store first: retracting first would let the periodic re-assert sweep
    # resurrect the job before the row is gone.
    case Flezha.Schedule.Store.remove(name) do
      :ok ->
        _ = Flezha.Schedule.Hub.retract(name)
        IO.puts("Schedule #{name} removed")

      {:error, :not_found} ->
        render_schedule_error({:not_found, name})
    end
  end

  # Deliberately hub-free: a manual fire works on any node whether or not the
  # schedule hub is up.
  def schedule_run(name) do
    with {:ok, entry} <- schedule_entry(name),
         instance_id = "#{name}@manual-#{System.system_time(:second)}",
         {:ok, %{operative_id: id}} <-
           Flezha.Schedule.Job.spawn_instance(entry, instance_id) do
      IO.puts("Spawned #{id}")
    else
      :duplicate -> IO.puts("instance already running; no-op")
      {:error, reason} -> render_schedule_error(reason)
    end
  end

  defp schedule_entry(name) do
    case Flezha.Schedule.Store.get(name) do
      {:ok, entry} -> {:ok, entry}
      :error -> {:error, {:not_found, name}}
    end
  end

  defp schedule_workflow_only(kind) when kind in ["", "workflow"], do: :ok
  defp schedule_workflow_only(kind), do: {:error, {:unschedulable_kind, kind}}

  # Full provider-resolving check at add time, so a typo fails at the CLI
  # rather than at 08:00. A slot instance id only exists at fire time, so the
  # check runs against a sentinel that could never be one.
  defp schedule_check_plugins(plugins_json, namespace, name) do
    case SpecDecoder.decode(plugins_json, namespace, "#{name}@spec-check") do
      {:ok, _} -> :ok
      {:error, _} = err -> err
    end
  end

  defp schedule_decode_init(""), do: {:ok, %{}}

  defp schedule_decode_init(json) do
    case Jason.decode(json) do
      {:ok, map} when is_map(map) -> {:ok, map}
      _ -> {:error, {:invalid_init_values, json}}
    end
  end

  # Warn-only: the factory's runner may simply not have connected yet.
  defp schedule_warn_absent_factory(namespace, factory_id) do
    case Flezha.Runner.FleetManager.runners_for_factory(namespace, factory_id) do
      [] ->
        IO.puts(
          "warning: factory #{factory_id} has no live runner in namespace #{namespace}; " <>
            "firings will fail until one connects"
        )

      _ ->
        :ok
    end
  catch
    :exit, _ -> :ok
  end

  defp schedule_next_fire(entry) do
    with {:ok, cron} <- Flezha.Schedule.Cron.parse(entry.cron),
         {:ok, next} <- Flezha.Schedule.Cron.next_fire(cron, entry.timezone, DateTime.utc_now()) do
      DateTime.to_iso8601(next)
    else
      _ -> "never"
    end
  end

  # First six columns padded; the last is printed raw.
  defp schedule_row(cells) do
    {padded, [last]} = Enum.split(cells, 8)

    Enum.zip_with(padded, [24, 18, 20, 24, 14, 22, 22, 22], &String.pad_trailing/2)
    |> Enum.join()
    |> Kernel.<>(last)
  end

  defp render_schedule_error(reason) do
    cli_error!("error: " <> schedule_error_message(reason), :stderr)
  end

  defp schedule_error_message({:not_found, name}), do: "schedule #{name} not found"
  defp schedule_error_message({:invalid_cron, msg}), do: "invalid --cron: #{msg}"

  defp schedule_error_message({:invalid_timezone, tz}),
    do: "invalid --tz: #{inspect(tz)} is not an IANA time zone"

  defp schedule_error_message({:invalid_name, msg}), do: "invalid schedule name: #{msg}"

  defp schedule_error_message({:invalid_init_values, _}),
    do: "--init_values must be a JSON object"

  defp schedule_error_message({:unschedulable_kind, kind}),
    do: "only workflow operatives are schedulable (got kind #{kind})"

  defp schedule_error_message({:invalid_plugins, msg}), do: "invalid --plugins: #{msg}"

  defp schedule_error_message({:invalid_plugins_json, reason}),
    do: "invalid --plugins JSON: #{inspect(reason)}"

  defp schedule_error_message({:unknown_provider, id}),
    do: "invalid --plugins: unknown provider #{id}"

  defp schedule_error_message({:invalid_subscription, detail}),
    do: "invalid --plugins subscription: #{inspect(detail)}"

  defp schedule_error_message({:invalid_opts, detail}),
    do: "invalid --plugins opts: #{inspect(detail)}"

  defp schedule_error_message(:already_exists), do: "a schedule with that name already exists"
  defp schedule_error_message(reason), do: inspect(reason)

  # Fail the surrounding `flzvm rpc` evaluation: print the message, then exit
  # only this rpc process. Never System.halt here — every CliManager call runs
  # ON the controller node via rpc, so halting would stop the controller
  # itself; :rpc turns the exit into {:badrpc, ...}, which the release script
  # converts into a non-zero local exit code for the calling script.
  # `device` mirrors each call site's historical stream (legacy operative and
  # plugin verbs print errors to stdout; the newer state/observe/audit/dlq and
  # schedule verbs print to stderr) — the stream is observable behavior.
  @spec cli_error!(String.t(), :stdio | :stderr) :: no_return()
  defp cli_error!(message, device \\ :stdio) do
    IO.puts(device, message)
    exit(:cli_error)
  end
end

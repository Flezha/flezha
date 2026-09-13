defmodule Flezha.Service.Cli.Operative do
  @moduledoc """
  `flz operative start | stop | scale | rewind | send | listen` and
  `flz operative entity-state`.
  """

  alias Flezha.AuthZ.Namespace
  alias Flezha.Identificator
  alias Flezha.Observability.Signal
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Process.Watch
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Workflow

  @doc """
  Starts an operative. The call's options carry `plugins` (a JSON array, `[]`
  when absent) and `kind` (`stream` when absent).
  """
  def operative_start(namespace, operative_id, factory_id, init_values, opts_json, %{opts: opts}) do
    opts_kw =
      Context.json!(opts_json, "--partitions/--snapshot-interval")
      |> Context.atom_converter()
      |> Keyword.new()

    {partitions, opts_kw} = pop_partitions(opts_kw)
    plugins_json = Keyword.get(opts, :plugins, "[]")
    kind = Keyword.get(opts, :kind, "stream")

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
      case SpecDecoder.decode(plugins_json, Namespace.or_default(namespace), operative_id) do
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
          Context.fail!("starting operative: #{Context.format_reason(reason)}")
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
        case Context.integer!(raw, "--partitions") do
          n when n >= 1 ->
            {n, rest}

          _ ->
            Context.fail!("starting operative: --partitions must be a positive integer")
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
      init_values: Context.json!(init_values, "--init_values"),
      opts: opts_kw
    }

    case PartitionGroup.start(
           Namespace.or_default(namespace),
           operative,
           partitions,
           plugins_json
         ) do
      {:ok, %{members: members, warnings: warnings} = result} ->
        Enum.each(warnings, &IO.puts("WARNING: #{&1}"))
        report_group_start(operative_id, members, Map.get(result, :already_running, []))

      {:error, reason} ->
        Context.fail!("starting operative: #{Context.format_reason(reason)}")
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

  defp start_operative(namespace, operative_id, factory_id, init_values, opts_kw, kind) do
    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: factory_id,
      kind: parse_kind(kind),
      init_values: Context.json!(init_values, "--init_values"),
      opts: opts_kw
    }

    case OperativeManager.start_operative(Namespace.or_default(namespace), operative) do
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
        Context.fail!("starting operative: #{Context.format_reason(reason)}")
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

  @doc "Stops a operative (cooperatively for a workflow operative, hard otherwise)."
  def operative_stop(namespace, operative_id, _call) do
    namespace_display = Namespace.or_default(namespace)

    case PartitionGroup.members(namespace_display, operative_id) do
      [] -> stop_one(namespace_display, operative_id)
      members -> stop_group(namespace_display, operative_id, members)
    end
  end

  defp stop_group(namespace, group_id, members) do
    results = Enum.map(members, &{&1.id, OperativeManager.request_stop(namespace, &1.id)})

    case Enum.reject(results, fn {_id, result} -> result == :ok or match?({:ok, _}, result) end) do
      [] ->
        IO.puts("Operative group #{group_id} stopped (#{length(results)} partitions)")

      failed ->
        Context.fail!(
          "stopping operative group #{group_id}: " <>
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
        Context.fail!("stopping operative: #{Context.format_reason(reason)}")
    end
  end

  @doc """
  `flz operative scale <group> <M>` — rescale a partition group to M members.

  Quiesces the group (sources off, input logs consumed, DLQs empty), moves
  every Entity whose key group changes owner, then starts M members with the
  recomputed ranges. Refuses — changing nothing — when the quiesce cannot be
  reached or the new member set fails preflight.
  """
  def operative_scale(namespace, group_id, target, opts_json, _call) do
    target =
      case Context.integer!(target, "<partitions>") do
        n when n >= 1 ->
          n

        _ ->
          Context.fail!("scaling operative: <partitions> must be a positive integer")
      end

    opts =
      case Context.json!(opts_json, "--timeout") do
        %{"timeout_ms" => raw} -> [quiesce_timeout_ms: Context.integer!(raw, "--timeout")]
        _ -> []
      end

    case PartitionGroup.rescale(Namespace.or_default(namespace), group_id, target, opts) do
      {:ok, result} ->
        Enum.each(result.warnings, &IO.puts("WARNING: #{&1}"))

        IO.puts(
          "Operative #{group_id} rescaled #{result.from} -> #{result.to} " <>
            "(#{result.entities_moved} entities moved, #{length(result.vacated)} trees removed):"
        )

        Enum.each(result.members, &IO.puts("  #{&1}"))

      {:error, reason} ->
        Context.fail!("scaling operative: #{Context.format_reason(reason)}")
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
  def operative_rewind(namespace, operative_id, to_iter, _call) do
    namespace = Namespace.or_default(namespace)

    case Workflow.rewind_instance(namespace, operative_id, to_iter) do
      {:error, :operative_not_running} ->
        Context.fail!("operative '#{operative_id}' not found")

      outcome ->
        print_rewind(operative_id, to_iter, outcome)
    end
  end

  defp print_rewind(operative_id, to_iter, :ok) do
    IO.puts(
      "Operative '#{operative_id}' rewound to iteration #{to_iter}. " <>
        "Iterations above it were compensated; the loop resumes at #{to_iter + 1}."
    )
  end

  defp print_rewind(_operative_id, _to_iter, {:error, {:uncommitted_iteration, []}}) do
    Context.fail!("this operative has no committed iterations to rewind to")
  end

  defp print_rewind(_operative_id, to_iter, {:error, {:uncommitted_iteration, committed}}) do
    Context.fail!(
      "Error: iteration #{to_iter} has not committed. " <>
        "Committed iterations: #{Enum.min(committed)}..#{Enum.max(committed)}"
    )
  end

  defp print_rewind(_operative_id, _to_iter, {:error, {:uncovered, channels}}) do
    Context.fail!(
      "Error: refusing to rewind — these channels were written by an unwound " <>
        "procedure that declares no `compensate` hook, so the output cannot be " <>
        "undone: #{Enum.join(channels, ", ")}"
    )
  end

  defp print_rewind(_operative_id, _to_iter, {:error, reason}) do
    Context.fail!("Error rewinding operative: #{Context.format_reason(reason)}")
  end

  @doc """
  Inject a payload into an operative's input stream (the SDK-facing `send_io`
  route), blocking for the confirming verdict so a parked or absent target is
  reported rather than silently dropped. `channel` is optional (`""` = none).

  With `await_ms` in the options the output listener is attached first, then
  the payload is delivered and every output frame within the window is
  printed.
  """
  def operative_send(namespace, operative_id, message, channel, %{opts: opts}) do
    deliver = fn -> deliver(namespace, operative_id, message, channel) end

    case Keyword.fetch(opts, :await_ms) do
      :error ->
        deliver.()

      {:ok, window} ->
        listen(
          namespace,
          operative_id,
          :on_output,
          :text,
          Context.integer!(window, "--await"),
          deliver
        )
    end
  end

  defp deliver(namespace, operative_id, message, channel) do
    metadata = if channel in [nil, ""], do: %{}, else: %{"channel" => channel}

    case OperativeManager.send_io_confirming(
           operative_id,
           Namespace.or_default(namespace),
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
        Context.fail!("operative '#{operative_id}' is not ready to accept input.")

      {:error, :not_found} ->
        Context.fail!("operative '#{operative_id}' not found.")

      {:error, :refused} ->
        Context.fail!("input refused — durable append failed for '#{operative_id}'.")
    end
  end

  @doc "Get operative state."
  def entity_state(namespace, operative_id, _call) do
    namespace_display = Namespace.or_default(namespace)

    case OperativeManager.get_entity_state(namespace_display, operative_id) do
      {:ok, json_state} when is_binary(json_state) ->
        IO.puts("State for operative '#{operative_id}' in namespace '#{namespace_display}':")
        IO.puts(json_state)

      {:ok, state} ->
        IO.puts("State for operative '#{operative_id}' in namespace '#{namespace_display}':")
        IO.puts(inspect(state))

      {:error, :not_found} ->
        IO.puts("Operative '#{operative_id}' not found in namespace '#{namespace_display}'.")

      {:error, {:unsupported_for_kind, :persistent_state}} ->
        Context.fail!(
          "Operative '#{operative_id}' in namespace '#{namespace_display}' has no per-entity state: " <>
            "its kind does not support state introspection."
        )

      {:error, reason} when is_binary(reason) ->
        Context.fail!("Error getting operative state: #{reason}")

      {:error, reason} ->
        Context.fail!("Error getting operative state: #{Context.format_reason(reason)}")

      other ->
        IO.puts("Unexpected result: #{inspect(other)}")
    end
  end

  @chains [output: :on_output, input: :on_input]

  @doc """
  Print every frame the operative emits, or ingests when `direction` is
  `input`, until the window closes, the caller goes away or this process is
  told to exit.
  """
  def operative_listen(namespace, operative_id, direction, format, %{opts: opts}) do
    chain = @chains[Context.atom!(direction, Keyword.keys(@chains), "direction")]
    format = Context.atom!(format, [:text, :json], "format")

    listen(namespace, operative_id, chain, format, Context.window(opts), fn -> :ok end)
  end

  # `before` runs after the attach, so a frame it causes cannot pass the
  # operative ahead of the subscription. The detach resolves a group's `#pK`
  # members as well as a plain id, and every route out of the loop reaches it.
  #
  # The caller's group leader lives on the `flz` node: its `DOWN` is the only
  # signal this process gets when that node goes.
  defp listen(namespace, operative_id, chain, format, window_ms, before) do
    plugin_id = Identificator.mint(:plugin, "io_listener")
    leader = Process.group_leader()
    caller = Watch.watch(Watch.new(), leader, :caller)
    trapping = Process.flag(:trap_exit, true)

    try do
      attach(plugin_id, Namespace.or_default(namespace), operative_id, chain)
      before.()
      Context.receive_for(window_ms, &listen_message(&1, caller, format))
      :ok
    after
      PluginManager.disable_plugin_group(plugin_id)
      Watch.forget(caller, leader)
      Process.flag(:trap_exit, trapping)
    end
  end

  defp attach(plugin_id, namespace, operative_id, chain) do
    plugin = %Plugin{
      id: plugin_id,
      provider: MonitoredIOForward,
      namespace: namespace,
      operative_id: operative_id,
      opts: %{
        recv_pid: self(),
        plugin_id: plugin_id,
        operative_id: operative_id,
        namespace: namespace
      },
      subscriptions: [{chain, []}]
    }

    case PluginManager.enable_plugin(plugin_id, plugin) do
      {:ok, summary} when is_binary(summary) ->
        :ok

      {:ok, :already_enabled} ->
        Context.fail!(
          "listening to '#{operative_id}': listener id #{plugin_id} is already in use"
        )

      {:error, reason} ->
        Context.fail!("listening to '#{operative_id}': #{Context.format_reason(reason)}")
    end
  end

  defp listen_message({:message_forward, {payload, metadata}}, _caller, format) do
    print_frame(payload, metadata, format)
    :continue
  end

  defp listen_message({:heartbeat_keepalive, from}, _caller, _format) do
    send(from, {:heartbeat_keepalive, :ok})
    :continue
  end

  defp listen_message({:DOWN, _ref, :process, pid, _reason}, caller, _format),
    do: if(Watch.watching?(caller, pid), do: :done, else: :continue)

  defp listen_message({:EXIT, _pid, reason}, _caller, _format) when reason != :normal, do: :done
  defp listen_message(_message, _caller, _format), do: :continue

  @stamps [{"flz.entity", "entity"}, {"flz.seq", "seq"}, {"flz.key", "key"}]

  defp print_frame(payload, metadata, :text) do
    stamps =
      for {key, label} <- @stamps, Map.has_key?(metadata, key), do: "#{label}=#{metadata[key]}"

    stamped = if stamps == [], do: [], else: [Enum.join(stamps, " ")]

    IO.puts(Enum.intersperse([metadata["channel"] || "-", payload | stamped], "\t"))
  end

  defp print_frame(payload, metadata, :json) do
    body =
      if String.valid?(payload),
        do: %{payload: payload},
        else: %{payload_base64: Base.encode64(payload)}

    body
    |> Map.merge(%{channel: metadata["channel"], metadata: Signal.metadata_json(metadata)})
    |> Jason.encode!()
    |> IO.puts()
  end
end

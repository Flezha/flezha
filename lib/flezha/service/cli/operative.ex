defmodule Flezha.Service.Cli.Operative do
  @moduledoc """
  `flz operative start | stop | scale | rewind | send | listen` and
  `flz operative entity-state`.
  """

  alias Flezha.Operative.PartitionGroup
  alias Flezha.Payloader
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Workflow

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
      |> Context.atom_converter()
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
      case SpecDecoder.decode(plugins_json, Context.normalize_namespace(namespace), operative_id) do
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
          Context.cli_error!("Error starting operative: #{inspect(reason)}")
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
          {n, ""} when n >= 1 ->
            {n, rest}

          _ ->
            Context.cli_error!(
              "Error starting operative: --partitions must be a positive integer"
            )
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
           Context.normalize_namespace(namespace),
           operative,
           partitions,
           plugins_json
         ) do
      {:ok, %{members: members, warnings: warnings} = result} ->
        Enum.each(warnings, &IO.puts("WARNING: #{&1}"))
        report_group_start(operative_id, members, Map.get(result, :already_running, []))

      {:error, reason} ->
        Context.cli_error!("Error starting operative: #{Context.format_reason(reason)}")
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
      init_values: Payloader.json_to_map(init_values),
      opts: opts_kw
    }

    case OperativeManager.start_operative(Context.normalize_namespace(namespace), operative) do
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
        Context.cli_error!("Error starting operative: #{inspect(reason)}")
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
    namespace_display = Context.normalize_namespace(namespace)

    Context.with_cli_authorization(
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
        Context.cli_error!(
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
        Context.cli_error!("Error stopping operative: #{inspect(reason)}")
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
        {n, ""} when n >= 1 ->
          n

        _ ->
          Context.cli_error!("Error scaling operative: <partitions> must be a positive integer")
      end

    opts =
      case Payloader.json_to_map(opts_json) do
        %{"timeout_ms" => raw} -> [quiesce_timeout_ms: String.to_integer(to_string(raw))]
        _ -> []
      end

    namespace = Context.normalize_namespace(namespace)

    case Workflow.rescale(namespace, group_id, target, opts, Context.cli_context([])) do
      {:ok, result} ->
        Enum.each(result.warnings, &IO.puts("WARNING: #{&1}"))

        IO.puts(
          "Operative #{group_id} rescaled #{result.from} -> #{result.to} " <>
            "(#{result.entities_moved} entities moved, #{length(result.vacated)} trees removed, " <>
            "#{result.forwards_refreshed} forwarding plugin(s) re-pointed):"
        )

        Enum.each(result.members, &IO.puts("  #{&1}"))

      {:error, reason} ->
        Context.cli_error!("Error scaling operative: #{Context.format_reason(reason)}")
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
    namespace = Context.normalize_namespace(namespace)

    case Workflow.rewind(namespace, operative_id, to_iter, Context.cli_context([])) do
      {:error, :operative_not_running} ->
        Context.cli_error!("Error: operative '#{operative_id}' not found")

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
    Context.cli_error!("Error: this operative has no committed iterations to rewind to")
  end

  defp print_rewind(_operative_id, to_iter, {:error, {:uncommitted_iteration, committed}}) do
    Context.cli_error!(
      "Error: iteration #{to_iter} has not committed. " <>
        "Committed iterations: #{Enum.min(committed)}..#{Enum.max(committed)}"
    )
  end

  defp print_rewind(_operative_id, _to_iter, {:error, {:uncovered, channels}}) do
    Context.cli_error!(
      "Error: refusing to rewind — these channels were written by an unwound " <>
        "procedure that declares no `compensate` hook, so the output cannot be " <>
        "undone: #{Enum.join(channels, ", ")}"
    )
  end

  defp print_rewind(_operative_id, _to_iter, {:error, reason}) do
    Context.cli_error!("Error rewinding operative: #{inspect(reason)}")
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
           Context.normalize_namespace(namespace),
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
        Context.cli_error!("Error: operative '#{operative_id}' is not ready to accept input.")

      {:error, :not_found} ->
        Context.cli_error!("Error: operative '#{operative_id}' not found.")

      {:error, :refused} ->
        Context.cli_error!("Error: input refused — durable append failed for '#{operative_id}'.")
    end
  end

  @doc "Get operative state."
  def entity_state(namespace, operative_id) do
    namespace_display = Context.normalize_namespace(namespace)

    case OperativeManager.get_entity_state(
           Context.normalize_namespace(namespace),
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
        Context.cli_error!(
          "Operative '#{operative_id}' in namespace '#{namespace_display}' has no per-entity state: " <>
            "its kind does not support state introspection."
        )

      {:error, reason} when is_binary(reason) ->
        Context.cli_error!("Error getting operative state: #{reason}")

      {:error, reason} ->
        Context.cli_error!("Error getting operative state: #{inspect(reason)}")

      other ->
        IO.puts("Unexpected result: #{inspect(other)}")
    end
  end

  @doc "Listens to a operative IO."
  def operative_listen(namespace, operative_id, io_type) do
    namespace_display = Context.normalize_namespace(namespace)

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
end

defmodule Flezha.Service.Cli.Operative do
  @moduledoc """
  `flz operative start | stop | scale | rewind | send | listen` and
  `flz operative cell-state`.
  """

  alias Flezha.AuthZ.Namespace
  alias Flezha.Identificator
  alias Flezha.Observability.Signal
  alias Flezha.Operative.Builder
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
  Starts an operative and prints the id it runs under. The call's options carry
  `id` (generated when absent), `plugins` (a JSON array, `[]` when absent) and,
  for a first message, `text` with an optional `channel`, delivered as the
  instance's first input on the mechanism a tasklet's spawn uses. The kind is
  the factory's, announced by its runners. Answers `{:ok, %{id:, outcome:}}`,
  which the start's audit record reads.
  """
  def operative_start(namespace, factory_id, init_values, format, opts_json, %{opts: opts}) do
    format = Context.atom!(format, [:text, :json], "format")

    opts_kw =
      Context.json!(opts_json, "--partitions/--snapshot-interval")
      |> Context.atom_converter()
      |> Keyword.new()

    {partitions, opts_kw} = pop_partitions(opts_kw)

    operative = %Builder{
      id: Keyword.get(opts, :id),
      factory_id: factory_id,
      init_values: Context.json!(init_values, "--init-values"),
      opts: seed_input(opts_kw, Keyword.get(opts, :text), Keyword.get(opts, :channel))
    }

    plugins_json = Keyword.get(opts, :plugins, "[]")
    namespace = Namespace.or_default(namespace)

    if partitions >= 2,
      do: start_partitioned(namespace, operative, partitions, plugins_json, format),
      else: start_operative(namespace, operative, plugins_json, format)
  end

  # `--text` is the instance's first input, on the entry channel `--channel`
  # names or the first declared one.
  defp seed_input(opts_kw, text, channel) do
    case Builder.seed_input(text, channel) do
      {:ok, nil} -> opts_kw
      {:ok, seed} -> Keyword.put(opts_kw, :seed_input, seed)
      {:error, reason} -> Context.halt!("error: #{reason}; give --text too")
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

  # JSON output is the one object `start` answers, so a warning goes to the
  # operator's stderr rather than ahead of it.
  defp start_partitioned(namespace, operative, partitions, plugins_json, format) do
    case PartitionGroup.start(namespace, operative, partitions, plugins_json) do
      {:ok, %{group: group, members: members, warnings: warnings} = result} ->
        outcome = if members == [], do: :already_running, else: :started
        answer = {:ok, %{id: group, outcome: outcome}}

        if format == :json do
          notify = Context.notifier()
          Enum.each(warnings, &notify.("WARNING: #{&1}"))
          render_start(answer, :json)
        else
          Enum.each(warnings, &IO.puts("WARNING: #{&1}"))
          report_group_start(group, members, Map.get(result, :already_running, []))
          answer
        end

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

  defp start_operative(namespace, %{id: nil} = operative, plugins_json, format) do
    start = &start_with_plugins(namespace, %{operative | id: &1}, plugins_json, generated: true)

    operative.factory_id
    |> OperativeManager.with_generated_id(start)
    |> OperativeManager.start_answer(nil)
    |> render_start(format)
  end

  defp start_operative(namespace, operative, plugins_json, format) do
    namespace
    |> start_with_plugins(operative, plugins_json, [])
    |> OperativeManager.start_answer(operative.id)
    |> render_start(format)
  end

  # A plugin spec names the operative it attaches to, so it is decoded under
  # the id the start runs under, which for a generated id is each one drawn.
  defp start_with_plugins(namespace, operative, plugins_json, opts) do
    with {:ok, plugins} <- SpecDecoder.decode(plugins_json, namespace, operative.id) do
      opts_kw =
        if plugins == [], do: operative.opts, else: Keyword.put(operative.opts, :plugins, plugins)

      OperativeManager.start_operative(namespace, %{operative | opts: opts_kw}, opts)
    end
  end

  # A live instance holding a named id is a successful no-op; a claim refused
  # with no live winner is the error clause.
  defp render_start({:ok, %{id: id, outcome: outcome}} = answer, :json) do
    IO.puts(Jason.encode!(%{id: id, outcome: outcome}))
    answer
  end

  defp render_start({:ok, %{id: id, outcome: :started}} = answer, :text) do
    IO.puts("Started #{id}")
    answer
  end

  defp render_start({:ok, %{id: id, outcome: :already_running}} = answer, :text) do
    IO.puts("#{id} is already running, left untouched")
    answer
  end

  defp render_start({:error, reason}, _format),
    do: Context.fail!("starting operative: #{Context.format_reason(reason)}")

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
  every Cell whose key group changes owner, then starts M members with the
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
            "(#{result.cells_moved} cells moved, #{length(result.vacated)} trees removed):"
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
  run's at a guarded sink, and the loop waits for its next input: the inputs
  those iterations consumed stay released and are not re-delivered.

  Refused, changing nothing, when the target has not committed or when an unwound
  tasklet emitted plugin output it cannot compensate.
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
        "tasklet that declares no `compensate` hook, so the output cannot be " <>
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
  def cell_state(namespace, operative_id, _call) do
    namespace_display = Namespace.or_default(namespace)

    case OperativeManager.get_cell_state(namespace_display, operative_id) do
      {:ok, json_state} when is_binary(json_state) ->
        IO.puts("State for operative '#{operative_id}' in namespace '#{namespace_display}':")
        IO.puts(json_state)

      {:ok, state} ->
        IO.puts("State for operative '#{operative_id}' in namespace '#{namespace_display}':")
        IO.puts(inspect(state))

      {:error, :not_found} ->
        IO.puts("Operative '#{operative_id}' not found in namespace '#{namespace_display}'.")

      {:error, :persistence_off} ->
        Context.fail!(
          "Operative '#{operative_id}' in namespace '#{namespace_display}' has no per-cell state: " <>
            "its class declares persistence off, so its Cells hold no durable state and " <>
            "live reads are refused."
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
      hooks: [{chain, []}]
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

  @stamps [{"flz.cell", "cell"}, {"flz.seq", "seq"}, {"flz.key", "key"}]

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

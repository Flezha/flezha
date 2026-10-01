defmodule Flezha.Service.Cli.Operator do
  @moduledoc """
  `flz operator start | stop | scale | rewind | send | listen` and
  `flz operator cell-state`.
  """

  alias Flezha.AuthZ.Namespace
  alias Flezha.Observability.Signal
  alias Flezha.Operator.Builder
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Plugin.SpecDecoder
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.Listener
  alias Flezha.Service.Steps

  @doc """
  Starts an operator and prints the id it runs under. The call's options carry
  `id` (generated when absent), `plugins` (a JSON array, `[]` when absent) and,
  for a first message, `text` with an optional `channel`, delivered as the
  instance's first input on the mechanism a tasklet's spawn uses. How the
  operator runs is the factory's, announced by its runners. Answers `{:ok, %{id:, outcome:}}`,
  which the start's audit record reads.
  """
  def operator_start(namespace, factory_id, init_values, opts_json, %{opts: opts}) do
    format = frame_format(opts)

    opts_kw =
      Context.json!(opts_json, "--partitions")
      |> Context.atom_converter()
      |> Keyword.new()

    {partitions, opts_kw} = pop_partitions(opts_kw)

    operator = %Builder{
      id: Keyword.get(opts, :id),
      factory_id: factory_id,
      init_values: Context.json!(init_values, "--init-values"),
      opts: seed_input(opts_kw, Keyword.get(opts, :text), Keyword.get(opts, :channel))
    }

    plugins_json = Keyword.get(opts, :plugins, "[]")
    namespace = Namespace.or_default(namespace)

    if partitions >= 2,
      do: start_partitioned(namespace, operator, partitions, plugins_json, format),
      else: start_operator(namespace, operator, plugins_json, format)
  end

  # `--text` is the instance's first input, on the entry channel `--channel`
  # names or the first declared one.
  defp seed_input(opts_kw, text, channel) do
    case Builder.seed_input(text, channel) do
      {:ok, nil} -> opts_kw
      {:ok, seed} -> Keyword.put(opts_kw, :seed_input, seed)
      {:error, reason} -> Context.fail!(reason)
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
        {positive!(raw, "--partitions"), rest}
    end
  end

  defp start_partitioned(namespace, operator, partitions, plugins_json, format) do
    case PartitionGroup.start(namespace, operator, partitions, plugins_json) do
      {:ok, %{group: group, members: members} = result} ->
        outcome = if members == [], do: :already_running, else: :started
        answer = {:ok, %{id: group, outcome: outcome}}

        if format == :json do
          render_start(answer, :json)
        else
          report_group_start(group, members, Map.get(result, :already_running, []))
          answer
        end

      {:error, _reason} = refused ->
        refused
    end
  end

  defp report_group_start(operator_id, members, []) do
    IO.puts("Operator #{operator_id} started with #{length(members)} partitions:")
    Enum.each(members, &IO.puts("  #{&1}"))
  end

  defp report_group_start(operator_id, [], already) do
    IO.puts(
      "Operator #{operator_id} is already running with #{length(already)} partitions; " <>
        "the existing members were left untouched."
    )
  end

  defp report_group_start(operator_id, members, already) do
    IO.puts(
      "Operator #{operator_id} converged: started #{length(members)} missing member(s), " <>
        "#{length(already)} already running:"
    )

    Enum.each(members, &IO.puts("  started #{&1}"))
    Enum.each(already, &IO.puts("  already running #{&1}"))
  end

  defp start_operator(namespace, %{id: nil} = operator, plugins_json, format) do
    start = &start_with_plugins(namespace, %{operator | id: &1}, plugins_json, generated: true)

    operator.factory_id
    |> OperatorManager.with_generated_id(start)
    |> OperatorManager.start_answer(nil)
    |> render_start(format)
  end

  defp start_operator(namespace, operator, plugins_json, format) do
    namespace
    |> start_with_plugins(operator, plugins_json, [])
    |> OperatorManager.start_answer(operator.id)
    |> render_start(format)
  end

  # A plugin spec names the operator it attaches to, so it is decoded under
  # the id the start runs under, which for a generated id is each one drawn.
  defp start_with_plugins(namespace, operator, plugins_json, opts) do
    with {:ok, plugins} <- SpecDecoder.decode(plugins_json, namespace, operator.id) do
      opts_kw =
        if plugins == [], do: operator.opts, else: Keyword.put(operator.opts, :plugins, plugins)

      OperatorManager.start_operator(namespace, %{operator | opts: opts_kw}, opts)
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

  defp render_start({:error, _reason} = refused, _format), do: refused

  @doc "Stops an operator (cooperatively for a steps operator, hard otherwise)."
  def operator_stop(namespace, operator_id, _call) do
    namespace_display = Namespace.or_default(namespace)

    case PartitionGroup.members(namespace_display, operator_id) do
      [] -> stop_one(namespace_display, operator_id)
      members -> stop_group(namespace_display, operator_id, members)
    end
  end

  defp stop_group(namespace, group_id, members) do
    results = Enum.map(members, &{&1.id, OperatorManager.request_stop(namespace, &1.id)})

    case Enum.reject(results, fn {_id, result} -> result == :ok or match?({:ok, _}, result) end) do
      [] ->
        IO.puts("Operator group #{group_id} stopped (#{length(results)} partitions)")

      failed ->
        {:error, {:group_stop_partial, group_id, Enum.map(failed, &member_failure/1)}}
    end
  end

  # A member that did not stop, named with its own refusal.
  defp member_failure({id, {:error, reason}}),
    do: "#{id} (#{Flezha.Error.from_reason(reason, :"operator.stop").message})"

  defp stop_one(namespace, operator_id) do
    case OperatorManager.request_stop(namespace, operator_id) do
      :ok ->
        IO.puts("Operator has been stopped")

      {:ok, :cooperative} ->
        IO.puts(
          "Cooperative stop requested for steps operator '#{operator_id}'. " <>
            "It will finish in-flight work, then complete (cancelled) and be removed."
        )

      {:error, _reason} = refused ->
        refused
    end
  end

  @doc """
  `flz operator scale <group> <M>` — rescale a partition group to M members.

  Quiesces the group (sources off, input logs consumed, DLQs empty), moves
  every Cell whose key group changes owner, then starts M members with the
  recomputed ranges. Refuses — changing nothing — when the quiesce cannot be
  reached or the new member set fails preflight.
  """
  def operator_scale(namespace, group_id, target, opts_json, _call) do
    target = positive!(target, "<partitions>")

    opts =
      case Context.json!(opts_json, "--timeout") do
        %{"timeout_ms" => raw} -> [quiesce_timeout_ms: Context.integer!(raw, "--timeout")]
        _ -> []
      end

    case PartitionGroup.rescale(Namespace.or_default(namespace), group_id, target, opts) do
      {:ok, result} ->
        IO.puts(
          "Operator #{group_id} rescaled #{result.from} -> #{result.to} " <>
            "(#{result.cells_moved} cells moved, #{length(result.vacated)} trees removed):"
        )

        Enum.each(result.members, &IO.puts("  #{&1}"))

        if result.sealed != [] do
          IO.puts(
            "Sealed #{Enum.join(result.sealed, ", ")} at their latest committed iteration: " <>
              "no rewind or rollback reaches a pass committed before this rescale."
          )
        end

      {:error, _reason} = refused ->
        refused
    end
  end

  @doc """
  Rewind a steps loop to the committed boundary of iteration `to_iter`: the
  iterations above it are compensated in reverse, state and handoff return to
  that boundary, the epoch is bumped so re-run output supersedes the unwound
  run's at a guarded sink, and the loop waits for its next input: the inputs
  those iterations consumed stay released and are not re-delivered.

  Refused, changing nothing, when the target has not committed or when an unwound
  tasklet emitted plugin output it cannot compensate.
  """
  def operator_rewind(namespace, operator_id, to_iter, _call) do
    namespace = Namespace.or_default(namespace)

    with :ok <- Steps.rewind_instance(namespace, operator_id, to_iter) do
      IO.puts(
        "Operator '#{operator_id}' rewound to iteration #{to_iter}. " <>
          "Iterations above it were compensated; the loop resumes at #{to_iter + 1}."
      )
    end
  end

  @doc """
  Inject a payload into an operator's input stream (the SDK-facing `send_io`
  route), blocking for the confirming verdict so a parked or absent target is
  reported rather than silently dropped. `channel` is optional (`""` = none);
  `key` in the options names the member of a partition group that takes it.

  With `await_ms` in the options the output listener is attached first, then
  the payload is delivered and every output frame within the window is
  printed.
  """
  def operator_send(namespace, operator_id, message, channel, %{opts: opts}) do
    namespace = Namespace.or_default(namespace)

    with {:ok, member} <-
           OperatorManager.resolve_member(namespace, operator_id, Keyword.get(opts, :key)) do
      deliver = fn -> deliver(namespace, member, message, channel) end

      case Keyword.fetch(opts, :await_ms) do
        :error ->
          deliver.()

        {:ok, window} ->
          listen(namespace, operator_id, :text,
            window_ms: Context.integer!(window, "--await"),
            before: deliver
          )
      end
    end
  end

  defp deliver(namespace, operator_id, message, channel) do
    case OperatorManager.send_io_confirming(
           operator_id,
           namespace,
           message,
           OperatorManager.channel_metadata(channel)
         ) do
      :ok ->
        IO.puts("Delivered to '#{operator_id}'.")

      {:ok, :absorbed} ->
        IO.puts("Absorbed (duplicate) by '#{operator_id}'.")

      {:ok, :suspended} ->
        IO.puts("Journaled for suspended '#{operator_id}' — delivers on wake.")

      {:error, _reason} = refused ->
        refused
    end
  end

  @doc """
  Call an operator with one input and print its answer's payload: the output
  a `response()` end produced from it. `timeout_ms` in the options is the
  deadline; `key` names the member of a partition group that takes the input.
  """
  def operator_call(namespace, operator_id, message, channel, %{opts: opts}) do
    written = Keyword.fetch!(opts, :timeout_ms)

    deadline_ms =
      case Context.integer!(written, "--timeout") do
        ms when ms > 0 ->
          ms

        _ ->
          Context.fail!(
            Flezha.Error.new("usage.invalid_number", option: "--timeout", value: written)
          )
      end

    with {:ok, answer, _metadata} <-
           OperatorManager.call_operator(
             Namespace.or_default(namespace),
             operator_id,
             message,
             OperatorManager.channel_metadata(channel),
             deadline_ms,
             Keyword.get(opts, :key)
           ) do
      IO.puts(answer)
    end
  end

  @doc "Get operator state."
  def cell_state(namespace, operator_id, _call) do
    namespace_display = Namespace.or_default(namespace)

    case OperatorManager.get_cell_state(namespace_display, operator_id) do
      {:ok, json_state} when is_binary(json_state) ->
        IO.puts("State for operator '#{operator_id}' in namespace '#{namespace_display}':")
        IO.puts(json_state)

      {:ok, state} ->
        IO.puts("State for operator '#{operator_id}' in namespace '#{namespace_display}':")
        IO.puts(inspect(state))

      {:error, _reason} = refused ->
        refused
    end
  end

  @directions [:output, :input]

  @doc """
  Print every frame the operator emits (with its parts and dead-letter
  notices), or ingests when `direction` is `input`, until the window closes,
  the caller goes away or the operator ends.
  """
  def operator_listen(namespace, operator_id, direction, %{opts: opts}) do
    direction = Context.atom!(direction, @directions, "direction")

    listen(namespace, operator_id, frame_format(opts),
      direction: direction,
      window_ms: Context.window(opts)
    )
  end

  defp frame_format(opts), do: if(Context.format(opts) == "json", do: :json, else: :text)

  # A count the caller typed that must be at least one.
  defp positive!(written, option) do
    case Context.integer!(written, option) do
      n when n >= 1 -> n
      n -> Context.fail!(Flezha.Error.new("usage.not_positive", option: option, value: n))
    end
  end

  # The listener is `Flezha.Service.Listener`'s; the verb only prints. The
  # caller's group leader lives on the `flz` node, so its exit is the one
  # signal that the node went away.
  defp listen(namespace, operator_id, format, opts) do
    opts = Keyword.put(opts, :watch, Process.group_leader())

    case Listener.listen(Namespace.or_default(namespace), operator_id, opts, &print(&1, format)) do
      {:ok, _outcome} -> :ok
      {:error, _error} = ended -> ended
    end
  end

  defp print({:dead_lettered, error, metadata}, format) do
    print_notice(error, metadata, format)
    :continue
  end

  defp print({kind, payload, metadata}, format) do
    print_frame(kind, payload, metadata, format)
    :continue
  end

  @stamps [{"flz.cell", "cell"}, {"flz.seq", "seq"}, {"flz.key", "key"}]

  defp print_frame(kind, payload, metadata, :text) do
    stamps =
      for {key, label} <- @stamps, Map.has_key?(metadata, key), do: "#{label}=#{metadata[key]}"

    stamps =
      if kind == :part,
        do: ["part=#{metadata["flz.part"]} attempt=#{metadata["flz.part_attempt"]}" | stamps],
        else: stamps

    stamped = if stamps == [], do: [], else: [Enum.join(stamps, " ")]

    IO.puts(Enum.intersperse([metadata["channel"] || "-", payload | stamped], "\t"))
  end

  defp print_frame(kind, payload, metadata, :json) do
    body =
      if String.valid?(payload),
        do: %{payload: payload},
        else: %{payload_base64: Base.encode64(payload)}

    body
    |> Map.merge(%{
      kind: kind,
      channel: metadata["channel"],
      metadata: Signal.metadata_json(metadata)
    })
    |> Jason.encode!()
    |> IO.puts()
  end

  defp print_notice(error, metadata, :text) do
    fields = "error=#{error.code} entry=#{error.details["entry"]}"
    IO.puts(Enum.join([metadata["channel"] || "-", error.message, fields], "\t"))
  end

  defp print_notice(error, metadata, :json) do
    %{
      kind: :dead_lettered,
      channel: metadata["channel"],
      error: error,
      metadata: Signal.metadata_json(metadata)
    }
    |> Jason.encode!()
    |> IO.puts()
  end
end

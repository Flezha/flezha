defmodule Flezha.Service.Cli.Operator do
  @moduledoc """
  `flz operator start | stop | send | call | state | scale | rewind | replay |
  listen`: the text forms of the operator verbs' answers, and the two that
  print frames as they come — `listen`, and `send --await`, which listens
  before it delivers.
  """

  alias Flezha.Service.Admin
  alias Flezha.Service.Cli
  alias Flezha.Service.Cli.Context

  @doc "`flz operator start`: the id it runs under, or a group's members."
  def start(%{members: members, already_running: already} = group, _args, _opts) do
    case {members, already} do
      {members, []} ->
        IO.puts("Operator #{group.id} started with #{length(members)} partitions:")
        Enum.each(members, &IO.puts("  #{&1}"))

      {[], already} ->
        IO.puts(
          "Operator #{group.id} is already running with #{length(already)} partitions; " <>
            "the existing members were left untouched."
        )

      {members, already} ->
        IO.puts(
          "Operator #{group.id} converged: started #{length(members)} missing member(s), " <>
            "#{length(already)} already running:"
        )

        Enum.each(members, &IO.puts("  started #{&1}"))
        Enum.each(already, &IO.puts("  already running #{&1}"))
    end
  end

  def start(%{id: id, outcome: :started}, _args, _opts), do: IO.puts("Started #{id}")

  def start(%{id: id, outcome: :already_running}, _args, _opts),
    do: IO.puts("#{id} is already running, left untouched")

  @doc "`flz operator stop`."
  def stop(%{stopped: :group} = stopped, _args, _opts),
    do: IO.puts("Operator group #{stopped.id} stopped (#{stopped.partitions} partitions)")

  def stop(%{stopped: :cooperative, id: id}, _args, _opts) do
    IO.puts(
      "Cooperative stop requested for steps operator '#{id}'. " <>
        "It will finish in-flight work, then complete (cancelled) and be removed."
    )
  end

  def stop(_stopped, _args, _opts), do: IO.puts("Operator has been stopped")

  @doc """
  `flz operator send`: the delivery verdict. With `await_ms` in the options the
  output listener is attached first, then the input is delivered and every
  output frame within the window is printed.
  """
  def send(args_json, context, opts) do
    case Keyword.fetch(opts, :await_ms) do
      :error ->
        "Operator.send" |> Admin.run(args_json, context, :given) |> deliver(args_json, opts)

      {:ok, window_ms} ->
        [namespace, operator_id | _] = Jason.decode!(args_json)
        format = frame_format(opts)

        # A refused send ends the listen with its refusal, which `heard/3`
        # renders once, after the listener is detached.
        emit = fn
          :attached ->
            case Admin.run("Operator.send", args_json, context, :given) do
              {:ok, _sent} = sent -> deliver(sent, args_json, opts)
              refused -> refused
            end

          event ->
            print(event, format)
        end

        "Operator.listen"
        |> Admin.run(
          [namespace, operator_id, "output", %{"window_ms" => window_ms}],
          Map.put(context, :emit, emit),
          :given
        )
        |> heard(args_json, opts)
    end
  end

  defp deliver(answer, args_json, opts), do: Cli.render(answer, "Operator.send", args_json, opts)

  @doc "`flz operator send`'s verdict."
  def delivered(%{verdict: :delivered, to: to}, _args, _opts),
    do: IO.puts("Delivered to '#{to}'.")

  def delivered(%{verdict: :absorbed, to: to}, _args, _opts),
    do: IO.puts("Absorbed (duplicate) by '#{to}'.")

  def delivered(%{verdict: :suspended, to: to}, _args, _opts),
    do: IO.puts("Journaled for suspended '#{to}' — delivers on wake.")

  @doc "`flz operator call`: the answer's payload, as the operator wrote it."
  def call([payload, _metadata], _args, _opts), do: IO.puts(payload)

  @doc "`flz operator state`."
  def cell_state([json, _change_seq], [namespace, operator_id | _], _opts) do
    IO.puts("State for operator '#{operator_id}' in namespace '#{namespace}':")
    IO.puts(json)
  end

  @doc "`flz operator scale`."
  def rescale(result, [_namespace, group_id | _], _opts) do
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
  end

  @doc "`flz operator rewind`."
  def rewind(:ok, [_namespace, operator_id, to_iter], _opts) do
    IO.puts(
      "Operator '#{operator_id}' rewound to iteration #{to_iter}. " <>
        "Iterations above it were compensated; the loop resumes at #{to_iter + 1}."
    )
  end

  @doc "`flz operator replay`."
  def replay(%{cell: cell, from_seq: seq}, _args, _opts) do
    IO.puts(
      "Replaying #{cell} from seq #{seq}. Outputs republish under a " <>
        "bumped epoch; a keyed sink overwrites the stale rows by natural key."
    )
  end

  @doc """
  `flz operator listen`: every frame the operator emits (with its parts and
  dead-letter notices), or ingests, until the window closes, the caller goes
  away or the operator ends.
  """
  def listen(args_json, context, opts) do
    format = frame_format(opts)

    emit = fn
      :attached -> :ok
      event -> print(event, format)
    end

    "Operator.listen"
    |> Admin.run(args_json, Map.put(context, :emit, emit), :given)
    |> heard(args_json, opts)
  end

  defp heard({:ok, _outcome}, _args_json, _opts), do: :ok

  defp heard(refused, args_json, opts),
    do: Cli.render(refused, "Operator.listen", args_json, opts)

  defp frame_format(opts), do: if(Context.format(opts) == "json", do: :json, else: :text)

  defp print({:dead_lettered, error, metadata}, format), do: print_notice(error, metadata, format)
  defp print({kind, payload, metadata}, format), do: print_frame(kind, payload, metadata, format)
  defp print(:idle, _format), do: :ok

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
    |> Map.merge(%{kind: kind, channel: metadata["channel"], metadata: metadata})
    |> Admin.json()
    |> IO.puts()
  end

  defp print_notice(error, metadata, :text) do
    fields = "error=#{error.code} entry=#{error.details["entry"]}"
    IO.puts(Enum.join([metadata["channel"] || "-", error.message, fields], "\t"))
  end

  defp print_notice(error, metadata, :json) do
    # The error renders as its own JSON object; the rest by the door's rule.
    %{kind: :dead_lettered, channel: metadata["channel"], metadata: metadata}
    |> Admin.json()
    |> Jason.decode!()
    |> Map.put("error", error)
    |> Jason.encode!()
    |> IO.puts()
  end
end

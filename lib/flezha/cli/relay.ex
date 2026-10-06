defmodule Flezha.Service.Cli.Relay do
  @moduledoc """
  `flz relay start | stop | send | call | state | scale | rewind | replay |
  listen`: the text forms of the relay verbs' answers, and the two that
  print frames as they come — `listen`, and `send --await`, which listens
  before it delivers.
  """

  alias Flezha.Service.Admin
  alias Flezha.Service.Cli
  alias Flezha.Service.Cli.Context

  @doc "`flz relay start`: the id it runs under, or a group's members."
  def start(%{members: members, already_running: already} = group, _args, _opts) do
    case {members, already} do
      {members, []} ->
        IO.puts("Relay #{group.id} started with #{length(members)} partitions:")
        Enum.each(members, &IO.puts("  #{&1}"))

      {[], already} ->
        IO.puts(
          "Relay #{group.id} is already running with #{length(already)} partitions; " <>
            "the existing members were left untouched."
        )

      {members, already} ->
        IO.puts(
          "Relay #{group.id} converged: started #{length(members)} missing member(s), " <>
            "#{length(already)} already running:"
        )

        Enum.each(members, &IO.puts("  started #{&1}"))
        Enum.each(already, &IO.puts("  already running #{&1}"))
    end
  end

  def start(%{id: id, outcome: :started}, _args, _opts), do: IO.puts("Started #{id}")

  def start(%{id: id, outcome: :already_running}, _args, _opts),
    do: IO.puts("#{id} is already running, left untouched")

  @doc "`flz relay stop`."
  def stop(%{stopped: :group} = stopped, _args, _opts),
    do: IO.puts("Relay group #{stopped.id} stopped (#{stopped.partitions} partitions)")

  def stop(%{stopped: :cooperative, id: id}, _args, _opts) do
    IO.puts(
      "Cooperative stop requested for steps relay '#{id}'. " <>
        "It will finish in-flight work, then complete (cancelled) and be removed."
    )
  end

  def stop(_stopped, _args, _opts), do: IO.puts("Relay has been stopped")

  @doc """
  `flz relay send`: the delivery verdict. With `await_ms` in the options the
  output listener is attached first, then the input is delivered and every
  output frame within the window is printed.
  """
  def send(args_json, context, opts) do
    case Keyword.fetch(opts, :await_ms) do
      :error ->
        "Relay.send" |> Admin.run(args_json, context, :given) |> deliver(args_json, opts)

      {:ok, window_ms} ->
        [namespace, relay_id | _] = Jason.decode!(args_json)
        format = frame_format(opts)

        # A refused send ends the listen with its refusal, which `heard/3`
        # renders once, after the listener is detached.
        emit = fn
          :attached ->
            case Admin.run("Relay.send", args_json, context, :given) do
              {:ok, _sent} = sent -> deliver(sent, args_json, opts)
              refused -> refused
            end

          event ->
            print(event, format)
        end

        "Relay.listen"
        |> Admin.run(
          [namespace, relay_id, "output", %{"window_ms" => window_ms}],
          Map.put(context, :emit, emit),
          :given
        )
        |> heard(args_json, opts)
    end
  end

  defp deliver(answer, args_json, opts), do: Cli.render(answer, "Relay.send", args_json, opts)

  @doc "`flz relay send`'s verdict."
  def delivered(%{verdict: :delivered, to: to}, _args, _opts),
    do: IO.puts("Delivered to '#{to}'.")

  def delivered(%{verdict: :absorbed, to: to}, _args, _opts),
    do: IO.puts("Absorbed (duplicate) by '#{to}'.")

  def delivered(%{verdict: :suspended, to: to}, _args, _opts),
    do: IO.puts("Journaled for suspended '#{to}' — delivers on wake.")

  @doc """
  `flz relay call`: the call through `Relay.call_channel`. With `coerce` in
  the options its value is `--arg` fields still as text, typed here by the
  channel's schema, which is read through the door first.
  """
  def call_channel(args_json, context, opts) do
    args = if Keyword.get(opts, :coerce), do: typed(args_json, context), else: {:ok, args_json}

    args
    |> then(fn
      {:ok, args} -> Admin.run("Relay.call_channel", args, context, :given)
      refused -> refused
    end)
    |> Cli.render("Relay.call_channel", args_json, opts)
  end

  @doc "`flz relay call`: the answer as its format reads, or `accepted` for a send."
  def answered(%{accepted: true}, _args, _opts), do: IO.puts("accepted")

  def answered(%{answer_codec: "json", answer: answer}, _args, _opts),
    do: IO.puts(Jason.encode!(answer))

  def answered(%{answer: answer}, _args, _opts), do: IO.puts(answer)

  defp typed(args_json, context) do
    [namespace, relay_id, channel, fields, call_opts] = Jason.decode!(args_json)

    with {:ok, [factory]} <- Admin.run("Relay.api", [namespace, relay_id], context, :given),
         {:ok, entry} <- typed_entry(factory, channel),
         {:ok, properties} <- properties(entry),
         {:ok, value} <- coerce_all(entry, properties, fields) do
      {:ok, [namespace, relay_id, channel, value, call_opts]}
    else
      :untyped -> {:ok, [namespace, relay_id, channel, fields, call_opts]}
      refused -> refused
    end
  end

  # The entry `--arg` types the fields by; a channel the API lacks, or a
  # factory with none, leaves them as written, for the call to refuse.
  defp typed_entry(%{channels: [first | _]}, ""), do: {:ok, first}

  defp typed_entry(factory, channel) do
    case Enum.find(factory.channels, &(&1.channel == channel)) do
      nil -> :untyped
      entry -> {:ok, entry}
    end
  end

  # `--arg` names the properties of a JSON channel's object schema.
  defp properties(%{codec: "json", schema: %{"properties" => %{} = properties}}),
    do: {:ok, properties}

  defp properties(entry),
    do:
      refuse(
        entry,
        "a json channel with an object schema",
        "--arg needs the channel's properties"
      )

  defp coerce_all(entry, properties, fields) do
    Enum.reduce_while(fields, {:ok, %{}}, fn {name, text}, {:ok, value} ->
      case coerce(get_in(properties, [name, "type"]), text) do
        {:ok, typed} -> {:cont, {:ok, Map.put(value, name, typed)}}
        {:expected, expected} -> {:halt, refuse(entry, expected, "#{name}: #{inspect(text)}")}
      end
    end)
  end

  defp coerce(type, text) when type in ["string", nil], do: {:ok, text}
  defp coerce("boolean", "true"), do: {:ok, true}
  defp coerce("boolean", "false"), do: {:ok, false}
  defp coerce("boolean", _text), do: {:expected, "true or false"}

  defp coerce("integer", text) do
    case Integer.parse(text) do
      {integer, ""} -> {:ok, integer}
      _not_whole -> {:expected, "an integer"}
    end
  end

  defp coerce("number", text) do
    case Jason.decode(text) do
      {:ok, number} when is_number(number) -> {:ok, number}
      _not_number -> {:expected, "a number"}
    end
  end

  defp coerce(type, _text), do: {:expected, "a #{type} property, which --arg cannot write"}

  defp refuse(entry, expected, reason),
    do:
      {:error,
       Flezha.Error.new("api.input_invalid",
         channel: entry.channel,
         expected: expected,
         reason: reason
       )}

  @doc "`flz relay state`."
  def cell_state([json, _change_seq], [namespace, relay_id | _], _opts) do
    IO.puts("State for relay '#{relay_id}' in namespace '#{namespace}':")
    IO.puts(json)
  end

  @doc "`flz relay scale`."
  def rescale(result, [_namespace, group_id | _], _opts) do
    IO.puts(
      "Relay #{group_id} rescaled #{result.from} -> #{result.to} " <>
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

  @doc "`flz relay rewind`."
  def rewind(:ok, [_namespace, relay_id, to_iter], _opts) do
    IO.puts(
      "Relay '#{relay_id}' rewound to iteration #{to_iter}. " <>
        "Iterations above it were compensated; the loop resumes at #{to_iter + 1}."
    )
  end

  @doc "`flz relay replay`."
  def replay(%{cell: cell, from_seq: seq}, _args, _opts) do
    IO.puts(
      "Replaying #{cell} from seq #{seq}. Outputs republish under a " <>
        "bumped epoch; a keyed output plugin overwrites the stale rows by natural key."
    )
  end

  @doc """
  `flz relay listen`: every frame the relay emits (with its parts and
  dead-letter notices), or ingests, until the window closes, the caller goes
  away or the relay ends.
  """
  def listen(args_json, context, opts) do
    format = frame_format(opts)

    emit = fn
      :attached -> :ok
      event -> print(event, format)
    end

    "Relay.listen"
    |> Admin.run(args_json, Map.put(context, :emit, emit), :given)
    |> heard(args_json, opts)
  end

  defp heard({:ok, _outcome}, _args_json, _opts), do: :ok

  defp heard(refused, args_json, opts),
    do: Cli.render(refused, "Relay.listen", args_json, opts)

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

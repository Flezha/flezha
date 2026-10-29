defmodule Flezha.Service.Cli.Logs do
  @moduledoc """
  `flz logs` (and `flz audit`, its spelling for the audit kind): the text forms
  of the kinds, read, files, stats and rotate answers, and the tail, which
  prints records as any controller writes them until its window closes.
  """

  alias Flezha.Service.Admin
  alias Flezha.Service.Cli
  alias Flezha.Service.Cli.Context

  @doc "`flz logs kinds`: one block per kind, with the filters it takes."
  def kinds(kinds, _args, _opts) do
    Enum.each(kinds, fn kind ->
      state = if kind.enabled, do: "on", else: "off: #{kind.disabled_reason}"
      IO.puts("#{kind.id}\t#{kind.title} (#{state})")
      IO.puts("  #{kind.description}")
      IO.puts("  filters: " <> Enum.map_join(kind.filters, ", ", &filter/1))
    end)
  end

  defp filter(%{field: field, values: values}) when is_list(values),
    do: "#{field} (#{Enum.join(values, "|")})"

  defp filter(%{field: field}), do: field

  @doc "`flz logs read`: one line per record, then the cursor of the next page."
  def read(%{records: records} = answer, _args, _opts) do
    Enum.each(records, &print_record/1)
    if answer.next, do: IO.puts("next: #{answer.next}")
    Context.unreachable(answer.unreachable)
  end

  @doc "`flz logs files`: one tab-separated line per file."
  def files(%{files: files} = answer, _args, _opts) do
    Context.rows(%{rows: files, unreachable: answer.unreachable}, fn files ->
      IO.puts(Enum.join(~w(controller filename first_ts last_ts count size), "\t"))

      for e <- files do
        [e.node, e.filename, e.first_ts_us, e.last_ts_us, e.record_count, e.size_bytes]
        |> Enum.join("\t")
        |> IO.puts()
      end
    end)
  end

  @stats ~w(current_file current_file_bytes total_bytes total_files oldest_ts_us record_count
            dropped_count timeout_count post_close_dropped fsync_mode)a

  @doc "`flz logs stats`: each controller's operational snapshot."
  def stats(%{controllers: rows} = answer, _args, _opts) do
    Context.per_controller(%{rows: rows, unreachable: answer.unreachable}, fn row ->
      for key <- @stats,
          do: IO.puts("  #{String.pad_trailing("#{key}:", 21)} #{Map.get(row, key)}")
    end)
  end

  @doc "`flz logs rotate`: the file each controller rotated to."
  def rotate(%{rotated: rows} = answer, _args, _opts) do
    Context.per_controller(%{rows: rows, unreachable: answer.unreachable}, fn row ->
      IO.puts("  rotated to #{row.rotated_to || "nothing (#{row[:error]})"}")
    end)
  end

  @doc """
  `flz logs tail`: print every record as it is written, on any controller,
  until the window closes.
  """
  def tail(args_json, context, opts) do
    format = Context.format(opts)
    context = Map.merge(context, %{emit: &print(&1, format), window_ms: Context.window(opts)})

    case Admin.run("Logs.tail", args_json, context, :given) do
      {:ok, _outcome} -> :ok
      refused -> Cli.render(refused, "Logs.tail", args_json, opts)
    end
  end

  defp print(record, "json"), do: IO.puts(Jason.encode!(record))
  defp print(record, _text), do: print_record(record)

  defp print_record(%{"event" => event} = r) do
    origin = Map.get(r, "origin") || %{}

    IO.puts(
      "[#{r["ts_iso"]}] #{r["node"]} #{event} #{r["namespace"]}/#{r["relay"]} " <>
        "factory=#{r["factory"]} origin=#{origin["kind"]}" <>
        if(event == "ended",
          do: " outcome=#{r["outcome"]} duration_ms=#{r["duration_ms"]}",
          else: ""
        ) <> if(r["error"] in [nil, ""], do: "", else: " error=#{inspect(r["error"])}")
    )
  end

  defp print_record(r) do
    caller = Map.get(r, "caller") || %{}

    IO.puts(
      "[#{r["ts_iso"]}] #{r["node"]} #{r["source"]}.#{r["name"]} " <>
        "caller=#{caller["source"]} request_id=#{caller["request_id"]}"
    )
  end
end

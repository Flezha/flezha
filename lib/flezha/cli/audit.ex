defmodule Flezha.Service.Cli.Audit do
  @moduledoc """
  `flz audit` — the text forms of the durable audit log's list, read, stats
  and rotate answers, and the tail, which prints records as the writer appends
  them until its window closes.
  """

  alias Flezha.Service.Admin
  alias Flezha.Service.Cli
  alias Flezha.Service.Cli.Context

  @doc "`flz audit list`: one tab-separated line per file."
  def list_files(entries, _args, _opts) do
    IO.puts(Enum.join(["filename", "first_ts", "last_ts", "count", "size"], "\t"))

    Enum.each(entries, fn e ->
      IO.puts(
        Enum.join(
          [e.filename, e.first_ts_us, e.last_ts_us, e.record_count, e.size_bytes],
          "\t"
        )
      )
    end)
  end

  @doc "`flz audit read`: one line per record."
  def read(records, _args, _opts), do: Enum.each(records, &print_record/1)

  @doc "`flz audit stats`: the operational snapshot."
  def stats(snap, _args, _opts) do
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

  @doc "`flz audit rotate`: the file the writer rotated to."
  def rotate(info, _args, _opts), do: IO.puts("rotated to: #{info.rotated_to}")

  @doc """
  `flz audit tail`: subscribe, print every record as it is written until the
  window closes or the writer does, then unsubscribe. A closed writer has
  already dropped the subscription.
  """
  def tail(args_json, context, opts) do
    case Admin.run("AuditLog.tail", args_json, context, :given) do
      {:ok, ref} ->
        if Context.receive_for(Context.window(opts), &tail_message(&1, ref, opts)) ==
             :window_closed,
           do: Admin.run("AuditLog.unsubscribe", [ref], context, :given)

      refused ->
        Cli.render(refused, "AuditLog.tail", args_json, opts)
    end
  end

  defp tail_message({:audit_record, ref, record}, ref, opts) do
    case Context.format(opts) do
      "json" -> IO.puts(Jason.encode!(record))
      _text -> print_record(record)
    end

    :continue
  end

  defp tail_message({:audit_writer_closed, ref}, ref, _opts), do: :done
  defp tail_message(_message, _ref, _opts), do: :continue

  defp print_record(r) do
    caller = Map.get(r, "caller") || %{}

    IO.puts(
      "[#{Map.get(r, "ts_iso")}] #{Map.get(r, "source")}.#{Map.get(r, "name")} " <>
        "caller=#{Map.get(caller, "source")} request_id=#{Map.get(caller, "request_id")}"
    )
  end
end

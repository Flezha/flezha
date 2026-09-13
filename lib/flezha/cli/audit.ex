defmodule Flezha.Service.Cli.Audit do
  @moduledoc """
  `flz audit` — the durable audit log's list, read, tail, stats and rotate
  renderers, over the effects `Flezha.Service.AuditLog` shares with the console.
  """

  alias Flezha.Observability.AuditLog.Writer
  alias Flezha.Service.AuditLog, as: AuditSvc
  alias Flezha.Service.Cli.Context

  @doc """
  CLI entry: `flz audit list`. Renders the catalog as text or JSON.
  """
  def audit_log_render_list(format, _call) when format in ["text", "json"] do
    case AuditSvc.catalog() do
      {:ok, entries} -> render_audit_list(entries, format)
      {:error, reason} -> render_audit_error(reason)
    end
  end

  @doc """
  CLI entry: `flz audit read <file> [filters]`. Streams matching records as
  one-record-per-line (text) or NDJSON.
  """
  def audit_log_render_read(file, format, %{opts: opts, context: context})
      when format in ["text", "json"] do
    params =
      %{file: file}
      |> Context.put_unless_nil(:source, Keyword.get(opts, :source))
      |> Context.put_unless_nil(:name, Keyword.get(opts, :name))
      |> Context.put_unless_nil(:since_us, Keyword.get(opts, :since_us))
      |> Context.put_unless_nil(:until_us, Keyword.get(opts, :until_us))
      |> Context.put_unless_nil(:limit, Keyword.get(opts, :limit))

    case AuditSvc.read_records(params, context) do
      {:ok, records} -> render_audit_records(records, format)
      {:error, reason} -> render_audit_error(reason)
    end
  end

  @doc """
  CLI entry: `flz audit tail`. Subscribes for live records and streams them
  to stdout until the duration elapses or the writer closes.
  """
  def audit_log_render_tail(format, %{opts: opts}) when format in ["text", "json"] do
    case AuditSvc.subscribe_tail() do
      {:ok, ref} -> audit_tail(ref, format, Context.window(opts))
      {:error, reason} -> render_audit_error(reason)
    end
  end

  @doc """
  CLI entry: `flz audit stats`. Prints the operational snapshot.
  """
  def audit_log_render_stats(format, _call) when format in ["text", "json"] do
    case AuditSvc.stats_snapshot() do
      {:ok, snap} -> render_audit_stats(snap, format)
      {:error, reason} -> render_audit_error(reason)
    end
  end

  @doc """
  CLI entry: `flz audit rotate`. Forces an immediate rotation.
  """
  def audit_log_render_rotate(format, _call) when format in ["text", "json"] do
    case AuditSvc.rotate_now() do
      {:ok, info} -> render_audit_rotate(info, format)
      {:error, reason} -> render_audit_error(reason)
    end
  end

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

  # A closed writer has already dropped the subscription, and asking it to drop
  # one would call a process that is gone.
  defp audit_tail(ref, format, window_ms) do
    if Context.receive_for(window_ms, &audit_tail_message(&1, ref, format)) == :window_closed,
      do: Writer.unsubscribe_tail(ref)

    :ok
  end

  defp audit_tail_message({:audit_record, ref, record}, ref, "json") do
    IO.puts(Jason.encode!(record))
    :continue
  end

  defp audit_tail_message({:audit_record, ref, record}, ref, _format) do
    render_audit_records([record], "text")
    :continue
  end

  defp audit_tail_message({:audit_writer_closed, ref}, ref, _format), do: :done
  defp audit_tail_message(_message, _ref, _format), do: :continue

  defp render_audit_error(reason), do: Context.fail!(reason, &audit_message/1)

  defp audit_message(:audit_log_disabled),
    do:
      "audit log is disabled; set FLEZHA_AUDIT_ENABLED=true or " <>
        ":audit_log, :enabled: true in config"

  defp audit_message(:observability_disabled), do: "observability is disabled"
  defp audit_message(:file_not_found), do: "file not found"
  defp audit_message(_reason), do: nil
end

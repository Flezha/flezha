defmodule Flezha.Service.Cli.Dlq do
  @moduledoc """
  `flz dlq` — list, show, replay and drop for parked records.

  Rendering only. `Flezha.Service.Dlq` owns the reads, the authorization, the
  audit and the re-drive, so the terminal and the console reach the store
  through one gate instead of two.
  """

  alias Flezha.Service.Cli.Context
  alias Flezha.Service.Dlq, as: DlqSvc

  @doc """
  `flz dlq list [operative]` — parked entries, nearest-to-expiry (oldest)
  first.
  """
  def dlq_list(operative_class \\ "", opts \\ []) do
    filter = if operative_class == "", do: nil, else: operative_class

    case DlqSvc.list(filter, Context.cli_context(opts)) do
      {:ok, []} -> IO.puts("dead-letter queue is empty")
      {:ok, entries} -> render_listing(entries)
      {:error, reason} -> render_dlq_error(reason, "dlq.list")
    end
  end

  @doc """
  `flz dlq show <operative/procedure/id>` — entry metadata + payload preview.
  """
  def dlq_show(entry_ref, opts \\ []) do
    case DlqSvc.get(entry_ref, Context.cli_context(opts)) do
      {:ok, entry, payload} -> render_entry(entry, payload)
      {:error, reason} -> render_dlq_error(reason, "dlq.show")
    end
  end

  @doc """
  `flz dlq replay <operative/procedure/id>` — re-dispatch a parked payload
  to its operative through the ordinary input path. A successful replay
  removes the entry; a failing one re-parks it with `generation`
  incremented.
  """
  def dlq_replay(entry_ref, opts \\ []) do
    case DlqSvc.replay(entry_ref, Context.cli_context(opts)) do
      {:ok, :removed} ->
        IO.puts("replay succeeded; #{entry_ref} removed")

      {:ok, {:re_parked, generation}} ->
        IO.puts("replay failed; #{entry_ref} re-parked with generation #{generation}")

      {:error, reason} ->
        render_dlq_error(reason, "dlq.replay")
    end
  end

  @doc """
  `flz dlq drop <operative/procedure/id>` — discard a parked entry and its
  payload. Audited: the drop is deliberate data loss.
  """
  def dlq_drop(entry_ref, opts \\ []) do
    case DlqSvc.drop(entry_ref, Context.cli_context(opts)) do
      :ok -> IO.puts("dropped #{entry_ref}")
      {:error, reason} -> render_dlq_error(reason, "dlq.drop")
    end
  end

  defp render_listing(entries) do
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

  defp render_entry(entry, payload) do
    Enum.each(
      ~w(id operative_class procedure_id entity_key channel origin_offset error attempts generation status ts_ms payload_bytes),
      fn key ->
        IO.puts(String.pad_trailing(key, 18) <> to_string(inspect(entry[key])))
      end
    )

    render_payload(payload)
  end

  defp render_payload(nil) do
    IO.puts("payload            <not byte-representable; entry is not replayable>")
  end

  defp render_payload(bytes) do
    preview = binary_part(bytes, 0, min(byte_size(bytes), 256))

    rendered =
      if String.printable?(preview),
        do: preview,
        else: Base.encode16(preview, case: :lower)

    IO.puts("payload            #{rendered}")
  end

  defp render_dlq_error({:forbidden, reason}, action), do: Context.refuse!(action, reason)

  defp render_dlq_error(:not_found, _action) do
    Context.cli_error!("error: DLQ entry not found", :stderr)
  end

  defp render_dlq_error(:bad_entry_ref, _action) do
    Context.cli_error!("error: expected <operative>/<procedure>/<entry-id>", :stderr)
  end

  defp render_dlq_error(:payload_not_replayable, _action) do
    Context.cli_error!(
      "error: entry carries no payload bytes (inter-procedure input); inspect or drop it",
      :stderr
    )
  end

  defp render_dlq_error(:operative_not_running, _action) do
    Context.cli_error!(
      "error: the entry's operative is not running; start it before replaying",
      :stderr
    )
  end

  defp render_dlq_error(reason, _action) do
    Context.cli_error!("error: #{inspect(reason)}", :stderr)
  end
end

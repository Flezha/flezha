defmodule Flezha.Service.Cli.Dlq do
  @moduledoc """
  `flz dlq` — list, show, replay and drop for parked records.

  Rendering only: the store call and the re-drive are `Flezha.Service.Dlq`'s,
  and the authorization and audit are the verb's row, so the terminal and the
  console reach the store through one gate.
  """

  alias Flezha.Dlq.Store
  alias Flezha.Service.Cli.Context
  alias Flezha.Service.Dlq, as: DlqSvc

  @doc "The row a `dlq list` is judged by: the named class's namespace, `_system` for none."
  def class_row([operative_class | _]), do: DlqSvc.class_row(class_filter(operative_class))

  @doc "The entry a `dlq show | replay | drop` names, or the family's not-found."
  def locate([entry_ref | _]) do
    case DlqSvc.entry_row(entry_ref) do
      {:ok, _row} = located -> located
      {:error, reason} -> render_dlq_error(reason)
    end
  end

  @doc """
  `flz dlq list [operative]` — parked entries, nearest-to-expiry (oldest)
  first.
  """
  def dlq_list(operative_class, _call) do
    case Store.list(class_filter(operative_class)) do
      [] -> IO.puts("dead-letter queue is empty")
      entries -> render_listing(entries)
    end
  end

  @doc """
  `flz dlq show <operative/procedure/id>` — entry metadata + payload preview.
  """
  def dlq_show(_entry_ref, %{row: %{entry: entry, payload: payload}}) do
    render_entry(entry, payload)
  end

  @doc """
  `flz dlq replay <operative/procedure/id>` — re-dispatch a parked payload
  to its operative through the ordinary input path. A successful replay
  removes the entry; a failing one re-parks it with `generation`
  incremented.
  """
  def dlq_replay(entry_ref, %{row: %{entry: entry, payload: payload}}) do
    case DlqSvc.redrive(entry_ref, entry, payload) do
      {:ok, :removed} ->
        IO.puts("replay succeeded; #{entry_ref} removed")

      {:ok, {:re_parked, generation}} ->
        IO.puts("replay failed; #{entry_ref} re-parked with generation #{generation}")

      {:error, reason} ->
        render_dlq_error(reason)
    end
  end

  @doc """
  `flz dlq drop <operative/procedure/id>` — discard a parked entry and its
  payload. Audited: the drop is deliberate data loss.
  """
  def dlq_drop(entry_ref, _call) do
    :ok = Store.drop(entry_ref)
    IO.puts("dropped #{entry_ref}")
  end

  defp class_filter(""), do: nil
  defp class_filter(operative_class), do: operative_class

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

  defp render_dlq_error(reason), do: Context.fail!(reason, &dlq_message/1)

  defp dlq_message(:not_found), do: "DLQ entry not found"
  defp dlq_message(:bad_entry_ref), do: "expected <operative>/<procedure>/<entry-id>"

  defp dlq_message(:payload_not_replayable),
    do: "entry carries no payload bytes (inter-procedure input); inspect or drop it"

  defp dlq_message(:operative_not_running),
    do: "the entry's operative is not running; start it before replaying"

  defp dlq_message(_reason), do: nil
end

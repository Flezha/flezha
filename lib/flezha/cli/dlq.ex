defmodule Flezha.Service.Cli.Dlq do
  @moduledoc """
  `flz dlq` — the text forms of list, show, replay and drop for parked records.
  """

  @doc "`flz dlq replay`."
  def replay(%{outcome: :removed}, [entry_ref | _], _opts),
    do: IO.puts("replay succeeded; #{entry_ref} removed")

  def replay(%{outcome: :re_parked, generation: generation}, [entry_ref | _], _opts),
    do: IO.puts("replay failed; #{entry_ref} re-parked with generation #{generation}")

  @doc "`flz dlq drop`."
  def drop(:ok, [entry_ref | _], _opts), do: IO.puts("dropped #{entry_ref}")

  @doc "`flz dlq list`: parked entries, nearest-to-expiry first."
  def list([], _args, _opts), do: IO.puts("dead-letter queue is empty")

  def list(entries, _args, _opts) do
    IO.puts(
      String.pad_trailing("ENTRY", 60) <>
        String.pad_trailing("CELL", 16) <>
        String.pad_trailing("CHANNEL", 12) <>
        String.pad_trailing("GEN", 5) <>
        String.pad_trailing("AGE", 10) <> "ERROR"
    )

    Enum.each(entries, fn e ->
      ref = "#{e["relay_class"]}/#{e["tasklet_id"]}/#{e["id"]}"
      age_s = div(System.system_time(:millisecond) - (e["ts_ms"] || 0), 1000)

      IO.puts(
        String.pad_trailing(ref, 60) <>
          String.pad_trailing(e["cell_key"] || "", 16) <>
          String.pad_trailing(e["channel"] || "", 12) <>
          String.pad_trailing(Integer.to_string(e["generation"] || 1), 5) <>
          String.pad_trailing("#{age_s}s", 10) <> String.slice(e["error"] || "", 0, 60)
      )
    end)
  end

  @doc "`flz dlq show`: the entry's fields and a preview of its payload."
  def show([entry, payload], _args, _opts) do
    Enum.each(
      ~w(id relay_class tasklet_id cell_key channel origin_offset error attempts generation status ts_ms payload_bytes),
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
end

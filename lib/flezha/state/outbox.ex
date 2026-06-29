defmodule Flezha.State.Outbox do
  @moduledoc """
  Persists `last_persisted_seq` per Segment into `<key>.outbox.dedup`.

  The file is rewritten on each WAL append via the
  `<key>.outbox.dedup.tmp` → `rename(<key>.outbox.dedup)` pattern so a
  crash mid-write leaves either the previous seq or the new one — never a
  torn write.

  The on-disk format is a single 8-byte big-endian unsigned integer.
  Files smaller than 8 bytes (or unreadable) report `0`, treated as
  "no records persisted yet".
  """

  alias Flezha.State.Registry

  @spec read_last_seq(Registry.segment_id()) :: non_neg_integer()
  def read_last_seq(segment_id) do
    path = Registry.outbox_dedup_path(segment_id)

    case File.read(path) do
      {:ok, <<seq::64-big>>} -> seq
      _ -> 0
    end
  end

  @spec write_last_seq(Registry.segment_id(), non_neg_integer()) :: :ok | {:error, term()}
  def write_last_seq(segment_id, seq) when is_integer(seq) and seq >= 0 do
    path = Registry.outbox_dedup_path(segment_id)
    tmp_path = path <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(tmp_path, <<seq::64-big>>),
         :ok <- File.rename(tmp_path, path) do
      :ok
    end
  end
end

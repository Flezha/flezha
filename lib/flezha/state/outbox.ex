defmodule Flezha.State.Outbox do
  @moduledoc """
  Per-Segment dedup file (`<key>.outbox.dedup`): the persistence watermark
  `last_persisted_seq`, the outbox flush cursor `outbox_flushed_seq`, and
  the realign `epoch`, written together with the atomic
  `<key>.outbox.dedup.tmp` → rename pattern so a crash mid-write leaves
  either the previous triple or the new one — never a torn write.

  The file is a rebuildable cache advanced at fsync-batch cadence (plus
  snapshot, rotation, realign, and clean shutdown) — never per record. On
  boot, `WalWriter`'s WAL scan is the authoritative `last_persisted_seq`
  source; a stale `outbox_flushed_seq` only widens the restart re-drive
  (sink dedup absorbs the repeats — the cursor may lag, never lead).

  On-disk format v2 is three 8-byte big-endian integers
  `last_persisted_seq (unsigned) | outbox_flushed_seq (signed, -1 ≡ nothing
  flushed yet) | epoch (unsigned)`. A legacy 8-byte file reports its value
  as `last_persisted_seq` with cursor `-1` and epoch `0`; shorter or
  unreadable files report the empty triple.
  """

  alias Flezha.State.Registry

  @type t :: %{
          last_persisted_seq: non_neg_integer(),
          outbox_flushed_seq: integer(),
          epoch: non_neg_integer()
        }

  @spec read(Registry.segment_id()) :: t()
  def read(segment_id) do
    path = Registry.outbox_dedup_path(segment_id)

    case File.read(path) do
      {:ok, <<seq::64-big, flushed::64-big-signed, epoch::64-big>>} ->
        %{last_persisted_seq: seq, outbox_flushed_seq: flushed, epoch: epoch}

      {:ok, <<seq::64-big>>} ->
        %{last_persisted_seq: seq, outbox_flushed_seq: -1, epoch: 0}

      _ ->
        %{last_persisted_seq: 0, outbox_flushed_seq: -1, epoch: 0}
    end
  end

  @spec write(Registry.segment_id(), t()) :: :ok | {:error, term()}
  def write(segment_id, %{
        last_persisted_seq: seq,
        outbox_flushed_seq: flushed,
        epoch: epoch
      })
      when is_integer(seq) and seq >= 0 and is_integer(flushed) and flushed >= -1 and
             is_integer(epoch) and epoch >= 0 do
    path = Registry.outbox_dedup_path(segment_id)
    tmp_path = path <> ".tmp"

    # No mkdir: the WAL open owns dir creation, so a cache write can never
    # resurrect a deleted Segment dir (it fails with :enoent instead).
    with :ok <- File.write(tmp_path, <<seq::64-big, flushed::64-big-signed, epoch::64-big>>),
         :ok <- File.rename(tmp_path, path) do
      :ok
    end
  end
end

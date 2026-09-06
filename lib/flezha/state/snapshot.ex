defmodule Flezha.State.Snapshot do
  @moduledoc """
  Snapshot file lifecycle: write `.tmp`, fsync, atomically rename, then truncate
  the WAL up to the snapshot's seq. The on-disk format is in
  `docs/internals/state_persistence.md`.

  The sequence has three failure windows, and no boot-time reconciliation step
  is required for any of them:

    1. **Crash before rename** — leaves an orphan `.tmp`. `Paths.scan_dir/1`
       ignores `.tmp` files; the orphan is overwritten on the next compaction.
    2. **Crash between rename and WAL-truncate** — the snapshot is durable but
       the WAL still holds records it covers. Restore applies the snapshot then
       folds the redundant records, and because `apply_change` is required to be
       idempotent over `(seq, change)` the result is identical.
    3. **Crash mid-WAL-truncate** — handled by `WalWriter.truncate_up_to/2`'s
       own `entity.wal.rewriting` -> rename sequence.
  """

  alias Flezha.State.{Metrics, Paths, RecordFrame, WalWriter, WriterRegistry}

  @header_size RecordFrame.header_size()

  @doc """
  Persist a snapshot for `entity_id`. Truncates the WAL up to `seq`
  on success, after the rename is durable. Returns the absolute path
  the snapshot was written to.
  """
  @spec write_snapshot(Paths.entity_id(), non_neg_integer(), binary(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def write_snapshot(entity_id, seq, header, snapshot_bytes)
      when is_integer(seq) and seq >= 0 and is_binary(header) and is_binary(snapshot_bytes) do
    if byte_size(header) != @header_size do
      {:error, {:bad_header_size, byte_size(header)}}
    else
      final_path = Paths.snapshot_path(entity_id, seq)

      crc = RecordFrame.crc32(header, snapshot_bytes)
      bytes = <<header::binary, snapshot_bytes::binary, crc::32-big>>

      with :ok <- Paths.atomic_install(final_path, &:file.write(&1, bytes), :snapshot) do
        case WriterRegistry.lookup(entity_id) do
          {:ok, pid} -> WalWriter.truncate_up_to(pid, seq)
          :error -> :ok
        end

        metrics = Metrics.for_entity(entity_id)
        Metrics.put(metrics, :snapshot_seq, seq)

        {:ok, final_path}
      end
    end
  end

  @doc """
  Load and verify a snapshot file. Returns `{header, bytes}` on success.
  """
  @spec read_snapshot(binary()) ::
          {:ok, binary(), binary()} | {:error, :crc_mismatch | :short_read | term()}
  def read_snapshot(path) do
    case File.read(path) do
      {:ok, body} when byte_size(body) >= @header_size + 4 ->
        payload_size = byte_size(body) - @header_size - 4

        <<header::binary-size(@header_size), bytes::binary-size(^payload_size), crc::32-big>> =
          body

        if RecordFrame.crc32(header, bytes) == crc do
          {:ok, header, bytes}
        else
          {:error, :crc_mismatch}
        end

      {:ok, _} ->
        {:error, :short_read}

      err ->
        err
    end
  end

  @doc """
  Highest-seq snapshot for the Entity, or `nil` if none exist.
  """
  @spec latest_snapshot(Paths.entity_id()) :: {non_neg_integer(), binary()} | nil
  def latest_snapshot(entity_id) do
    case Paths.snapshot_paths(entity_id) do
      [] -> nil
      paths -> List.last(paths)
    end
  end

  @doc """
  Remove a single snapshot file by seq. No-op if absent.
  """
  @spec delete_snapshot(Paths.entity_id(), non_neg_integer()) :: :ok
  def delete_snapshot(entity_id, seq) do
    _ = File.rm(Paths.snapshot_path(entity_id, seq))
    :ok
  end
end

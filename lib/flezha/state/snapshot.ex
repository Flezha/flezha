defmodule Flezha.State.Snapshot do
  @moduledoc """
  Snapshot file lifecycle: write `.tmp`, fsync, atomically rename, then
  truncate the WAL up to the snapshot's seq.

  The on-disk format is `<<header::binary-16, snapshot_bytes::binary, crc32::32-big>>`
  where `crc32 = :erlang.crc32(<<header::binary, snapshot_bytes::binary>>)`.

  ## Crash safety

  The write sequence is `open .tmp → write → fsync → close → rename →
  truncate WAL`. There are three failure windows:

    1. **Crash before rename** — leaves an orphan `.tmp`. `Registry.scan_dir/1`
       ignores `.tmp` files; the orphan is overwritten on the next compaction.
    2. **Crash between rename and WAL-truncate** — the snapshot is durable
       but the WAL still contains records covered by the snapshot. Restore
       applies the snapshot then folds the redundant WAL records: because
       `apply_change` is required to be idempotent over `(seq, change)`
       pairs (umbrella spec), the result is identical to having truncated.
    3. **Crash mid-WAL-truncate** — handled by `WalWriter.truncate_up_to/2`'s
       own `segment.wal.rewriting → rename` sequence.

  No reconciliation step is required at boot.
  """

  alias Flezha.State.{Metrics, Registry, Supervisor, WalWriter}

  @header_size 16

  @doc """
  Persist a snapshot for `segment_id`. Truncates the WAL up to `seq`
  on success, after the rename is durable. Returns the absolute path
  the snapshot was written to.
  """
  @spec write_snapshot(Registry.segment_id(), non_neg_integer(), binary(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def write_snapshot(segment_id, seq, header, snapshot_bytes)
      when is_integer(seq) and seq >= 0 and is_binary(header) and is_binary(snapshot_bytes) do
    if byte_size(header) != @header_size do
      {:error, {:bad_header_size, byte_size(header)}}
    else
      dir = Registry.state_dir(segment_id)
      File.mkdir_p!(dir)

      final_path = Registry.snapshot_path(segment_id, seq)
      tmp_path = final_path <> ".tmp"

      crc = :erlang.crc32(<<header::binary, snapshot_bytes::binary>>)
      bytes = <<header::binary, snapshot_bytes::binary, crc::32-big>>

      with {:ok, dst} <- :file.open(tmp_path, [:raw, :write, :binary]),
           :ok <- :file.write(dst, bytes),
           :ok <- :file.sync(dst),
           :ok <- :file.close(dst),
           :ok <- File.rename(tmp_path, final_path) do
        case Supervisor.lookup(segment_id) do
          {:ok, pid} -> WalWriter.truncate_up_to(pid, seq)
          :error -> :ok
        end

        metrics = Metrics.for_segment(segment_id)
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

        if :erlang.crc32(<<header::binary, bytes::binary>>) == crc do
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
  Highest-seq snapshot for the Segment, or `nil` if none exist.
  """
  @spec latest_snapshot(Registry.segment_id()) :: {non_neg_integer(), binary()} | nil
  def latest_snapshot(segment_id) do
    case Registry.snapshot_paths(segment_id) do
      [] -> nil
      paths -> List.last(paths)
    end
  end

  @doc """
  Remove a single snapshot file by seq. No-op if absent.
  """
  @spec delete_snapshot(Registry.segment_id(), non_neg_integer()) :: :ok
  def delete_snapshot(segment_id, seq) do
    _ = File.rm(Registry.snapshot_path(segment_id, seq))
    :ok
  end
end

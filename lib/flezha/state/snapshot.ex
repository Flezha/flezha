defmodule Flezha.State.Snapshot do
  @moduledoc """
  Snapshot file lifecycle: write `.tmp`, fsync, atomically rename, then truncate
  the WAL up to the snapshot's seq. The on-disk format is in
  `docs/internal/state_persistence.md`.

  The sequence has three failure windows, and no boot-time reconciliation step
  is required for any of them:

    1. **Crash before rename** — leaves an orphan `.tmp`. `Paths.scan_dir/1`
       ignores `.tmp` files; the orphan is overwritten on the next compaction.
    2. **Crash between rename and WAL-truncate** — the snapshot is durable but
       the WAL still holds records it covers. Restore applies the snapshot then
       folds the redundant records, and because `apply_change` is required to be
       idempotent over `(seq, change)` the result is identical.
    3. **Crash mid-WAL-truncate** — handled by `WalWriter.truncate_up_to/2`'s
       own `cell.wal.rewriting` -> rename sequence.
  """

  require Logger

  alias Flezha.State.Frame
  alias Flezha.State.{Metrics, Paths, WalWriter, Watchdog}
  alias Flezha.State.WalWriter.Epoch

  @header_size Frame.header_size()

  @doc """
  Persist a snapshot for `cell_id`, the rename durable before it answers.
  Returns the absolute path the snapshot was written to.
  """
  @spec write_snapshot(Paths.cell_id(), non_neg_integer(), binary(), binary()) ::
          {:ok, binary()} | {:error, term()}
  def write_snapshot(cell_id, seq, header, snapshot_bytes)
      when is_integer(seq) and seq >= 0 and is_binary(header) and is_binary(snapshot_bytes) do
    if byte_size(header) != @header_size do
      {:error, {:bad_header_size, byte_size(header)}}
    else
      final_path = Paths.snapshot_path(cell_id, seq)

      crc = Frame.crc32(header, snapshot_bytes)
      bytes = <<header::binary, snapshot_bytes::binary, crc::32-big>>

      with :ok <- Paths.atomic_install(final_path, &:file.write(&1, bytes), :snapshot) do
        metrics = Metrics.for_cell(cell_id)
        Metrics.put(metrics, :snapshot_seq, seq)

        {:ok, final_path}
      end
    end
  end

  @doc """
  Whether `runner_id`'s snapshot is installed, from its writer's loop: only the
  owner's, or with no owner (a rewind) one the log reaches, so no stale instance's.
  """
  @spec install(WalWriter.t(), binary(), non_neg_integer(), {binary(), binary()}) :: boolean()
  def install(%{cell_id: cell_id} = state, runner_id, seq, {header, body}) do
    with true <-
           Epoch.owner?(state, runner_id) or
             (state.owning_runner == nil and seq <= state.last_persisted_seq),
         {:ok, _path} <- write_snapshot(cell_id, seq, header, body) do
      Watchdog.compaction_completed(cell_id)
      true
    else
      false ->
        false

      {:error, reason} ->
        Logger.warning("[wal] cell=#{inspect(cell_id)}: snapshot refused: #{inspect(reason)}")
        false
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

        if Frame.crc32(header, bytes) == crc do
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
  Highest-seq snapshot for the Cell, or `nil` if none exist.
  """
  @spec latest_snapshot(Paths.cell_id()) :: {non_neg_integer(), binary()} | nil
  def latest_snapshot(cell_id) do
    case Paths.snapshot_paths(cell_id) do
      [] -> nil
      paths -> List.last(paths)
    end
  end

  @doc """
  Remove a single snapshot file by seq. No-op if absent.
  """
  @spec delete_snapshot(Paths.cell_id(), non_neg_integer()) :: :ok
  def delete_snapshot(cell_id, seq) do
    _ = File.rm(Paths.snapshot_path(cell_id, seq))
    :ok
  end
end

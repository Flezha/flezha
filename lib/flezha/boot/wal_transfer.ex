defmodule Flezha.Boot.WalTransfer do
  @moduledoc """
  Cross-controller WAL/snapshot transfer for the auto-restore-on-boot
  capability.

  Two operations:

    * `query/1` — given a Partition triple, return the local
      `last_persisted_seq` (or `:none` if no on-disk record). Used by a
      peer that finds an empty local payload during
      `dispatch_restore_init_state/2` and needs to discover which peer
      currently holds the WAL for a relay placed elsewhere by ProcessHub.

    * `stream/2` — chunked Erlang-message file streaming with
      backpressure. Files are written into `.transferring` siblings in
      the destination's flat state layout, fsynced, and atomically
      renamed on success. Failure deletes the `.transferring` artefact
      and falls through to cold start.

  This is one-shot per Partition per cluster lifecycle. Not ongoing
  replication.
  """

  alias Flezha.State.Registry
  alias Flezha.State.Restore

  require Logger

  @chunk_bytes 64 * 1024

  @type partition_id :: {binary(), binary(), binary()}

  @type query_result ::
          :none
          | {:ok,
             %{
               last_persisted_seq: non_neg_integer(),
               wal_path: binary() | nil,
               snapshot_paths: [{non_neg_integer(), binary()}]
             }}

  @doc """
  Local query: do we hold any state for this Partition?
  """
  @spec query(partition_id()) :: query_result()
  def query({_rc, _sh, _pk} = pid) do
    %{wal_files: wals, snapshot_files: snaps, outbox_files: _} = scan(pid)

    wal_path = Enum.find_value(wals, fn {_, path} -> path end)
    snapshot_paths = Enum.map(snaps, fn {_k, seq, path} -> {seq, path} end)

    case Restore.has_persisted_state?(pid) do
      false ->
        :none

      true ->
        last_seq =
          case Restore.assemble_payload(pid) do
            {:ok, %{terminal_seq_hint: seq}} -> seq
            _ -> 0
          end

        {:ok, %{last_persisted_seq: last_seq, wal_path: wal_path, snapshot_paths: snapshot_paths}}
    end
  end

  defp scan({rc, sh, pk}) do
    full = Registry.scan_dir({rc, sh, ""})

    %{
      wal_files: Enum.filter(full.wal_files, fn {k, _} -> k == pk end),
      snapshot_files: Enum.filter(full.snapshot_files, fn {k, _, _} -> k == pk end),
      outbox_files: Enum.filter(full.outbox_files, fn {k, _} -> k == pk end)
    }
  end

  @doc """
  Stream a single file from a peer in chunks. Each chunk is
  `{:wal_chunk, ref, offset, bytes}`; the final marker is
  `{:wal_done, ref}`. Writes to `<dest_path>.transferring`, fsyncs,
  then atomically renames.

  Returns `:ok` on success, `{:error, reason}` otherwise. On error the
  `.transferring` file is removed.
  """
  @spec stream(binary(), binary()) :: :ok | {:error, term()}
  def stream(source_path, dest_path) do
    transferring = dest_path <> ".transferring"
    File.mkdir_p!(Path.dirname(dest_path))

    with {:ok, src} <- :file.open(source_path, [:raw, :read, :binary]),
         {:ok, dst} <- :file.open(transferring, [:raw, :write, :binary]) do
      try do
        do_stream(src, dst, 0)
      after
        :file.close(src)
      end
      |> finalize(dst, transferring, dest_path)
    else
      {:error, reason} ->
        _ = File.rm(transferring)
        {:error, reason}
    end
  end

  defp do_stream(src, dst, offset) do
    case :file.read(src, @chunk_bytes) do
      :eof ->
        :ok

      {:ok, data} ->
        case :file.write(dst, data) do
          :ok -> do_stream(src, dst, offset + byte_size(data))
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp finalize(:ok, dst, transferring, dest_path) do
    case :file.datasync(dst) do
      :ok ->
        :file.close(dst)

        case File.rename(transferring, dest_path) do
          :ok ->
            emit_telemetry(:wal_pulled, %{path: dest_path})
            :ok

          {:error, reason} ->
            _ = File.rm(transferring)
            emit_telemetry(:wal_pull_failed, %{path: dest_path, reason: reason})
            {:error, reason}
        end

      {:error, reason} ->
        _ = :file.close(dst)
        _ = File.rm(transferring)
        emit_telemetry(:wal_pull_failed, %{path: dest_path, reason: reason})
        {:error, reason}
    end
  end

  defp finalize({:error, reason}, dst, transferring, dest_path) do
    _ = :file.close(dst)
    _ = File.rm(transferring)
    emit_telemetry(:wal_pull_failed, %{path: dest_path, reason: reason})
    {:error, reason}
  end

  defp emit_telemetry(event, metadata) do
    :telemetry.execute([:flezha, :boot, event], %{count: 1}, metadata)
  end
end

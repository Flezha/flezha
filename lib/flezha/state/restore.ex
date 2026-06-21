defmodule Flezha.State.Restore do
  @moduledoc """
  Assembles the byte payload shipped with `RestoreInitState`.

  The controller reads:
    * The latest snapshot (if any) — supplies `snapshot_bytes`.
    * The tail-WAL records strictly newer than the snapshot's `seq` —
      supplies `change_log_bytes` (records concatenated in their on-disk
      `[seq][len][header][bytes][crc32]` framing).

  When no snapshot exists, `snapshot_bytes = <<>>` and `change_log_bytes`
  contains the entire WAL. When neither snapshot nor WAL exist (true
  cold start), `terminal_seq_hint = 0` and both bodies are empty.
  """

  alias Flezha.State.{Registry, Snapshot}

  @header_size 16

  @type payload :: %{
          snapshot_bytes: binary(),
          change_log_bytes: binary(),
          terminal_seq_hint: non_neg_integer()
        }

  @spec assemble_payload(Registry.partition_id()) :: {:ok, payload()}
  def assemble_payload(partition_id) do
    # `-1` (not `0`) is the "no snapshot" sentinel: the WAL is 0-based, so a
    # `0` sentinel with the `seq > snapshot_seq` tail filter would silently
    # drop the very first change record. `-1` includes seq 0.
    {snapshot_bytes, snapshot_seq} =
      case Snapshot.latest_snapshot(partition_id) do
        nil ->
          {<<>>, -1}

        {seq, path} ->
          case Snapshot.read_snapshot(path) do
            {:ok, _header, bytes} -> {bytes, seq}
            {:error, _} -> {<<>>, -1}
          end
      end

    {change_log_bytes, terminal_seq} =
      read_tail_wal(partition_id, snapshot_seq)

    terminal_seq_hint = max(snapshot_seq, terminal_seq)

    {:ok,
     %{
       snapshot_bytes: snapshot_bytes,
       change_log_bytes: change_log_bytes,
       terminal_seq_hint: terminal_seq_hint
     }}
  end

  @doc """
  Whether the Partition has any persisted state to restore.
  """
  @spec has_persisted_state?(Registry.partition_id()) :: boolean()
  def has_persisted_state?(partition_id) do
    Snapshot.latest_snapshot(partition_id) != nil or
      File.exists?(Registry.wal_path(partition_id))
  end

  # Read the WAL once and parse records in memory; a per-record `:file.pread`
  # churns small transient refc binaries that fragment the binary allocator.
  defp read_tail_wal(partition_id, snapshot_seq) do
    case :file.read_file(Registry.wal_path(partition_id)) do
      {:ok, data} ->
        {iodata, terminal_seq} = gather_records(data, snapshot_seq, [], snapshot_seq)
        {IO.iodata_to_binary(iodata), terminal_seq}

      {:error, _} ->
        {<<>>, snapshot_seq}
    end
  end

  defp gather_records(<<seq::64-big, len::32-big, tail::binary>> = data, snapshot_seq, acc, term_seq)
       when len >= @header_size and byte_size(tail) >= len + 4 do
    <<header::binary-size(@header_size), change::binary-size(len - @header_size), crc::32-big,
      _::binary>> = tail

    if :erlang.crc32(<<header::binary, change::binary>>) == crc do
      # Keep the on-disk bytes verbatim rather than re-framing them.
      <<record::binary-size(12 + len + 4), rest::binary>> = data
      acc = if seq > snapshot_seq, do: [record | acc], else: acc
      gather_records(rest, snapshot_seq, acc, max(term_seq, seq))
    else
      # Stop at the first corrupt record, mirroring the device-scan contract.
      {Enum.reverse(acc), term_seq}
    end
  end

  defp gather_records(_remaining, _snapshot_seq, acc, term_seq) do
    {Enum.reverse(acc), term_seq}
  end
end

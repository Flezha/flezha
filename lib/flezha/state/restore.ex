defmodule Flezha.State.Restore do
  @moduledoc """
  Assembles the byte payload shipped with `RestoreInitState`.

  The agent reads:
    * The latest snapshot (if any) — supplies `snapshot_bytes`.
    * The tail-WAL records strictly newer than the snapshot's `seq` —
      supplies `change_log_bytes` (records concatenated in their on-disk
      `[seq][len][header][bytes][crc32]` framing).

  When no snapshot exists, `snapshot_bytes = <<>>` and `change_log_bytes`
  contains the entire WAL. When neither snapshot nor WAL exist (true
  cold start), `terminal_seq_hint = 0` and both bodies are empty.
  """

  alias Flezha.State.{Registry, Snapshot, WalWriter}

  @type payload :: %{
          snapshot_bytes: binary(),
          change_log_bytes: binary(),
          terminal_seq_hint: non_neg_integer()
        }

  @spec assemble_payload(Registry.partition_id()) :: {:ok, payload()}
  def assemble_payload(partition_id) do
    {snapshot_bytes, snapshot_seq} =
      case Snapshot.latest_snapshot(partition_id) do
        nil ->
          {<<>>, 0}

        {seq, path} ->
          case Snapshot.read_snapshot(path) do
            {:ok, _header, bytes} -> {bytes, seq}
            {:error, _} -> {<<>>, 0}
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

  defp read_tail_wal(partition_id, snapshot_seq) do
    path = Registry.wal_path(partition_id)

    if File.exists?(path) do
      {:ok, dev} = :file.open(path, [:raw, :read, :binary])

      try do
        gather_records(dev, 0, snapshot_seq, [], snapshot_seq)
      after
        :file.close(dev)
      end
    else
      {<<>>, snapshot_seq}
    end
  end

  defp gather_records(dev, position, snapshot_seq, acc, terminal_seq) do
    case WalWriter.decode_record(dev, position) do
      {:ok, seq, header, bytes, consumed} ->
        new_acc =
          if seq > snapshot_seq do
            [WalWriter.encode_record(seq, header, bytes) | acc]
          else
            acc
          end

        gather_records(dev, position + consumed, snapshot_seq, new_acc, max(terminal_seq, seq))

      {:error, _} ->
        {acc |> Enum.reverse() |> IO.iodata_to_binary(), terminal_seq}
    end
  end
end

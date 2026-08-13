defmodule Flezha.State.RecordLog do
  @moduledoc """
  Per-instance append-only record log on the Entity persistence substrate.

  The shared core behind a workflow instance's two durable logs — its
  `proc.journal` (`Flezha.State.ProcJournal`) and its `inbox.wal`
  (`Flezha.State.ProcInbox`). Both are WAL-framed record streams written through
  the existing `WalWriter` + `FsyncBatcher` path under a reserved
  `Flezha.State.Registry` identity, and both need exactly two procedures from
  that substrate: a **durable append** (assign the next seq, write, fsync before
  the caller proceeds) and a **torn-tail-tolerant read**. Those live here, so
  neither log owns a copy of the substrate protocol and the next per-instance
  log gets both for free.

  A log is not an Entity: its reserved identity carries no effect records and no
  dedup side-file (`WalWriter` skips it), and `Registry.scan_dir/1` never
  surfaces it.
  """

  alias Flezha.State.{Supervisor, WalWriter}

  @typedoc """
  Turns one raw record into an entry. `{:error, _}` skips the record, which is
  what keeps a log written by a newer build readable by an older one.
  """
  @type decoder :: (non_neg_integer(), binary(), binary() -> {:ok, term()} | {:error, term()})

  @doc """
  Append one record and make it durable (fsync) before returning, answering the
  seq it was assigned. The seq comes from the log's own tail, so callers never
  track it.
  """
  @spec append(Registry.entity_id(), binary(), binary()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def append(log_id, header, body) when is_binary(header) and is_binary(body) do
    with {:ok, pid} <- Supervisor.find_or_start(log_id),
         seq = WalWriter.last_persisted_seq(pid) + 1,
         :ok <- WalWriter.append(pid, seq, header, body),
         :ok <- WalWriter.fsync(pid) do
      {:ok, seq}
    else
      {:error, _} = err -> err
      other -> {:error, {:record_append_failed, other}}
    end
  end

  @doc """
  Decode every complete record through `decode`, in append order. `[]` when the
  log does not exist. Reads the file directly (no writer needed) and stops at
  the first invalid record — the WAL torn-tail contract.
  """
  @spec read(binary(), decoder()) :: [term()]
  def read(path, decode) when is_binary(path) and is_function(decode, 3) do
    case :file.open(path, [:raw, :read, :binary]) do
      {:ok, device} ->
        try do
          fold(device, decode, [])
        after
          :file.close(device)
        end

      {:error, _} ->
        []
    end
  end

  defp fold(device, decode, acc) do
    device
    |> WalWriter.fold_records(acc, fn {seq, header, body}, acc ->
      case decode.(seq, header, body) do
        {:ok, entry} -> {:cont, [entry | acc]}
        {:error, _} -> {:cont, acc}
      end
    end)
    |> Enum.reverse()
  end
end

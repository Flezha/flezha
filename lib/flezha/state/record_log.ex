defmodule Flezha.State.RecordLog do
  @moduledoc """
  Per-instance append-only record log on the Cell persistence substrate.

  The shared core behind a workflow instance's two durable logs — its
  `task.journal` (`Flezha.State.TaskJournal`) and its `inbox.wal`
  (`Flezha.State.TaskInbox`). Both are WAL-framed record streams written through
  the existing `WalWriter` + `FsyncBatcher` path under a reserved
  `Flezha.State.Paths` identity, and both need exactly two tasklets from that
  substrate: a **durable append** (assign the next seq, write, answer once the
  record is synced) and a **torn-tail-tolerant read**. Those live here, so
  neither log owns a copy of the substrate protocol.

  A log is not a Cell: its reserved identity carries no effect records and no
  dedup side-file (`WalWriter` skips it), and `Paths.scan_dir/1` never surfaces
  it.
  """

  alias Flezha.State.{Paths, RecordFrame, Supervisor, WalWriter}

  @typedoc """
  Turns one raw record into an entry. `{:error, _}` skips the record, which is
  what keeps a log written by a newer build readable by an older one.
  """
  @type decoder :: (non_neg_integer(), binary(), binary() -> {:ok, term()} | {:error, term()})

  @doc """
  Append one record and answer the seq it was assigned once it is durable. The
  seq comes from the log's own tail, so callers never track it. `opts` are
  `WalWriter.append_next/4`'s: `sync: :batched` answers when the batch holding
  the record has synced, `dedup: true` answers `{:duplicate, seq}` for a record
  whose header is already in the log.
  """
  @spec append(Paths.cell_id(), binary(), binary(), keyword()) ::
          {:ok, non_neg_integer()} | {:duplicate, non_neg_integer()} | {:error, term()}
  def append(log_id, header, body, opts \\ []) when is_binary(header) and is_binary(body) do
    with {:ok, pid} <- Supervisor.find_or_start(log_id),
         {:ok, seq} <- WalWriter.append_next(pid, header, body, opts) do
      {:ok, seq}
    else
      {:duplicate, _seq} = duplicate -> duplicate
      {:error, _} = err -> err
      other -> {:error, {:record_append_failed, other}}
    end
  end

  @doc """
  `append/4` without waiting: issued with `:gen_server.send_request/4` under
  `label` into the caller's request collection, whose answer — the same reply
  as `append/4`, or the writer's exit — the caller collects with
  `:gen_server.check_response/3`. With `sync: :batched` the answer arrives when
  the record's batch has synced, never before.
  """
  @spec append_async(
          Paths.cell_id(),
          binary(),
          binary(),
          keyword(),
          :gen_server.request_id_collection(),
          term()
        ) :: :gen_server.request_id_collection()
  def append_async(log_id, header, body, opts, reqids, label)
      when is_binary(header) and is_binary(body) do
    {:ok, pid} = Supervisor.find_or_start(log_id)
    WalWriter.append_next_async(pid, header, body, opts, reqids, label)
  end

  @doc """
  Decode every complete record through `decode`, in append order. `[]` when the
  log does not exist. Reads the file directly (no writer needed) and stops at a
  torn tail — the WAL contract. A record that fails its crc with complete
  records behind it raises rather than reading as the end of the log.
  """
  @spec read(binary(), decoder()) :: [term()]
  def read(path, decode) when is_binary(path) and is_function(decode, 3) do
    path
    |> RecordFrame.fold_path!([], fn {seq, header, body}, acc ->
      case decode.(seq, header, body) do
        {:ok, entry} -> {:cont, [entry | acc]}
        {:error, _} -> {:cont, acc}
      end
    end)
    |> Enum.reverse()
  end
end

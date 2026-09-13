defmodule Flezha.Plugin.Provider.File.WriteWorker do
  @moduledoc """
  The writing half of `Flezha.Plugin.Provider.File.Sink`: one process per
  enabled plugin, holding the file open for append and a bounded queue of
  records waiting for the next batch.

  A batch is one write of every record's bytes and a newline, then a data sync,
  and only then the outbox confirmation for the holds it carries. A failed write
  is cut back to the byte the batch began at and retried under the backoff
  ladder, so nothing is dropped and the retry never appends after half a line.
  Callers park past `queue_max`.

  A file this worker creates is made `0600`, one that already exists keeps its
  mode, and a path that is a symbolic link when it is opened is refused.
  """

  alias Flezha.Config.Backoff
  alias Flezha.Dlq.Entry, as: DlqEntry
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox

  use GenServer
  use Flezha.Process.Timer, keys: [:linger, :retry]

  require Logger

  # The prefix is derived from the provider's id; nothing here names a root.
  @provider "file_sink"

  defstruct [
    :plugin_id,
    :path,
    :operative_id,
    :linger_ms,
    :max_batch,
    :queue_max,
    :retry_backoff_ms,
    :retry_max_backoff_ms,
    :fd,
    :inode,
    :undo,
    :open_error,
    :last_error,
    :last_flush_ms,
    timers: %{},
    buffer: :queue.new(),
    buffer_len: 0,
    waiters: [],
    consecutive_failures: 0
  ]

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)

  @doc "Started by the extension table at activation, from the declared spec."
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts) when is_map(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Map.fetch!(opts, :name))

  @doc """
  Enqueue one record. Replies `:ok` at once while the queue is under
  `:queue_max`; past it the caller parks until a flush frees space.
  """
  @spec enqueue(GenServer.server(), binary(), map()) :: :ok
  def enqueue(server, msg, metadata) when is_binary(msg) and is_map(metadata),
    do: GenServer.call(server, {:enqueue, msg, metadata}, :infinity)

  @doc "Synchronous counters read (plugin health contract)."
  @spec stats(GenServer.server()) :: map()
  def stats(server), do: GenServer.call(server, :stats, 5_000)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    fields =
      opts
      |> Map.take([
        :plugin_id,
        :path,
        :linger_ms,
        :max_batch,
        :queue_max,
        :retry_backoff_ms,
        :retry_max_backoff_ms
      ])
      # Seeded at start: a sink that has never synced is measured from its own
      # birth, so a failing first write is judged by the same window.
      |> Map.merge(%{operative_id: opts[:source_operative_id], last_flush_ms: now()})

    {:ok, struct(__MODULE__, fields), {:continue, :open}}
  end

  # Opened before any record arrives, so a file that cannot be opened is a
  # verdict at once rather than at the first write.
  @impl true
  def handle_continue(:open, state) do
    case ensure_open(state) do
      {:ok, state} -> {:noreply, state}
      {:error, _reason, state} -> {:noreply, state}
    end
  end

  @impl true
  def handle_call({:enqueue, msg, metadata}, from, state) do
    if :binary.match(msg, "\n") == :nomatch do
      state |> buffer(msg, metadata) |> ProviderBehaviour.park_or_reply(from)
    else
      park(state, msg, metadata)
      {:reply, :ok, state}
    end
  end

  def handle_call(:stats, _from, state) do
    {:reply,
     %{
       buffer_len: state.buffer_len,
       consecutive_failures: state.consecutive_failures,
       last_error: state.last_error,
       last_flush_age_ms: now() - state.last_flush_ms,
       open_error: state.open_error
     }, state}
  end

  @impl true
  def handle_info(:linger, state), do: {:noreply, state |> timer_fired(:linger) |> flush()}
  def handle_info(:retry, state), do: {:noreply, state |> timer_fired(:retry) |> flush()}
  def handle_info(_msg, state), do: {:noreply, state}

  # A graceful stop writes what it holds, so a disable does not leave records
  # the outbox handed over waiting for a re-drive.
  @impl true
  def terminate(_reason, state) do
    state = if state.buffer_len > 0, do: flush(state), else: state
    close(state)
    :ok
  end

  # ---------- the queue ----------

  defp buffer(state, msg, metadata) do
    state = %{
      state
      | buffer: :queue.in({msg, metadata}, state.buffer),
        buffer_len: state.buffer_len + 1
    }

    cond do
      state.buffer_len >= state.max_batch and not timer_armed?(state, :retry) ->
        state |> cancel_timer(:linger) |> flush()

      not timer_armed?(state, :linger) and not timer_armed?(state, :retry) ->
        arm_timer(state, :linger, state.linger_ms, :linger)

      true ->
        state
    end
  end

  # A record holding a newline cannot be one line, and the sink writes bytes
  # rather than editing them, so it is parked whole. A delivery-guaranteed
  # record's hold is answered once the park is durable, which is what moves its
  # cursor past it.
  defp park(state, msg, metadata) do
    DlqEntry.new!(%{
      operative_class: state.operative_id || @provider,
      procedure_id: state.plugin_id,
      entity_key: Map.get(metadata, "flz.entity") || state.plugin_id,
      channel: Map.get(metadata, "channel", ""),
      error: "embedded_newline: a record holding a newline cannot be one line of #{state.path}",
      attempts: 1,
      payload: msg
    })
    |> DlqStore.append_async(fn _entry_ref -> Outbox.confirm(metadata) end)

    Logger.warning(
      "[Plugin.Provider.File.Sink] parked a record holding a newline path=#{state.path} " <>
        "bytes=#{byte_size(msg)}"
    )

    emit(:refused, %{bytes: byte_size(msg)}, Map.put(labels(state), :reason, :embedded_newline))
  end

  # ---------- flush ----------

  defp flush(%{buffer_len: 0} = state), do: state

  defp flush(state) do
    {taken, rest} = :queue.split(min(state.buffer_len, state.max_batch), state.buffer)
    records = :queue.to_list(taken)

    case write(state, Enum.map(records, fn {msg, _metadata} -> [msg, ?\n] end)) do
      {:ok, state} ->
        # The sync is the confirmation: every guaranteed record in the batch
        # answers its hold in one report, and only now may its cursor advance.
        Outbox.confirm_many(Enum.map(records, &elem(&1, 1)))
        emit(:shipped, %{count: length(records)}, labels(state))

        %{
          state
          | buffer: rest,
            buffer_len: state.buffer_len - length(records),
            consecutive_failures: Backoff.reset(:file_flush, state.plugin_id),
            last_error: nil,
            last_flush_ms: now()
        }
        |> ProviderBehaviour.release_waiters()
        |> flush()

      {:error, reason, state} ->
        retry(state, reason)
    end
  end

  defp retry(state, reason) do
    failures = state.consecutive_failures + 1

    Logger.warning(
      "[Plugin.Provider.File.Sink] write failed path=#{state.path} attempt=#{failures} " <>
        "pending=#{state.buffer_len}: #{inspect(reason)}"
    )

    emit(:failed, %{consecutive_failures: failures}, Map.put(labels(state), :reason, reason))

    delay =
      Backoff.arm(:file_flush, failures, state.plugin_id,
        base: state.retry_backoff_ms,
        cap: state.retry_max_backoff_ms
      )

    arm_timer(
      %{state | consecutive_failures: failures, last_error: reason},
      :retry,
      delay,
      :retry
    )
  end

  defp write(state, iodata) do
    with {:ok, state} <- ensure_open(state), do: append(state, iodata)
  end

  defp append(state, iodata) do
    case :file.position(state.fd, :eof) do
      {:ok, mark} ->
        with :ok <- :file.write(state.fd, iodata),
             :ok <- :file.datasync(state.fd) do
          {:ok, state}
        else
          {:error, reason} -> {:error, reason, undo(state, mark)}
        end

      {:error, reason} ->
        {:error, reason, close(state)}
    end
  end

  # Cut the file back to where the batch began: on its own descriptor when that
  # still works, else on the next open of the same file.
  defp undo(state, mark) do
    case cut(state.fd, mark) do
      :ok -> close(state)
      _failed -> %{close(state) | undo: {state.inode, mark}}
    end
  end

  # Only ever shorter: truncating to an offset past the end would pad the file.
  defp cut(fd, mark) do
    case :file.position(fd, :eof) do
      {:ok, eof} when eof <= mark -> :ok
      {:ok, _eof} -> with {:ok, _mark} <- :file.position(fd, mark), do: :file.truncate(fd)
      {:error, reason} -> {:error, reason}
    end
  end

  # ---------- the file ----------

  # The path still names the file this worker holds, or the file is opened
  # again: a file moved or deleted under an open descriptor would take every
  # later write somewhere no reader of the path looks.
  defp ensure_open(%{fd: fd} = state) when fd != nil do
    case File.lstat(state.path) do
      {:ok, %File.Stat{type: :regular, inode: inode}} when inode == state.inode -> {:ok, state}
      _moved -> state |> close() |> ensure_open()
    end
  end

  defp ensure_open(state) do
    with {:ok, fd} <- open_file(state.path),
         {:ok, state} <- opened(%{state | fd: fd}) do
      {:ok, %{state | open_error: nil}}
    else
      {:error, reason} -> refuse_open(close(state), reason)
    end
  end

  defp open_file(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} -> :file.open(path, [:append, :raw, :binary])
      {:ok, %File.Stat{type: :symlink}} -> {:error, :symlink}
      {:ok, %File.Stat{type: type}} -> {:error, {:not_a_regular_file, type}}
      {:error, :enoent} -> create(path)
      {:error, reason} -> {:error, reason}
    end
  end

  # Erlang's open takes no creation mode, so a created file is made private
  # straight after. `:exclusive` is what keeps that from ever narrowing a file
  # somebody else created in between.
  defp create(path) do
    with {:ok, fd} <- :file.open(path, [:append, :exclusive, :raw, :binary]) do
      case :file.change_mode(path, 0o600) do
        :ok ->
          {:ok, fd}

        {:error, reason} ->
          _ = :file.close(fd)
          {:error, {:chmod, reason}}
      end
    end
  end

  # A file replaced between the open and this look is caught by the next batch's
  # `ensure_open/1`, which compares the path's inode with this one.
  defp opened(state) do
    with {:ok, %File.Stat{inode: inode}} <- File.lstat(state.path) do
      finish_undo(%{state | inode: inode})
    end
  end

  # A cut its own descriptor could not make is made here, on the same file. A
  # different file is not that batch's to cut.
  defp finish_undo(%{undo: {inode, mark}, inode: inode} = state) do
    with :ok <- cut(state.fd, mark), do: {:ok, %{state | undo: nil}}
  end

  defp finish_undo(state), do: {:ok, %{state | undo: nil}}

  defp refuse_open(state, reason) do
    error = {reason, state.path}

    if state.open_error != error,
      do:
        Logger.warning(
          "[Plugin.Provider.File.Sink] cannot open path=#{state.path}: #{inspect(reason)}"
        )

    {:error, {:open, reason}, %{state | open_error: error}}
  end

  defp close(%{fd: nil} = state), do: state

  defp close(state) do
    _ = :file.close(state.fd)
    %{state | fd: nil}
  end

  defp labels(state), do: %{plugin_id: state.plugin_id, path: state.path}

  defp now(), do: :erlang.monotonic_time(:millisecond)
end

defmodule Flezha.Plugin.Provider.File.ReadWorker do
  @moduledoc """
  The reading half of `Flezha.Plugin.Provider.File.Source`: one process per
  enabled plugin, holding the descriptor, the byte offset where the last
  delivered line ended, and the bytes read past it.

  A line is injected through the confirming ingest and the offset moves only
  once the operative has taken it, so a refused line is offered again. A chunk
  is read only when no whole line is buffered, so the reader is never more than
  one chunk ahead of what the operative confirmed, and one chunk is handled per
  message, so the mailbox stays answerable behind a large file.

  Log lines, telemetry and verdicts name the path, an offset or a count, never
  the bytes of a line.
  """

  alias Flezha.Plugin.Host
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Process.Watch

  use GenServer
  use Flezha.Process.Timer, keys: [:poll]

  require Logger

  # The prefix is derived from the provider's id; nothing here names a root.
  @provider "file_source"

  @fields [
    :path,
    :entry_channel,
    :plugin_id,
    :source_plugin_id,
    :overwatcher_pid,
    :start_at,
    :poll_interval_ms,
    :max_line_bytes,
    :read_chunk_bytes,
    :commit_interval_ms
  ]

  defp emit(verb, measurements, metadata),
    do: ProviderBehaviour.emit(@provider, verb, measurements, metadata)

  @doc "Started by the extension table at activation, from the declared spec. Performs no I/O."
  @spec start_link(map()) :: GenServer.on_start()
  def start_link(opts) when is_map(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Map.fetch!(opts, :name))

  @doc """
  The facts the provider's `health/1` judges: the path, where the reader
  stopped if it did, whether the file is missing, the failure run and the time
  of the last settled poll. `:timeout` when the worker does not answer, which is
  also what a reader waiting out a parked operative's ingest looks like.
  """
  @spec probe(pid()) :: {:ok, map()} | :timeout
  def probe(pid), do: ProviderBehaviour.ask(pid, :health_probe)

  @impl true
  def init(opts) do
    now = now()

    state =
      opts
      |> Map.take(@fields)
      |> Map.merge(%{
        watch: Watch.watch(Watch.new(), Map.fetch!(opts, :overwatcher_pid), :host),
        fd: nil,
        inode: nil,
        # The committed offset the plugin was prepared with, or nil until the
        # first open places the reader by `start_at`.
        offset: opts[:offset],
        committed: opts[:offset],
        committed_at: now,
        buffer: "",
        missing: false,
        stopped: nil,
        consecutive_failures: 0,
        last_error: nil,
        last_settled_at: now,
        timers: %{}
      })

    # Scheduled, never run here: activation is synchronous in the host.
    {:ok, arm_timer(state, :poll, 0, :poll)}
  end

  @impl true
  def handle_info(:poll, state), do: {:noreply, state |> timer_fired(:poll) |> poll()}

  # The operative this reader feeds is gone.
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Watch.down(state.watch, ref) do
      {[:host], _watch} -> {:stop, :normal, close(state)}
      _stale -> {:noreply, state}
    end
  end

  def handle_info({:health_probe, from, ref}, state) do
    facts =
      Map.take(state, [
        :path,
        :stopped,
        :missing,
        :consecutive_failures,
        :last_error,
        :last_settled_at
      ])

    send(from, {ref, facts})
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # ---------- one poll ----------

  # A stopped reader reads nothing more: the line it stopped at is still the
  # next one, and a restart lands on it again.
  defp poll(%{stopped: stop} = state) when stop != nil, do: state

  defp poll(state) do
    {next, state} = step(state)

    %{state | last_settled_at: now()}
    |> commit_when_due(next)
    |> schedule(next)
  end

  defp schedule(state, :more), do: arm_timer(state, :poll, 0, :poll)
  defp schedule(state, :stopped), do: state

  defp schedule(state, _caught_up_or_waiting),
    do: arm_timer(state, :poll, state.poll_interval_ms, :poll)

  defp step(state) do
    case File.stat(state.path) do
      {:ok, %File.Stat{type: :regular} = stat} -> open_and_drain(%{state | missing: false}, stat)
      {:ok, %File.Stat{type: type}} -> {:waiting, failed(state, {:not_a_regular_file, type})}
      {:error, :enoent} -> {:waiting, missing(state)}
      {:error, reason} -> {:waiting, failed(state, {:stat, reason})}
    end
  end

  defp open_and_drain(state, stat) do
    state = rewind_if_moved(state, stat)

    case ensure_open(state, stat) do
      {:ok, state} -> drain(state)
      {:error, reason} -> {:waiting, failed(state, {:open, reason})}
    end
  end

  # ---------- the file ----------

  # Replaced by another file, or shorter than what was already delivered. An
  # inode is known only once this worker has opened the file, so a reader
  # starting from a committed offset judges by size alone, and a reader with no
  # offset yet has nothing to judge.
  defp rewind_if_moved(%{offset: nil} = state, _stat), do: state

  defp rewind_if_moved(state, stat) do
    if (state.inode != nil and stat.inode != state.inode) or stat.size < state.offset,
      do: state |> close() |> rewind(stat),
      else: state
  end

  defp ensure_open(%{fd: nil} = state, stat) do
    with {:ok, fd} <- :file.open(state.path, [:read, :raw, :binary]),
         do: {:ok, place(%{state | fd: fd}, stat)}
  end

  defp ensure_open(state, _stat), do: {:ok, state}

  # Where a freshly opened reader starts: its committed offset, else `start_at`.
  defp place(%{offset: nil} = state, stat),
    do: %{state | inode: stat.inode, offset: start_offset(state.start_at, stat)}

  defp place(state, stat), do: %{state | inode: stat.inode}

  defp start_offset("end", stat), do: stat.size
  defp start_offset("beginning", _stat), do: 0

  defp rewind(state, stat) do
    Logger.warning(
      "[Plugin.Provider.File.Source] truncated or replaced path=#{state.path} " <>
        "offset=#{state.offset}; reading it again from byte 0"
    )

    emit(:truncated, %{offset: state.offset}, labels(state))
    commit(%{state | offset: 0, buffer: "", inode: stat.inode})
  end

  defp missing(%{missing: true} = state), do: state

  # Not a truncation: the descriptor is closed, the offset and the inode kept,
  # and polling goes on until the file is back. A file that returns as another
  # inode is read from byte 0, which is how a rotation reaches the new file.
  defp missing(state) do
    Logger.warning(
      "[Plugin.Provider.File.Source] missing path=#{state.path} offset=#{state.offset}; " <>
        "polling until it appears"
    )

    %{close(state) | missing: true}
  end

  defp close(%{fd: nil} = state), do: state

  defp close(state) do
    _ = :file.close(state.fd)
    %{state | fd: nil}
  end

  # ---------- lines ----------

  # Deliver every whole line already read; with none left, read one chunk.
  defp drain(state) do
    case :binary.split(state.buffer, "\n") do
      [line, rest] -> deliver(state, line, rest)
      [fragment] -> read_chunk(state, fragment)
    end
  end

  defp deliver(state, line, rest) do
    cond do
      byte_size(line) > state.max_line_bytes ->
        {:stopped, stop(state, :line_too_long)}

      not String.valid?(line) ->
        {:stopped, stop(state, :invalid_utf8)}

      true ->
        case confirm(state, line) do
          :ok -> state |> delivered(line, rest) |> drain()
          {:error, reason} -> {:waiting, failed(state, {:ingest, reason})}
        end
    end
  end

  defp delivered(state, line, rest) do
    offset = state.offset + byte_size(line) + 1
    emit(:shipped, %{bytes: byte_size(line), offset: offset}, labels(state))

    %{
      state
      | offset: offset,
        buffer: rest,
        consecutive_failures: 0,
        last_error: nil,
        last_settled_at: now()
    }
  end

  # A fragment already past the cap is a line past the cap whatever follows it.
  defp read_chunk(state, fragment) when byte_size(fragment) > state.max_line_bytes,
    do: {:stopped, stop(state, :line_too_long)}

  defp read_chunk(state, fragment) do
    case :file.pread(state.fd, state.offset + byte_size(fragment), state.read_chunk_bytes) do
      {:ok, bytes} -> {:more, %{state | buffer: fragment <> bytes}}
      :eof -> {:caught_up, state}
      {:error, reason} -> {:waiting, failed(state, {:read, reason})}
    end
  end

  # A line the reader cannot deliver stops it where the line starts: skipping
  # would lose the record, and the committed offset never passes it.
  defp stop(state, reason) do
    Logger.warning(
      "[Plugin.Provider.File.Source] #{reason} path=#{state.path} offset=#{state.offset}; " <>
        "the reader stopped there and delivers nothing past it"
    )

    emit(:failed, %{offset: state.offset}, Map.put(labels(state), :reason, reason))
    %{state | stopped: {reason, state.offset}}
  end

  # The confirming ingest, waited out for as long as the operative takes: a park
  # at zero credit is the back-pressure the reader exists to feel, and a timeout
  # would re-offer a line the operative may still take. A host that dies ends
  # the call through the monitor every call holds.
  defp confirm(state, line) do
    Host.inject(
      state.overwatcher_pid,
      line,
      %{"channel" => state.entry_channel},
      :verdict,
      :infinity
    )
  end

  defp failed(state, reason) do
    failures = state.consecutive_failures + 1

    Logger.warning(
      "[Plugin.Provider.File.Source] read failed path=#{state.path} offset=#{state.offset} " <>
        "consecutive=#{failures} reason=#{inspect(reason)}"
    )

    emit(
      :failed,
      %{consecutive_failures: failures},
      Map.put(labels(state), :reason, reason)
    )

    %{state | consecutive_failures: failures, last_error: reason}
  end

  # ---------- the committed offset ----------

  # Once the reader has caught up, stopped or must wait, or once
  # `commit_interval_ms` has passed while it has not: one registry write per
  # batch of deliveries rather than per line.
  defp commit_when_due(%{offset: offset, committed: offset} = state, _next), do: state

  defp commit_when_due(state, :more) do
    if now() - state.committed_at >= state.commit_interval_ms, do: commit(state), else: state
  end

  defp commit_when_due(state, _next), do: commit(state)

  defp commit(state) do
    Host.persist_opt(
      state.overwatcher_pid,
      Map.get(state, :source_plugin_id),
      :offset,
      state.offset
    )

    %{state | committed: state.offset, committed_at: now()}
  end

  defp labels(state),
    do: %{plugin_id: state.plugin_id, path: state.path, channel: state.entry_channel}

  defp now(), do: :erlang.monotonic_time(:millisecond)
end

defmodule Flezha.Plugin.Provider.File.ReadWorker do
  @moduledoc """
  The reading half of `Flezha.Plugin.Provider.File.Source`: one process per
  enabled plugin, holding its handle on the target, the byte offset where the
  last delivered line ended, and the bytes read past it. The target is a file
  or an S3 object alike, read through `Flezha.Plugin.Provider.File.Storage`.

  A line is injected through the confirming ingest and the offset moves only
  once the operator has taken it, so a refused line is offered again. A chunk
  is read only when no whole line is buffered, so the reader is never more than
  one chunk ahead of what the operator confirmed, and one chunk is handled per
  message, so the mailbox stays answerable behind a large target.

  Log lines, telemetry and verdicts name the path, an offset or a count, never
  the bytes of a line.
  """

  alias Flezha.Plugin.Host
  alias Flezha.Plugin.Provider.File.Storage
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
  stopped if it did, whether the target is missing, the failure run and the
  time of the last settled poll. `:timeout` when the worker does not answer,
  which is also what a reader waiting out a parked operator's ingest looks
  like.
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
        storage: Storage.Config.of(opts.path),
        handle: nil,
        # The inode or the ETag: what tells a replaced target from the one read.
        identity: nil,
        # The target's size at the last look; nothing past it is read.
        size: 0,
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

  # The operator this reader feeds is gone.
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
    |> Host.commit_position(:offset, next)
    |> schedule(next)
  end

  defp schedule(state, :more), do: arm_timer(state, :poll, 0, :poll)
  defp schedule(state, :stopped), do: state

  defp schedule(state, _caught_up_or_waiting),
    do: arm_timer(state, :poll, state.poll_interval_ms, :poll)

  defp step(state) do
    case state.storage.stat(state.path) do
      {:ok, %{type: :regular} = stat} -> open_and_drain(%{state | missing: false}, stat)
      {:ok, %{type: type}} -> {:waiting, failed(state, {:not_a_regular_file, type})}
      {:error, :enoent} -> {:waiting, missing(state)}
      {:error, reason} -> {:waiting, failed(state, {:stat, reason})}
    end
  end

  defp open_and_drain(state, stat) do
    state = rewind_if_moved(%{state | size: stat.size}, stat)

    case ensure_open(state, stat) do
      {:ok, state} -> drain(state)
      {:error, reason} -> {:waiting, failed(state, {:open, reason})}
    end
  end

  # ---------- the target ----------

  # Replaced by another target, or shorter than what was already delivered. An
  # identity is known only once this worker has opened the target, so a reader
  # starting from a committed offset judges by size alone, and a reader with no
  # offset yet has nothing to judge.
  defp rewind_if_moved(%{offset: nil} = state, _stat), do: state

  defp rewind_if_moved(state, stat) do
    if (state.identity != nil and stat.identity != state.identity) or stat.size < state.offset,
      do: state |> close() |> rewind(stat.identity),
      else: state
  end

  defp ensure_open(%{handle: nil} = state, stat) do
    with {:ok, handle} <- state.storage.open_read(state.path, stat.identity),
         do: {:ok, place(%{state | handle: handle}, stat)}
  end

  defp ensure_open(state, _stat), do: {:ok, state}

  # Where a freshly opened reader starts: its committed offset, else `start_at`.
  defp place(%{offset: nil} = state, stat),
    do: %{state | identity: stat.identity, offset: start_offset(state.start_at, stat)}

  defp place(state, stat), do: %{state | identity: stat.identity}

  defp start_offset("end", stat), do: stat.size
  defp start_offset("beginning", _stat), do: 0

  # A rewind whose new identity is not known yet takes it from the next look.
  defp rewind(state, identity) do
    Logger.warning(
      "[Plugin.Provider.File.Source] truncated or replaced path=#{state.path} " <>
        "offset=#{state.offset}; reading it again from byte 0"
    )

    emit(:truncated, %{offset: state.offset}, labels(state))
    Host.commit_position(%{state | offset: 0, buffer: "", identity: identity}, :offset, :rewound)
  end

  defp missing(%{missing: true} = state), do: state

  # Not a truncation: the handle is closed, the offset and the identity kept,
  # and polling goes on until the target is back. One that returns as another
  # is read from byte 0, which is how a rotation reaches the new file.
  defp missing(state) do
    Logger.warning(
      "[Plugin.Provider.File.Source] missing path=#{state.path} offset=#{state.offset}; " <>
        "polling until it appears"
    )

    %{close(state) | missing: true}
  end

  defp close(%{handle: nil} = state), do: state

  defp close(state) do
    _ = state.storage.close(state.handle)
    %{state | handle: nil}
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
    from = state.offset + byte_size(fragment)

    case min(state.read_chunk_bytes, state.size - from) do
      bytes when bytes > 0 -> read_range(state, fragment, from, bytes)
      _nothing_new -> {:caught_up, state}
    end
  end

  # A target replaced under the read is read again from byte 0 at once, and one
  # gone since the look is missing.
  defp read_range(state, fragment, from, bytes) do
    case state.storage.pread(state.handle, from, bytes) do
      {:ok, data} -> {:more, %{state | buffer: fragment <> data}}
      :eof -> {:caught_up, state}
      {:error, :replaced} -> {:more, state |> close() |> rewind(nil)}
      {:error, :enoent} -> {:waiting, missing(state)}
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

  # The confirming ingest, waited out for as long as the operator takes: a park
  # at zero credit is the back-pressure the reader exists to feel, and a timeout
  # would re-offer a line the operator may still take. A host that dies ends
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

  defp labels(state),
    do: %{plugin_id: state.plugin_id, path: state.path, channel: state.entry_channel}

  defp now(), do: :erlang.monotonic_time(:millisecond)
end

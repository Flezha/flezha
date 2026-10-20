defmodule Flezha.State.FsyncBatcher do
  @moduledoc """
  Single shared `GenServer` that drives `:file.datasync/1` across every dirty
  `WalWriter` on a configurable cadence.

  Writers cast `mark_dirty/1` on the first append of an fsync window; the tick
  casts one `:fsync` to every dirty writer, so they sync in their own processes
  and a writer behind a deep mailbox delays no one else's durability. Default
  tick interval is `5 ms`, giving <=10 ms total durability when paired with the
  SDK flusher's 5 ms linger.

  The tick is armed exactly while something is dirty, so an idle batcher never
  wakes. A mark that finds it idle arms the tick for the next point of the
  rhythm the last tick set, one interval apart, so the tick falls due no later
  than a tick that had kept running would have.

  A (re)started batcher seeds its dirty set with every registered writer: a
  writer marks once per window, so a dirty set lost with a crashed batcher would
  otherwise never be rebuilt.

  Validates `:flezha, :agent_fsync_interval_ms` at `init/1`. Legal range `[1,
  1000]`; out-of-range values prevent the batcher from booting.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:tick]

  alias Flezha.Config.Durations
  alias Flezha.State.{WalWriter, WriterRegistry}

  # `last_tick_at`: monotonic ms the tick's rhythm counts from, the last tick or
  # the start.
  defstruct [:dirty_set, :tick_interval_ms, :last_tick_at, timers: %{}]

  # ---------- Public API ----------

  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # `server` defaults to the shared singleton; tests pass an isolated
  # instance to assert coalescing without the 5 ms background tick.
  @spec mark_dirty(tuple(), GenServer.server()) :: :ok
  def mark_dirty(cell_id, server \\ __MODULE__) do
    GenServer.cast(server, {:mark_dirty, cell_id})
  end

  @doc """
  Drop any pending dirty mark for `cell_id`. Called when a Cell
  is hard-deleted so the next tick does not try to fsync a writer that is
  being torn down.
  """
  @spec forget(tuple(), GenServer.server()) :: :ok
  def forget(cell_id, server \\ __MODULE__) do
    GenServer.cast(server, {:forget, cell_id})
  end

  @doc """
  Force an immediate tick, primarily for tests. Idempotent and safe
  under load (a writer it could not reach keeps the next scheduled tick).
  """
  @spec flush_now(GenServer.server()) :: :ok
  def flush_now(server \\ __MODULE__) do
    GenServer.call(server, :flush_now)
  end

  @doc """
  Synchronously flush every dirty `WalWriter`. Equivalent to
  `flush_now/0`; named separately so the node stop's flush row reads as
  what it is.
  """
  @spec flush_all(GenServer.server(), timeout()) :: :ok
  def flush_all(server \\ __MODULE__, timeout \\ 5_000) do
    GenServer.call(server, :flush_all, timeout)
  end

  @doc "The node stop's first finalise row: the writes synced before the hubs stop."
  def shutdown_steps,
    do: [%{scope: :node, order: 20, name: :fsync, run: &__MODULE__.flush_step/2}]

  @doc false
  def flush_step(_target, budget) do
    if Process.whereis(__MODULE__),
      do: flush_all(__MODULE__, min(budget, Durations.get(:shutdown_finalise_call_ms))),
      else: :ok
  end

  @spec dirty_set(GenServer.server()) :: MapSet.t()
  def dirty_set(server \\ __MODULE__), do: GenServer.call(server, :dirty_set)

  # ---------- GenServer ----------

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, configured_interval_ms())

    cond do
      not is_integer(interval) ->
        {:stop, {:invalid_interval, interval}}

      interval < Durations.get(:agent_fsync_min_interval_ms) or
          interval > Durations.get(:agent_fsync_max_interval_ms) ->
        {:stop,
         {:interval_out_of_range, interval,
          [
            Durations.get(:agent_fsync_min_interval_ms),
            Durations.get(:agent_fsync_max_interval_ms)
          ]}}

      true ->
        {:ok,
         schedule(%__MODULE__{
           dirty_set: MapSet.new(WriterRegistry.registered_ids()),
           tick_interval_ms: interval,
           last_tick_at: now_ms()
         })}
    end
  end

  @impl true
  def handle_cast({:mark_dirty, cell_id}, state) do
    {:noreply, schedule(%{state | dirty_set: MapSet.put(state.dirty_set, cell_id)})}
  end

  def handle_cast({:forget, cell_id}, state) do
    {:noreply, schedule(%{state | dirty_set: MapSet.delete(state.dirty_set, cell_id)})}
  end

  @impl true
  def handle_call(flush, _from, state) when flush in [:flush_now, :flush_all] do
    {:reply, :ok, state |> drain(:call) |> schedule()}
  end

  def handle_call(:dirty_set, _from, state) do
    {:reply, state.dirty_set, state}
  end

  @impl true
  def handle_info(:tick, state) do
    state = state |> timer_fired(:tick) |> drain(:cast)
    {:noreply, schedule(%{state | last_tick_at: now_ms()})}
  end

  # A mark on an armed batcher leaves the tick alone: re-arming cancels, so a
  # steady stream of marks would push the tick back for ever.
  defp schedule(state) do
    case {MapSet.size(state.dirty_set) == 0, timer_armed?(state, :tick)} do
      {false, false} -> arm_timer(state, :tick, until_next_tick(state), :tick)
      {true, true} -> cancel_timer(state, :tick)
      _settled -> state
    end
  end

  # The next point of the last tick's rhythm, never sooner than one interval
  # after that tick: a mark soon after a tick falls due where a re-armed tick
  # would have, and a mark after a quiet spell falls due at the end of the
  # current interval rather than a whole one later.
  defp until_next_tick(%{last_tick_at: last_tick, tick_interval_ms: interval}) do
    elapsed = now_ms() - last_tick
    max(interval - elapsed, Integer.mod(-elapsed, interval))
  end

  defp now_ms(), do: System.monotonic_time(:millisecond)

  # The tick casts; `flush_now`/`flush_all` call, because their callers need
  # every sync done when they return.
  defp drain(state, mode) do
    count = MapSet.size(state.dirty_set)

    # A writer that did not confirm its sync stays dirty for the next drain.
    still_dirty =
      Enum.filter(state.dirty_set, fn cell_id ->
        case WriterRegistry.lookup(cell_id) do
          {:ok, pid} -> fsync(pid, mode) == :unreachable
          :error -> false
        end
      end)

    # Only emit when work happened: a flush that found nothing carries no signal.
    if count > 0 do
      :telemetry.execute(
        [:flezha, :state, :fsync, :tick],
        %{count: count},
        %{}
      )
    end

    %{state | dirty_set: MapSet.new(still_dirty)}
  end

  # A cast reports nothing, so a tick can never observe a failed sync; a
  # call-mode drain is what re-marks one.
  defp fsync(pid, :cast) do
    :ok = WalWriter.fsync_async(pid)
    :synced
  end

  # `WalWriter.fsync/1` declares its refusal rather than exiting, so there is no
  # exit to catch and the answer says which of two things happened.
  defp fsync(pid, :call) do
    case WalWriter.fsync(pid) do
      :ok ->
        :synced

      # The writer could not be reached: gone, wedged or mid-restart, all of
      # which the next drain may find healthy.
      {:error, {:wal_writer_unavailable, _reason}} ->
        :unreachable

      # The writer refused its own file, and answers every write with that
      # refusal for the rest of its life. Re-marking it is a sync that can never
      # happen; the refusal is what leaves its relay unbound.
      {:error, {_reason, _last_seq, _offset}} ->
        :refused

      # A shape neither of the two above: keep the cell rather than drop a
      # sync on an answer nobody has read yet.
      _other ->
        :unreachable
    end
  end

  defp configured_interval_ms() do
    Durations.get(:agent_fsync_interval_ms)
  end
end

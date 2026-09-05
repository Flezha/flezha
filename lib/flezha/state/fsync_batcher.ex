defmodule Flezha.State.FsyncBatcher do
  @moduledoc """
  Single shared `GenServer` that drives `:file.datasync/1` across every dirty
  `WalWriter` on a configurable cadence.

  Writers cast `mark_dirty/1` on the first append of an fsync window; the tick
  casts one `:fsync` to every dirty writer, so they sync in their own processes
  and a writer behind a deep mailbox delays no one else's durability. Default
  tick interval is `5 ms` (~200 Hz), giving <=10 ms total durability when paired
  with the SDK flusher's 5 ms linger.

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

  defstruct [:dirty_set, :tick_interval_ms, timers: %{}]

  # ---------- Public API ----------

  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  # `server` defaults to the shared singleton; tests pass an isolated
  # instance to assert coalescing without the 5 ms background tick.
  @spec mark_dirty(tuple(), GenServer.server()) :: :ok
  def mark_dirty(entity_id, server \\ __MODULE__) do
    GenServer.cast(server, {:mark_dirty, entity_id})
  end

  @doc """
  Drop any pending dirty mark for `entity_id`. Called when an Entity
  is hard-deleted so the next tick does not try to fsync a writer that is
  being torn down.
  """
  @spec forget(tuple(), GenServer.server()) :: :ok
  def forget(entity_id, server \\ __MODULE__) do
    GenServer.cast(server, {:forget, entity_id})
  end

  @doc """
  Force an immediate tick, primarily for tests. Idempotent and safe
  under load (the next scheduled tick is preserved).
  """
  @spec flush_now(GenServer.server()) :: :ok
  def flush_now(server \\ __MODULE__) do
    GenServer.call(server, :flush_now)
  end

  @doc """
  Synchronously flush every dirty `WalWriter`. Equivalent to
  `flush_now/0`; named separately so the graceful-shutdown
  coordinator's intent is unambiguous at the call site.
  """
  @spec flush_all(GenServer.server()) :: :ok
  def flush_all(server \\ __MODULE__) do
    GenServer.call(server, :flush_all)
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
         arm_tick(%__MODULE__{
           dirty_set: MapSet.new(WriterRegistry.registered_ids()),
           tick_interval_ms: interval
         })}
    end
  end

  @impl true
  def handle_cast({:mark_dirty, entity_id}, state) do
    {:noreply, %{state | dirty_set: MapSet.put(state.dirty_set, entity_id)}}
  end

  def handle_cast({:forget, entity_id}, state) do
    {:noreply, %{state | dirty_set: MapSet.delete(state.dirty_set, entity_id)}}
  end

  @impl true
  def handle_call(:flush_now, _from, state) do
    new_state = drain(state, :call)
    {:reply, :ok, new_state}
  end

  def handle_call(:flush_all, _from, state) do
    new_state = drain(state, :call)
    {:reply, :ok, new_state}
  end

  def handle_call(:dirty_set, _from, state) do
    {:reply, state.dirty_set, state}
  end

  @impl true
  def handle_info(:tick, state) do
    {:noreply, state |> drain(:cast) |> arm_tick()}
  end

  defp arm_tick(state), do: arm_timer(state, :tick, state.tick_interval_ms, :tick)

  # The tick casts; `flush_now`/`flush_all` call, because their callers need
  # every sync done when they return.
  defp drain(state, mode) do
    count = MapSet.size(state.dirty_set)

    # A writer that did not confirm its sync stays dirty for the next drain.
    still_dirty =
      Enum.filter(state.dirty_set, fn entity_id ->
        case WriterRegistry.lookup(entity_id) do
          {:ok, pid} -> fsync(pid, mode) == :error
          :error -> false
        end
      end)

    # Only emit when work happened. An idle tick (≈200 Hz) carries no signal
    # and its constant fan-out to the observability bus races subsystem
    # teardown into spurious :noproc errors.
    if count > 0 do
      :telemetry.execute(
        [:flezha, :state, :fsync, :tick],
        %{count: count},
        %{}
      )
    end

    %{state | dirty_set: MapSet.new(still_dirty)}
  end

  defp fsync(pid, :cast), do: WalWriter.fsync_async(pid)

  # `try` because a writer can exit between `mark_dirty/1` and the drain.
  defp fsync(pid, :call) do
    WalWriter.fsync(pid)
  catch
    :exit, _ -> :error
  end

  defp configured_interval_ms() do
    Durations.get(:agent_fsync_interval_ms)
  end
end

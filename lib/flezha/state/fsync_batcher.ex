defmodule Flezha.State.FsyncBatcher do
  @moduledoc """
  Single shared `GenServer` that drives `:file.datasync/1` across every
  dirty `WalWriter` on a configurable cadence.

  Writers cast `mark_dirty/1` after each append; the batcher coalesces
  the calls into a single per-tick fsync per writer. Default tick
  interval is `5 ms` (≈ 200 Hz), giving ≤10 ms total durability when
  paired with the SDK flusher's 5 ms linger.

  Validates `:flezha, :agent_fsync_interval_ms` at `init/1`. Legal range
  `[1, 1000]`; out-of-range values prevent the batcher from booting.
  """

  use GenServer

  alias Flezha.State.{Supervisor, WalWriter}

  @default_interval_ms 5
  @min_interval_ms 1
  @max_interval_ms 1000

  defstruct [:dirty_set, :tick_ref, :tick_interval_ms]

  # ---------- Public API ----------

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]}
    }
  end

  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec mark_dirty(tuple()) :: :ok
  def mark_dirty(segment_id) do
    GenServer.cast(__MODULE__, {:mark_dirty, segment_id})
  end

  @doc """
  Drop any pending dirty mark for `segment_id`. Called when a Segment
  is hard-deleted so the next tick does not try to fsync a writer that is
  being torn down.
  """
  @spec forget(tuple()) :: :ok
  def forget(segment_id) do
    GenServer.cast(__MODULE__, {:forget, segment_id})
  end

  @doc """
  Force an immediate tick, primarily for tests. Idempotent and safe
  under load (the next scheduled tick is preserved).
  """
  @spec flush_now() :: :ok
  def flush_now() do
    GenServer.call(__MODULE__, :flush_now)
  end

  @doc """
  Synchronously flush every dirty `WalWriter`. Equivalent to
  `flush_now/0`; named separately so the graceful-shutdown
  coordinator's intent is unambiguous at the call site.
  """
  @spec flush_all() :: :ok
  def flush_all() do
    GenServer.call(__MODULE__, :flush_all)
  end

  @spec dirty_set() :: MapSet.t()
  def dirty_set(), do: GenServer.call(__MODULE__, :dirty_set)

  # ---------- GenServer ----------

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, configured_interval_ms())

    cond do
      not is_integer(interval) ->
        {:stop, {:invalid_interval, interval}}

      interval < @min_interval_ms or interval > @max_interval_ms ->
        {:stop, {:interval_out_of_range, interval, [@min_interval_ms, @max_interval_ms]}}

      true ->
        ref = Process.send_after(self(), :tick, interval)

        {:ok,
         %__MODULE__{
           dirty_set: MapSet.new(),
           tick_ref: ref,
           tick_interval_ms: interval
         }}
    end
  end

  @impl true
  def handle_cast({:mark_dirty, segment_id}, state) do
    {:noreply, %{state | dirty_set: MapSet.put(state.dirty_set, segment_id)}}
  end

  def handle_cast({:forget, segment_id}, state) do
    {:noreply, %{state | dirty_set: MapSet.delete(state.dirty_set, segment_id)}}
  end

  @impl true
  def handle_call(:flush_now, _from, state) do
    new_state = drain(state)
    {:reply, :ok, new_state}
  end

  def handle_call(:flush_all, _from, state) do
    new_state = drain(state)
    {:reply, :ok, new_state}
  end

  def handle_call(:dirty_set, _from, state) do
    {:reply, state.dirty_set, state}
  end

  @impl true
  def handle_info(:tick, state) do
    new_state = drain(state)
    ref = Process.send_after(self(), :tick, state.tick_interval_ms)
    {:noreply, %{new_state | tick_ref: ref}}
  end

  defp drain(state) do
    count = MapSet.size(state.dirty_set)

    Enum.each(state.dirty_set, fn segment_id ->
      case Supervisor.lookup(segment_id) do
        {:ok, pid} ->
          # `try` because a writer can exit between `mark_dirty/1` and the tick.
          try do
            WalWriter.fsync(pid)
          catch
            :exit, _ -> :ok
          end

        :error ->
          :ok
      end
    end)

    :telemetry.execute(
      [:flezha, :state, :fsync, :tick],
      %{count: count},
      %{}
    )

    %{state | dirty_set: MapSet.new()}
  end

  defp configured_interval_ms() do
    Application.get_env(:flezha, :agent_fsync_interval_ms, @default_interval_ms)
  end
end

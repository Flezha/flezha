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
  def handle_cast({:mark_dirty, entity_id}, state) do
    {:noreply, %{state | dirty_set: MapSet.put(state.dirty_set, entity_id)}}
  end

  def handle_cast({:forget, entity_id}, state) do
    {:noreply, %{state | dirty_set: MapSet.delete(state.dirty_set, entity_id)}}
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

    Enum.each(state.dirty_set, fn entity_id ->
      case Supervisor.lookup(entity_id) do
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

    %{state | dirty_set: MapSet.new()}
  end

  defp configured_interval_ms() do
    Application.get_env(:flezha, :agent_fsync_interval_ms, @default_interval_ms)
  end
end

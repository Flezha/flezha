defmodule Flezha.State.Watchdog do
  @moduledoc """
  Sweeper that makes a stalled per-Partition persistence/output path
  *detectable and self-healing* instead of silently frozen. Holds its own
  sweep timer (`:output_watchdog_interval_ms`, default 10 s); the
  convergence `Loop` consumes the `:output_stall` telemetry the sweep
  emits.

  Two sweeps run on each pass:

    * **Output staleness.** For every persistence-enabled Partition with a
      live `WalWriter`, compare `last_change_seen_at` (any Change frame
      arriving) against `last_advance_at` (the persisted seq actually
      moving). A Partition that is still *receiving* Changes but whose seq
      has not advanced within `output_stall_timeout_ms` is stalled — its
      outbox has wedged while the SDK keeps emitting. A genuinely idle
      Partition (no Changes arriving) is NOT flagged, because a
      non-advancing seq is correct when nothing is being produced.

    * **Compaction timeout.** A `CompactPartition` whose
      `partition_state_snapshot` response does not arrive within
      `compaction_timeout_ms` is timed out so it cannot block persistence.

  Both emit telemetry and trigger the shared `Flezha.State.Recovery`
  resync. Because WAL persistence and output-plugin delivery share the
  per-Partition outbox, recovering WAL advance also resumes output.
  """

  use GenServer

  require Logger

  alias Flezha.State.{PersistenceResolver, Recovery, Registry, Supervisor, WalWriter}

  defstruct compactions: %{}, last_resync: %{}

  # ---------- Public API ----------

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Run both sweeps synchronously. Used by tests and ops tooling."
  @spec sweep_now() :: :ok
  def sweep_now(), do: GenServer.call(__MODULE__, :sweep_now)

  @doc """
  Record that a `CompactPartition` was shipped for `partition_id` so the
  handshake is bounded by `compaction_timeout_ms`. No-op when the
  Watchdog is not running (e.g. isolated unit tests).
  """
  @spec compaction_started(tuple()) :: :ok
  def compaction_started(partition_id) do
    GenServer.cast(__MODULE__, {:compaction_started, partition_id})
  end

  @doc "Clear a tracked compaction once its snapshot response arrives."
  @spec compaction_completed(tuple()) :: :ok
  def compaction_completed(partition_id) do
    GenServer.cast(__MODULE__, {:compaction_completed, partition_id})
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    schedule_sweep()
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_info(:sweep_tick, state) do
    schedule_sweep()
    {:noreply, sweep(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp schedule_sweep() do
    interval = Application.get_env(:flezha, :output_watchdog_interval_ms, 10_000)
    if interval > 0, do: Process.send_after(self(), :sweep_tick, interval)
  end

  @impl true
  def handle_cast({:compaction_started, partition_id}, state) do
    if PersistenceResolver.compaction_timeout_ms() > 0 do
      now = :erlang.monotonic_time(:millisecond)
      {:noreply, %{state | compactions: Map.put(state.compactions, partition_id, now)}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:compaction_completed, partition_id}, state) do
    {:noreply, %{state | compactions: Map.delete(state.compactions, partition_id)}}
  end

  @impl true
  def handle_call(:sweep_now, _from, state) do
    {:reply, :ok, sweep(state)}
  end

  # ---------- Sweep ----------

  defp sweep(state) do
    now = :erlang.monotonic_time(:millisecond)

    state
    |> sweep_output_staleness(now)
    |> sweep_compactions(now)
  end

  defp sweep_output_staleness(state, now) do
    Enum.reduce(Registry.all_partitions(), state, fn {rc, _sh, _pk} = pid, acc ->
      timeout = PersistenceResolver.output_stall_timeout_ms(rc)

      with true <- timeout > 0,
           {:ok, writer} <- Supervisor.lookup(pid),
           {:ok, status} <- safe_stall_status(writer),
           true <- stalled?(status, now, timeout),
           false <- recently_resynced?(acc, pid, now, timeout) do
        flag_output_stall(pid, status, now, timeout)
        %{acc | last_resync: Map.put(acc.last_resync, pid, now)}
      else
        _ -> acc
      end
    end)
  end

  # Stalled = Changes still arriving (not idle) but the persisted seq has
  # frozen past the timeout.
  defp stalled?(status, now, timeout) do
    now - status.last_change_seen_at < timeout and
      now - status.last_advance_at >= timeout
  end

  defp recently_resynced?(state, pid, now, timeout) do
    case Map.get(state.last_resync, pid) do
      nil -> false
      at -> now - at < timeout
    end
  end

  defp flag_output_stall({rc, sh, pk} = pid, status, now, _timeout) do
    :telemetry.execute(
      [:flezha, :state, :output_stall],
      %{stalled_ms: now - status.last_advance_at, last_seq: status.last_persisted_seq},
      %{relay: rc, shard: sh, key: pk}
    )

    _ = Recovery.resync(pid, reason: :output_stall)
    :ok
  end

  defp sweep_compactions(state, now) do
    timeout = PersistenceResolver.compaction_timeout_ms()

    if timeout <= 0 do
      state
    else
      {expired, kept} =
        Enum.split_with(state.compactions, fn {_pid, started_at} ->
          now - started_at >= timeout
        end)

      Enum.each(expired, fn {pid, started_at} ->
        flag_compaction_timeout(pid, now - started_at)
      end)

      %{state | compactions: Map.new(kept)}
    end
  end

  defp flag_compaction_timeout({rc, sh, pk} = pid, waited_ms) do
    :telemetry.execute(
      [:flezha, :state, :compaction_timeout],
      %{waited_ms: waited_ms},
      %{relay: rc, shard: sh, key: pk}
    )

    _ = Recovery.resync(pid, reason: :compaction_timeout)
    :ok
  end

  # A writer mid-rewrite or terminating must not crash the sweep.
  defp safe_stall_status(writer) do
    {:ok, WalWriter.stall_status(writer)}
  catch
    :exit, _ -> :error
  end
end

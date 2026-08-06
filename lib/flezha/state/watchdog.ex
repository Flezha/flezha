defmodule Flezha.State.Watchdog do
  @moduledoc """
  Sweeper that makes a stalled per-Entity persistence/output path
  *detectable and self-healing* instead of silently frozen. Holds its own
  sweep timer (`:output_watchdog_interval_ms`, default 10 s); the
  convergence `Loop` consumes the `:output_stall` telemetry the sweep
  emits.

  Three sweeps run on each pass:

    * **Output staleness.** For every persistence-enabled Entity with a
      live `WalWriter`, compare `last_change_seen_at` (any Change frame
      arriving) against `last_advance_at` (the persisted seq actually
      moving). An Entity that is still *receiving* Changes but whose seq
      has not advanced within `output_stall_timeout_ms` is stalled — its
      outbox has wedged while the SDK keeps emitting. A genuinely idle
      Entity (no Changes arriving) is NOT flagged, because a
      non-advancing seq is correct when nothing is being produced.

    * **Compaction trigger (size + age).** An Entity whose un-snapshotted
      WAL bytes exceed its resolved `compact_after_bytes`, or whose oldest
      un-snapshotted record is older than `compact_after_age_ms` (`0`
      disables age), gets a `CompactEntity` dispatched to its owning
      runner. The in-flight `compactions` map guards re-enqueue, so size
      and age can never double-enqueue.

    * **Compaction timeout.** A `CompactEntity` whose
      `entity_state_snapshot` response does not arrive within
      `compaction_timeout_ms` is timed out so it cannot block persistence.

  Both emit telemetry and trigger the shared `Flezha.State.Recovery`
  resync. Because WAL persistence and output-plugin delivery share the
  per-Entity outbox, recovering WAL advance also resumes output.
  """

  use GenServer

  require Logger

  alias Flezha.Convergence.Backoff
  alias Flezha.Operative.{ClassRegistry, Overwatch}
  alias Flezha.State.{PersistenceResolver, Recovery, Registry, Supervisor, WalWriter}

  # Consecutive compaction failures at which the miss stops being treated as
  # transient. The backoff schedule has four rungs (5s/30s/60s/300s); crossing
  # the last is the give-up boundary — a loud, operator-alertable alarm.
  @compaction_give_up_attempts 4

  # `compactions` maps `entity_id => {started_at_ms, runner_id}`: the in-flight
  # handshake plus the runner that owns it, so the timeout path can recover.
  # `backoff` is the shared per-`{:compaction, entity_id}` strike/cooldown table
  # that keeps a repeatedly-failing Entity from re-triggering every sweep.
  defstruct compactions: %{}, last_resync: %{}, catastrophic: %{}, backoff: Backoff.new()

  # ---------- Public API ----------

  def child_spec(opts) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}}
  end

  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Run both sweeps synchronously. Used by tests and procs tooling."
  @spec sweep_now() :: :ok
  def sweep_now(), do: GenServer.call(__MODULE__, :sweep_now)

  @doc """
  Stall/advance evidence (`WalWriter.stall_status/1`) for every Entity with
  a live writer. Stateless read used by the sweeps here and by the
  convergence Loop's producing-liveness classification at tick cadence.
  """
  @spec entity_statuses() :: [{Registry.entity_id(), WalWriter.stall_status()}]
  def entity_statuses(), do: collect_statuses(Registry.all_entities())

  @doc """
  Record that a `CompactEntity` was shipped for `entity_id` so the
  handshake is bounded by `compaction_timeout_ms`. No-op when the
  Watchdog is not running (e.g. isolated unit tests).
  """
  @spec compaction_started(tuple(), binary()) :: :ok
  def compaction_started(entity_id, runner_id) do
    GenServer.cast(__MODULE__, {:compaction_started, entity_id, runner_id})
  end

  @doc "Clear a tracked compaction once its snapshot response arrives."
  @spec compaction_completed(tuple()) :: :ok
  def compaction_completed(entity_id) do
    GenServer.cast(__MODULE__, {:compaction_completed, entity_id})
  end

  @doc """
  Record a compaction the SDK reported as failed — a `snapshot_seq = 0`
  response against a non-empty WAL (a stub build or an unaddressable Entity).
  Advances the backoff strike and alarms at the give-up threshold, so a stub is
  caught without waiting out the timeout sweep.
  """
  @spec compaction_failed(tuple()) :: :ok
  def compaction_failed(entity_id) do
    GenServer.cast(__MODULE__, {:compaction_failed, entity_id})
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
  def handle_cast({:compaction_started, entity_id, runner_id}, state) do
    if PersistenceResolver.compaction_timeout_ms() > 0 do
      now = :erlang.monotonic_time(:millisecond)
      {:noreply, %{state | compactions: Map.put(state.compactions, entity_id, {now, runner_id})}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:compaction_completed, {rc, sh, pk} = entity_id}, state) do
    :telemetry.execute(
      [:flezha, :state, :compaction_completed],
      %{},
      %{operative: rc, procedure: sh, key: pk}
    )

    now = :erlang.monotonic_time(:millisecond)

    {:noreply,
     %{
       state
       | compactions: Map.delete(state.compactions, entity_id),
         backoff: Backoff.update(state.backoff, {:compaction, entity_id}, :ok, now)
     }}
  end

  def handle_cast({:compaction_failed, entity_id}, state) do
    now = :erlang.monotonic_time(:millisecond)
    backoff = Backoff.update(state.backoff, {:compaction, entity_id}, :error, now)
    attempt = Backoff.attempt(backoff, {:compaction, entity_id})
    # A reported failure no resync can fix (the SDK shipped no snapshot). Alarm
    # at the threshold; the backoff-gated trigger sweep is the retry.
    maybe_give_up(entity_id, attempt)

    {:noreply, %{state | compactions: Map.delete(state.compactions, entity_id), backoff: backoff}}
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
    |> sweep_catastrophic_dataflow(now)
    |> sweep_compaction_triggers(now)
    |> sweep_compactions(now)
  end

  # The automatic snapshot trigger: size (un-snapshotted WAL bytes past the
  # resolved `compact_after_bytes`) or age (oldest un-snapshotted record past
  # `compact_after_age_ms`). The `compactions` in-flight map — marked here
  # directly so a failed dispatch still backs off until the timeout sweep
  # clears it — is the shared guard that keeps size+age to one enqueue.
  defp sweep_compaction_triggers(state, now) do
    entity_statuses()
    |> Enum.reduce(state, fn {{rc, sh, _pk} = entity_id, status}, acc ->
      with false <- Map.has_key?(acc.compactions, entity_id),
           true <- Backoff.eligible?(acc.backoff, {:compaction, entity_id}, now),
           runner_id when is_binary(runner_id) <- status.owning_runner,
           trigger when trigger != nil <- compaction_trigger(status, now, rc, sh) do
        flag_compaction_trigger(entity_id, runner_id, status, trigger)
        %{acc | compactions: Map.put(acc.compactions, entity_id, {now, runner_id})}
      else
        _ -> acc
      end
    end)
  end

  # The age floor resolves per operative (a correction-declaring class must not
  # compact away the history inside its own window), so it is read here rather
  # than hoisted out of the loop. The size floor already resolved per entity,
  # so this adds no new cost class.
  defp compaction_trigger(status, now, operative_class, _procedure_id) do
    age_ms = PersistenceResolver.compact_after_age_ms(operative_class)

    cond do
      status.wal_size_bytes >= PersistenceResolver.compact_after_bytes(operative_class) ->
        :size

      age_ms > 0 and is_integer(status.oldest_unsnapshotted_at) and
          now - status.oldest_unsnapshotted_at >= age_ms ->
        :age

      true ->
        nil
    end
  end

  defp flag_compaction_trigger({rc, sh, pk} = entity_id, runner_id, status, trigger) do
    :telemetry.execute(
      [:flezha, :state, :compaction_trigger],
      %{wal_size_bytes: status.wal_size_bytes},
      %{operative: rc, procedure: sh, key: pk, trigger: trigger, runner: runner_id}
    )

    _ = Overwatch.dispatch_compact_entity(runner_id, entity_id)
    :ok
  end

  # Detects a operative that produced output but received zero Change frames for the
  # full timeout — the condition the output-staleness sweep cannot see (no Changes
  # arriving means the entity looks idle, not stalled). Latched per operative_class.
  defp sweep_catastrophic_dataflow(state, now) do
    timeout = Application.get_env(:flezha, :catastrophic_dataflow_timeout_ms, 60_000)

    if timeout <= 0 do
      state
    else
      entity_statuses()
      |> Enum.group_by(fn {{rc, _sh, _pk}, _status} -> rc end)
      |> Enum.reduce(state, fn {rc, list}, acc ->
        # The dataflow-stop watchdog is unbounded-only: a durable :bounded
        # operative legitimately goes quiet between stages/iterations — and so
        # does a checkpoint-ack one between checkpoints, where making nothing
        # durable for a whole interval is the steady state, not a dead feed.
        if ClassRegistry.capable?(rc, :output_stall_watch?) and
             not ClassRegistry.checkpoint_ack?(rc) do
          any_dead? = Enum.any?(list, fn {_pid, s} -> input_dead?(s, now, timeout) end)
          latched? = Map.has_key?(acc.catastrophic, rc)

          cond do
            any_dead? and not latched? ->
              flag_catastrophic(rc, list, now, timeout)
              %{acc | catastrophic: Map.put(acc.catastrophic, rc, now)}

            not any_dead? and latched? ->
              Logger.info("[watchdog] catastrophic data-flow recovered operative=#{inspect(rc)}")
              %{acc | catastrophic: Map.delete(acc.catastrophic, rc)}

            true ->
              acc
          end
        else
          acc
        end
      end)
    end
  end

  defp collect_statuses(entities) do
    Enum.flat_map(entities, fn pid ->
      with {:ok, writer} <- Supervisor.lookup(pid),
           {:ok, status} <- safe_stall_status(writer) do
        [{pid, status}]
      else
        _ -> []
      end
    end)
  end

  # Produced output before (seq advanced past 0) but no Change frame has arrived
  # for the whole timeout window => the upstream feed into this operative is dead.
  defp input_dead?(status, now, timeout) do
    status.last_persisted_seq > 0 and now - status.last_change_seen_at >= timeout
  end

  defp flag_catastrophic(rc, _list, _now, timeout) do
    :telemetry.execute(
      [:flezha, :state, :catastrophic_dataflow_stop],
      %{timeout_ms: timeout},
      %{operative_class: rc, operative_instance: rc}
    )

    Logger.critical(
      "[watchdog] data-flow stop operative=#{inspect(rc)}: no input Changes for >= #{timeout}ms despite prior output"
    )
  end

  defp sweep_output_staleness(state, now) do
    Enum.reduce(Registry.all_entities(), state, fn {rc, _sh, _pk} = pid, acc ->
      timeout = PersistenceResolver.output_stall_timeout_ms(rc)

      with true <- ClassRegistry.capable?(rc, :output_stall_watch?),
           true <- timeout > 0,
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

  # The entity's `operative_class` is, by the flezha operative convention
  # (`OperativeManager`: "the operative_id is used as the operative_class identifier"),
  # the operative-instance id the convergence Loop binds. Emitting it as
  # `operative_instance` lets the Loop rebind the exact stalled instance
  # rather than the factory/class name.
  defp flag_output_stall({rc, sh, pk} = pid, status, now, _timeout) do
    :telemetry.execute(
      [:flezha, :state, :output_stall],
      %{stalled_ms: now - status.last_advance_at, last_seq: status.last_persisted_seq},
      %{operative_class: rc, operative_instance: rc, procedure: sh, key: pk}
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
        Enum.split_with(state.compactions, fn {_pid, {started_at, _runner_id}} ->
          now - started_at >= timeout
        end)

      backoff =
        Enum.reduce(expired, state.backoff, fn {pid, {started_at, runner_id}}, acc ->
          acc = Backoff.update(acc, {:compaction, pid}, :error, now)

          flag_compaction_timeout(
            pid,
            now - started_at,
            runner_id,
            Backoff.attempt(acc, {:compaction, pid})
          )

          acc
        end)

      %{state | compactions: Map.new(kept), backoff: backoff}
    end
  end

  defp flag_compaction_timeout({rc, sh, pk} = pid, waited_ms, runner_id, attempt) do
    :telemetry.execute(
      [:flezha, :state, :compaction_timeout],
      %{waited_ms: waited_ms},
      %{operative: rc, procedure: sh, key: pk, runner: runner_id, attempt: attempt}
    )

    maybe_give_up(pid, attempt, runner_id)
    :ok
  end

  # Below the give-up threshold the backoff-gated trigger sweep is the retry — a
  # still-live, still-correct Entity is NOT resynced (re-restoring a healthy
  # Entity fixes nothing and risks availability). At the threshold the WAL has
  # grown across every backoff rung, so the loud alarm fires; and when a
  # `runner_id` is known (the timeout path carries one; the stub-response path
  # does not), it ALSO dispatches a real resync — the fix for the previously
  # silent `_ = Recovery.resync/2` no-op whose `{:error, :no_runner_id}` was
  # discarded.
  defp maybe_give_up({rc, sh, pk} = pid, attempt, runner_id \\ nil) do
    if attempt >= @compaction_give_up_attempts do
      emit_give_up(pid, attempt)
      if runner_id, do: resync_on_give_up(pid, runner_id)
    else
      Logger.debug(
        "[watchdog] compaction unresolved operative=#{rc} procedure=#{sh} key=#{pk} " <>
          "attempt=#{attempt}; retry gated by backoff"
      )
    end
  end

  defp resync_on_give_up({rc, sh, pk} = pid, runner_id) do
    case Recovery.resync(pid, reason: :compaction_gave_up, runner_id: runner_id) do
      {:ok, _} ->
        :ok

      other ->
        Logger.warning(
          "[watchdog] compaction give-up resync failed operative=#{rc} procedure=#{sh} " <>
            "key=#{pk}: #{inspect(other)}"
        )
    end
  end

  defp emit_give_up({rc, sh, pk}, attempt) do
    Logger.error(
      "[watchdog] compaction GAVE UP after #{attempt} failed attempts operative=#{rc} " <>
        "procedure=#{sh} key=#{pk}; the Entity's WAL is not being reclaimed — investigate " <>
        "the runner's snapshot path"
    )

    :telemetry.execute(
      [:flezha, :state, :compaction_gave_up],
      %{attempts: attempt},
      %{operative: rc, procedure: sh, key: pk}
    )
  end

  # A writer mid-rewrite or terminating must not crash the sweep.
  defp safe_stall_status(writer) do
    {:ok, WalWriter.stall_status(writer)}
  catch
    :exit, _ -> :error
  end
end

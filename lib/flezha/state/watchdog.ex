defmodule Flezha.State.Watchdog do
  @moduledoc """
  Sweeper that makes a stalled per-Cell persistence/output path *detectable*
  instead of silently frozen. Holds its own sweep timer
  (`:output_watchdog_interval_ms`, 10 s); the convergence `Loop` consumes the
  telemetry it emits.

  This is a **sensor**: it observes and emits, and decides no remedy of its own.
  The compaction handshake it dispatched it also times out, and a compaction it
  gives up on is declared to the convergence loop, which plans the resync. Each
  completed sweep stamps `Convergence.Snapshot`, so a Watchdog that wedges
  without crashing is visible on `/healthz` rather than turning detection off
  in silence.

  Four sweeps run on each pass: **output staleness** (Changes still arriving
  while the persisted seq has not advanced — an idle Cell is not flagged),
  **compaction trigger** on un-snapshotted bytes or age, **compaction timeout**,
  and **remote RPO** when a durability backend is configured.
  """

  use GenServer
  use Flezha.Process.Timer, keys: [:sweep]

  require Logger

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.Quotas
  alias Flezha.Config.Durations
  alias Flezha.Config.Backoff
  alias Flezha.Convergence.{Facts, Snapshot}
  alias Flezha.Relay.ClassRegistry

  alias Flezha.State.{
    Commands,
    DurabilityTier,
    Paths,
    PersistenceResolver,
    WalWriter,
    WriterRegistry
  }

  # `compactions` maps `cell_id => {started_at_ms, runner_id}`: the in-flight
  # handshake plus the runner that owns it, so the timeout path can recover.
  # `backoff` is the shared per-`{:compaction, cell_id}` strike/cooldown table
  # that keeps a repeatedly-failing Cell from re-triggering every sweep; it is
  # filled by `init/1` rather than defaulted here, because a struct default is
  # evaluated at compile time and would make this a compile-time dependency on
  # a module a sensor only calls.
  defstruct compactions: %{},
            input_silence: MapSet.new(),
            rpo_breached: MapSet.new(),
            disk_breaches: MapSet.new(),
            backoff: nil,
            timers: %{}

  # ---------- Public API ----------

  def start_link(opts) when is_list(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Run both sweeps synchronously. Used by tests and ops tooling."
  @spec sweep_now() :: :ok
  def sweep_now(), do: GenServer.call(__MODULE__, :sweep_now)

  @doc """
  Stall/advance evidence (`WalWriter.stall_status/1`) for every Cell with
  a live writer. Stateless read used by the sweeps here and by the
  convergence Loop's producing-liveness classification at tick cadence.
  """
  @spec cell_statuses() :: [{Paths.cell_id(), WalWriter.stall_status()}]
  def cell_statuses(), do: collect_statuses(Paths.all_cells())

  @doc """
  Record that a `CompactCell` was shipped for `cell_id` so the
  handshake is bounded by `compaction_timeout_ms`. No-op when the
  Watchdog is not running (e.g. isolated unit tests).
  """
  @spec compaction_started(tuple(), binary()) :: :ok
  def compaction_started(cell_id, runner_id) do
    GenServer.cast(__MODULE__, {:compaction_started, cell_id, runner_id})
  end

  @doc "Clear a tracked compaction once its snapshot response arrives."
  @spec compaction_completed(tuple()) :: :ok
  def compaction_completed(cell_id) do
    GenServer.cast(__MODULE__, {:compaction_completed, cell_id})
  end

  @doc """
  Record a compaction the SDK reported as failed — a `snapshot_seq = 0`
  response against a non-empty WAL (a stub build or an unaddressable Cell).
  Advances the backoff strike and alarms at the give-up threshold, so a stub is
  caught without waiting out the timeout sweep.
  """
  @spec compaction_failed(tuple()) :: :ok
  def compaction_failed(cell_id) do
    GenServer.cast(__MODULE__, {:compaction_failed, cell_id})
  end

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    # Per-namespace disk counters are rebuilt from the on-disk truth on every
    # boot, so accounting drift is bounded by one uptime
    # (finish-platform-authn-authz §6.2).
    rebuild_disk_counters()
    {:ok, arm_sweep(%__MODULE__{backoff: Backoff.new(:compaction)})}
  end

  @impl true
  def handle_info(:sweep_tick, state) do
    state = arm_sweep(state)
    {:noreply, sweep(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # A non-positive interval turns the sweep off: nothing is armed and the key
  # simply holds no timer.
  defp arm_sweep(state) do
    interval = sweep_interval_ms()
    if interval > 0, do: arm_timer(state, :sweep, interval, :sweep_tick), else: state
  end

  defp sweep_interval_ms(), do: Durations.get(:output_watchdog_interval_ms)

  @impl true
  def handle_cast({:compaction_started, cell_id, runner_id}, state) do
    if PersistenceResolver.compaction_timeout_ms() > 0 do
      now = :erlang.monotonic_time(:millisecond)
      {:noreply, %{state | compactions: Map.put(state.compactions, cell_id, {now, runner_id})}}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:compaction_completed, {rc, sh, pk} = cell_id}, state) do
    :telemetry.execute(
      [:flezha, :state, :compaction_completed],
      %{},
      %{relay: rc, tasklet: sh, key: pk}
    )

    now = :erlang.monotonic_time(:millisecond)

    # A snapshot just replaced the WAL prefix it subsumes — the exact boundary
    # at which the remote copy should move from record bundles to a baseline.
    DurabilityTier.on_compaction_completed(cell_id)
    Facts.declare({:compaction_completed, cell_id, %{}})

    {:noreply,
     %{
       state
       | compactions: Map.delete(state.compactions, cell_id),
         backoff: Backoff.update(state.backoff, {:compaction, cell_id}, :ok, now)
     }}
  end

  def handle_cast({:compaction_failed, cell_id}, state) do
    now = :erlang.monotonic_time(:millisecond)
    backoff = Backoff.update(state.backoff, {:compaction, cell_id}, :error, now)
    attempt = Backoff.attempt(backoff, {:compaction, cell_id})
    # A reported failure no resync can fix (the SDK shipped no snapshot). Alarm
    # at the threshold; the backoff-gated trigger sweep is the retry.
    maybe_give_up(cell_id, attempt)

    {:noreply, %{state | compactions: Map.delete(state.compactions, cell_id), backoff: backoff}}
  end

  @impl true
  def handle_call(:sweep_now, _from, state) do
    {:reply, :ok, sweep(state)}
  end

  # ---------- Sweep ----------

  defp sweep(state) do
    now = :erlang.monotonic_time(:millisecond)

    swept =
      state
      |> sweep_output_staleness(now)
      |> sweep_input_silence(now)
      |> sweep_compaction_triggers(now)
      |> sweep_compactions(now)
      |> sweep_rpo()
      |> sweep_disk_quotas()

    # Stamped AFTER the last sweep, so the mark means "a full pass completed",
    # not "a pass started". A Watchdog wedged mid-sweep therefore goes stale.
    Snapshot.observe_sensor(:watchdog, now, sweep_interval_ms())
    swept
  end

  # ---------- Disk-quota breach (finish-platform-authn-authz §6.2) ----------
  #
  # A running namespace over `max_state_bytes` gets a loud, latched alarm and
  # stops admitting NEW relay allocations (checked at start_relay) —
  # in-flight durable writes are never interrupted: killing durability
  # mid-write loses data, which is worse than a quota breach.

  defp sweep_disk_quotas(state) do
    if Flezha.AuthZ.enabled?() do
      breached =
        Enum.reduce(Paths.namespaces(), MapSet.new(), fn namespace, acc ->
          bytes = Quotas.refresh_disk_bytes(namespace)

          case namespace_disk_limit(namespace) do
            limit when is_integer(limit) and bytes > limit ->
              unless MapSet.member?(state.disk_breaches, namespace) do
                :telemetry.execute(
                  [:flezha, :state, :disk_quota_breach],
                  %{bytes: bytes, limit: limit},
                  %{namespace: namespace}
                )

                Logger.error(
                  "[watchdog] namespace #{namespace} exceeds max_state_bytes " <>
                    "(#{bytes} > #{limit}); new relay allocation is refused, " <>
                    "durable writes continue"
                )
              end

              MapSet.put(acc, namespace)

            _ ->
              acc
          end
        end)

      %{state | disk_breaches: breached}
    else
      state
    end
  end

  defp namespace_disk_limit(namespace) do
    case NamespaceStore.get(namespace) do
      {:ok, %{quotas: quotas}} -> Map.get(quotas, :max_state_bytes)
      :error -> nil
    end
  end

  defp rebuild_disk_counters() do
    if Flezha.AuthZ.enabled?() do
      Enum.each(Paths.namespaces(), &Quotas.rebuild_disk_bytes/1)
    end

    :ok
  rescue
    # The auth stores may not be running (opt-out mode / isolated tests);
    # the counters are then unused anyway.
    _ -> :ok
  end

  # A remote-durability reference whose local progress has been unshipped past
  # the RPO target. Latched per reference so a sustained backend outage alarms
  # once per breach rather than every sweep, and cleared when it catches up —
  # the same shape as the input-silence latch.
  defp sweep_rpo(state) do
    breaches = DurabilityTier.breaches()
    breached = MapSet.new(breaches, & &1.ref)

    breaches
    |> Enum.reject(&MapSet.member?(state.rpo_breached, &1.ref))
    |> Enum.each(&flag_rpo_exceeded(&1.ref, &1.lag_ms))

    %{state | rpo_breached: breached}
  end

  defp flag_rpo_exceeded(ref, lag) do
    Logger.error(
      "[watchdog] remote durability RPO exceeded for #{inspect(ref)}: #{lag}ms of local " <>
        "progress is not on the remote store — investigate the durability backend"
    )

    :telemetry.execute(
      [:flezha, :state, :rpo_exceeded],
      %{lag_ms: lag, target_ms: DurabilityTier.rpo_target_ms()},
      %{ref: inspect(ref)}
    )
  end

  # The automatic snapshot trigger: size (un-snapshotted WAL bytes past the
  # resolved `compact_after_bytes`) or age (oldest un-snapshotted record past
  # `compact_after_age_ms`). The `compactions` in-flight map — marked here
  # directly so a failed dispatch still backs off until the timeout sweep
  # clears it — is the shared guard that keeps size+age to one enqueue.
  defp sweep_compaction_triggers(state, now) do
    cell_statuses()
    |> Enum.reduce(state, fn {{rc, sh, _pk} = cell_id, status}, acc ->
      with false <- Map.has_key?(acc.compactions, cell_id),
           true <- Backoff.eligible?(acc.backoff, {:compaction, cell_id}, now),
           runner_id when is_binary(runner_id) <- status.owning_runner,
           trigger when trigger != nil <- compaction_trigger(status, now, rc, sh) do
        flag_compaction_trigger(cell_id, runner_id, status, trigger)
        %{acc | compactions: Map.put(acc.compactions, cell_id, {now, runner_id})}
      else
        _ -> acc
      end
    end)
  end

  # The age floor resolves per relay (a correction-declaring class must not
  # compact away the history inside its own window), so it is read here rather
  # than hoisted out of the loop. The size floor already resolved per cell,
  # so this adds no new cost class.
  defp compaction_trigger(status, now, relay_class, _tasklet_id) do
    age_ms = PersistenceResolver.compact_after_age_ms(relay_class)

    cond do
      status.wal_size_bytes >= PersistenceResolver.compact_after_bytes(relay_class) ->
        :size

      age_ms > 0 and is_integer(status.oldest_unsnapshotted_at) and
          now - status.oldest_unsnapshotted_at >= age_ms ->
        :age

      true ->
        nil
    end
  end

  defp flag_compaction_trigger({rc, sh, pk} = cell_id, runner_id, status, trigger) do
    :telemetry.execute(
      [:flezha, :state, :compaction_trigger],
      %{wal_size_bytes: status.wal_size_bytes},
      %{relay: rc, tasklet: sh, key: pk, trigger: trigger, runner: runner_id}
    )

    _ = Commands.dispatch_compact_cell(runner_id, cell_id)
    :ok
  end

  # Detects a relay that produced output but received zero Change frames for
  # the full timeout: the condition the output-staleness sweep cannot see (no
  # Changes arriving means the cell looks idle, not stalled). Latched per
  # relay_class.
  #
  # Silence is all this establishes. A dead upstream, a source that reports
  # hourly and a quiet night are indistinguishable here, which is why the
  # window defaults to 0 and the sweep does not run unless a user asks
  # for it. The convergence judge holds the same rule for the same observation:
  # a flat marker with no input-side advance is idle, and idle is healthy.
  defp sweep_input_silence(state, now) do
    timeout = Durations.get(:input_silence_timeout_ms)

    if timeout <= 0 do
      state
    else
      cell_statuses()
      |> Enum.group_by(fn {{rc, _sh, _pk}, _status} -> rc end)
      |> Enum.reduce(state, fn {rc, list}, acc ->
        # A sleeping instance receives no Change by design, and a
        # checkpoint-ack one makes nothing durable for a whole interval
        # between checkpoints: neither silence is a dead feed.
        if not ClassRegistry.ack_checkpoint?(rc) and is_nil(ClassRegistry.parked(rc)) do
          any_silent? = Enum.any?(list, fn {_pid, s} -> input_silent?(s, now, timeout) end)
          latched? = MapSet.member?(acc.input_silence, rc)

          cond do
            any_silent? and not latched? ->
              flag_input_silence(rc, timeout)
              %{acc | input_silence: MapSet.put(acc.input_silence, rc)}

            not any_silent? and latched? ->
              Logger.info("[watchdog] input resumed for relay=#{inspect(rc)}")
              %{acc | input_silence: MapSet.delete(acc.input_silence, rc)}

            true ->
              acc
          end
        else
          acc
        end
      end)
    end
  end

  defp collect_statuses(cells) do
    Enum.flat_map(cells, fn pid ->
      with {:ok, writer} <- WriterRegistry.lookup(pid),
           {:ok, status} <- safe_stall_status(writer) do
        [{pid, status}]
      else
        _ -> []
      end
    end)
  end

  # Produced output before (seq advanced past 0) but no Change frame has arrived
  # for the whole timeout window.
  defp input_silent?(status, now, timeout) do
    status.last_persisted_seq > 0 and now - status.last_change_seen_at >= timeout
  end

  defp flag_input_silence(rc, timeout) do
    :telemetry.execute(
      [:flezha, :state, :input_silence],
      %{timeout_ms: timeout},
      %{relay_class: rc, relay_instance: rc}
    )

    Logger.warning(
      "[watchdog] input silent for relay=#{inspect(rc)}: no Changes received for >= " <>
        "#{timeout}ms, having produced output earlier"
    )
  end

  defp sweep_output_staleness(state, now) do
    Enum.each(Paths.all_cells(), fn {rc, _sh, _pk} = pid ->
      timeout = PersistenceResolver.output_stall_timeout_ms(rc)

      with true <- timeout > 0,
           {:ok, writer} <- WriterRegistry.lookup(pid),
           {:ok, status} <- safe_stall_status(writer),
           true <- stalled?(status, now, timeout) do
        flag_output_stall(pid, status, now)
      end
    end)

    state
  end

  # Stalled = Changes still arriving (not idle) but the persisted seq has
  # frozen past the timeout.
  defp stalled?(status, now, timeout) do
    now - status.last_change_seen_at < timeout and
      now - status.last_advance_at >= timeout
  end

  # The cell's `relay_class` is, by the flezha relay convention
  # (`RelayManager`: "the relay_id is used as the relay_class identifier"),
  # the relay-instance id the convergence Loop binds. Emitting it as
  # `relay_instance` lets the Loop rebind the exact stalled instance
  # rather than the factory/class name.
  defp flag_output_stall({rc, sh, pk}, status, now) do
    :telemetry.execute(
      [:flezha, :state, :output_stall],
      %{stalled_ms: now - status.last_advance_at, last_seq: status.last_persisted_seq},
      %{relay_class: rc, relay_instance: rc, tasklet: sh, key: pk}
    )
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
      %{relay: rc, tasklet: sh, key: pk, runner: runner_id, attempt: attempt}
    )

    maybe_give_up(pid, attempt, runner_id)
    :ok
  end

  # Below the give-up threshold the backoff-gated trigger sweep is the retry — a
  # still-live, still-correct Cell is NOT resynced (re-restoring a healthy
  # Cell fixes nothing and risks availability). At the threshold the WAL has
  # grown across every backoff rung, so the loud alarm fires; and when a
  # `runner_id` is known (the timeout path carries one; the stub-response path
  # does not), the give-up is declared to the loop, which plans the resync on
  # its own table. A give-up that cannot name a runner declares nothing: a
  # resync with no runner would be a silent no-op.
  defp maybe_give_up({rc, sh, pk} = pid, attempt, runner_id \\ nil) do
    if Backoff.give_up?(:compaction, attempt) do
      emit_give_up(pid, attempt)
      if runner_id, do: Facts.declare({:compaction_gave_up, pid, %{runner_id: runner_id}})
    else
      Logger.debug(
        "[watchdog] compaction unresolved relay=#{rc} tasklet=#{sh} key=#{pk} " <>
          "attempt=#{attempt}; retry gated by backoff"
      )
    end
  end

  defp emit_give_up({rc, sh, pk}, attempt) do
    Logger.error(
      "[watchdog] compaction GAVE UP after #{attempt} failed attempts relay=#{rc} " <>
        "tasklet=#{sh} key=#{pk}; the Cell's WAL is not being reclaimed — investigate " <>
        "the runner's snapshot path"
    )

    :telemetry.execute(
      [:flezha, :state, :compaction_gave_up],
      %{attempts: attempt},
      %{relay: rc, tasklet: sh, key: pk}
    )
  end

  # A writer mid-rewrite or terminating must not crash the sweep.
  # `WalWriter.stall_status/1` declares its refusal instead of exiting, so a
  # writer that is gone, wedged or mid-restart answers a value, and nothing here
  # has an exit to catch. Reading that value as a status map is what put an
  # error tuple into `stalled?/3` and `input_dead?/3` and took this node-wide
  # sensor down on a sweep.
  defp safe_stall_status(writer) do
    case WalWriter.stall_status(writer) do
      %{} = status -> {:ok, status}
      {:error, _refused} -> :error
    end
  end
end

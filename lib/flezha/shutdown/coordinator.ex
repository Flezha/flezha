defmodule Flezha.Shutdown.Coordinator do
  @moduledoc """
  Drives the controller's three-phase graceful shutdown
  (`quiesce → drain → finalise`) on `shutdown/2`.

  - **Quiesce** sets a process-tree-visible flag (`shutting_down?/0`)
    so `Flezha.Service.OperativeManager.start_operative/2` returns
    `{:error, :shutting_down}` and `Flezha.Runner.FleetManager`
    rejects new runner connect attempts.
  - **Drain** migrates locally-hosted Operatives to peers while they
    are still alive: `ProcessHub.Service.Migration.drain/2` per running
    namespace hub redistributes each child through the migration-consent
    gate (live HotSwap handover) and force-migrates what still defers at
    the deadline. The hub supervisor stops only after the drains return.
    Solo nodes (`{:error, :no_target_nodes}`) skip the migration wait;
    other drain preconditions degrade to the same path rather than
    wedging shutdown.
  - **Finalise** flushes `Flezha.State.FsyncBatcher` and stops
    transports so file descriptors/sockets are torn down before the
    application supervision tree's normal shutdown takes over.

  The whole sequence is bounded by
  `:flezha, :graceful_shutdown_timeout_ms`. Idempotent —
  concurrent callers register as waiters and receive the same
  outcome.

  Subscribe to `[:flezha, :shutdown, :*]` telemetry events for
  observability; structured logs accompany each phase boundary.
  """

  use GenServer
  require Logger

  alias Flezha.Service.{HubManager, Identificator}
  alias Flezha.State.FsyncBatcher
  alias Flezha.Transport.Server, as: TransportServer

  @ets_table __MODULE__.State

  @finalise_call_timeout_ms 5_000
  # Slack reserved off the drain budget so finalise still runs inside the
  # overall bound; also the floor so `Migration.drain/2` always gets a
  # positive deadline.
  @drain_budget_slack_ms 1_000
  # A force pass fires AT the drain deadline and its handover is async, so
  # this window is carved out of the drain timeout to let it land before the
  # hub stops.
  @forced_handover_grace_ms 2_000

  defstruct phase: :idle,
            reason: nil,
            opts: [],
            timeout_ms: nil,
            start_ms: nil,
            phase_start_ms: nil,
            waiters: [],
            captured_local: %{},
            still_local: [],
            outcome: nil,
            signal_handler_status: :ok,
            timeout_timer: nil

  # ---------- Public API ----------

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      shutdown: 30_000,
      type: :worker
    }
  end

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Drive the shutdown state machine. Synchronous — blocks until the
  coordinator returns or its bounded timeout elapses.

  `reason` is one of `:sigterm | :sigint | :prep_stop | :cli | :force`.

  Options:
    * `:timeout_ms` — overrides `:flezha, :graceful_shutdown_timeout_ms`
      for this invocation.
    * `:force` — skip quiesce + drain; run finalise only.
    * `:wait_timeout_ms` — caller's GenServer.call timeout
      (defaults to `timeout_ms + 30_000`).

  Idempotent: a second call during an in-flight shutdown registers
  as a waiter and receives the first call's outcome.
  """
  @spec shutdown(atom(), keyword()) :: :ok | {:partial, [String.t()]}
  def shutdown(reason, opts \\ []) do
    timeout_ms = Keyword.get(opts, :timeout_ms, configured_timeout_ms())
    call_timeout = Keyword.get(opts, :wait_timeout_ms, timeout_ms + 30_000)
    GenServer.call(__MODULE__, {:shutdown, reason, opts}, call_timeout)
  end

  @doc """
  Snapshot of the coordinator state — phase, configuration knobs,
  and signal-handler status. Read by `flz controller status`.
  """
  @spec status() :: map()
  def status() do
    GenServer.call(__MODULE__, :status)
  end

  @doc false
  def __reset_for_tests__() do
    GenServer.call(__MODULE__, :__reset_for_tests__)
  end

  @doc """
  Hot-path read: returns `true` while the coordinator is in
  `:quiescing | :draining | :finalising`. Backed by an ETS table so
  callers do not pay for a GenServer hop.
  """
  @spec shutting_down?() :: boolean()
  def shutting_down?() do
    case :ets.whereis(@ets_table) do
      :undefined ->
        false

      _ ->
        case :ets.lookup(@ets_table, :phase) do
          [{:phase, p}] when p in [:quiescing, :draining, :finalising] -> true
          _ -> false
        end
    end
  end

  @doc """
  Called by `Flezha.Shutdown.SignalHandler` after the OS signal
  install attempt. Surfaces `:degraded` when `:os.set_signal/2`
  raised so `flz controller status` can report the condition.
  """
  @spec report_signal_handler_status(:ok | :degraded) :: :ok
  def report_signal_handler_status(handler_status) when handler_status in [:ok, :degraded] do
    GenServer.cast(__MODULE__, {:signal_handler_status, handler_status})
  end

  # ---------- GenServer ----------

  @impl true
  def init(_) do
    :ets.new(@ets_table, [:set, :protected, :named_table, read_concurrency: true])
    :ets.insert(@ets_table, {:phase, :idle})
    {:ok, %__MODULE__{}}
  end

  @impl true
  def handle_call({:shutdown, reason, opts}, from, %__MODULE__{phase: :idle} = state) do
    timeout_ms = Keyword.get(opts, :timeout_ms, configured_timeout_ms())
    now = monotonic_ms()

    emit_telemetry(:requested, %{}, %{
      reason: reason,
      timestamp_ms: System.system_time(:millisecond)
    })

    state = %{
      state
      | reason: reason,
        opts: opts,
        timeout_ms: timeout_ms,
        start_ms: now,
        waiters: [from]
    }

    state = arm_timeout(state)

    cond do
      Keyword.get(opts, :force, false) ->
        Logger.info("[shutdown] forced (skipping drain); phase=finalise")
        {:noreply, enter_finalise(state, now)}

      true ->
        Logger.info("[shutdown] phase=quiesce reason=#{inspect(reason)}")
        state = transition(state, :quiescing, now)
        emit_telemetry(:phase_started, %{}, %{phase: :quiesce})
        send(self(), :do_quiesce)
        {:noreply, state}
    end
  end

  def handle_call({:shutdown, _reason, _opts}, _from, %__MODULE__{phase: :done} = state) do
    {:reply, state.outcome || :ok, state}
  end

  def handle_call({:shutdown, _reason, _opts}, from, %__MODULE__{} = state) do
    Logger.info("[shutdown] already in progress (phase=#{state.phase}), joining")
    {:noreply, %{state | waiters: [from | state.waiters]}}
  end

  def handle_call(:__reset_for_tests__, _from, state) do
    if is_reference(state.timeout_timer), do: Process.cancel_timer(state.timeout_timer)
    :ets.insert(@ets_table, {:phase, :idle})
    {:reply, :ok, %__MODULE__{signal_handler_status: state.signal_handler_status}}
  end

  def handle_call(:status, _from, state) do
    reply = %{
      shutdown_in_progress: state.phase in [:quiescing, :draining, :finalising],
      shutdown_phase: external_phase(state.phase),
      signal_handler_status: state.signal_handler_status,
      graceful_shutdown_enabled: Application.get_env(:flezha, :graceful_shutdown_enabled, true),
      graceful_shutdown_timeout_ms: configured_timeout_ms()
    }

    {:reply, reply, state}
  end

  @impl true
  def handle_cast({:signal_handler_status, handler_status}, state) do
    {:noreply, %{state | signal_handler_status: handler_status}}
  end

  @impl true
  def handle_info(:do_quiesce, state) do
    now = monotonic_ms()
    duration_ms = now - state.phase_start_ms
    emit_telemetry(:phase_completed, %{duration_ms: duration_ms}, %{phase: :quiesce})

    captured = capture_local_operatives()
    locally_hosted = Enum.reduce(captured, 0, fn {_ns, ids}, acc -> acc + length(ids) end)

    Logger.info("[shutdown] phase=drain locally_hosted=#{locally_hosted}")
    emit_telemetry(:phase_started, %{}, %{phase: :drain})

    state = transition(state, :draining, now)
    state = %{state | captured_local: captured}

    if captured == %{} do
      # Nothing was running locally; drain trivially complete.
      send(self(), {:drain_done, %{migrated: 0, forced: 0}})
      {:noreply, state}
    else
      # `Migration.drain/2` blocks up to its deadline, so it runs off the
      # coordinator loop; the `:phase_timeout` timer is the backstop if the
      # runner dies without reporting.
      start_drain_runner(self(), Map.keys(captured), drain_budget(state, now), now)
      {:noreply, state}
    end
  end

  def handle_info(
        {:drain_done, %{migrated: migrated, forced: forced}},
        %__MODULE__{phase: :draining} = state
      ) do
    Logger.info("[shutdown] drain migrated=#{migrated} forced=#{forced}")

    # The children were claimed by peers (or degraded to next-boot WAL
    # restore) while still alive; only now does the hub supervisor stop.
    safe_finalise_step(:stop_hub_manager, &HubManager.stop_hub_manager/0)

    now = monotonic_ms()
    duration_ms = now - state.phase_start_ms
    emit_telemetry(:phase_completed, %{duration_ms: duration_ms}, %{phase: :drain})

    Logger.info("[shutdown] phase=finalise")
    {:noreply, enter_finalise(state, now)}
  end

  def handle_info({:drain_done, _}, state) do
    # Phase advanced (timeout or force) — discard the stale report.
    {:noreply, state}
  end

  def handle_info(:do_finalise, %__MODULE__{phase: :finalising} = state) do
    now = monotonic_ms()
    safe_close_audit_writer()
    safe_flush_fsync_batcher()
    safe_stop_transports()
    duration_ms = monotonic_ms() - now
    emit_telemetry(:phase_completed, %{duration_ms: duration_ms}, %{phase: :finalise})

    finish(state, finalise_outcome(state))
  end

  def handle_info(:do_finalise, state), do: {:noreply, state}

  def handle_info(:phase_timeout, %__MODULE__{phase: phase} = state)
      when phase in [:quiescing, :draining] do
    still_local = Enum.flat_map(state.captured_local, fn {_ns, ids} -> ids end)

    Logger.error("[shutdown] timeout phase=#{phase} still_local=#{length(still_local)}")

    emit_telemetry(:timeout, %{}, %{phase: external_phase(phase), still_local: still_local})

    # The drain runner is abandoned, not awaited: its late `:drain_done`
    # lands in the stale clause. The hub still has to stop before finalise.
    if phase == :draining do
      safe_finalise_step(:stop_hub_manager, &HubManager.stop_hub_manager/0)
    end

    Logger.info("[shutdown] phase=finalise")
    {:noreply, enter_finalise(%{state | still_local: still_local}, monotonic_ms())}
  end

  def handle_info(:phase_timeout, state), do: {:noreply, state}

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- Phase helpers ----------

  defp transition(state, new_phase, now) do
    :ets.insert(@ets_table, {:phase, new_phase})
    %{state | phase: new_phase, phase_start_ms: now}
  end

  defp enter_finalise(state, now) do
    emit_telemetry(:phase_started, %{}, %{phase: :finalise})
    state = transition(state, :finalising, now)
    send(self(), :do_finalise)
    state
  end

  defp arm_timeout(state) do
    if is_reference(state.timeout_timer), do: Process.cancel_timer(state.timeout_timer)
    timer = Process.send_after(self(), :phase_timeout, state.timeout_ms)
    %{state | timeout_timer: timer}
  end

  defp finalise_outcome(state) do
    cond do
      state.outcome != nil -> state.outcome
      Keyword.get(state.opts, :force, false) -> :ok
      state.still_local == [] -> :ok
      true -> {:partial, state.still_local}
    end
  end

  defp finish(state, outcome) do
    if is_reference(state.timeout_timer), do: Process.cancel_timer(state.timeout_timer)
    duration_ms = monotonic_ms() - state.start_ms
    outcome_tag = outcome_tag(outcome, state)

    Logger.info("[shutdown] complete duration_ms=#{duration_ms} outcome=#{outcome_tag}")

    emit_telemetry(
      :complete,
      %{duration_ms: duration_ms},
      %{outcome: outcome_tag, reason: state.reason}
    )

    Enum.each(state.waiters, fn from -> GenServer.reply(from, outcome) end)
    :ets.insert(@ets_table, {:phase, :done})

    {:noreply, %{state | phase: :done, outcome: outcome, waiters: []}}
  end

  defp outcome_tag(:ok, %{opts: opts}) do
    if Keyword.get(opts, :force, false), do: :forced, else: :clean
  end

  defp outcome_tag({:partial, _}, _state), do: :partial
  defp outcome_tag(_other, _state), do: :clean

  defp external_phase(:quiescing), do: :quiesce
  defp external_phase(:draining), do: :drain
  defp external_phase(:finalising), do: :finalise
  defp external_phase(_), do: nil

  # ---------- Drain helpers ----------

  defp capture_local_operatives() do
    HubManager.get_running_namespaces()
    |> Enum.into(%{}, fn ns ->
      hub_id = Identificator.hub(ns)

      ids =
        case ProcessHub.is_alive?(hub_id) do
          true ->
            ProcessHub.process_list(hub_id, :local)
            |> Enum.map(fn {operative_id, _pid} -> operative_id end)

          false ->
            []
        end

      {ns, ids}
    end)
  end

  defp drain_budget(state, now) do
    max(
      state.timeout_ms - (now - state.start_ms) - @drain_budget_slack_ms,
      @drain_budget_slack_ms
    )
  end

  # Runs the per-hub drains off the coordinator loop; one `:drain_progress`
  # per completed hub, one `{:drain_done, totals}` at the end. Unlinked on
  # purpose — a crash here is covered by the `:phase_timeout` backstop.
  defp start_drain_runner(coordinator, namespaces, budget, phase_start_ms) do
    spawn(fn ->
      total = length(namespaces)

      stream =
        Task.async_stream(
          namespaces,
          fn ns -> drain_hub(ns, budget) end,
          max_concurrency: total,
          timeout: budget + 2_000,
          on_timeout: :kill_task
        )

      {migrated, forced} =
        namespaces
        |> Enum.zip(stream)
        |> Enum.with_index(1)
        |> Enum.reduce({0, 0}, fn {{ns, task_result}, idx}, {m_acc, f_acc} ->
          # A task killed by the async_stream timeout degrades like any
          # other drain error.
          result =
            case task_result do
              {:ok, drain_result} -> drain_result
              {:exit, reason} -> {:error, {:task_exit, reason}}
            end

          {m, f} = fold_drain_outcome(ns, result)

          emit_telemetry(
            :drain_progress,
            %{
              migrated: m,
              forced: f,
              remaining_hubs: total - idx,
              elapsed_ms: monotonic_ms() - phase_start_ms
            },
            %{namespace: ns}
          )

          {m_acc + m, f_acc + f}
        end)

      send(coordinator, {:drain_done, %{migrated: migrated, forced: forced}})
    end)
  end

  defp drain_hub(ns, budget) do
    hub_id = Identificator.hub(ns)
    deadline = monotonic_ms() + budget

    if ProcessHub.is_alive?(hub_id) do
      drain_timeout = max(budget - @forced_handover_grace_ms, 1_000)
      result = ProcessHub.Service.Migration.drain(hub_id, timeout: drain_timeout)

      # `drain/2` returns once the deferred list is empty, but the migration
      # start requests are async and HotSwap stops a local child only AFTER
      # the remote start succeeded. Locally-empty is the completion signal;
      # stopping the hub earlier kills mid-flight handovers.
      with {:ok, _} <- result, do: await_local_children_gone(hub_id, deadline)

      result
    else
      {:error, :no_target_nodes}
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp await_local_children_gone(hub_id, deadline) do
    cond do
      ProcessHub.process_list(hub_id, :local) == [] ->
        :ok

      monotonic_ms() >= deadline ->
        :deadline

      true ->
        Process.sleep(100)
        await_local_children_gone(hub_id, deadline)
    end
  catch
    _kind, _reason -> :ok
  end

  # One hub's drain verdict → `{migrated, forced}`. Every error degrades to
  # the no-peer path: the operatives terminate with the hub supervisor and
  # the next boot restores them from on-disk WAL/snapshot — never a wedge.
  defp fold_drain_outcome(ns, {:ok, %{migrated: migrated, forced: forced}}) do
    if forced > 0 do
      Logger.warning("[shutdown] drain ns=#{ns} force-migrated #{forced} deferred operative(s)")
    end

    {migrated, forced}
  end

  defp fold_drain_outcome(_ns, {:error, :no_target_nodes}) do
    Logger.info(
      "[shutdown] drain skipped (no peers); state will be restored on next boot via on-disk WAL/snapshot"
    )

    {0, 0}
  end

  defp fold_drain_outcome(ns, {:error, reason}) do
    Logger.warning(
      "[shutdown] drain ns=#{ns} degraded (#{inspect(reason)}); " <>
        "local operatives terminate and restore on next boot"
    )

    {0, 0}
  end

  defp safe_close_audit_writer() do
    if Process.whereis(Flezha.Observability.AuditLog.Writer) do
      safe_finalise_step(:audit_log_writer, fn ->
        Flezha.Observability.AuditLog.Writer.close(@finalise_call_timeout_ms)
      end)
    else
      :ok
    end
  end

  defp safe_flush_fsync_batcher() do
    if Process.whereis(FsyncBatcher) do
      safe_finalise_step(:fsync_batcher, fn ->
        GenServer.call(FsyncBatcher, :flush_all, @finalise_call_timeout_ms)
      end)
    else
      :ok
    end
  end

  defp safe_stop_transports() do
    safe_finalise_step(:transport, fn -> TransportServer.stop_all() end)
  end

  defp safe_finalise_step(label, fun) do
    try do
      fun.()
    catch
      kind, reason ->
        Logger.warning("[shutdown] #{label} #{kind}: #{inspect(reason)}")
        :ok
    end
  end

  # The one home of the shutdown-budget default — `HubManager.consent_settings/0`
  # derives `max_defer_time` from it so the two never drift.
  @doc false
  def configured_timeout_ms() do
    Application.get_env(:flezha, :graceful_shutdown_timeout_ms, 60_000)
  end

  defp monotonic_ms(), do: System.monotonic_time(:millisecond)

  defp emit_telemetry(event, measurements, metadata) do
    :telemetry.execute([:flezha, :shutdown, event], measurements, metadata)
  rescue
    _ -> :ok
  end
end

defmodule Flezha.Shutdown.Coordinator do
  @moduledoc """
  Drives the controller's three-phase graceful shutdown
  (`quiesce → drain → finalise`) on `shutdown/2`.

  - **Quiesce** marks the node as quiescing (`Flezha.Shutdown.quiesced?/2`)
    so `Flezha.Service.RelayManager.start_relay/2` returns
    `{:error, :shutting_down}` and `Flezha.Runner.FleetManager`
    rejects new runner connect attempts, and declares `{:node_draining, …}`
    to the convergence loop so it plans nothing onto this node meanwhile.
  - **Drain** is the `:node` walk of the shutdown plan: it migrates
    locally-hosted Relays to peers while they are still alive —
    `ProcessHub.Service.Migration.drain/2` per running namespace hub, through
    the one drain seam, redistributes each child through the migration-consent
    gate with its namespace's migration strategy, force-migrates what still
    defers at the deadline, and waits until each moved child has arrived. Solo nodes (`{:error, :no_target_nodes}`) skip the migration
    wait; other drain preconditions degrade to the same path rather than
    wedging shutdown.
  - **Finalise** is the rest of the `:node` walk, the rows from order 20:
    the fsync flush, the audit log close, then the hub supervisor and the
    transports, terminated in `Flezha.Supervisor` so neither is restarted.
    A failed row is logged and the next one runs. `shutdown_and_stop/3` stops
    the transports after its report instead.

  The drain is bounded by `:flezha, :graceful_shutdown_timeout_ms`, finalise
  by the headroom the orchestrator grace keeps above it. Idempotent —
  concurrent callers register as waiters and receive the same
  outcome.

  Subscribe to `[:flezha, :shutdown, :*]` telemetry events for
  observability; structured logs accompany each phase boundary.
  """

  use GenServer, shutdown: 30_000
  use Flezha.Process.Timer, keys: [:phase_timeout]
  require Logger

  alias Flezha.Config

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown
  alias Flezha.Shutdown.SignalHandler

  @finalise_from 20
  # The finalise rows a reporting stop runs after its report: the transports
  # carry that report to a `flz` on the control socket.
  @after_report [:transports]
  @finalise_target %{supervisor: Flezha.Supervisor}

  defstruct phase: :idle,
            reason: nil,
            opts: [],
            timeout_ms: nil,
            start_ms: nil,
            phase_start_ms: nil,
            waiters: [],
            captured_local: %{},
            still_local: [],
            walk: nil,
            outcome: nil,
            timers: %{}

  # ---------- Public API ----------

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
  Drain with `reason`, hand the outcome to `report`, then stop the node.

  This is how a signal and `flz controller stop` end a controller. `report`
  runs before the transports stop and before `:init.stop/0`, so a caller can
  print the outcome while the node still answers, also to a `flz` on the
  control socket, and its answer is returned. A coordinator that exits is
  reported as `{:exit, reason}` and the node is stopped anyway. The stop
  re-enters the application's `prep_stop/1`, whose drain lands on the
  `phase: :done` short-circuit.
  """
  @spec shutdown_and_stop(atom(), keyword(), (term() -> answer)) :: answer when answer: term()
  def shutdown_and_stop(reason, opts \\ [], report \\ fn _outcome -> :ok end) do
    outcome =
      try do
        shutdown(reason, Keyword.put(opts, :defer, @after_report))
      catch
        :exit, exit_reason -> {:exit, exit_reason}
      end

    answer = report.(outcome)
    _ = finalise_rows(@finalise_target, finalise_budget_ms(), &(&1.name in @after_report))
    :init.stop()
    answer
  end

  @doc """
  Snapshot of the coordinator state — phase, configuration knobs,
  and signal-handler status. Read by `flz controller status`.
  """
  @spec status() :: map()
  def status() do
    GenServer.call(__MODULE__, :status, Durations.get(:shutdown_coordinator_call_ms))
  end

  @doc """
  Walk the finalise rows in the caller under the finalise budget, a failed
  row logged and the next one run. The application's stop does this itself
  when no coordinator drives the node.
  """
  @spec finalise(map()) :: {:ok, Shutdown.report()} | {:partial, Shutdown.report()}
  def finalise(target \\ @finalise_target),
    do: walk({:finalise, []}, target, finalise_budget_ms())

  @doc false
  def __reset_for_tests__() do
    GenServer.call(__MODULE__, :__reset_for_tests__, Durations.get(:shutdown_coordinator_call_ms))
  end

  # ---------- GenServer ----------

  @impl true
  def init(_), do: {:ok, %__MODULE__{}}

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

    if Keyword.get(opts, :force, false) do
      Logger.info("[shutdown] forced (skipping drain); phase=finalise")
      {:noreply, enter_finalise(state, now)}
    else
      Logger.info("[shutdown] phase=quiesce reason=#{inspect(reason)}")

      state =
        state
        |> transition(:quiescing, now)
        |> arm_timer(:phase_timeout, timeout_ms, :phase_timeout)

      # The loop is told the node is leaving, so it plans nothing onto it
      # while the drain moves its relays away.
      Facts.declare({:node_draining, node(), %{deadline: now + timeout_ms}})
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
    _ = cancel_timer(state, :phase_timeout)
    stop_walk(state)
    Shutdown.node_phase(:idle)
    Facts.declare({:node_drained, node(), %{}})
    {:reply, :ok, %__MODULE__{}}
  end

  def handle_call(:status, _from, state) do
    phase = external_phase(state.phase)

    reply = %{
      shutdown_in_progress: phase != nil,
      shutdown_phase: phase,
      signal_handler_status: SignalHandler.status(),
      graceful_shutdown_enabled: Config.get(:graceful_shutdown_enabled),
      graceful_shutdown_timeout_ms: configured_timeout_ms()
    }

    {:reply, reply, state}
  end

  @impl true
  def handle_info(:do_quiesce, state) do
    now = monotonic_ms()
    phase_completed(state, :quiesce, now)

    captured = capture_local_relays()
    locally_hosted = Enum.reduce(captured, 0, fn {_ns, ids}, acc -> acc + length(ids) end)

    Logger.info("[shutdown] phase=drain locally_hosted=#{locally_hosted}")
    emit_telemetry(:phase_started, %{}, %{phase: :drain})

    state = transition(state, :draining, now)
    state = %{state | captured_local: captured}

    if captured == %{} do
      # Nothing was running locally; drain trivially complete.
      drain_done(state, %{migrated: 0, forced: 0})
    else
      target = %{namespaces: Map.keys(captured), phase_start_ms: now}
      {:noreply, start_walk(state, :drain, target, drain_budget(state, now))}
    end
  end

  def handle_info(
        {:walk_done, ref, report},
        %__MODULE__{phase: :draining, walk: {_, ref}} = state
      ),
      do: drain_done(state, Map.get(report.results, :drain, %{migrated: 0, forced: 0}))

  def handle_info(
        {:walk_done, ref, _report},
        %__MODULE__{phase: :finalising, walk: {_, ref}} = state
      ) do
    phase_completed(state, :finalise, monotonic_ms())
    finish(state, finalise_outcome(state))
  end

  # A report from a walk that is no longer this shutdown's: discarded.
  def handle_info({:walk_done, _ref, _report}, state), do: {:noreply, state}

  def handle_info(:phase_timeout, %__MODULE__{phase: phase} = state)
      when phase in [:quiescing, :draining, :finalising] do
    still_local =
      if phase == :finalising,
        do: state.still_local,
        else: Enum.flat_map(state.captured_local, fn {_ns, ids} -> ids end)

    Logger.error("[shutdown] timeout phase=#{phase} still_local=#{length(still_local)}")

    emit_telemetry(:timeout, %{}, %{phase: external_phase(phase), still_local: still_local})

    # The walk ends with its budget; a report it sent first is discarded by its ref.
    stop_walk(state)
    state = %{state | still_local: still_local}

    if phase == :finalising do
      finish(state, finalise_outcome(state))
    else
      Logger.info("[shutdown] phase=finalise")
      {:noreply, enter_finalise(state, monotonic_ms())}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- Phase helpers ----------

  defp drain_done(state, %{migrated: migrated, forced: forced}) do
    Logger.info("[shutdown] drain migrated=#{migrated} forced=#{forced}")
    now = monotonic_ms()
    phase_completed(state, :drain, now)

    Logger.info("[shutdown] phase=finalise")
    {:noreply, enter_finalise(state, now)}
  end

  defp transition(state, new_phase, now) do
    Shutdown.node_phase(new_phase)
    %{state | phase: new_phase, phase_start_ms: now}
  end

  # Finalise has its own budget, so a drain that spent the shutdown timeout
  # leaves it whole.
  defp enter_finalise(state, now) do
    emit_telemetry(:phase_started, %{}, %{phase: :finalise})
    Facts.declare({:node_drained, node(), %{}})
    budget = finalise_budget_ms()

    state
    |> transition(:finalising, now)
    |> arm_timer(:phase_timeout, budget + Durations.get(:drain_budget_slack_ms), :phase_timeout)
    |> start_walk({:finalise, Keyword.get(state.opts, :defer, [])}, @finalise_target, budget)
  end

  defp phase_completed(state, phase, now) do
    emit_telemetry(:phase_completed, %{duration_ms: now - state.phase_start_ms}, %{phase: phase})
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
    state = cancel_timer(state, :phase_timeout)
    duration_ms = monotonic_ms() - state.start_ms
    outcome_tag = outcome_tag(outcome, state)

    Logger.info("[shutdown] complete duration_ms=#{duration_ms} outcome=#{outcome_tag}")

    emit_telemetry(
      :complete,
      %{duration_ms: duration_ms},
      %{outcome: outcome_tag, reason: state.reason}
    )

    Enum.each(state.waiters, fn from -> GenServer.reply(from, outcome) end)
    Shutdown.node_phase(:done)

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

  defp capture_local_relays() do
    HubManager.get_running_namespaces()
    |> Map.new(fn ns ->
      {ns, for({id, _pid} <- HubManager.local_children(ns, :relays), do: id)}
    end)
  end

  # The slack keeps the drain's answer inside the shutdown timeout and is the
  # floor, so `Migration.drain/2` always gets a positive deadline.
  defp drain_budget(state, now) do
    max(
      state.timeout_ms - (now - state.start_ms) - Durations.get(:drain_budget_slack_ms),
      Durations.get(:drain_budget_slack_ms)
    )
  end

  # The headroom the orchestrator grace keeps above the shutdown timeout.
  defp finalise_budget_ms(),
    do: Durations.get(:orchestrator_grace_ms) - configured_timeout_ms()

  # The drain is the `:node` rows below 20 and halts on a failure; finalise is
  # the rows from 20 less the deferred ones, each failure logged and the next
  # row run.
  defp walk(:drain, target, budget),
    do: Shutdown.run(:node, target, budget, rows(&(&1.order < @finalise_from)))

  defp walk({:finalise, deferred}, target, budget),
    do: finalise_rows(target, budget, &(&1.order >= @finalise_from and &1.name not in deferred))

  defp finalise_rows(target, budget, row?),
    do: Shutdown.run(:node, target, budget, rows(row?), on_error: :continue)

  defp rows(row?), do: Enum.filter(Shutdown.steps(:node), row?)

  # Runs a walk off the coordinator loop: the drain blocks up to its deadline
  # and a hub stop up to its own bound. One `{:walk_done, ref, report}` lands
  # here at the end. Unlinked on purpose — a crash here is covered by the
  # `:phase_timeout` backstop. Killing the drain strands no handover: the
  # hub's own processes start each move and stop its source only once the
  # target runs.
  defp start_walk(state, phase, target, budget) do
    coordinator = self()
    ref = make_ref()

    pid =
      spawn(fn ->
        {_outcome, report} = walk(phase, target, budget)
        send(coordinator, {:walk_done, ref, report})
      end)

    %{state | walk: {pid, ref}}
  end

  defp stop_walk(%__MODULE__{walk: {pid, _ref}}), do: Process.exit(pid, :kill)
  defp stop_walk(_state), do: true

  # The one home of the shutdown-budget default — `HubManager.consent_settings/0`
  # derives `max_defer_time` from it so the two never drift.
  @doc false
  def configured_timeout_ms() do
    Durations.get(:graceful_shutdown_timeout_ms)
  end

  defp monotonic_ms(), do: System.monotonic_time(:millisecond)

  defp emit_telemetry(event, measurements, metadata) do
    :telemetry.execute([:flezha, :shutdown, event], measurements, metadata)
  rescue
    _ -> :ok
  end
end

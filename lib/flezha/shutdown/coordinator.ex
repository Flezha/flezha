defmodule Flezha.Shutdown.Coordinator do
  @moduledoc """
  Drives the controller's three-phase graceful shutdown
  (`quiesce → drain → finalise`) on `shutdown/2`.

  - **Quiesce** marks the node as quiescing (`Flezha.Shutdown.quiesced?/2`)
    so `Flezha.Service.OperatorManager.start_operator/2` returns
    `{:error, :shutting_down}` and `Flezha.Runner.FleetManager`
    rejects new runner connect attempts, and declares `{:node_draining, …}`
    to the convergence loop so it plans nothing onto this node meanwhile.
  - **Drain** is the `:node` walk of the shutdown plan: it migrates
    locally-hosted Operators to peers while they are still alive —
    `ProcessHub.Service.Migration.drain/2` per running namespace hub, through
    the one drain seam, redistributes each child through the migration-consent
    gate (live HotSwap handover) and force-migrates what still defers at the
    deadline. The hub supervisor stops only after the drains return. Solo
    nodes (`{:error, :no_target_nodes}`) skip the migration wait; other drain
    preconditions degrade to the same path rather than wedging shutdown.
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

  use GenServer, shutdown: 30_000
  use Flezha.Process.Timer, keys: [:phase_timeout]
  require Logger

  alias Flezha.Config

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Identificator
  alias Flezha.Observability.AuditLog.Writer
  alias Flezha.Process.Hub
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown
  alias Flezha.Shutdown.SignalHandler
  alias Flezha.State.FsyncBatcher
  alias Flezha.Transport.Server, as: TransportServer

  # Slack reserved off the drain budget so finalise still runs inside the
  # overall bound; also the floor so `Migration.drain/2` always gets a
  # positive deadline.

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
  runs before `:init.stop/0`, so a caller can print the outcome while the node
  still answers, and its answer is returned. A coordinator that exits is
  reported as `{:exit, reason}` and the node is stopped anyway. The stop
  re-enters `Flezha.Application.prep_stop/1`, whose drain lands on the
  `phase: :done` short-circuit.
  """
  @spec shutdown_and_stop(atom(), keyword(), (term() -> answer)) :: answer when answer: term()
  def shutdown_and_stop(reason, opts \\ [], report \\ fn _outcome -> :ok end) do
    outcome =
      try do
        shutdown(reason, opts)
      catch
        :exit, exit_reason -> {:exit, exit_reason}
      end

    answer = report.(outcome)
    :init.stop()
    answer
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

  # ---------- GenServer ----------

  @impl true
  def init(_) do
    Shutdown.create_table()
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

    if Keyword.get(opts, :force, false) do
      Logger.info("[shutdown] forced (skipping drain); phase=finalise")
      {:noreply, enter_finalise(state, now)}
    else
      Logger.info("[shutdown] phase=quiesce reason=#{inspect(reason)}")
      state = transition(state, :quiescing, now)
      # The loop is told the node is leaving, so it plans nothing onto it
      # while the drain moves its operators away.
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
    Shutdown.node_phase(:idle)
    Facts.declare({:node_drained, node(), %{}})
    {:reply, :ok, %__MODULE__{}}
  end

  def handle_call(:status, _from, state) do
    reply = %{
      shutdown_in_progress: state.phase in [:quiescing, :draining, :finalising],
      shutdown_phase: external_phase(state.phase),
      signal_handler_status: SignalHandler.status(),
      graceful_shutdown_enabled: Config.get(:graceful_shutdown_enabled),
      graceful_shutdown_timeout_ms: configured_timeout_ms()
    }

    {:reply, reply, state}
  end

  @impl true
  def handle_info(:do_quiesce, state) do
    now = monotonic_ms()
    duration_ms = now - state.phase_start_ms
    emit_telemetry(:phase_completed, %{duration_ms: duration_ms}, %{phase: :quiesce})

    captured = capture_local_operators()
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
    Facts.declare({:node_drained, node(), %{}})
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
    Shutdown.node_phase(new_phase)
    %{state | phase: new_phase, phase_start_ms: now}
  end

  defp enter_finalise(state, now) do
    emit_telemetry(:phase_started, %{}, %{phase: :finalise})
    state = transition(state, :finalising, now)
    send(self(), :do_finalise)
    state
  end

  defp arm_timeout(state),
    do: arm_timer(state, :phase_timeout, state.timeout_ms, :phase_timeout)

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

  defp capture_local_operators() do
    HubManager.get_running_namespaces()
    |> Enum.into(%{}, fn ns ->
      hub_id = Identificator.hub(ns)

      ids =
        case Hub.alive?(hub_id) do
          true ->
            ProcessHub.process_list(hub_id, :local)
            |> Enum.map(fn {operator_id, _pid} -> operator_id end)

          false ->
            []
        end

      {ns, ids}
    end)
  end

  defp drain_budget(state, now) do
    max(
      state.timeout_ms - (now - state.start_ms) - Durations.get(:drain_budget_slack_ms),
      Durations.get(:drain_budget_slack_ms)
    )
  end

  # Runs the `:node` walk off the coordinator loop: the drain step drains
  # every hub concurrently and emits one `:drain_progress` per hub; one
  # `{:drain_done, totals}` lands here at the end. Unlinked on purpose — a
  # crash here is covered by the `:phase_timeout` backstop.
  defp start_drain_runner(coordinator, namespaces, budget, phase_start_ms) do
    spawn(fn ->
      {_outcome, report} =
        Shutdown.run(:node, %{namespaces: namespaces, phase_start_ms: phase_start_ms}, budget)

      totals = Map.get(report.results, :drain, %{migrated: 0, forced: 0})
      send(coordinator, {:drain_done, totals})
    end)
  end

  defp safe_close_audit_writer() do
    if Process.whereis(Flezha.Observability.AuditLog.Writer) do
      safe_finalise_step(:audit_log_writer, fn ->
        Writer.close(Durations.get(:shutdown_finalise_call_ms))
      end)
    else
      :ok
    end
  end

  defp safe_flush_fsync_batcher() do
    if Process.whereis(FsyncBatcher) do
      safe_finalise_step(:fsync_batcher, fn ->
        GenServer.call(FsyncBatcher, :flush_all, Durations.get(:shutdown_finalise_call_ms))
      end)
    else
      :ok
    end
  end

  defp safe_stop_transports() do
    safe_finalise_step(:transport, fn -> TransportServer.stop_all() end)
  end

  defp safe_finalise_step(label, fun) do
    fun.()
  catch
    kind, reason ->
      Logger.warning("[shutdown] #{label} #{kind}: #{inspect(reason)}")
      :ok
  end

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

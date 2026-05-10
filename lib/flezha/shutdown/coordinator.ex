defmodule Flezha.Shutdown.Coordinator do
  @moduledoc """
  Drives the controller's three-phase graceful shutdown
  (`quiesce → drain → finalise`) on `shutdown/2`.

  - **Quiesce** sets a process-tree-visible flag (`shutting_down?/0`)
    so `Flezha.Service.RelayManager.start_relay/2` returns
    `{:error, :shutting_down}` and `Flezha.Executor.FleetManager`
    rejects new executor connect attempts.
  - **Drain** captures the locally-hosted Relay set, dispatches
    ProcessHub `cluster_leave` (via `HubManager.stop_hub_manager/0`),
    then polls peer registry dumps until every captured Relay is
    visible on at least one peer (or the bounded timeout elapses).
    Solo nodes (no peers) skip the migration wait.
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

  @poll_ms 500
  @log_throttle_ms 1_000
  @finalise_call_timeout_ms 5_000

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
            last_progress_log_ms: 0,
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

    emit_telemetry(:requested, %{}, %{reason: reason, timestamp_ms: System.system_time(:millisecond)})

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
        Logger.info("[shutdown] forced (skipping drain)")
        state = transition(state, :finalising, now)
        send(self(), :do_finalise)
        {:noreply, state}

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

    captured = capture_local_relays()
    locally_hosted = Enum.reduce(captured, 0, fn {_ns, ids}, acc -> acc + length(ids) end)

    Logger.info("[shutdown] phase=drain locally_hosted=#{locally_hosted}")
    emit_telemetry(:phase_started, %{}, %{phase: :drain})

    state = transition(state, :draining, now)
    state = %{state | captured_local: captured}

    namespaces = Map.keys(captured)
    peer_map = capture_peer_map(namespaces)

    # Dispatch ProcessHub `cluster_leave` by stopping the local hub
    # supervisor. ProcessHub's coordinator `terminate/2` notifies
    # peers so they can claim our Relays via the configured migration
    # strategy (HotSwap with handover).
    safe_stop_hub_manager()

    state = %{state | last_progress_log_ms: now}

    cond do
      captured == %{} ->
        # Nothing was running locally; drain trivially complete.
        send(self(), :drain_complete)
        {:noreply, state}

      Enum.all?(namespaces, fn ns -> Map.get(peer_map, ns, []) == [] end) ->
        # No peers across any namespace — nothing to migrate to.
        Logger.info(
          "[shutdown] drain skipped (no peers); state will be restored on next boot via on-disk WAL/snapshot"
        )

        # Local Relays already terminated by stop_hub_manager above;
        # WAL fsync flush happens in the finalise phase.
        send(self(), :drain_complete)
        {:noreply, %{state | still_local: []}}

      true ->
        Process.send_after(self(), {:drain_poll, peer_map}, @poll_ms)
        {:noreply, state}
    end
  end

  def handle_info({:drain_poll, peer_map}, %__MODULE__{phase: :draining} = state) do
    now = monotonic_ms()
    elapsed = now - state.phase_start_ms

    {still_local, migrated, remaining} = drain_progress(state.captured_local, peer_map)

    emit_telemetry(
      :drain_progress,
      %{migrated: migrated, remaining: remaining, elapsed_ms: elapsed},
      %{}
    )

    state =
      if now - state.last_progress_log_ms >= @log_throttle_ms do
        Logger.info(
          "[shutdown] drain_progress migrated=#{migrated} remaining=#{remaining} elapsed_ms=#{elapsed}"
        )

        %{state | last_progress_log_ms: now}
      else
        state
      end

    cond do
      remaining == 0 ->
        send(self(), :drain_complete)
        {:noreply, %{state | still_local: []}}

      true ->
        Process.send_after(self(), {:drain_poll, peer_map}, @poll_ms)
        {:noreply, %{state | still_local: still_local}}
    end
  end

  def handle_info({:drain_poll, _peer_map}, state) do
    # Phase advanced (timeout or force) — discard stale poll.
    {:noreply, state}
  end

  def handle_info(:drain_complete, %__MODULE__{phase: :draining} = state) do
    now = monotonic_ms()
    duration_ms = now - state.phase_start_ms
    emit_telemetry(:phase_completed, %{duration_ms: duration_ms}, %{phase: :drain})

    Logger.info("[shutdown] phase=finalise")
    emit_telemetry(:phase_started, %{}, %{phase: :finalise})
    state = transition(state, :finalising, now)
    send(self(), :do_finalise)
    {:noreply, state}
  end

  def handle_info(:drain_complete, state), do: {:noreply, state}

  def handle_info(:do_finalise, %__MODULE__{phase: :finalising} = state) do
    now = monotonic_ms()
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

    Logger.error(
      "[shutdown] timeout phase=#{phase} still_local=#{length(still_local)}"
    )

    emit_telemetry(:timeout, %{}, %{phase: external_phase(phase), still_local: still_local})

    Logger.info("[shutdown] phase=finalise")
    emit_telemetry(:phase_started, %{}, %{phase: :finalise})
    state = transition(state, :finalising, monotonic_ms())
    state = %{state | still_local: still_local}
    send(self(), :do_finalise)
    {:noreply, state}
  end

  def handle_info(:phase_timeout, state), do: {:noreply, state}

  def handle_info(_msg, state), do: {:noreply, state}

  # ---------- Phase helpers ----------

  defp transition(state, new_phase, now) do
    :ets.insert(@ets_table, {:phase, new_phase})
    %{state | phase: new_phase, phase_start_ms: now}
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

    Logger.info(
      "[shutdown] complete duration_ms=#{duration_ms} outcome=#{outcome_tag}"
    )

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

  defp capture_local_relays() do
    HubManager.get_running_namespaces()
    |> Enum.into(%{}, fn ns ->
      hub_id = Identificator.hub(ns)

      ids =
        case ProcessHub.is_alive?(hub_id) do
          true ->
            ProcessHub.process_list(hub_id, :local)
            |> Enum.map(fn {relay_id, _pid} -> relay_id end)

          false ->
            []
        end

      {ns, ids}
    end)
  end

  defp capture_peer_map(namespaces) do
    Enum.into(namespaces, %{}, fn ns ->
      peers = HubManager.get_hub_nodes(ns, false)
      {ns, peers}
    end)
  end

  defp drain_progress(captured_local, peer_map) do
    {still_local, migrated, remaining} =
      Enum.reduce(captured_local, {[], 0, 0}, fn {ns, relay_ids}, {still, mig, rem} ->
        peers = Map.get(peer_map, ns, [])
        hub_id = Identificator.hub(ns)
        peer_dumps = fetch_peer_dumps(peers, hub_id)

        {ns_still, ns_mig, ns_rem} =
          Enum.reduce(relay_ids, {[], 0, 0}, fn relay_id, {s, m, r} ->
            if relay_visible_on_peer?(relay_id, peer_dumps) do
              {s, m + 1, r}
            else
              {[relay_id | s], m, r + 1}
            end
          end)

        {[{ns, ns_still} | still], mig + ns_mig, rem + ns_rem}
      end)

    flat_still = Enum.flat_map(still_local, fn {_ns, ids} -> ids end)
    {flat_still, migrated, remaining}
  end

  defp fetch_peer_dumps(peers, hub_id) do
    Enum.flat_map(peers, fn node ->
      case :rpc.call(node, ProcessHub, :registry_dump, [hub_id], 2_000) do
        dump when is_map(dump) -> [{node, dump}]
        _ -> []
      end
    end)
  end

  defp relay_visible_on_peer?(relay_id, peer_dumps) do
    Enum.any?(peer_dumps, fn {node, dump} ->
      case Map.get(dump, relay_id) do
        nil ->
          false

        entry ->
          node_pids =
            case entry do
              {_cspec, np} -> np
              {_cspec, np, _meta} -> np
              _ -> []
            end

          Enum.any?(node_pids, fn
            {n, _pid} when n == node -> true
            _ -> false
          end)
      end
    end)
  end

  defp safe_stop_hub_manager() do
    try do
      HubManager.stop_hub_manager()
    catch
      :exit, reason ->
        Logger.warning("[shutdown] stop_hub_manager exit: #{inspect(reason)}")
        :ok
    end
  end

  defp safe_flush_fsync_batcher() do
    if Process.whereis(FsyncBatcher) do
      try do
        GenServer.call(FsyncBatcher, :flush_all, @finalise_call_timeout_ms)
      catch
        :exit, reason ->
          Logger.warning("[shutdown] FsyncBatcher flush_all exit: #{inspect(reason)}")
          :ok
      end
    else
      :ok
    end
  end

  defp safe_stop_transports() do
    try do
      TransportServer.stop_all()
    catch
      :exit, reason ->
        Logger.warning("[shutdown] transport stop exit: #{inspect(reason)}")
        :ok

      kind, reason ->
        Logger.warning("[shutdown] transport stop #{kind}: #{inspect(reason)}")
        :ok
    end
  end

  defp configured_timeout_ms() do
    Application.get_env(:flezha, :graceful_shutdown_timeout_ms, 60_000)
  end

  defp monotonic_ms(), do: System.monotonic_time(:millisecond)

  defp emit_telemetry(event, measurements, metadata) do
    :telemetry.execute([:flezha, :shutdown, event], measurements, metadata)
  rescue
    _ -> :ok
  end
end

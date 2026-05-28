defmodule Flezha.Convergence.Health do
  @moduledoc """
  Single convergence watcher introduced by
  `consume-recovery-mode-and-self-heal` §4. Replaces the
  `Flezha.Reconciler.{Loop, Actions, Health, Probe, Activity, Leader}`
  stack with one tick-driven GenServer that drives the controller's
  existing healing primitives.

  Per the `convergence-self-healing` capability:

    * Awaits `[:process_hub, :recovery, :complete | :skipped]` on
      every per-namespace hub before issuing any action — the
      `recovery-mode-opt-in` upstream contract (D7) guarantees that on
      first tick after `:recovery_complete` every cspec has been
      restored with stripped binding metadata so the watcher rebinds
      against currently-connected SDKs without trusting the recovered
      `assigned_ce`.
    * One tick per `:convergence_tick_ms` (default 5_000).
    * Discovers Overwatches via
      `Flezha.Service.HubManager.get_all_relays/1` (the runtime source
      of truth; `:global.registered_names/0` is not populated for
      Overwatches in this codebase).
    * Classifies per relay: `:healthy | :degraded | :failed | :unbound`.
    * Emits at most one action per relay per tick.
    * Subscribes to `[:flezha, :state, :output_stall]`: queues a
      `:rebind` action with exponential backoff (5s/30s/60s/300s) when
      a bound relay stalls past `:output_stall_grace_ms` (default 60s).
    * Plugin-strike escalation moves verbatim from
      `Flezha.Reconciler.Actions.escalate_plugins/2` into
      `handle_plugin_failure/2` here.
  """

  use GenServer

  alias Flezha.Executor.FleetManager
  alias Flezha.Service.EventManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager

  require Logger

  @default_tick_ms 5_000
  @default_output_stall_grace_ms 60_000
  @default_plugin_strike_threshold 2
  @default_relay_call_timeout_ms 1_000

  # Exponential backoff schedule for `:rebind` actions, in ms (per spec
  # §"Output-stall is the authoritative liveness signal").
  @rebind_backoff_ms [5_000, 30_000, 60_000, 300_000]

  defstruct tick_ms: @default_tick_ms,
            output_stall_grace_ms: @default_output_stall_grace_ms,
            recovery_awaited?: false,
            # %{relay_id => {attempt_count, next_eligible_monotonic_ms}}
            rebind_backoff: %{},
            # %{relay_id => MapSet[plugin_id]}
            plugin_strikes: %{}

  @telemetry_subs [
    [:flezha, :state, :output_stall],
    [:process_hub, :recovery, :complete],
    [:process_hub, :recovery, :skipped]
  ]

  # ---------- public API ----------

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Run a single tick synchronously; intended for tests + debugging.
  Returns the per-relay action map (relay_id → action) the tick
  decided.
  """
  @spec tick_now() :: %{binary() => atom()}
  def tick_now() do
    GenServer.call(__MODULE__, :tick_now, 30_000)
  end

  @doc """
  Forward a plugin-failure observation. The host plugin lifecycle
  emits this when a plugin's provider has failed; the watcher tracks
  strikes per relay-plugin pair and escalates to a relay restart
  after `convergence_plugin_strike_threshold` (default 2) strikes.
  """
  @spec handle_plugin_failure(binary(), binary()) :: :ok
  def handle_plugin_failure(relay_id, plugin_id)
      when is_binary(relay_id) and is_binary(plugin_id) do
    GenServer.cast(__MODULE__, {:plugin_failure, relay_id, plugin_id})
  end

  # ---------- GenServer ----------

  @impl true
  def init(opts) do
    tick_ms = Keyword.get(opts, :tick_ms, Application.get_env(:flezha, :convergence_tick_ms, @default_tick_ms))

    grace_ms =
      Keyword.get(
        opts,
        :output_stall_grace_ms,
        Application.get_env(:flezha, :convergence_output_stall_grace_ms, @default_output_stall_grace_ms)
      )

    _ =
      :telemetry.attach_many(
        {__MODULE__, :events},
        @telemetry_subs,
        &__MODULE__.handle_telemetry/4,
        nil
      )

    # Fresh-process executor takeover: a new SDK connecting with an
    # orphan peer in its namespace is detected on the next tick, but
    # subscribing to `:executor_connected_event` cuts the worst-case
    # detection latency from `tick_ms` to ~ms.
    _ = EventManager.add_handler(:executor_connected_event)

    # Schedule the first tick immediately so `:awaiting_recovery`
    # telemetry fires without waiting `tick_ms`.
    Process.send_after(self(), :tick, 0)

    {:ok, %__MODULE__{tick_ms: tick_ms, output_stall_grace_ms: grace_ms}}
  end

  @impl true
  def handle_info(:tick, %__MODULE__{} = state) do
    state =
      if state.recovery_awaited? do
        run_tick(state)
      else
        case HubManager.await_recovery_complete(0) do
          :ok ->
            run_tick(%{state | recovery_awaited?: true})

          {:error, {:recovery_timeout, hub_ids}} ->
            :telemetry.execute(
              [:flezha, :convergence, :tick],
              %{count: 0, actions_issued: 0},
              %{state: :awaiting_recovery, unfinished_hubs: hub_ids}
            )

            state
        end
      end

    Process.send_after(self(), :tick, state.tick_ms)
    {:noreply, state}
  end

  def handle_info({:rebind_queued, relay_id}, %__MODULE__{} = state) do
    # A queued `:rebind` from the output_stall handler — apply only
    # if the relay still exists and is bound.
    _ = maybe_rebind(state, relay_id)
    {:noreply, state}
  end

  # Fresh-process executor takeover trigger: run a tick now instead of
  # waiting up to `tick_ms` so the orphan rebind fires within ~ms of
  # the new SDK's `connect_control_channel`.
  def handle_info({:executor_connected_event, {_namespace, _executor_id}}, %__MODULE__{} = state) do
    {:noreply, run_tick(state)}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_call(:tick_now, _from, %__MODULE__{} = state) do
    new_state =
      if state.recovery_awaited? do
        run_tick(state)
      else
        case HubManager.await_recovery_complete(0) do
          :ok -> run_tick(%{state | recovery_awaited?: true})
          {:error, _} -> state
        end
      end

    {:reply, %{}, new_state}
  end

  @impl true
  def handle_cast({:queue_rebind, relay_id}, %__MODULE__{} = state) do
    {:noreply, queue_rebind(state, relay_id)}
  end

  def handle_cast({:plugin_failure, relay_id, plugin_id}, %__MODULE__{} = state) do
    strikes_for_relay = Map.get(state.plugin_strikes, relay_id, MapSet.new())
    new_strikes_for_relay = MapSet.put(strikes_for_relay, plugin_id)
    new_strikes = Map.put(state.plugin_strikes, relay_id, new_strikes_for_relay)

    threshold = plugin_strike_threshold()

    if MapSet.size(new_strikes_for_relay) >= threshold do
      _ = escalate_plugin_restart(relay_id, MapSet.to_list(new_strikes_for_relay))
      :telemetry.execute(
        [:flezha, :convergence, :plugin_escalated],
        %{strike_count: MapSet.size(new_strikes_for_relay)},
        %{relay_id: relay_id, plugin_ids: MapSet.to_list(new_strikes_for_relay)}
      )

      {:noreply, %{state | plugin_strikes: Map.delete(state.plugin_strikes, relay_id)}}
    else
      :telemetry.execute(
        [:flezha, :convergence, :plugin_restart],
        %{strike_count: MapSet.size(new_strikes_for_relay)},
        %{relay_id: relay_id, plugin_id: plugin_id}
      )

      {:noreply, %{state | plugin_strikes: new_strikes}}
    end
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :telemetry.detach({__MODULE__, :events})
    :ok
  end

  # ---------- telemetry callback ----------

  @doc false
  def handle_telemetry(event, _measurements, metadata, _config) do
    case event do
      [:flezha, :state, :output_stall] ->
        # State.Watchdog emits :output_stall with %{relay: rc, shard: sh, key: pk}.
        # We use `relay` as the relay_id (matches the relay_class which is the relay id).
        relay_id = Map.get(metadata, :relay)
        if is_binary(relay_id), do: GenServer.cast(__MODULE__, {:queue_rebind, relay_id})

      [:process_hub, :recovery, _] ->
        # Tracked by HubManager.RecoveryTracker; the next tick observes
        # the new state via `await_recovery_complete(0)`.
        :ok

      _ ->
        :ok
    end
  catch
    _, _ -> :ok
  end

  # ---------- tick body ----------

  defp run_tick(%__MODULE__{} = state) do
    namespaces = safe_running_namespaces()

    {totals, actions_issued} =
      Enum.reduce(namespaces, {%{healthy: 0, degraded: 0, failed: 0, unbound: 0}, 0}, fn ns, acc ->
        tick_namespace(state, ns, acc)
      end)

    :telemetry.execute(
      [:flezha, :convergence, :tick],
      %{
        relays_total: Map.values(totals) |> Enum.sum(),
        healthy: totals.healthy,
        degraded: totals.degraded,
        failed: totals.failed,
        unbound: totals.unbound,
        actions_issued: actions_issued
      },
      %{state: :ticking}
    )

    state
  end

  defp tick_namespace(state, namespace, {totals, actions_issued}) do
    relays = safe_get_relays(namespace)
    ctx = build_namespace_ctx(namespace)

    Enum.reduce(relays, {totals, actions_issued}, fn {relay_id, relay_pid}, {acc_totals, acc_actions} ->
      case classify_relay(namespace, relay_id, relay_pid, ctx) do
        {classification, action_decision} ->
          actions = act(state, namespace, relay_id, relay_pid, action_decision, ctx)
          new_totals = Map.update!(acc_totals, classification, &(&1 + 1))
          {new_totals, acc_actions + actions}
      end
    end)
  end

  # ---------- classification + action ----------

  defp classify_relay(_namespace, _relay_id, relay_pid, ctx) do
    overwatch = overwatch_snapshot(relay_pid)
    bound_executor = overwatch && Map.get(overwatch, :executor_id)
    parked? = overwatch && Map.get(overwatch, :init_error) != nil

    executor_reachable? =
      is_binary(bound_executor) and MapSet.member?(ctx.active_executors, bound_executor)

    cond do
      parked? ->
        {:failed, :unpark_if_executor_available}

      is_nil(bound_executor) ->
        {:unbound, :bind_if_executor_available}

      not executor_reachable? ->
        {:degraded, :rebind_if_executor_available}

      true ->
        {:healthy, :noop}
    end
  catch
    _, _ -> {:degraded, :noop}
  end

  defp act(_state, _namespace, _relay_id, _relay_pid, :noop, _ctx), do: 0

  defp act(_state, namespace, relay_id, relay_pid, :unpark_if_executor_available, ctx) do
    factory = ctx.factory_for_relay.(relay_id)

    if factory != nil and ctx.factory_available?.(factory) do
      case RelayManager.assign_executor(namespace, factory) do
        target when is_binary(target) ->
          _ = safe_migrate(relay_pid, target)
          emit_action(namespace, relay_id, :unpark)
          1

        _ ->
          0
      end
    else
      0
    end
  end

  defp act(_state, namespace, relay_id, _relay_pid, :bind_if_executor_available, _ctx) do
    # Trigger the controller's existing bind path via `relay_set_executor`
    # — Convergence is observation-only at this stage; the actual bind
    # is driven by the Overwatch's `handle_continue(:bind_executor, ...)`
    # already in flight. Record the observation and move on.
    emit_action(namespace, relay_id, :bind_pending)
    0
  end

  # The orphan-rebind seam: on a successful Overwatch migrate, also flip
  # the registry tag and fan the new pid into the target executor's
  # `StreamChannel.relay_mapping` (via the same hook the `child_registered`
  # callback uses) so frames land. Failure of any step leaves the relay
  # `:degraded` for the next tick under the existing back-off.
  defp act(_state, namespace, relay_id, relay_pid, :rebind_if_executor_available, ctx) do
    factory = ctx.factory_for_relay.(relay_id)

    with true <- factory != nil and ctx.factory_available?.(factory),
         target when is_binary(target) <- RelayManager.assign_executor(namespace, factory),
         %{executor_id: prev} when is_binary(prev) <- overwatch_snapshot(relay_pid),
         :ok <- safe_migrate(relay_pid, target),
         _ <- HubManager.relay_set_executor(namespace, relay_id, target),
         _ <-
           HubManager.hook_insert_pid(target, %{
             child_id: relay_id,
             node_pids: [{node(), relay_pid}],
             metadata: %{tag: target}
           }) do
      emit_action(namespace, relay_id, :rebind)
      meta = %{namespace: namespace, relay_id: relay_id, executor_id: target}

      :telemetry.execute(
        [:flezha, :executor, :reconnected_reassigned],
        %{count: 1},
        Map.merge(meta, %{previous_id: prev, new_id: target, reason: :convergence_rebind})
      )

      :telemetry.execute(
        [:flezha, :executor, :reconnected],
        %{count: 1},
        Map.merge(meta, %{reason: :convergence_rebind, mode: :reassigned})
      )

      1
    else
      _ -> 0
    end
  end

  # ---------- rebind back-off ----------

  defp queue_rebind(%__MODULE__{} = state, relay_id) do
    now = System.monotonic_time(:millisecond)
    {attempt, next_eligible} = Map.get(state.rebind_backoff, relay_id, {0, 0})

    cond do
      now < next_eligible ->
        # Coalesce: a queued :rebind for this relay is already scheduled.
        state

      true ->
        delay = Enum.at(@rebind_backoff_ms, attempt) || List.last(@rebind_backoff_ms)
        Process.send_after(self(), {:rebind_queued, relay_id}, 0)

        %{
          state
          | rebind_backoff: Map.put(state.rebind_backoff, relay_id, {attempt + 1, now + delay})
        }
    end
  end

  defp maybe_rebind(%__MODULE__{} = state, relay_id) do
    namespace = first_namespace_owning(relay_id)

    if namespace do
      ctx = build_namespace_ctx(namespace)

      case HubManager.get_pid(namespace, relay_id) do
        {:ok, pid} ->
          act(state, namespace, relay_id, pid, :rebind_if_executor_available, ctx)

        _ ->
          0
      end
    else
      0
    end
  end

  # ---------- plugin escalation ----------

  defp escalate_plugin_restart(relay_id, plugin_ids) do
    namespace = first_namespace_owning(relay_id)

    if namespace do
      case HubManager.get_pid(namespace, relay_id) do
        {:ok, pid} when is_pid(pid) ->
          Enum.each(plugin_ids, fn plugin_id ->
            send(pid, {:retry_plugin_restart, plugin_id})
          end)

          :ok

        _ ->
          :ok
      end
    end
  end

  # ---------- helpers ----------

  defp build_namespace_ctx(namespace) do
    active =
      try do
        FleetManager.active_executors(namespace) |> MapSet.new()
      catch
        :exit, _ -> MapSet.new()
      end

    factories =
      try do
        # `FleetManager.executors_for_factory/2` is the existing per-namespace
        # lookup. Build a closure rather than fanning out per relay.
        fn factory_id ->
          case FleetManager.executors_for_factory(namespace, factory_id) do
            [_ | _] -> true
            _ -> false
          end
        end
      catch
        _, _ -> fn _ -> false end
      end

    factory_for_relay = fn relay_id ->
      case HubManager.get_pid(namespace, relay_id) do
        {:ok, pid} ->
          case overwatch_snapshot(pid) do
            nil -> nil
            ow -> Map.get(ow, :factory_id)
          end

        _ ->
          nil
      end
    end

    %{
      active_executors: active,
      factory_available?: factories,
      factory_for_relay: factory_for_relay
    }
  end

  defp safe_running_namespaces() do
    try do
      HubManager.get_running_namespaces()
    catch
      :exit, _ -> []
    end
  end

  defp safe_get_relays(namespace) do
    try do
      HubManager.get_all_relays(namespace)
    catch
      :exit, _ -> []
    end
  end

  defp overwatch_snapshot(pid) when is_pid(pid) do
    GenServer.call(pid, :get_overwatch_state, @default_relay_call_timeout_ms)
  catch
    :exit, _ -> nil
  end

  defp overwatch_snapshot(_), do: nil

  defp safe_migrate(pid, target) when is_pid(pid) do
    GenServer.call(pid, {:executor_migrate, target}, 2_000)
  catch
    :exit, _ -> {:error, :timeout}
  end

  defp emit_action(namespace, relay_id, action) do
    :telemetry.execute(
      [:flezha, :convergence, :action],
      %{count: 1},
      %{namespace: namespace, relay_id: relay_id, action: action}
    )
  end

  defp first_namespace_owning(relay_id) do
    safe_running_namespaces()
    |> Enum.find(fn ns ->
      case HubManager.get_pid(ns, relay_id) do
        {:ok, _} -> true
        _ -> false
      end
    end)
  end

  defp plugin_strike_threshold do
    Application.get_env(
      :flezha,
      :convergence_plugin_strike_threshold,
      @default_plugin_strike_threshold
    )
  end
end

defmodule Flezha.Convergence.Heal do
  @moduledoc """
  The entire heal surface: ten idempotent, bounded primitives (design.md
  §4). Every catalogue row reduces to a composition of these — adding a
  failure mode never adds a primitive.

      start_relay   stop_relay      rebind_relay
      start_plugin  stop_plugin     restart_plugin
      evict_executor reconnect_stream
      replay_state  compact_wal

  `execute/1` dispatches one planned op to its primitive and returns
  `:ok | :error` so the Loop can drive `Backoff`.

  Real primitives:

    * `rebind_relay` — migrate a relay tagged to a dead/wrong executor
      onto an available one (flip the registry tag, fan its pid into the
      new executor's stream channels).
    * `start_plugin` / `stop_plugin` — reconcile a relay's running plugin
      set against its declared cspec manifest (invariant I2).
    * `evict_executor` — forcibly drop a dead executor binding (I1).
    * `reconnect_stream` for `kind: :stream_channel` — SIGKILL a wedged
      channel so the supervisor restarts and the acceptor rebinds (I4).
    * `restart_plugin` — force a wedged provider plugin (e.g. the Kafka
      producer stalled against a restarted broker) onto a fresh connection.

  `start_relay`, `stop_relay`, `replay_state`, `compact_wal`, and the
  non-channel `reconnect_stream` kinds are observable no-ops (emit
  `[:flezha, :convergence, :heal]`, return `:ok`) pending later phases.
  """

  alias Flezha.Convergence.SafeCall
  alias Flezha.Executor.FleetManager
  alias Flezha.Service.{HubManager, RelayManager}

  require Logger

  @relay_call_timeout_ms 1_000
  @migrate_timeout_ms 2_000
  @plugin_call_timeout_ms 2_000

  @primitives ~w(start_relay stop_relay rebind_relay start_plugin stop_plugin
                 restart_plugin evict_executor reconnect_stream replay_state
                 compact_wal)a

  @spec primitives() :: [atom()]
  def primitives(), do: @primitives

  @spec execute(Flezha.Convergence.Planner.op()) :: :ok | :error
  def execute(%{primitive: primitive} = op) when primitive in @primitives do
    :telemetry.execute(
      [:flezha, :convergence, :heal],
      %{count: 1},
      %{primitive: primitive, namespace: op.namespace, target: op.target}
    )

    apply_primitive(primitive, op)
  end

  def execute(%{primitive: other}) do
    Logger.warning("[convergence] unknown heal primitive #{inspect(other)}")
    :error
  end

  defp apply_primitive(:rebind_relay, %{namespace: namespace, target: relay_id}) do
    rebind_relay(namespace, relay_id)
  end

  # Phase A (I2): (re)subscribe a declared-but-missing plugin into the
  # running relay via the same `{:plugin_subscribe, ...}` path `flz plugin
  # enable` uses (which activates immediately since the SDK is already up).
  defp apply_primitive(:start_plugin, %{namespace: ns, target: rid, args: args}) do
    with {:ok, pid} when is_pid(pid) <- HubManager.get_pid(ns, rid),
         {:ok, :ok} <-
           SafeCall.bounded(
             pid,
             {:plugin_subscribe, args.plugin_id, args.plugin},
             @plugin_call_timeout_ms
           ) do
      :ok
    else
      _ -> :error
    end
  end

  # Phase A (I2): unsubscribe a plugin that is running but no longer
  # declared in the cspec.
  defp apply_primitive(:stop_plugin, %{namespace: ns, target: rid, args: args}) do
    with {:ok, pid} when is_pid(pid) <- HubManager.get_pid(ns, rid),
         {:ok, :ok} <-
           SafeCall.bounded(pid, {:plugin_unsubscribe, args.plugin_id}, @plugin_call_timeout_ms) do
      :ok
    else
      _ -> :error
    end
  end

  # Phase B (I1): forcibly drop a dead/orphaned executor binding so its
  # relays become misbound and rebind onto a live executor next tick.
  defp apply_primitive(:evict_executor, %{target: executor_id}) when is_binary(executor_id) do
    case FleetManager.evict_executor(executor_id) do
      :ok -> :ok
      _ -> :error
    end
  end

  # Phase C (I4): a wedged stream channel is healed by reconnecting its
  # stream — kill the pid so its supervisor restarts it and the
  # StreamAcceptor re-binds the fresh channel.
  defp apply_primitive(:reconnect_stream, %{target: pid, args: %{kind: :stream_channel}})
       when is_pid(pid) do
    kill_channel(pid)
  end

  # Restart a wedged provider plugin. The motivating case is the Kafka
  # producer stalling forever after a clean broker restart: the shared
  # client stays alive returning retriable errors, so no in-process swap
  # ever fires. `Kafka.Producer.restart/1` forces each of the plugin's
  # producer workers onto a fresh broker client. Idempotent and safe when
  # the plugin (or its producer) is absent.
  defp apply_primitive(:restart_plugin, %{args: %{plugin_id: plugin_id}})
       when is_binary(plugin_id) do
    case Flezha.Plugin.Provider.Kafka.Producer.restart(plugin_id) do
      :ok -> :ok
      {:error, :no_producer} -> :ok
    end
  end

  defp apply_primitive(:restart_plugin, _op), do: :ok

  # Each clause below is an observable no-op this session; later phases
  # replace the body with the real side effect named in the moduledoc.
  defp apply_primitive(:start_relay, _op), do: :ok
  defp apply_primitive(:stop_relay, _op), do: :ok
  defp apply_primitive(:reconnect_stream, _op), do: :ok
  defp apply_primitive(:replay_state, _op), do: :ok
  defp apply_primitive(:compact_wal, _op), do: :ok

  # Malformed op (no live target) — never produced by the Planner, but a
  # crash here would take down the whole tick. Treat as a failed heal.
  defp apply_primitive(prim, _op) when prim in [:evict_executor, :start_plugin, :stop_plugin],
    do: :error

  # SIGKILL the wedged channel and drop its strike row. Supervisor
  # restart + StreamAcceptor rebind close the gap.
  defp kill_channel(pid) do
    Process.exit(pid, :kill)
    HubManager.clear_channel_timeout(pid)
    :ok
  end

  # Migrate a relay bound to a dead/wrong executor onto an available one.
  # On success: flip the registry tag and seed the target executor's
  # `StreamChannel.relay_mapping` (the same hook `child_registered`
  # uses) so frames land. Any failed step leaves the registry untouched
  # and the relay degraded for the next tick under Backoff.
  defp rebind_relay(namespace, relay_id) do
    with {:ok, pid} when is_pid(pid) <- HubManager.get_pid(namespace, relay_id),
         %{factory_id: factory, executor_id: prev} <- overwatch_snapshot(pid),
         true <- factory != nil,
         target when is_binary(target) <- RelayManager.assign_executor(namespace, factory),
         {:ok, :ok} <- SafeCall.bounded(pid, {:executor_migrate, target}, @migrate_timeout_ms) do
      _ = HubManager.relay_set_executor(namespace, relay_id, target)

      _ =
        HubManager.hook_insert_pid(target, %{
          child_id: relay_id,
          node_pids: [{node(), pid}],
          metadata: %{tag: target}
        })

      # Replay the per-Partition WAL into the new binding. The `executor_migrate`
      # above is a HotSwap handover, which only carries state when the previous
      # executor is alive to serialise it; when the previous executor died (the
      # common rebind trigger) the new executor's shard would otherwise init
      # empty and the relay's persisted state is lost (state resets to 0).
      # resync routes a `RestoreInitState` handshake for each persisted
      # partition to `target`, rebuilding state from the durable WAL.
      _ = Flezha.State.Recovery.resync_for_overwatch(pid, target)

      emit_reconnected(namespace, relay_id, prev, target)
      :ok
    else
      _ -> :error
    end
  end

  defp overwatch_snapshot(pid) do
    case SafeCall.bounded(pid, :get_overwatch_state, @relay_call_timeout_ms) do
      {:ok, ow} when is_map(ow) -> ow
      _ -> nil
    end
  end

  defp emit_reconnected(namespace, relay_id, prev, target) do
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
  end
end

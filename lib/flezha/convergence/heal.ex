defmodule Flezha.Convergence.Heal do
  @moduledoc """
  The entire heal surface: five idempotent, bounded primitives. Every
  catalogue row reduces to a composition of these — adding a failure mode
  never adds a primitive.

      rebind_operative
      start_plugin  stop_plugin  restart_plugin
      evict_runner

  `execute/1` dispatches one planned op to its primitive and returns
  `:ok | :error` so the Loop can drive `Backoff`.

    * `rebind_operative` — migrate a operative tagged to a dead/wrong runner (or
      not yet bound) onto an available one: flip the registry tag, fan its
      pid into the new runner's stream channels, replay the WAL.
    * `start_plugin` / `stop_plugin` — reconcile a operative's running plugin
      set against its declared cspec manifest.
    * `restart_plugin` — tear down and re-subscribe a declared plugin whose
      provider self-reports unhealthy, so `activate/1` reopens its
      upstream resources.
    * `evict_runner` — forcibly drop a dead runner binding.
  """

  alias Flezha.Convergence.SafeCall
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{HubManager, OperativeManager}

  require Logger

  @operative_call_timeout_ms 1_000
  @migrate_timeout_ms 2_000
  @plugin_call_timeout_ms 2_000

  @primitives ~w(rebind_operative start_plugin stop_plugin restart_plugin evict_runner)a

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

  defp apply_primitive(:rebind_operative, %{namespace: namespace, target: operative_id} = op) do
    rebind_operative(namespace, operative_id, Map.get(op, :args, %{}))
  end

  # (re)subscribe a declared-but-missing plugin into the running operative via
  # the same `{:plugin_subscribe, ...}` path `flz plugin enable` uses (which
  # activates immediately since the SDK is already up).
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

  # Unsubscribe a plugin that is running but no longer declared in the cspec.
  defp apply_primitive(:stop_plugin, %{namespace: ns, target: rid, args: args}) do
    with {:ok, pid} when is_pid(pid) <- HubManager.get_pid(ns, rid),
         {:ok, :ok} <-
           SafeCall.bounded(pid, {:plugin_unsubscribe, args.plugin_id}, @plugin_call_timeout_ms) do
      :ok
    else
      _ -> :error
    end
  end

  # Tear down + re-subscribe a declared plugin whose provider self-reported
  # unhealthy. On failure the plugin is left unsubscribed and the next tick
  # retries it through `start_plugin` (it diffs as missing) under Backoff.
  defp apply_primitive(:restart_plugin, %{namespace: ns, target: rid, args: args}) do
    with {:ok, pid} when is_pid(pid) <- HubManager.get_pid(ns, rid),
         {:ok, :ok} <-
           SafeCall.bounded(
             pid,
             {:plugin_restart, args.plugin_id, args.plugin},
             @plugin_call_timeout_ms
           ) do
      :ok
    else
      _ -> :error
    end
  end

  # Forcibly drop a dead/orphaned runner binding so its operatives become
  # misbound and rebind onto a live runner next tick.
  defp apply_primitive(:evict_runner, %{target: runner_id}) when is_binary(runner_id) do
    case FleetManager.evict_runner(runner_id) do
      :ok -> :ok
      _ -> :error
    end
  end

  # Malformed op (no live target) — never produced by the Planner, but a
  # crash here would take down the whole tick. Treat as a failed heal.
  defp apply_primitive(prim, _op)
       when prim in [:evict_runner, :start_plugin, :stop_plugin, :restart_plugin],
       do: :error

  # Migrate a operative bound to a dead/wrong runner onto an available one.
  # On success: flip the registry tag and seed the target runner's
  # `StreamChannel.operative_mapping` (the same hook `child_registered`
  # uses) so frames land. Any failed step leaves the registry untouched
  # and the operative degraded for the next tick under Backoff.
  defp rebind_operative(namespace, operative_id, args) do
    with {:ok, pid} when is_pid(pid) <- HubManager.get_pid(namespace, operative_id),
         %{factory_id: factory, runner_id: prev} <- overwatch_snapshot(pid),
         true <- factory != nil,
         false <- skip_rebind?(namespace, factory, prev, args),
         target when is_binary(target) <-
           OperativeManager.assign_runner(namespace, factory, operative_id),
         {:ok, :ok} <- SafeCall.bounded(pid, {:runner_migrate, target}, @migrate_timeout_ms) do
      _ = HubManager.operative_set_runner(namespace, operative_id, target)
      _ = HubManager.hook_insert_local_pid(target, operative_id, pid)

      # Replay the per-Entity WAL into the new binding. The `runner_migrate`
      # above is a HotSwap handover, which only carries state when the previous
      # runner is alive to serialise it; when the previous runner died (the
      # common rebind trigger) the new runner's procedure would otherwise init
      # empty and the operative's persisted state is lost (state resets to 0).
      # resync routes a `RestoreInitState` handshake for each persisted
      # entity to `target`, rebuilding state from the durable WAL.
      _ = Flezha.State.Recovery.resync_for_overwatch(pid, target)

      emit_reconnected(namespace, operative_id, prev, target)
      :ok
    else
      # `skip_rebind?` returned true — a MISBOUND operative whose bound runner
      # is already active and owns the factory, so a stale-snapshot rebind
      # would only disrupt a healthy operative. Safe no-op.
      true -> :ok
      _ -> :error
    end
  end

  # A misbound rebind is a safe no-op once the bound runner is back in the
  # live, factory-owning active set (a reconnect between snapshot and heal
  # time): re-reading the authoritative set here stops a detection slip from
  # churning a healthy operative.

  # Output-stall proves the binding is not producing — always proceed regardless
  # of whether the bound runner is still in the active set.
  defp skip_rebind?(_ns, _factory, _prev, %{reason: :output_stall}), do: false

  defp skip_rebind?(_namespace, _factory, prev, _args) when prev in [nil, ""], do: false

  defp skip_rebind?(namespace, factory, prev, _args) do
    prev in FleetManager.runners_for_factory(namespace, factory)
  end

  defp overwatch_snapshot(pid) do
    case SafeCall.bounded(pid, :get_overwatch_state, @operative_call_timeout_ms) do
      {:ok, ow} when is_map(ow) -> ow
      _ -> nil
    end
  end

  defp emit_reconnected(namespace, operative_id, prev, target) do
    meta = %{namespace: namespace, operative_id: operative_id, runner_id: target}

    :telemetry.execute(
      [:flezha, :runner, :reconnected_reassigned],
      %{count: 1},
      Map.merge(meta, %{previous_id: prev, new_id: target, reason: :convergence_rebind})
    )

    :telemetry.execute(
      [:flezha, :runner, :reconnected],
      %{count: 1},
      Map.merge(meta, %{reason: :convergence_rebind, mode: :reassigned})
    )
  end
end

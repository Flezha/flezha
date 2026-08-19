defmodule Flezha.Convergence.Heal do
  @moduledoc """
  The entire heal surface: seven idempotent, bounded primitives. Every
  catalogue row reduces to a composition of these — adding a failure mode
  never adds a primitive.

      rebind_operative  replace_operative  resync_runner
      start_plugin  stop_plugin  restart_plugin
      evict_runner

  `execute/1` dispatches one planned op to its primitive and returns
  `:ok | :error` so the Loop can drive `Backoff`.

    * `rebind_operative` — migrate a operative tagged to a dead/wrong runner (or
      not yet bound) onto an available one: flip the registry tag, fan its
      pid into the new runner's stream channels, replay the WAL.
    * `replace_operative` — move an operative whose factory no LOCAL runner
      advertises onto a node whose fleet has one, through the hub's per-child
      handover (`Migration.migrate_child/3`): state rides the HotSwap
      handover or is adopted from the source's WAL, the declared entry is
      untouched, and the drain's consent gate decides whether the operative
      may move now.
    * `resync_runner` — re-run the `RestoreInitState` handshake for a
      operative against the runner it is already bound to. The rung below
      `rebind_operative`: no placement decision, no migration, no handover —
      the remedy for a binding that is intact but has stopped producing.
    * `start_plugin` / `stop_plugin` — reconcile a operative's running plugin
      set against its declared cspec manifest.
    * `restart_plugin` — tear down and re-subscribe a declared plugin whose
      provider self-reports unhealthy, so `activate/1` reopens its
      upstream resources.
    * `evict_runner` — forcibly drop a dead runner binding; a wedge the
      classifier proved also waives the "never evict a live runner" guard,
      since a still-connected runner processing nothing is precisely the
      case that guard would otherwise protect forever.
  """

  alias Flezha.Cluster.Directory
  alias Flezha.Convergence.{Planner, SafeCall}
  alias Flezha.Operative.Overwatch
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{HubManager, Identificator, OperativeManager}
  alias Flezha.State.Recovery
  alias ProcessHub.Service.Migration

  require Logger

  @operative_call_timeout_ms 1_000
  @migrate_timeout_ms 2_000
  @plugin_call_timeout_ms 2_000

  @primitives ~w(rebind_operative replace_operative resync_runner start_plugin stop_plugin restart_plugin evict_runner)a

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

  @doc """
  One confirmed rebind, outside a planned op — the runner-disconnect reroute's
  entry into the exact contract the `rebind_operative` primitive enforces:
  factory-filtered placement via `assign_runner/3`, and a registry flip only
  after the operative confirms the migrate.
  """
  @spec rebind(atom() | binary(), binary()) :: :ok | :error
  def rebind(namespace, operative_id) do
    do_rebind(%{namespace: namespace, target: operative_id, args: %{}})
  end

  defp do_rebind(%{namespace: namespace, target: operative_id} = op) do
    on_operative(namespace, operative_id, &rebind_operative(op, &1))
  end

  defp apply_primitive(:rebind_operative, op), do: do_rebind(op)

  # Move the operative to a node whose fleet can actually host it. Runs only
  # on the node hosting the Overwatch (every controller's Loop plans the same
  # op; the others fail it under Backoff) so the consent read inspects the
  # right disk. The declared entry never changes — a crash mid-move heals
  # through the hub reconcile rather than losing the operative.
  defp apply_primitive(:replace_operative, %{namespace: namespace, target: operative_id}) do
    on_operative(namespace, operative_id, fn pid ->
      with true <- node(pid) == node(),
           %Overwatch{} = ow <- overwatch_snapshot(pid),
           :ready <- Overwatch.migration_consent(ow),
           target_node when target_node != nil <-
             replacement_node(namespace, ow.factory_id, operative_id),
           :ok <- Migration.migrate_child(Identificator.hub(namespace), operative_id, target_node) do
        Logger.info(
          "[convergence] re-placed operative #{operative_id} (#{namespace}) onto " <>
            "#{target_node} — no local runner advertises factory #{inspect(ow.factory_id)}"
        )

        :ok
      else
        _ -> :error
      end
    end)
  end

  # Re-issue `RestoreInitState` for every persisted Entity of a operative against
  # the runner it is ALREADY bound to — the same replay a rebind performs after
  # migrating, without the migration. A operative with no live pid or no binding
  # has nothing to resync; that is a failed heal, and the next tick reaches it
  # through `rebind_operative` instead.
  defp apply_primitive(:resync_runner, %{namespace: namespace, target: operative_id}) do
    on_operative(namespace, operative_id, fn pid ->
      case overwatch_snapshot(pid) do
        %{runner_id: runner_id} when is_binary(runner_id) ->
          Recovery.resync_for_overwatch(pid, runner_id)

        _ ->
          :error
      end
    end)
  end

  # (re)subscribe a declared-but-missing plugin into the running operative via
  # the same `{:plugin_subscribe, ...}` path `flz plugin enable` uses (which
  # activates immediately since the SDK is already up).
  defp apply_primitive(:start_plugin, %{namespace: ns, target: rid, args: args}) do
    plugin_call(ns, rid, {:plugin_subscribe, args.plugin_id, args.plugin})
  end

  # Unsubscribe a plugin that is running but no longer declared in the cspec.
  defp apply_primitive(:stop_plugin, %{namespace: ns, target: rid, args: args}) do
    plugin_call(ns, rid, {:plugin_unsubscribe, args.plugin_id})
  end

  # Tear down + re-subscribe a declared plugin whose provider self-reported
  # unhealthy. On failure the plugin is left unsubscribed and the next tick
  # retries it through `start_plugin` (it diffs as missing) under Backoff.
  defp apply_primitive(:restart_plugin, %{namespace: ns, target: rid, args: args}) do
    plugin_call(ns, rid, {:plugin_restart, args.plugin_id, args.plugin})
  end

  # Forcibly drop a dead/orphaned runner binding so its operatives become
  # misbound and rebind onto a live runner next tick.
  defp apply_primitive(:evict_runner, %{target: runner_id} = op) when is_binary(runner_id) do
    case FleetManager.evict_runner(runner_id, proved_wedged: Planner.proved_wedged?(op)) do
      :ok -> :ok
      _ -> :error
    end
  end

  # Malformed op (no live target) — never produced by the Planner, but a
  # crash here would take down the whole tick. Treat as a failed heal.
  defp apply_primitive(prim, _op)
       when prim in [
              :evict_runner,
              :resync_runner,
              :replace_operative,
              :start_plugin,
              :stop_plugin,
              :restart_plugin
            ],
       do: :error

  # Migrate a operative bound to a dead/wrong runner onto an available one.
  # On success: flip the registry tag and seed the target runner's
  # `StreamChannel.operative_mapping` (the same hook `child_registered`
  # uses) so frames land. Any failed step leaves the registry untouched
  # and the operative degraded for the next tick under Backoff.
  defp rebind_operative(%{namespace: namespace, target: operative_id} = op, pid) do
    with %{factory_id: factory, runner_id: prev} <- overwatch_snapshot(pid),
         true <- factory != nil,
         false <- skip_rebind?(namespace, factory, prev, op),
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
      _ = Recovery.resync_for_overwatch(pid, target)

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
  # churning a healthy operative. An op whose reason PROVED the target is not
  # producing is never skipped — that guard protects the healthy, and this
  # target is not.
  defp skip_rebind?(_namespace, _factory, prev, _op) when prev in [nil, ""], do: false

  defp skip_rebind?(namespace, factory, prev, op) do
    not Planner.proved_wedged?(op) and
      prev in FleetManager.runners_for_factory(namespace, factory)
  end

  # Resolve the operative's live pid or fail the heal — the prologue every
  # operative-scoped primitive shares.
  defp on_operative(namespace, operative_id, fun) do
    case HubManager.get_pid(namespace, operative_id) do
      {:ok, pid} when is_pid(pid) -> fun.(pid)
      _ -> :error
    end
  end

  # The three plugin primitives differ only in the message they send.
  defp plugin_call(namespace, operative_id, message) do
    on_operative(namespace, operative_id, fn pid ->
      case SafeCall.bounded(pid, message, @plugin_call_timeout_ms) do
        {:ok, :ok} -> :ok
        _ -> :error
      end
    end)
  end

  # The reply is always the full `%Overwatch{}` struct — callers hand it to
  # `Overwatch`'s own functions (the consent gate) or read its fields.
  defp overwatch_snapshot(pid) do
    case SafeCall.bounded(pid, :get_overwatch_state, @operative_call_timeout_ms) do
      {:ok, %Overwatch{} = ow} -> ow
      _ -> nil
    end
  end

  # The OTHER nodes advertising the factory, picked through the same HRW the
  # start path uses — so repeated ticks and concurrent loops converge on one
  # destination.
  defp replacement_node(_namespace, nil, _operative_id), do: nil

  defp replacement_node(namespace, factory_id, operative_id) do
    namespace
    |> Directory.factory_nodes(factory_id)
    |> List.delete(node())
    |> OperativeManager.pick_runner(operative_id)
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

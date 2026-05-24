defmodule Flezha.Reconciler.Actions do
  @moduledoc """
  Idempotent convergence actions. Each reads the relay's probe and acts *only* on
  divergence, so a healthy relay is a no-op and repeated, concurrent, or
  transient-dual-leader application is safe.

  The actions drive the controller's existing, already-implemented healing
  primitives to completion rather than reinventing them:

    * `:purge_dead_node` — `ProcessHub.Service.Cluster.purge_node/2`
    * `:replace`         — `ProcessHub.start_child/3` (re-place via ConsistentHashing)
    * `:unpark`          — `Overwatch.executor_migrate` (parked clause: ping,
      start, clear `init_error`, re-activate plugins)
    * `:reissue`         — `Overwatch.executor_migrate` (running clause) for a
      relay whose assigned executor went away, then records the new executor
      in the registry (subsumes `FleetManager.reissue_local_orphans`)
    * `:rebind_stream`   — `FleetManager.rebind_data_streams/1` (reissue ConnectSocket)

  Leadership scope (D2): cluster-wide, registry-mutating actions (`:purge_dead_node`,
  `:replace`) run only on the elected leader so peers do not race the same entries;
  node-local actions (`:unpark`, `:reissue`, `:rebind_stream`) run on whichever node
  owns the relay pid, leadership-independent. Returns the list of actions applied.
  """

  alias Flezha.Executor.FleetManager
  alias Flezha.Reconciler.Probe
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.RelayManager
  alias ProcessHub.Service.Cluster

  require Logger

  @migrate_timeout_ms 2_000

  @type action() ::
          :purge_dead_node | :replace | :unpark | :reissue | :rebind_stream | :restart_plugin

  @doc """
  Apply every convergence action whose divergence guard fires. `is_leader?`
  gates the cluster-wide actions.
  """
  @spec converge(Probe.t(), Flezha.Reconciler.Health.t(), boolean()) :: [action()]
  def converge(%Probe{} = p, _health, is_leader?) do
    []
    |> maybe_purge_dead_node(p, is_leader?)
    |> maybe_replace(p, is_leader?)
    |> maybe_unpark(p)
    |> maybe_reissue(p)
    |> maybe_rebind_stream(p)
    |> Enum.reverse()
  end

  # ---------- cluster-wide (leader-only) ----------

  defp maybe_purge_dead_node(acc, %Probe{dead_node_entries: []}, _leader?), do: acc
  defp maybe_purge_dead_node(acc, _p, false), do: acc

  defp maybe_purge_dead_node(acc, %Probe{} = p, true) do
    hub_id = Identificator.hub(p.namespace)

    dead_nodes =
      p.dead_node_entries
      |> Enum.map(fn {n, _pid} -> n end)
      |> Enum.uniq()

    Enum.each(dead_nodes, fn dead ->
      try do
        Cluster.purge_node(hub_id, dead)
      catch
        kind, reason ->
          Logger.warning(
            "[reconciler] purge_node #{inspect(dead)} for #{p.relay_id} failed: " <>
              "#{inspect({kind, reason})}"
          )
      end
    end)

    emit(p, :purge_dead_node)
    [:purge_dead_node | acc]
  end

  # Re-place a relay whose only locations were dead nodes, provided an executor
  # advertises its factory. ConsistentHashing picks the node; `child_data_alter`
  # picks the executor. Best-effort here; full node-loss redistribution is
  # verified in Docker (§13.4).
  defp maybe_replace(acc, %Probe{relay_alive?: true}, _leader?), do: acc
  defp maybe_replace(acc, %Probe{executor_available?: false}, _leader?), do: acc
  defp maybe_replace(acc, _p, false), do: acc
  defp maybe_replace(acc, %Probe{cspec: nil}, true), do: acc

  defp maybe_replace(acc, %Probe{} = p, true) do
    hub_id = Identificator.hub(p.namespace)

    try do
      ProcessHub.start_child(hub_id, p.cspec, awaitable: false, disable_logging: true)
    catch
      kind, reason ->
        Logger.warning("[reconciler] replace #{p.relay_id} failed: #{inspect({kind, reason})}")
    end

    emit(p, :replace)
    [:replace | acc]
  end

  # ---------- node-local (any node that owns the pid) ----------

  # Un-park a parked relay once an executor for its factory is available. The
  # target is re-resolved (the parked relay's stored executor id is stale after
  # the executor it lost reconnected under a fresh id).
  defp maybe_unpark(acc, %Probe{parked?: true, local?: true, executor_available?: true} = p) do
    case RelayManager.assign_executor(p.namespace, p.factory_id) do
      target when is_binary(target) ->
        migrate(p, target)
        emit(p, :unpark)
        [:unpark | acc]

      _ ->
        acc
    end
  end

  defp maybe_unpark(acc, _p), do: acc

  # Re-issue `AddRelay` for a *running* relay whose assigned executor went away,
  # once another executor for its factory is available. Drives the same
  # `executor_migrate` (running clause: rebind, ping, re-issue start) and records
  # the new executor only on success, so a failed migrate leaves the relay for the
  # next pass to re-drive. Subsumes `FleetManager.reissue_local_orphans`.
  defp maybe_reissue(
         acc,
         %Probe{
           relay_alive?: true,
           parked?: false,
           local?: true,
           executor_reachable?: false,
           executor_available?: true
         } = p
       ) do
    with target when is_binary(target) <- RelayManager.assign_executor(p.namespace, p.factory_id),
         :ok <- migrate(p, target) do
      record_executor(p, target)
      emit(p, :reissue)
      [:reissue | acc]
    else
      _ -> acc
    end
  end

  defp maybe_reissue(acc, _p), do: acc

  # Rebind a running (non-parked) relay whose data stream dropped: re-issue
  # `ConnectSocket` so the SDK re-opens its data sockets and the acceptor binds
  # them to the unbound StreamChannel (executor-controller-connection-healing).
  defp maybe_rebind_stream(
         acc,
         %Probe{
           stream_bound?: false,
           parked?: false,
           local?: true,
           executor_reachable?: true,
           assigned_executor: executor_id
         } = p
       )
       when is_binary(executor_id) do
    case FleetManager.rebind_data_streams(executor_id) do
      :ok ->
        emit(p, :rebind_stream)
        [:rebind_stream | acc]

      _ ->
        acc
    end
  end

  defp maybe_rebind_stream(acc, _p), do: acc

  @doc """
  Escalate persistently-unhealthy plugins by restarting them via the relay's
  host (node-local; the host re-inits + re-activates the provider). Driven by
  the loop only after a plugin has been unhealthy across repeated probes — that
  strike threshold plus the host's own retry backoff is the bounded governor.
  Returns `[:restart_plugin]` when at least one restart was triggered.
  """
  @spec escalate_plugins(Probe.t(), [binary()]) :: [action()]
  def escalate_plugins(_p, []), do: []

  def escalate_plugins(%Probe{local?: true, relay_pid: pid} = p, plugin_ids) when is_pid(pid) do
    Enum.each(plugin_ids, fn plugin_id ->
      send(pid, {:retry_plugin_restart, plugin_id})
      emit(p, :restart_plugin)
    end)

    [:restart_plugin]
  end

  def escalate_plugins(_p, _plugin_ids), do: []

  defp migrate(%Probe{relay_pid: relay_pid, relay_id: relay_id}, target) do
    GenServer.call(relay_pid, {:executor_migrate, target}, @migrate_timeout_ms)
  catch
    :exit, reason ->
      Logger.warning(
        "[reconciler] executor_migrate for #{relay_id} → #{target} timed out: #{inspect(reason)}"
      )

      {:error, {:exit, reason}}
  end

  # Record the relay's new executor in the durable registry so the placement is
  # realized for `flz relay list` / `cluster status` and survives the relay pid.
  defp record_executor(%Probe{namespace: ns, relay_id: relay_id}, target) do
    HubManager.relay_set_executor(ns, relay_id, target)
  catch
    _, _ -> :ok
  end

  defp emit(%Probe{} = p, action) do
    :telemetry.execute(
      [:flezha, :reconcile, :action],
      %{count: 1},
      %{node: node(), namespace: p.namespace, relay_id: p.relay_id, action: action}
    )
  end
end

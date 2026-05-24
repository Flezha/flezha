defmodule Flezha.Reconciler.Actions do
  @moduledoc """
  Idempotent convergence actions. Each reads the relay's probe and acts *only* on
  divergence, so a healthy relay is a no-op and repeated, concurrent, or
  transient-dual-leader application is safe.

  The actions drive the controller's existing, already-implemented healing
  primitives to completion rather than reinventing them:

    * `:purge_dead_node` — `ProcessHub.Service.Cluster.purge_node/2`
    * `:replace`         — `ProcessHub.start_child/3` (re-place via ConsistentHashing)
    * `:unpark`          — `Overwatch.executor_migrate` (ping/result/init_error
      composition fix is Phase 6.4; the loop keeps re-driving until it converges)
    * `:rebind_stream`   — `FleetManager.reset_control_channel/1`

  Leadership scope (D2): cluster-wide, registry-mutating actions (`:purge_dead_node`,
  `:replace`) run only on the elected leader so peers do not race the same entries;
  node-local actions (`:unpark`, `:rebind_stream`) run on whichever node owns the
  relay pid, leadership-independent. Returns the list of actions actually applied.
  """

  alias Flezha.Executor.FleetManager
  alias Flezha.Reconciler.Probe
  alias Flezha.Service.Identificator
  alias Flezha.Service.RelayManager
  alias ProcessHub.Service.Cluster

  require Logger

  @migrate_timeout_ms 2_000

  @type action() :: :purge_dead_node | :replace | :unpark | :rebind_stream

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

  # Rebind a running (non-parked) relay whose data stream dropped: clear the
  # ControlChannel stream_ref so the SDK's next data stream re-binds via the
  # existing acceptor path.
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
    case FleetManager.reset_control_channel(executor_id) do
      :ok ->
        emit(p, :rebind_stream)
        [:rebind_stream | acc]

      _ ->
        acc
    end
  end

  defp maybe_rebind_stream(acc, _p), do: acc

  defp migrate(%Probe{relay_pid: relay_pid, relay_id: relay_id}, target) do
    GenServer.call(relay_pid, {:executor_migrate, target}, @migrate_timeout_ms)
  catch
    :exit, reason ->
      Logger.warning(
        "[reconciler] executor_migrate for #{relay_id} → #{target} timed out: #{inspect(reason)}"
      )
  end

  defp emit(%Probe{} = p, action) do
    :telemetry.execute(
      [:flezha, :reconcile, :action],
      %{count: 1},
      %{node: node(), namespace: p.namespace, relay_id: p.relay_id, action: action}
    )
  end
end

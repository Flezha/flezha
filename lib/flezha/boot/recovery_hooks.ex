defmodule Flezha.Boot.RecoveryHooks do
  @moduledoc """
  Thin telemetry forwarders for ProcessHub's orphan-reconcile hooks.

  Upstream owns the recovery state machine: each round starts the
  difference between this node's durable registry and what the cluster
  is observed to hold. These handlers only surface that lifecycle to
  Flezha's observability layer.

  `reconcile_round` is forwarded because `Convergence.Loop` consumes it:
  a round that started orphans triggers one immediate tick, so the plan
  adopts what the round just started instead of a later tick finding them
  half-bound.

  `reconcile_duplicate` is an **alarm**, not a metric. Every start path
  resolves through the registry's id claim, so one operative observed on
  two nodes means either a cross-node partition recovery (inherent, and
  already resolved keep-one/stop-others by the time the hook fires) or a
  start-authority violation. The kept/stopped nodes ride the event so the
  two cases are distinguishable at a glance.

  When the kill-switch `:flezha, :auto_restore_enabled` is `false`,
  ProcessHub does not fire these hooks (auto_recovery is off), so
  `register/1` is a no-op in that mode.
  """

  require Logger

  alias Flezha.Service.Identificator
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager

  # Handler name doubles as the HookManager id tag — it is already unique
  # per hook, so nothing else has to be kept in sync.
  @handlers [
    {&Hook.pre_recovery_replay/0, :pre_recovery_replay},
    {&Hook.post_recovery_replay/0, :post_recovery_replay},
    {&Hook.reconcile_round/0, :reconcile_round},
    {&Hook.reconcile_duplicate/0, :reconcile_duplicate}
  ]

  @spec register(atom()) :: :ok
  def register(hub_id) when is_atom(hub_id) do
    case Application.get_env(:flezha, :auto_restore_enabled, true) do
      false ->
        :ok

      _ ->
        Enum.each(@handlers, fn {hook, handler} ->
          ProcessHub.register_hook_handlers(hub_id, hook.(), [
            %HookManager{
              id: {__MODULE__, handler, hub_id},
              m: __MODULE__,
              f: handler,
              a: [hub_id, :_]
            }
          ])
        end)

        :ok
    end
  end

  @doc """
  Synchronous `pre_recovery_replay` handler. ProcessHub blocks the
  coordinator until this returns, so the body reads the payload rather
  than the registry.
  """
  @spec pre_recovery_replay(atom(), map()) :: :ok
  def pre_recovery_replay(hub_id, hook_data) do
    emit(:pre_recovery_replay, hub_id, %{
      cspec_count: Map.get(hook_data, :child_count, 0),
      system_time: System.system_time()
    })
  end

  @doc """
  Async `post_recovery_replay` handler: the first round's outcome.
  """
  @spec post_recovery_replay(atom(), map()) :: :ok
  def post_recovery_replay(hub_id, hook_data) do
    emit(
      :recovery_complete,
      hub_id,
      %{
        child_count: Map.get(hook_data, :child_count, 0),
        succeeded: Map.get(hook_data, :succeeded, 0),
        failed: Map.get(hook_data, :failed, 0)
      },
      %{replay_outcome: Map.get(hook_data, :reason)}
    )
  end

  @doc """
  Every reconcile round, not only the first. `measurements` carries
  `candidates` / `orphans` / `started` / `duplicates` / `elapsed_ms`.
  """
  @spec reconcile_round(atom(), map()) :: :ok
  def reconcile_round(hub_id, hook_data) do
    measurements = Map.get(hook_data, :measurements, %{})

    emit(
      :reconcile_round,
      hub_id,
      Map.take(measurements, [:candidates, :orphans, :started, :duplicates, :elapsed_ms]),
      %{first_round: Map.get(hook_data, :first_round, false)}
    )
  end

  @doc """
  One operative observed running on more than one node, already resolved
  keep-one/stop-others upstream. Alarm-grade: outside a cross-node
  partition-recovery window it means a start bypassed the id claim.
  """
  @spec reconcile_duplicate(atom(), map()) :: :ok
  def reconcile_duplicate(hub_id, hook_data) do
    operative_id = Map.get(hook_data, :child_id)
    kept = Map.get(hook_data, :kept_node)
    stopped = Map.get(hook_data, :stopped_nodes, [])

    Logger.error(
      "[boot] duplicate operative binding resolved: #{inspect(operative_id)} was running on " <>
        "#{Map.get(hook_data, :instance_count, 0)} nodes; kept #{inspect(kept)}, stopped " <>
        "#{inspect(stopped)}. Expected only while recovering from a cross-node partition — " <>
        "otherwise a start bypassed the operative id claim"
    )

    emit(
      :reconcile_duplicate,
      hub_id,
      %{instance_count: Map.get(hook_data, :instance_count, 0)},
      %{operative_id: operative_id, kept_node: kept, stopped_nodes: stopped}
    )
  end

  # Every event on this source carries the hub and its namespace; only the
  # measurements and the extra metadata differ per hook.
  defp emit(event, hub_id, measurements, metadata \\ %{}) do
    :telemetry.execute(
      [:flezha, :boot, event],
      measurements,
      Map.merge(metadata, %{
        hub_id: hub_id,
        namespace: Identificator.namespace_from_hub(hub_id)
      })
    )

    :ok
  end
end

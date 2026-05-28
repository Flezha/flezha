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

  `rebind_relay` is real this session: it carries the fresh-process
  executor-takeover migration that already shipped (a relay tagged to a
  dead/wrong executor is migrated onto an available one, its registry
  tag flipped, and its pid fanned into the new executor's stream
  channels). Every other primitive is an observable no-op (emits
  `[:flezha, :convergence, :heal]` and returns `:ok`) so the
  Intent→Probe→Diff→Planner→Heal pipeline runs end to end with behaviour
  unchanged from the chaos baseline. Sessions 3-4 fill in the bodies:
  Phase A `start_plugin`/`stop_plugin`/`restart_plugin`, Phase B
  `evict_executor`, Phase C wedged-channel kill, Phase D/F
  `reconnect_stream`.
  """

  alias Flezha.Convergence.SafeCall
  alias Flezha.Service.{HubManager, RelayManager}

  require Logger

  @relay_call_timeout_ms 1_000
  @migrate_timeout_ms 2_000

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

  # Each clause below is an observable no-op this session; Sessions 3-4
  # replace the body with the real side effect named in the moduledoc.
  defp apply_primitive(:start_relay, _op), do: :ok
  defp apply_primitive(:stop_relay, _op), do: :ok
  defp apply_primitive(:start_plugin, _op), do: :ok
  defp apply_primitive(:stop_plugin, _op), do: :ok
  defp apply_primitive(:restart_plugin, _op), do: :ok
  defp apply_primitive(:evict_executor, _op), do: :ok
  defp apply_primitive(:reconnect_stream, _op), do: :ok
  defp apply_primitive(:replay_state, _op), do: :ok
  defp apply_primitive(:compact_wal, _op), do: :ok

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

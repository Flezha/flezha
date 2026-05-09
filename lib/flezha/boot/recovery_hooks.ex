defmodule Flezha.Boot.RecoveryHooks do
  @moduledoc """
  Flezha-side handlers for ProcessHub's `pre_recovery_replay` and
  `post_recovery_replay` hooks.

  `pre_recovery_replay` runs synchronously in the ProcessHub coordinator
  before the persistent-registry replay dispatches `start_children`. It
  blocks until at least one active executor exists for any factory
  referenced by the hub's persistent registry rows, OR
  `:flezha, :pre_replay_fleet_wait_ms` (default `30_000`) elapses.
  Without this gate, replay would dispatch `start_children` against an
  empty `FleetManager` and `child_data_alter` would raise.

  `post_recovery_replay` emits `[:flezha, :boot, :recovery_complete]`
  telemetry for downstream observability (Phoenix LiveDashboard,
  Prometheus scraper). When the kill-switch
  `:flezha, :auto_restore_enabled` is `false`, ProcessHub does not fire
  these hooks, so `register/1` is a no-op in that mode.
  """

  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Service.ProcessRegistry

  require Logger

  @default_pre_replay_fleet_wait_ms 30_000
  @poll_interval_ms 200

  @spec register(atom()) :: :ok
  def register(hub_id) when is_atom(hub_id) do
    case Application.get_env(:flezha, :auto_restore_enabled, true) do
      false ->
        :ok

      _ ->
        ProcessHub.register_hook_handlers(hub_id, Hook.pre_recovery_replay(), [
          %HookManager{
            id: {__MODULE__, :pre, hub_id},
            m: __MODULE__,
            f: :pre_recovery_replay,
            a: [hub_id, :_]
          }
        ])

        ProcessHub.register_hook_handlers(hub_id, Hook.post_recovery_replay(), [
          %HookManager{
            id: {__MODULE__, :post, hub_id},
            m: __MODULE__,
            f: :post_recovery_replay,
            a: [hub_id, :_]
          }
        ])

        :ok
    end
  end

  @doc """
  Synchronous `pre_recovery_replay` handler. ProcessHub blocks the
  coordinator until this returns.
  """
  @spec pre_recovery_replay(atom(), map()) :: :ok
  def pre_recovery_replay(hub_id, _hook_data) do
    namespace = namespace_from_hub(hub_id)
    factory_ids = factory_ids_from_registry(hub_id)

    Logger.info(
      "[recovery_hooks] pre_recovery_replay enter hub=#{hub_id} factories=#{inspect(factory_ids)}"
    )

    case factory_ids do
      [] ->
        Logger.info("[recovery_hooks] pre_recovery_replay: empty registry; nothing to wait on")

      _ ->
        wait_for_executor(namespace, factory_ids, deadline())
    end

    Logger.info("[recovery_hooks] pre_recovery_replay exit hub=#{hub_id}")
    :ok
  end

  @doc """
  Async `post_recovery_replay` handler. Emits Flezha telemetry.
  """
  @spec post_recovery_replay(atom(), map()) :: :ok
  def post_recovery_replay(hub_id, hook_data) do
    namespace = namespace_from_hub(hub_id)

    :telemetry.execute(
      [:flezha, :boot, :recovery_complete],
      %{
        child_count: Map.get(hook_data, :child_count, 0),
        succeeded: Map.get(hook_data, :succeeded, 0),
        failed: Map.get(hook_data, :failed, 0)
      },
      %{
        hub_id: hub_id,
        namespace: namespace,
        replay_outcome: Map.get(hook_data, :reason)
      }
    )

    :ok
  end

  # ---------- helpers ----------

  defp factory_ids_from_registry(hub_id) do
    try do
      ProcessRegistry.dump(hub_id)
      |> Enum.flat_map(fn {_id, entry} -> extract_factory_id(entry) end)
      |> Enum.uniq()
    rescue
      _ -> []
    end
  end

  defp extract_factory_id({child_spec, _node_pids, _metadata}), do: extract_factory_id(child_spec)
  defp extract_factory_id({child_spec, _node_pids}), do: extract_factory_id(child_spec)

  defp extract_factory_id(%{start: {_m, _f, [%{factory_id: factory_id} | _]}})
       when is_binary(factory_id) or is_atom(factory_id),
       do: [factory_id]

  defp extract_factory_id(%{start: {_m, _f, [%{relay: %{factory_id: factory_id}} | _]}})
       when is_binary(factory_id) or is_atom(factory_id),
       do: [factory_id]

  defp extract_factory_id(_), do: []

  defp wait_for_executor(namespace, factory_ids, deadline) do
    cond do
      any_executor_available?(namespace, factory_ids) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        Logger.warning(
          "[recovery_hooks] pre_recovery_replay: timed out waiting for executor; " <>
            "namespace=#{inspect(namespace)} factories=#{inspect(factory_ids)}"
        )

        :ok

      true ->
        Process.sleep(@poll_interval_ms)
        wait_for_executor(namespace, factory_ids, deadline)
    end
  end

  defp any_executor_available?(namespace, factory_ids) do
    Enum.any?(factory_ids, fn factory_id ->
      try do
        case Flezha.Executor.FleetManager.executors_for_factory(namespace, factory_id) do
          [_ | _] -> true
          _ -> false
        end
      catch
        :exit, _ -> false
      end
    end)
  end

  defp deadline() do
    wait_ms =
      Application.get_env(:flezha, :pre_replay_fleet_wait_ms, @default_pre_replay_fleet_wait_ms)

    System.monotonic_time(:millisecond) + wait_ms
  end

  defp namespace_from_hub(hub_id) do
    hub_id
    |> Atom.to_string()
    |> String.replace_suffix("_hub", "")
  end
end

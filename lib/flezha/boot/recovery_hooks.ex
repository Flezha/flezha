defmodule Flezha.Boot.RecoveryHooks do
  @moduledoc """
  Thin telemetry forwarders for ProcessHub's `pre_recovery_replay` and
  `post_recovery_replay` hooks.

  The upstream `recovery-mode-opt-in` contract owns the recovery
  state machine, the cluster-event queue, and the cspecs-only replay
  path; Flezha no longer needs to poll FleetManager readiness or strip
  dead-node entries from the registry. These handlers exist only to
  surface the replay lifecycle to Flezha's observability layer.

  When the kill-switch `:flezha, :auto_restore_enabled` is `false`,
  ProcessHub does not fire these hooks (auto_recovery is off), so
  `register/1` is a no-op in that mode.
  """

  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias ProcessHub.Service.ProcessRegistry

  require Logger

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
  Synchronous `pre_recovery_replay` handler. Emits Flezha telemetry and
  returns immediately. ProcessHub blocks the coordinator until this
  returns; keeping the body small is the contract.
  """
  @spec pre_recovery_replay(atom(), map()) :: :ok
  def pre_recovery_replay(hub_id, _hook_data) do
    namespace = namespace_from_hub(hub_id)

    :telemetry.execute(
      [:flezha, :boot, :pre_recovery_replay],
      %{cspec_count: cspec_count(hub_id), system_time: System.system_time()},
      %{hub_id: hub_id, namespace: namespace}
    )

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

  defp cspec_count(hub_id) do
    try do
      ProcessRegistry.dump(hub_id) |> length()
    rescue
      _ -> 0
    catch
      :exit, _ -> 0
    end
  end

  defp namespace_from_hub(hub_id) do
    hub_id
    |> Atom.to_string()
    |> String.replace_suffix("_hub", "")
  end
end

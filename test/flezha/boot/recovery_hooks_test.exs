defmodule Flezha.Boot.RecoveryHooksTest do
  use ExUnit.Case, async: false

  alias Flezha.Boot.RecoveryHooks

  setup do
    Application.put_env(:flezha, :pre_replay_fleet_wait_ms, 100)
    Application.put_env(:flezha, :auto_restore_enabled, true)

    on_exit(fn ->
      Application.delete_env(:flezha, :pre_replay_fleet_wait_ms)
      Application.delete_env(:flezha, :auto_restore_enabled)
    end)

    :ok
  end

  test "pre_recovery_replay returns :ok when persistent registry is empty" do
    hub_id = :"empty_test_hub_#{System.unique_integer([:positive])}"

    assert :ok = RecoveryHooks.pre_recovery_replay(hub_id, %{})
  end

  test "pre_recovery_replay times out gracefully when no executor connects" do
    hub_id = :"timeout_test_hub_#{System.unique_integer([:positive])}"
    Application.put_env(:flezha, :pre_replay_fleet_wait_ms, 50)

    started = System.monotonic_time(:millisecond)
    assert :ok = RecoveryHooks.pre_recovery_replay(hub_id, %{})
    elapsed = System.monotonic_time(:millisecond) - started

    assert elapsed < 1_000
  end

  test "post_recovery_replay emits telemetry" do
    test_pid = self()
    handler_id = "recovery-hook-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:flezha, :boot, :recovery_complete],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    hub_id = :market_hub
    hook_data = %{child_count: 3, succeeded: 3, failed: 0, reason: :ok}

    assert :ok = RecoveryHooks.post_recovery_replay(hub_id, hook_data)

    assert_receive {:telemetry, [:flezha, :boot, :recovery_complete], measurements, metadata},
                   500

    assert measurements.child_count == 3
    assert measurements.succeeded == 3
    assert metadata.hub_id == :market_hub
    assert metadata.namespace == "market"
    assert metadata.replay_outcome == :ok
  end

  test "register/1 is a no-op when kill-switch is disabled" do
    Application.put_env(:flezha, :auto_restore_enabled, false)

    assert :ok = RecoveryHooks.register(:any_hub_id)
  end
end

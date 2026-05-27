defmodule Flezha.Boot.RecoveryHooksTest do
  use ExUnit.Case, async: false

  alias Flezha.Boot.RecoveryHooks

  setup do
    Application.put_env(:flezha, :auto_restore_enabled, true)

    on_exit(fn ->
      Application.delete_env(:flezha, :auto_restore_enabled)
    end)

    :ok
  end

  test "pre_recovery_replay emits telemetry and returns quickly" do
    test_pid = self()
    handler_id = "pre-recovery-replay-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:flezha, :boot, :pre_recovery_replay],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    hub_id = :"pre_recovery_replay_test_hub_#{System.unique_integer([:positive])}"

    started = System.monotonic_time(:millisecond)
    assert :ok = RecoveryHooks.pre_recovery_replay(hub_id, %{})
    elapsed = System.monotonic_time(:millisecond) - started

    assert elapsed < 100,
           "pre_recovery_replay should be near-instant; took #{elapsed}ms"

    assert_receive {:telemetry, [:flezha, :boot, :pre_recovery_replay], measurements, metadata},
                   500

    assert is_integer(measurements.cspec_count)
    assert metadata.hub_id == hub_id
    assert is_binary(metadata.namespace)
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

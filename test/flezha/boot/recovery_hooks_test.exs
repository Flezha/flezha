defmodule Flezha.Boot.RecoveryHooksTest do
  # async: false: writes Application env; the observability registry; the primary Logger.
  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.Boot.RecoveryHooks
  alias Flezha.Observability.TelemetryBridge

  setup do
    # The key row defaults this to `false` under `:test`, so auto-restore left
    # ON boots later hubs into `:recovering`, where they hold until their first
    # reconcile round and stall `RecoveryGate` and any convergence assertion
    # behind it.
    Application.put_env(:flezha, :auto_restore_enabled, true)

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

  test "reconcile_duplicate alarms with the kept and stopped nodes (D8)" do
    test_pid = self()
    handler_id = "reconcile-duplicate-test-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:flezha, :boot, :reconcile_duplicate],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert :ok =
                 RecoveryHooks.reconcile_duplicate(:market_hub, %{
                   child_id: "btc_feed",
                   instance_count: 2,
                   kept_node: :a@host,
                   stopped_nodes: [:b@host]
                 })
      end)

    # Alarm-grade: a start-authority violation must be greppable at error level,
    # not buried in a metric only a dashboard reads.
    assert log =~ "[error]"
    assert log =~ "btc_feed"

    assert_receive {:telemetry, [:flezha, :boot, :reconcile_duplicate], measurements, metadata},
                   500

    assert measurements.instance_count == 2
    assert metadata.operator_id == "btc_feed"
    assert metadata.kept_node == :a@host
    assert metadata.stopped_nodes == [:b@host]
  end

  test "the duplicate alarm is bridged onto the observability bus" do
    assert Map.fetch!(
             TelemetryBridge.mapping(),
             [:flezha, :boot, :reconcile_duplicate]
           ) == {:flezha_boot, :reconcile_duplicate, :event}
  end
end

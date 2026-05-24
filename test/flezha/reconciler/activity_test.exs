defmodule Flezha.Reconciler.ActivityTest do
  @moduledoc """
  Egress-activity table (self-healing-control-plane, Phase 5 / task 6.1).
  """
  use ExUnit.Case, async: false

  alias Flezha.Reconciler.Activity

  describe "table absent (reconciler disabled)" do
    test "reads are nil and writes/forgets are safe no-ops" do
      # :test config sets reconciler_enabled: false, so the table is not started
      # globally — the hot-path write must never crash the StreamChannel.
      assert :ets.whereis(:flezha_relay_activity) == :undefined
      assert Activity.last_output_ms("missing") == nil
      assert Activity.last_drop_ms("missing") == nil
      assert Activity.record_output("missing") == :ok
      assert Activity.forget("missing") == :ok
    end

    test "record_drop emits [:flezha, :stream, :drop] but records nothing when the table is absent" do
      attach_drop_telemetry()
      assert Activity.record_drop("ghost") == :ok
      assert_receive {:drop, %{count: 1}, %{relay_id: "ghost"}}
      assert Activity.last_drop_ms("ghost") == nil
    end
  end

  describe "table started" do
    setup do
      start_supervised!(Activity)
      :ok
    end

    test "record_output then last_output_ms returns a monotonic timestamp" do
      assert Activity.record_output("relay_a") == :ok
      ts = Activity.last_output_ms("relay_a")
      assert is_integer(ts)
      assert ts <= System.monotonic_time(:millisecond)
    end

    test "last_output_ms is nil for a relay with no recorded output" do
      assert Activity.last_output_ms("never") == nil
    end

    test "a later write advances the timestamp" do
      Activity.record_output("relay_b")
      t1 = Activity.last_output_ms("relay_b")
      Process.sleep(2)
      Activity.record_output("relay_b")
      assert Activity.last_output_ms("relay_b") >= t1
    end

    test "forget drops both the output and drop entries" do
      Activity.record_output("relay_c")
      Activity.record_drop("relay_c")
      assert is_integer(Activity.last_output_ms("relay_c"))
      assert is_integer(Activity.last_drop_ms("relay_c"))
      assert Activity.forget("relay_c") == :ok
      assert Activity.last_output_ms("relay_c") == nil
      assert Activity.last_drop_ms("relay_c") == nil
    end

    test "record_drop records a drop timestamp and emits telemetry (6.2 + 6.3)" do
      attach_drop_telemetry()
      assert Activity.record_drop("relay_d") == :ok
      assert_receive {:drop, %{count: 1}, %{relay_id: "relay_d"}}
      ts = Activity.last_drop_ms("relay_d")
      assert is_integer(ts)
      assert ts <= System.monotonic_time(:millisecond)
      # output and drop are independent keys
      assert Activity.last_output_ms("relay_d") == nil
    end
  end

  defp attach_drop_telemetry do
    test_pid = self()
    id = "activity-drop-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      id,
      [:flezha, :stream, :drop],
      fn _e, m, md, _ -> send(test_pid, {:drop, m, md}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
  end
end

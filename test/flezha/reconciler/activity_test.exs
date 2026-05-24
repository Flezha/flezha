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
      assert Activity.record_output("missing") == :ok
      assert Activity.forget("missing") == :ok
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

    test "forget drops the entry" do
      Activity.record_output("relay_c")
      assert is_integer(Activity.last_output_ms("relay_c"))
      assert Activity.forget("relay_c") == :ok
      assert Activity.last_output_ms("relay_c") == nil
    end
  end
end

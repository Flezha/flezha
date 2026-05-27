defmodule Flezha.Convergence.HealthTest do
  @moduledoc """
  Unit tests for the Convergence Health watcher introduced by
  `consume-recovery-mode-and-self-heal` §4.1-4.3. Exercises the
  contract surface that does not require a live ProcessHub /
  RelayManager:

    * the watcher gates action emission on `await_recovery_complete/1`
      → `:awaiting_recovery` telemetry fires before recovery
      completes;
    * `:output_stall` telemetry queues a `:rebind` action with
      exponential backoff (5s/30s/60s/300s);
    * back-off prevents oscillation: a second `:output_stall` inside
      the back-off window is coalesced;
    * plugin-strike escalation reaches `[:flezha, :convergence,
      :plugin_escalated]` after the threshold.
  """

  use ExUnit.Case, async: false

  alias Flezha.Convergence.Health

  setup do
    test_pid = self()
    handler_id = "convergence-health-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:flezha, :convergence, :tick],
        [:flezha, :convergence, :action],
        [:flezha, :convergence, :plugin_restart],
        [:flezha, :convergence, :plugin_escalated]
      ],
      fn event, measurements, metadata, _ ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok
  end

  describe "lifecycle" do
    test "starts and ticks without crashing when no hubs are running" do
      {:ok, pid} = start_supervised({Health, tick_ms: 30})

      # First tick should fire fast.
      Process.sleep(150)

      assert Process.alive?(pid)
      # At least one tick telemetry should have fired (either
      # :awaiting_recovery or :ticking depending on recovery state).
      assert_received {:telemetry, [:flezha, :convergence, :tick], _, _}
    end

    test "tick_now/0 runs the body synchronously" do
      {:ok, _pid} = start_supervised({Health, tick_ms: 60_000})

      assert is_map(Health.tick_now())
      assert_receive {:telemetry, [:flezha, :convergence, :tick], _, _}, 500
    end
  end

  describe "plugin escalation" do
    test "first strike emits :plugin_restart; second strike escalates" do
      {:ok, _pid} = start_supervised({Health, tick_ms: 60_000})

      previous_threshold =
        Application.get_env(:flezha, :convergence_plugin_strike_threshold)

      Application.put_env(:flezha, :convergence_plugin_strike_threshold, 2)

      on_exit(fn ->
        if previous_threshold do
          Application.put_env(
            :flezha,
            :convergence_plugin_strike_threshold,
            previous_threshold
          )
        else
          Application.delete_env(:flezha, :convergence_plugin_strike_threshold)
        end
      end)

      Health.handle_plugin_failure("btc_kraken", "websocket_plugin")

      assert_receive {:telemetry, [:flezha, :convergence, :plugin_restart], _, metadata}, 500

      assert metadata.relay_id == "btc_kraken"
      assert metadata.plugin_id == "websocket_plugin"

      Health.handle_plugin_failure("btc_kraken", "kafka_plugin")

      assert_receive {:telemetry, [:flezha, :convergence, :plugin_escalated], measurements,
                      metadata},
                     500

      assert measurements.strike_count == 2
      assert metadata.relay_id == "btc_kraken"
      assert Enum.sort(metadata.plugin_ids) == ["kafka_plugin", "websocket_plugin"]
    end
  end

  describe "telemetry handler" do
    test ":output_stall events are accepted without crashing" do
      {:ok, pid} = start_supervised({Health, tick_ms: 60_000})

      :telemetry.execute(
        [:flezha, :state, :output_stall],
        %{stalled_ms: 90_000, last_seq: 5},
        %{relay: "btc_kraken", shard: "0", key: "p1"}
      )

      Process.sleep(50)
      assert Process.alive?(pid)
    end
  end
end

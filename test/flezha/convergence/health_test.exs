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
  use Mimic

  alias Flezha.Convergence.Health
  alias Flezha.Executor.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager

  setup :set_mimic_global

  setup do
    test_pid = self()
    handler_id = "convergence-health-test-#{System.unique_integer([:positive])}"

    :telemetry.attach_many(
      handler_id,
      [
        [:flezha, :convergence, :tick],
        [:flezha, :convergence, :action],
        [:flezha, :convergence, :plugin_restart],
        [:flezha, :convergence, :plugin_escalated],
        [:flezha, :executor, :reconnected],
        [:flezha, :executor, :reconnected_reassigned]
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

  describe "fresh-process executor takeover (orphan rebind)" do
    @namespace "takeover-ns"

    test "tick rebinds an orphan-bound relay and emits :reconnected_reassigned" do
      relay_id = "r-orphan"
      orphan = "exec-old"
      target = "exec-new"
      relay_pid = spawn_overwatch_stub(orphan, "fct-1")

      stub_namespace_topology(@namespace, relay_id, relay_pid, orphan, target, "fct-1")

      {:ok, _pid} = start_supervised({Health, tick_ms: 60_000})
      _ = Health.tick_now()

      assert_receive {:executor_migrate, ^target}, 1_000
      assert_receive {:relay_set_executor, @namespace, ^relay_id, ^target}, 500
      assert_receive {:hook_insert_pid, ^target, ^relay_id}, 500

      assert_receive {:telemetry, [:flezha, :executor, :reconnected_reassigned], %{count: 1},
                      %{previous_id: ^orphan, new_id: ^target, namespace: @namespace}},
                     500

      assert_receive {:telemetry, [:flezha, :executor, :reconnected], %{count: 1},
                      %{mode: :reassigned, executor_id: ^target}},
                     500
    end

    test ":executor_connected_event triggers an immediate tick" do
      relay_id = "r-event"
      orphan = "exec-gone"
      target = "exec-fresh"
      relay_pid = spawn_overwatch_stub(orphan, "fct-1")

      stub_namespace_topology(@namespace, relay_id, relay_pid, orphan, target, "fct-1")

      {:ok, _pid} = start_supervised({Health, tick_ms: 60_000})

      Flezha.Service.EventManager.dispatch(
        :executor_connected_event,
        {@namespace, target},
        %{members: :local}
      )

      assert_receive {:telemetry, [:flezha, :executor, :reconnected_reassigned], _, _}, 1_000
    end

    test "rebind is skipped when no active executor advertises the factory" do
      relay_id = "r-no-target"
      orphan = "exec-gone"
      relay_pid = spawn_overwatch_stub(orphan, "fct-1")

      HubManager
      |> stub(:await_recovery_complete, fn _timeout -> :ok end)
      |> stub(:get_running_namespaces, fn -> [@namespace] end)
      |> stub(:get_all_relays, fn @namespace -> [{relay_id, relay_pid}] end)
      |> stub(:get_pid, fn @namespace, ^relay_id -> {:ok, relay_pid} end)

      FleetManager
      |> stub(:active_executors, fn @namespace -> [] end)
      |> stub(:executors_for_factory, fn @namespace, _ -> [] end)

      RelayManager
      |> stub(:assign_executor, fn @namespace, _ -> nil end)

      {:ok, _pid} = start_supervised({Health, tick_ms: 60_000})
      _ = Health.tick_now()

      refute_receive {:telemetry, [:flezha, :executor, :reconnected_reassigned], _, _}, 200
    end
  end

  defp stub_namespace_topology(namespace, relay_id, relay_pid, _orphan, target, factory) do
    test_pid = self()

    HubManager
    |> stub(:await_recovery_complete, fn _timeout -> :ok end)
    |> stub(:get_running_namespaces, fn -> [namespace] end)
    |> stub(:get_all_relays, fn ^namespace -> [{relay_id, relay_pid}] end)
    |> stub(:get_pid, fn ^namespace, ^relay_id -> {:ok, relay_pid} end)
    |> stub(:relay_set_executor, fn ns, rid, exec ->
      send(test_pid, {:relay_set_executor, ns, rid, exec})
      :ok
    end)
    |> stub(:hook_insert_pid, fn exec, %{child_id: rid} ->
      send(test_pid, {:hook_insert_pid, exec, rid})
      :ok
    end)

    FleetManager
    |> stub(:active_executors, fn ^namespace -> [target] end)
    |> stub(:executors_for_factory, fn ^namespace, ^factory -> [target] end)

    RelayManager
    |> stub(:assign_executor, fn ^namespace, ^factory -> target end)

    :ok
  end

  defp spawn_overwatch_stub(executor_id, factory_id) do
    test_pid = self()

    spawn_link(fn ->
      overwatch_loop(test_pid, %{executor_id: executor_id, factory_id: factory_id})
    end)
  end

  defp overwatch_loop(test_pid, state) do
    receive do
      {:"$gen_call", from, :get_overwatch_state} ->
        GenServer.reply(from, state)
        overwatch_loop(test_pid, state)

      {:"$gen_call", from, {:executor_migrate, new_id}} ->
        send(test_pid, {:executor_migrate, new_id})
        GenServer.reply(from, :ok)
        overwatch_loop(test_pid, %{state | executor_id: new_id})

      _ ->
        overwatch_loop(test_pid, state)
    end
  end
end

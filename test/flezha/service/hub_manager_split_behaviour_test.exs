defmodule Flezha.Service.HubManagerSplitBehaviourTest do
  @moduledoc """
  `FLEZHA_SPLIT_BEHAVIOUR` and its parameters become one ProcessHub partition
  tolerance strategy for every hub, a setting that cannot work is refused with
  the variable's name, and a hub without quorum answers starts and stops with
  `no_quorum`. `FLEZHA_PROCESS_HUB_NODEDOWN_GRACE_MS`, the wait before a split
  or a failure moves anything, reaches every namespace hub.
  """

  # async: false: writes Application env; starts and stops the schedule hub.

  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.Config.Keys
  alias Flezha.Schedule.Hub, as: ScheduleHub
  alias Flezha.Schedule.Store, as: ScheduleStore
  alias Flezha.Service.Schedule
  alias Flezha.Service.HubManager
  alias Flezha.Service.HubManager.Lifecycle
  alias Flezha.Service.RelayManager.{Init, Teardown}
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ScheduleTestHelper

  alias ProcessHub.Strategy.PartitionTolerance.{Divergence, DynamicQuorum, StaticQuorum}

  @namespace_hub :split_behaviour_test_hub
  @ops %{source: :ui, principal: "op@_system", role: :ops, namespace: "_system"}

  defp split(behaviour, settings \\ []) do
    Application.put_env(:flezha, :split_behaviour, behaviour)
    Enum.each(settings, fn {key, value} -> Application.put_env(:flezha, key, value) end)
  end

  defp strategy, do: Lifecycle.partition_tolerance_strategy()

  describe "the strategy builder" do
    test "nothing set builds Divergence" do
      assert strategy() == %Divergence{}
    end

    test "static takes the quorum in nodes and checks it at start" do
      split(:static, quorum_nodes: 3)
      assert strategy() == %StaticQuorum{quorum_size: 3, startup_confirm: true}
    end

    test "static without FLEZHA_QUORUM_NODES is refused, naming the variable" do
      split(:static)
      assert_raise ArgumentError, ~r/^FLEZHA_QUORUM_NODES is required/, fn -> strategy() end
    end

    test "dynamic maps the percent, the window in whole seconds rounded up, and the startup count" do
      split(:dynamic)
      assert strategy() == %DynamicQuorum{quorum_size: 51, threshold_time: 30, startup_nodes: nil}

      split(:dynamic, quorum_percent: 75, quorum_window_ms: 1_500, quorum_nodes: 4)
      assert strategy() == %DynamicQuorum{quorum_size: 75, threshold_time: 2, startup_nodes: 4}

      split(:dynamic, quorum_window_ms: 1_000)
      assert strategy().threshold_time == 1
    end

    test "dynamic refuses a percent outside 51..100, naming the variable" do
      for percent <- [50, 101] do
        split(:dynamic, quorum_percent: percent)

        assert_raise ArgumentError, ~r/^FLEZHA_QUORUM_PERCENT is #{percent}/, fn ->
          strategy()
        end
      end
    end

    test "the running schedule hub was built from the setting" do
      split(:dynamic)
      {:ok, _pid} = HubManager.start_schedule_hub()
      on_exit(&HubManager.stop_schedule_hub/0)

      hub = ProcessHub.Hub.get(HubManager.schedule_hub_id())
      stored = ProcessHub.Service.Storage.get(hub.storage.misc, :partition_tolerance_strategy)
      assert %DynamicQuorum{quorum_size: 51} = stored
    end
  end

  describe "the failure grace" do
    test "every namespace hub takes the failure grace, 30 s unless set; 0 turns it off" do
      namespace = "nodedown-grace-test"
      on_exit(fn -> HubManager.stop_hub(namespace) end)
      assert Keys.fetch!(:process_hub_nodedown_grace_ms).default == 30_000

      for grace <- [30_000, 0] do
        Application.put_env(:flezha, :process_hub_nodedown_grace_ms, grace)
        {:ok, _pid} = HubManager.start_hub(namespace)

        hub = ProcessHub.Hub.get(Flezha.Identificator.hub(namespace))
        assert ProcessHub.Service.Storage.get(hub.storage.misc, :nodedown_grace_period) == grace

        HubManager.stop_hub(namespace)
      end
    end
  end

  describe "a hub without quorum" do
    # A static quorum of two on a one-node test cluster: the hub locks at start.
    setup do
      split(:static, quorum_nodes: 2)
      :ok
    end

    defp start_locked_hub(hub_id) do
      {:ok, pid} =
        ProcessHub.start_link(%ProcessHub{
          hub_id: hub_id,
          partition_tolerance_strategy: strategy()
        })

      Process.unlink(pid)
      on_exit(fn -> ProcessHub.stop(hub_id) end)
      assert NodeTestHelper.wait_until(fn -> ProcessHub.is_partitioned?(hub_id) end, 5_000)
    end

    test "is reported only when a quorum behaviour is set" do
      start_locked_hub(@namespace_hub)
      assert HubManager.quorum(@namespace_hub) == {:error, :no_quorum}

      split(:divergence)
      assert HubManager.quorum(@namespace_hub) == :ok
    end

    test "is not reported for a hub that is not running" do
      assert HubManager.quorum(:no_such_hub_running_here) == :ok
    end

    test "answers a relay start and stop with no_quorum" do
      namespace = "split-behaviour-test"
      start_locked_hub(Flezha.Identificator.hub(namespace))

      spec = %{id: "r1", start: {Agent, :start_link, [fn -> :ok end]}}
      assert Init.start_declared(namespace, spec, %{}, "r1") == {:error, :no_quorum}
      assert Teardown.stop_relay(namespace, "r1", []) == {:error, :no_quorum}

      relay = %{id: "r1", factory_id: nil, opts: %{}}

      assert Init.init_relay(fn _ns, _id -> false end, namespace, relay, %{
               announce?: false,
               record?: false
             }) ==
               {:error, :no_quorum}
    end

    test "is told to the caller as controller.no_quorum" do
      error = Flezha.Error.from_reason(:no_quorum, :relay_start)

      assert error.code == "controller.no_quorum"
      assert error.class == :unavailable
      assert error.message =~ "minority side of a network split"
    end

    test "refuses a schedule add and remove, and the sweep asserts nothing" do
      ScheduleTestHelper.isolated_store_setup()
      {:ok, _pid} = HubManager.start_schedule_hub()
      on_exit(&HubManager.stop_schedule_hub/0)

      assert NodeTestHelper.wait_until(
               fn -> ProcessHub.is_partitioned?(HubManager.schedule_hub_id()) end,
               5_000
             )

      assert {:error, %Flezha.Error{code: "controller.no_quorum"}} =
               Schedule.add(ScheduleTestHelper.entry_fields(%{name: "split-job"}), @ops)

      assert {:error, %Flezha.Error{code: "controller.no_quorum"}} =
               Schedule.remove("split-job", @ops)

      {:ok, entry} = ScheduleStore.add(ScheduleTestHelper.entry_fields(%{name: "held-job"}))
      assert ScheduleHub.assert_all() == :ok
      assert ProcessHub.child_lookup(HubManager.schedule_hub_id(), entry.name) == nil
    end
  end
end

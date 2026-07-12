defmodule FlezhaTest.Operative.OverwatchSuspensionTest do
  @moduledoc """
  `bounded-durable-suspension` lifecycle on the Overwatch: park (evict from the
  executor, keep the registry entry), rehydrate on a new input or the durable
  timer, exactly one rehydration when both wake sources race, and rediscovery of
  a suspended instance from durable state alone after a controller redeploy.

  The callbacks are driven directly on a struct (no live GenServer), the way
  `overwatch_restore_async_test` does — the state machine is the unit here.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Operative.{Builder, Overwatch}
  alias Flezha.Plugin.Extension.Table
  alias Flezha.State.{OpInbox, OpJournal, TestHelpers}

  setup :set_mimic_global

  setup do
    root = TestHelpers.tmp_app_data_dir()

    # The rehydrate drives the real (un-park) bind task, which has no executor to
    # reach in a unit test: bound its wait + retry budget so the assertions about
    # the state machine do not pay the production backoff.
    on_exit(TestHelpers.override_env(:overwatch_executor_wait_ms, 50))
    on_exit(TestHelpers.override_env(:overwatch_retry_backoff_ms, [1]))

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(root)
    end)

    :ok
  end

  defp instance_id(), do: "susp_#{System.unique_integer([:positive])}"

  defp state(inst, overrides \\ []) do
    base = %Overwatch{
      id: inst,
      namespace: "default",
      executor_id: "ex_1",
      factory_id: "chat",
      # The test process stands in for the operative's StreamChannel: a
      # forwarded input arrives as an `{:io_in, ...}` cast.
      channel: self(),
      builder: %Builder{id: inst, factory_id: "chat", kind: :bounded, opts: []},
      extensions: Table.new(Overwatch.__extensions_catalog__()),
      durable_bounded: true
    }

    struct!(base, overrides)
  end

  test "park evicts the instance from its executor and keeps it registered" do
    inst = instance_id()
    test_pid = self()

    stub(Flezha.Executor.ControlUnit, :stop_operative_async, fn "ex_1", ^inst ->
      send(test_pid, :evicted)
      :ok
    end)

    stub(Flezha.Service.HubManager, :operative_set_executor, fn _ns, ^inst, executor ->
      send(test_pid, {:registry_executor, executor})
      :ok
    end)

    {:noreply, suspended} = Overwatch.handle_cast({:suspend, 2, nil}, state(inst))

    # Zero live executor resources; the registry entry (and this process) stay.
    assert_receive :evicted
    assert_receive {:registry_executor, nil}

    assert suspended.suspended
    assert suspended.executor_id == nil
    assert suspended.channel == nil
    assert suspended.wake_timer == nil, "no wake_after declared ⇒ wait for input alone"
  end

  test "a turn that landed while the loop was parking is never stranded" do
    inst = instance_id()
    stub(Flezha.Executor.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_executor, fn _, _, _ -> :ok end)
    stub(Flezha.Service.OperativeManager, :assign_executor, fn _, _, _ -> "ex_2" end)

    # Iteration 0 consumed offset 1; offset 2 arrived while the driver was on its
    # way out and was never drained — durable, but undelivered.
    {:ok, 1} = OpInbox.append(inst, "consumed", %{})
    {:ok, 2} = OpInbox.append(inst, "in-flight", %{})
    :ok = OpJournal.append_iter_committed(inst, 0, 1, [])

    {:noreply, after_park} = Overwatch.handle_cast({:suspend, 0, nil}, state(inst))

    assert after_park.migrate_task != nil,
           "an instance with undelivered input must rehydrate, not sit suspended " <>
             "waiting for a message that already arrived"

    # With nothing pending, the same park suspends and stays there.
    :ok = OpJournal.append_iter_committed(inst, 1, 2, [])
    {:noreply, idle} = Overwatch.handle_cast({:suspend, 1, nil}, state(inst))
    assert idle.suspended
    assert idle.migrate_task == nil
  end

  test "a park with a wake deadline arms the durable timer" do
    inst = instance_id()
    stub(Flezha.Executor.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_executor, fn _, _, _ -> :ok end)

    deadline = System.system_time(:millisecond) + 50_000
    {:noreply, suspended} = Overwatch.handle_cast({:suspend, 1, deadline}, state(inst))

    assert is_reference(suspended.wake_timer)
    assert Process.read_timer(suspended.wake_timer) > 40_000
  end

  test "input for a suspended instance is journaled and rehydrates it" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_executor, fn _ns, "chat", ^inst -> "ex_2" end)

    suspended = state(inst, suspended: true, executor_id: nil, channel: nil)

    {:noreply, rehydrating} =
      Overwatch.handle_cast({:io_in, "hello", %{"channel" => "chat"}}, suspended)

    # Durable first: the input is in the inbox before anything else happens, so a
    # crash mid-rehydrate cannot lose it.
    assert [%{offset: 1, message: "hello"}] = OpInbox.undelivered(inst)

    assert rehydrating.migrate_task != nil,
           "the rehydrate runs through the normal (un-park) bind task"

    # Racing wake sources resolve to exactly one rehydration: a second input
    # while the first is still in flight journals, and reuses the same bind.
    {:noreply, still_one} = Overwatch.handle_cast({:io_in, "second", %{}}, rehydrating)

    assert still_one.migrate_task == rehydrating.migrate_task,
           "a second wake source does not start a second rehydration"

    assert Enum.map(OpInbox.undelivered(inst), & &1.message) == ["hello", "second"]
  end

  test "the wake deadline rehydrates with a timer tick and no input" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_executor, fn _, _, _ -> "ex_2" end)

    suspended = state(inst, suspended: true, executor_id: nil, channel: nil)
    {:noreply, rehydrating} = Overwatch.handle_info(:wake_deadline, suspended)

    assert rehydrating.migrate_task != nil
    assert rehydrating.wake_timer == nil

    assert [%{offset: 1, message: "", metadata: %{"flz.wake" => "timer"}}] =
             OpInbox.undelivered(inst),
           "the timer wake rides the inbox — one wake path, so racing sources collapse"
  end

  test "with no executor to rehydrate onto, the instance stays suspended" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_executor, fn _, _, _ -> nil end)

    suspended = state(inst, suspended: true, executor_id: nil, channel: nil)
    {:noreply, still} = Overwatch.handle_cast({:io_in, "hi", %{}}, suspended)

    assert still.suspended
    assert still.migrate_task == nil
    assert [%{message: "hi"}] = OpInbox.undelivered(inst), "the input is not lost"
  end

  test "a suspended instance is rediscovered from durable state after a redeploy" do
    inst = instance_id()
    builder = %Builder{id: inst, factory_id: "chat", kind: :bounded, opts: []}

    # Durable state of a suspended instance: iteration 0 committed, then parked.
    :ok = OpJournal.append_op_committed(inst, 0, 0, [], [])
    :ok = OpJournal.append_iter_committed(inst, 0, 0, [])
    :ok = OpJournal.append_parked(inst, 0, System.system_time(:millisecond) + 60_000)

    fresh = %Overwatch{
      id: inst,
      namespace: "default",
      factory_id: "chat",
      builder: builder
    }

    {:noreply, booted} =
      Overwatch.handle_continue({:bind_executor, "stale-executor", builder}, fresh)

    assert booted.suspended, "the boot never binds a suspended instance to an executor"
    assert booted.executor_id == nil
    assert is_reference(booted.wake_timer), "the journaled wake deadline is re-armed"

    # Undelivered input outranks the park: an instance with work waiting binds
    # instead of coming back suspended.
    {:ok, 1} = OpInbox.append(inst, "arrived-while-down", %{})

    stub(Flezha.Service.OperativeManager, :assign_executor, fn _, _, _ -> nil end)

    assert {:stop, :executor_not_ready, _} =
             Overwatch.handle_continue({:bind_executor, "stale-executor", builder}, fresh),
           "it takes the normal bind path (which fails here only for lack of an executor)"
  end
end

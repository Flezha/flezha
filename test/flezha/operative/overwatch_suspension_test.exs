defmodule FlezhaTest.Operative.OverwatchSuspensionTest do
  @moduledoc """
  `workflow-durable-suspension` lifecycle on the Overwatch: park (evict from the
  runner, keep the registry entry), rehydrate on a new input or the durable
  timer, exactly one rehydration when both wake sources race, and rediscovery of
  a suspended instance from durable state alone after a controller redeploy.

  Plus `workflow-loop-rewind`, which rides the same evict → rehydrate path: the
  refusals (uncommitted target, uncompensable output) and the durable effect of
  the ones that go through.

  The callbacks are driven directly on a struct (no live GenServer), the way
  `overwatch_restore_async_test` does — the state machine is the unit here.
  """
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Operative.{Builder, ClassRegistry, Overwatch}
  alias Flezha.Plugin.Extension.Table
  alias Flezha.State.{ProcInbox, ProcJournal, TestHelpers}

  setup :set_mimic_global_until_exit

  setup do
    root = TestHelpers.tmp_app_data_dir()

    # The rehydrate drives the real (un-park) bind task, which has no runner to
    # reach in a unit test: bound its wait + retry budget so the assertions about
    # the state machine do not pay the production backoff.
    on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 50))
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
      runner_id: "ex_1",
      factory_id: "chat",
      # The test process stands in for the operative's StreamChannel: a
      # forwarded input arrives as an `{:io_in, ...}` cast.
      channel: self(),
      builder: %Builder{id: inst, factory_id: "chat", kind: :workflow, opts: []},
      extensions: Table.new(Overwatch.__extensions_catalog__()),
      durable_workflow: true
    }

    struct!(base, overrides)
  end

  # fails when: an un-parked instance rebinds onto a runner but publishes neither
  # the registry tag nor the runner's route to its pid, so every frame it produces
  # hits `stream_channel.ex`'s `[] -> :ok` drop while it still reads as healthy.
  test "un-parking publishes the registry tag and the runner route" do
    inst = instance_id()
    test_pid = self()

    stub(Flezha.Service.HubManager, :operative_set_runner, fn ns, ^inst, runner ->
      send(test_pid, {:tag_published, ns, runner})
      :ok
    end)

    stub(Flezha.Service.HubManager, :hook_insert_local_pid, fn runner, ^inst, pid ->
      send(test_pid, {:route_published, runner, pid})
      :ok
    end)

    parked = state(inst, migrate_task: %{ref: make_ref(), pid: self(), mode: :unpark})
    recovered = %{parked | runner_id: "ex_woken", suspended: false}

    {:noreply, unparked} =
      Overwatch.handle_info({:migrate_task_result, self(), {:unparked, recovered}}, parked)

    assert unparked.migrate_task == nil
    assert_receive {:tag_published, "default", "ex_woken"}
    assert_receive {:route_published, "ex_woken", ^test_pid}
  end

  test "park evicts the instance from its runner and keeps it registered" do
    inst = instance_id()
    test_pid = self()

    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn "ex_1", ^inst ->
      send(test_pid, :evicted)
      :ok
    end)

    stub(Flezha.Service.HubManager, :operative_set_runner, fn _ns, ^inst, runner ->
      send(test_pid, {:registry_runner, runner})
      :ok
    end)

    {:noreply, suspended, :hibernate} = Overwatch.handle_cast({:suspend, 2, nil}, state(inst))

    # Zero live runner resources; the registry entry (and this process) stay.
    assert_receive :evicted
    assert_receive {:registry_runner, nil}

    assert suspended.suspended
    assert suspended.runner_id == nil
    assert suspended.channel == nil
    assert suspended.wake_timer == nil, "no wake_after declared ⇒ wait for input alone"
  end

  test "a turn that landed while the loop was parking is never stranded" do
    inst = instance_id()
    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> "ex_2" end)

    # Iteration 0 consumed offset 1; offset 2 arrived while the driver was on its
    # way out and was never drained — durable, but undelivered.
    {:ok, 1} = ProcInbox.append(inst, "consumed", %{})
    {:ok, 2} = ProcInbox.append(inst, "in-flight", %{})
    :ok = ProcJournal.append_iter_committed(inst, 0, 1, [])

    {:noreply, after_park, :hibernate} = Overwatch.handle_cast({:suspend, 0, nil}, state(inst))

    assert after_park.migrate_task != nil,
           "an instance with undelivered input must rehydrate, not sit suspended " <>
             "waiting for a message that already arrived"

    # With nothing pending, the same park suspends and stays there.
    :ok = ProcJournal.append_iter_committed(inst, 1, 2, [])
    {:noreply, idle, :hibernate} = Overwatch.handle_cast({:suspend, 1, nil}, state(inst))
    assert idle.suspended
    assert idle.migrate_task == nil
  end

  test "a park with a wake deadline arms the durable timer" do
    inst = instance_id()
    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)

    deadline = System.system_time(:millisecond) + 50_000

    {:noreply, suspended, :hibernate} =
      Overwatch.handle_cast({:suspend, 1, deadline}, state(inst))

    assert is_reference(suspended.wake_timer)
    assert Process.read_timer(suspended.wake_timer) > 40_000
  end

  test "input for a suspended instance is journaled and rehydrates it" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _ns, "chat", ^inst -> "ex_2" end)

    suspended = state(inst, suspended: true, runner_id: nil, channel: nil)

    {:noreply, journaling} =
      Overwatch.handle_cast({:io_in, "hello", %{"channel" => "chat"}}, suspended)

    assert journaling.migrate_task == nil, "nothing wakes before the input is durable"
    rehydrating = settle_journal(journaling)

    # Durable first: the input is in the inbox before anything else happens, so a
    # crash mid-rehydrate cannot lose it.
    assert [%{offset: 1, message: "hello"}] = ProcInbox.undelivered(inst)

    assert rehydrating.migrate_task != nil,
           "the rehydrate runs through the normal (un-park) bind task"

    # Racing wake sources resolve to exactly one rehydration: a second input
    # while the first is still in flight journals, and reuses the same bind.
    {:noreply, journaling} = Overwatch.handle_cast({:io_in, "second", %{}}, rehydrating)
    still_one = settle_journal(journaling)

    assert still_one.migrate_task == rehydrating.migrate_task,
           "a second wake source does not start a second rehydration"

    assert Enum.map(ProcInbox.undelivered(inst), & &1.message) == ["hello", "second"]
  end

  test "the wake deadline rehydrates with a timer tick and no input" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> "ex_2" end)

    suspended = state(inst, suspended: true, runner_id: nil, channel: nil)
    {:noreply, rehydrating} = Overwatch.handle_info(:wake_deadline, suspended)

    assert rehydrating.migrate_task != nil
    assert rehydrating.wake_timer == nil

    assert [%{offset: 1, message: "", metadata: %{"flz.wake" => "timer"}}] =
             ProcInbox.undelivered(inst),
           "the timer wake rides the inbox — one wake path, so racing sources collapse"
  end

  test "with no runner to rehydrate onto, the instance stays suspended" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> nil end)

    suspended = state(inst, suspended: true, runner_id: nil, channel: nil)
    {:noreply, journaling} = Overwatch.handle_cast({:io_in, "hi", %{}}, suspended)
    still = settle_journal(journaling)

    assert still.suspended
    assert still.migrate_task == nil
    assert [%{message: "hi"}] = ProcInbox.undelivered(inst), "the input is not lost"
  end

  # The inbox writer's answer to the append a cast issued — the host acts on it
  # when it reaches its mailbox, which this test process stands in for.
  defp settle_journal(state) do
    assert_receive {[:alias | _], _} = reply, 2_000
    {:noreply, state} = Overwatch.handle_info(reply, state)
    state
  end

  test "a suspended instance is rediscovered from durable state after a redeploy" do
    inst = instance_id()
    builder = %Builder{id: inst, factory_id: "chat", kind: :workflow, opts: []}

    # Durable state of a suspended instance: iteration 0 committed, then parked.
    :ok = ProcJournal.append_proc_committed(inst, 0, 0, [], [])
    :ok = ProcJournal.append_iter_committed(inst, 0, 0, [])
    :ok = ProcJournal.append_parked(inst, 0, System.system_time(:millisecond) + 60_000)

    fresh = %Overwatch{
      id: inst,
      namespace: "default",
      factory_id: "chat",
      builder: builder
    }

    {:noreply, booted, :hibernate} =
      Overwatch.handle_continue({:bind_runner, "stale-runner", builder}, fresh)

    assert booted.suspended, "the boot never binds a suspended instance to a runner"
    assert booted.runner_id == nil
    assert is_reference(booted.wake_timer), "the journaled wake deadline is re-armed"

    # Undelivered input outranks the park: an instance with work waiting binds
    # instead of coming back suspended.
    {:ok, 1} = ProcInbox.append(inst, "arrived-while-down", %{})

    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> nil end)

    assert {:stop, :runner_not_ready, _} =
             Overwatch.handle_continue({:bind_runner, "stale-runner", builder}, fresh),
           "it takes the normal bind path (which fails here only for lack of a runner)"
  end

  # ---------- workflow-loop-rewind ----------

  # The rewind rides the same evict → rehydrate path a park does, so the same
  # stubs stand in for the runner side.
  defp stub_rebind() do
    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> "ex_2" end)
  end

  defp commit_turns(inst, count, outbox) do
    for iter <- 0..(count - 1) do
      :ok = ProcJournal.append_proc_committed(inst, 0, iter, [], [], outbox)
      :ok = ProcJournal.append_iter_committed(inst, iter, iter + 1, [])
    end
  end

  defp emitting_outbox(),
    do: [%{entity_id: "seg", seq_in_proc: 0, channel: "replies", payload: "hi"}]

  test "rewind journals the marker, evicts, and rehydrates" do
    inst = instance_id()
    stub_rebind()
    commit_turns(inst, 3, [])

    {:reply, :ok, state} = Overwatch.handle_call({:rewind, 1}, self(), state(inst))

    # The marker is durable, the epoch bumped, and the loop folded back to
    # iteration 2 with iteration 2's proc outstanding as undo.
    assert ProcJournal.current_epoch(inst) == 1
    assert ProcJournal.committed_iterations(inst) == [0, 1]
    assert [%{proc_idx: 0, iter: 2}] = ProcJournal.restore_state(inst).rewind_undo

    # A rewind is a rebind, not a park: the instance comes straight back up and
    # nothing was journaled as `parked`.
    assert state.migrate_task != nil
    {:ok, entries} = ProcJournal.read(inst)
    refute Enum.any?(entries, &(&1.type == :parked))
  end

  test "rewind to an uncommitted iteration is refused, changing nothing" do
    inst = instance_id()
    commit_turns(inst, 3, [])

    assert {:reply, {:error, {:uncommitted_iteration, [0, 1, 2]}}, _} =
             Overwatch.handle_call({:rewind, 7}, self(), state(inst))

    assert ProcJournal.current_epoch(inst) == 0
    {:ok, entries} = ProcJournal.read(inst)
    refute Enum.any?(entries, &(&1.type == :rewound))
  end

  test "rewind is refused when an unwound procedure emitted and cannot compensate" do
    inst = instance_id()
    commit_turns(inst, 3, emitting_outbox())

    # No `compensate` hook declared on proc 0 — its output cannot be undone.
    :ok = ClassRegistry.put_compensable_ops(inst, [])

    assert {:reply, {:error, {:uncovered, ["replies"]}}, _} =
             Overwatch.handle_call({:rewind, 1}, self(), state(inst))

    assert ProcJournal.current_epoch(inst) == 0, "refused before anything durable changed"
  end

  test "an emitting procedure that declares compensate rewinds" do
    inst = instance_id()
    stub_rebind()
    commit_turns(inst, 3, emitting_outbox())
    :ok = ClassRegistry.put_compensable_ops(inst, [0])

    assert {:reply, :ok, _} = Overwatch.handle_call({:rewind, 1}, self(), state(inst))
    assert ProcJournal.current_epoch(inst) == 1
  end

  test "iterations that emitted nothing need no coverage" do
    inst = instance_id()
    stub_rebind()
    commit_turns(inst, 3, [])
    :ok = ClassRegistry.put_compensable_ops(inst, [])

    assert {:reply, :ok, _} = Overwatch.handle_call({:rewind, 1}, self(), state(inst))
  end

  describe "migration_consent/1 (processhub-deferred-migration)" do
    test "a pending correction defers" do
      inst = instance_id()

      st =
        state(inst,
          durable_workflow: false,
          corrections: %{{inst, "proc", "k"} => 100}
        )

      assert Overwatch.migration_consent(st) == :defer
    end

    test "an unsuspended durable workflow defers; a suspended one consents" do
      inst = instance_id()
      assert Overwatch.migration_consent(state(inst)) == :defer
      assert Overwatch.migration_consent(state(inst, suspended: true)) == :ready
    end

    test "an effectively-once instance with pending outbox defers, drained consents" do
      inst = instance_id()
      st = state(inst, durable_workflow: false, effectively_once: true)

      stub(Flezha.State.Supervisor, :live_writers, fn _class -> [self()] end)

      stub(Flezha.State.WalWriter, :outbox_status, fn _pid, _timeout ->
        %{pending: 3, awaiting: 0}
      end)

      assert Overwatch.migration_consent(st) == :defer

      # Dispatched but unconfirmed still owes output that would strand here.
      stub(Flezha.State.WalWriter, :outbox_status, fn _pid, _timeout ->
        %{pending: 0, awaiting: 2}
      end)

      assert Overwatch.migration_consent(st) == :defer

      stub(Flezha.State.WalWriter, :outbox_status, fn _pid, _timeout ->
        %{pending: 0, awaiting: 0}
      end)

      assert Overwatch.migration_consent(st) == :ready
    end

    test "an outbox-query failure fails open to :ready" do
      inst = instance_id()
      st = state(inst, durable_workflow: false, effectively_once: true)

      stub(Flezha.State.Supervisor, :live_writers, fn _class -> [self()] end)
      stub(Flezha.State.WalWriter, :outbox_status, fn _pid, _timeout -> exit(:timeout) end)

      assert Overwatch.migration_consent(st) == :ready
    end

    test "an idle stream instance consents immediately" do
      inst = instance_id()
      assert Overwatch.migration_consent(state(inst, durable_workflow: false)) == :ready
    end
  end

  describe "migration_ready barriers (processhub-deferred-migration)" do
    test "suspension signals migration_ready; :not_deferred leaves the park intact" do
      inst = instance_id()
      test_pid = self()
      stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
      stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)

      stub(ProcessHub.Service.Migration, :migration_ready, fn hub_id, child_id ->
        send(test_pid, {:migration_ready, hub_id, child_id})
        {:error, :not_deferred}
      end)

      {:noreply, suspended, :hibernate} = Overwatch.handle_cast({:suspend, 2, nil}, state(inst))

      assert_receive {:migration_ready, hub_id, ^inst}
      assert hub_id == Flezha.Service.Identificator.hub("default")
      assert suspended.suspended
    end

    test "the last correction draining signals migration_ready; a remaining one does not" do
      inst = instance_id()
      test_pid = self()

      stub(ProcessHub.Service.Migration, :migration_ready, fn _hub_id, child_id ->
        send(test_pid, {:migration_ready, child_id})
        {:error, :not_deferred}
      end)

      # A stale debounce timer for an entity no longer pending: the pop leaves
      # `corrections` empty, which is the completion condition.
      st = state(inst, durable_workflow: false, corrections: %{})
      {:noreply, _} = Overwatch.handle_info({:run_correction, {inst, "proc", "k"}}, st)
      assert_receive {:migration_ready, ^inst}

      # Another entity still pending: not complete, no signal.
      busy =
        state(inst,
          durable_workflow: false,
          corrections: %{{inst, "proc", "other"} => 5}
        )

      {:noreply, _} = Overwatch.handle_info({:run_correction, {inst, "proc", "gone"}}, busy)
      refute_received {:migration_ready, ^inst}
    end
  end
end

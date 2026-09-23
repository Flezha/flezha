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
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Identificator
  alias Flezha.Process.Timer
  alias Flezha.Operative.{Builder, ClassRegistry, Overwatch}
  alias Flezha.Plugin.Extension.Table
  alias Flezha.State.{InputLog, TaskJournal, TestHelpers}

  setup :set_mimic_global_until_exit

  setup do
    # The rehydrate drives the real (un-park) bind task, which has no runner to
    # reach in a unit test: bound its wait so the assertions about the state
    # machine do not pay the production window.
    on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 50))

    :ok
  end

  defp instance_id(), do: "susp_#{System.unique_integer([:positive])}"

  defp park(deadline \\ nil), do: %{wake_deadline_ms: deadline, consumed_offset: nil}

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

    stub(Flezha.Runner.StreamPool, :put_route, fn runner, ^inst, pid ->
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

  # fails when: an un-park that binds a runner keeps a snapshot stored before it,
  # so a handover after that runner dies hard ships the older state and skips
  # the pull of every change the runner made.
  test "un-parking onto a runner drops the snapshot stored before it" do
    inst = instance_id()
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Runner.StreamPool, :put_route, fn _, _, _ -> :ok end)

    parked =
      state(inst,
        snapshot: "stored_before_the_park",
        migrate_task: %{ref: make_ref(), pid: self(), mode: :unpark}
      )

    recovered = %{parked | runner_id: "ex_woken", suspended: false}

    {:noreply, unparked} =
      Overwatch.handle_info({:migrate_task_result, self(), {:unparked, recovered}}, parked)

    assert unparked.runner_id == "ex_woken"
    assert unparked.snapshot == nil
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

    {:noreply, suspended, :hibernate} = Overwatch.handle_cast({:suspend, 2, park()}, state(inst))

    # Zero live runner resources; the registry entry (and this process) stay.
    assert_receive :evicted
    assert_receive {:registry_runner, nil}

    assert suspended.suspended
    assert suspended.runner_id == nil
    assert suspended.channel == nil
    refute Timer.armed?(suspended.timers, :wake), "no pending timer ⇒ wait for input alone"
  end

  # fails when: a park clears the ownership record, so a cell frame the
  # parking runner sent before it parked, still in flight on the data stream, is
  # refused and never reaches the WAL a rehydrate restores from.
  test "a park keeps the ownership record naming the runner that parked" do
    inst = instance_id()
    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)
    :ok = ClassRegistry.put_overwatch(inst, self(), "ex_1")
    on_exit(fn -> ClassRegistry.delete(inst) end)

    {:noreply, suspended, :hibernate} = Overwatch.handle_cast({:suspend, 2, park()}, state(inst))

    assert suspended.runner_id == nil
    assert ClassRegistry.owner(inst) == {:ok, self(), "ex_1", nil}
  end

  test "a turn that landed while the loop was parking is never stranded" do
    inst = instance_id()
    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> "ex_2" end)

    # Iteration 0 consumed offset 0; offset 1 arrived while the driver was on its
    # way out and was never drained — durable, but undelivered. The commit's ack
    # reported offset 0 into the log's watermark.
    {:ok, 0} = InputLog.append(inst, "consumed", %{})
    {:ok, 1} = InputLog.append(inst, "in-flight", %{})
    :ok = TaskJournal.append_iter_committed(inst, 0, 0, [])
    :ok = TaskJournal.release_input(inst, 0)

    {:noreply, after_park, :hibernate} = Overwatch.handle_cast({:suspend, 0, park()}, state(inst))

    assert after_park.migrate_task != nil,
           "an instance with undelivered input must rehydrate, not sit suspended " <>
             "waiting for a message that already arrived"

    :ok = await_migrate_task(after_park)

    # With nothing pending, the same park suspends and stays there.
    :ok = TaskJournal.append_iter_committed(inst, 1, 1, [])
    :ok = TaskJournal.release_input(inst, 1)
    {:noreply, idle, :hibernate} = Overwatch.handle_cast({:suspend, 1, park()}, state(inst))
    assert idle.suspended
    assert idle.migrate_task == nil
  end

  test "a park with a wake deadline arms the durable timer" do
    inst = instance_id()
    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)

    deadline = System.system_time(:millisecond) + 50_000

    {:noreply, suspended, :hibernate} =
      Overwatch.handle_cast({:suspend, 1, park(deadline)}, state(inst))

    assert Timer.armed?(suspended.timers, :wake)
    assert Process.read_timer(elem(Map.fetch!(suspended.timers, :wake), 0)) > 40_000
  end

  test "input for a suspended instance is journaled and rehydrates it" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _ns, "chat", ^inst -> "ex_2" end)

    suspended = state(inst, suspended: true, runner_id: nil, channel: nil)

    {:noreply, journaling} =
      Overwatch.handle_cast({:io_in, "hello", %{"channel" => "chat"}}, suspended)

    assert journaling.migrate_task == nil, "nothing wakes before the input is durable"
    rehydrating = settle_journal(journaling)

    # Durable first: the input is in the log before anything else happens, so a
    # crash mid-rehydrate cannot lose it.
    assert [{0, "hello", _metadata}] = InputLog.undelivered(inst)

    assert rehydrating.migrate_task != nil,
           "the rehydrate runs through the normal (un-park) bind task"

    # Racing wake sources resolve to exactly one rehydration: a second input
    # while the first is still in flight journals, and reuses the same bind.
    {:noreply, journaling} = Overwatch.handle_cast({:io_in, "second", %{}}, rehydrating)
    still_one = settle_journal(journaling)

    assert still_one.migrate_task == rehydrating.migrate_task,
           "a second wake source does not start a second rehydration"

    assert Enum.map(InputLog.undelivered(inst), &elem(&1, 1)) == ["hello", "second"]

    :ok = await_migrate_task(rehydrating)
  end

  test "the wake deadline rehydrates and appends nothing" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> "ex_2" end)

    suspended = state(inst, suspended: true, runner_id: nil, channel: nil)
    {:noreply, rehydrating} = Overwatch.handle_info(:wake_deadline, suspended)

    assert rehydrating.migrate_task != nil
    refute Timer.armed?(rehydrating.timers, :wake)

    assert InputLog.undelivered(inst) == [],
           "the restored Cells' own timer fires; nothing rides the input log"

    :ok = await_migrate_task(rehydrating)
  end

  test "with no runner to rehydrate onto, the instance stays suspended" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> nil end)

    suspended = state(inst, suspended: true, runner_id: nil, channel: nil)
    {:noreply, journaling} = Overwatch.handle_cast({:io_in, "hi", %{}}, suspended)
    still = settle_journal(journaling)

    assert still.suspended
    assert still.migrate_task == nil
    assert [{0, "hi", _metadata}] = InputLog.undelivered(inst), "the input is not lost"
  end

  test "a wake deadline with no runner to rehydrate onto is retried a convergence tick later" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> nil end)

    suspended = state(inst, suspended: true, runner_id: nil, channel: nil)
    {:noreply, still} = Overwatch.handle_info(:wake_deadline, suspended)

    assert still.suspended
    assert still.migrate_task == nil
    assert Timer.armed?(still.timers, :wake), "nothing else would wake it"
    retry_in = Process.read_timer(elem(Map.fetch!(still.timers, :wake), 0))
    assert retry_in > 0 and retry_in <= Flezha.Config.Durations.get(:convergence_tick_ms)
  end

  # The input log's answer to the append a cast issued — the host acts on it
  # when it reaches its mailbox, which this test process stands in for.
  defp settle_journal(state) do
    assert_receive {[:alias | _], _} = reply, 2_000
    {:noreply, state} = Overwatch.handle_info(reply, state)
    state
  end

  # The rehydrate/rebind runs the real bind task, which has no runner to reach
  # here and so always comes back parked. Awaiting that result is both the
  # stronger assertion — the task the test started actually resolves, and on the
  # missing runner rather than on anything else — and what keeps it from
  # outliving its test and landing on the next test's instance-pinned stub
  # carrying this test's id.
  defp await_migrate_task(%Overwatch{migrate_task: %{pid: pid}}) do
    assert_receive {:migrate_task_result, ^pid, {:park, _parked, {:ping_failed, _}}}, 2_000
    :ok
  end

  test "a park writes the record before it evicts, and a record that fails leaves it bound" do
    inst = instance_id()
    test_pid = self()

    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> send(test_pid, :evicted) end)

    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)

    {:noreply, asleep, :hibernate} = Overwatch.handle_cast({:suspend, 3, park(nil)}, state(inst))
    assert asleep.suspended
    assert ClassRegistry.parked(inst) == park(nil)
    assert_received :evicted

    stub(ClassRegistry, :put_parked, fn _, _ -> {:error, :eio} end)
    other = instance_id()

    ExUnit.CaptureLog.capture_log(fn ->
      {:noreply, bound, :hibernate} = Overwatch.handle_cast({:suspend, 3, park()}, state(other))
      refute bound.suspended
      assert bound.runner_id == "ex_1"
    end)

    refute_received :evicted
  end

  test "a park landing while an un-park binds keeps its record and the suspension" do
    inst = instance_id()
    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)
    binding = state(inst, migrate_task: %{ref: make_ref(), pid: self(), mode: :unpark})

    {:noreply, parked, :hibernate} = Overwatch.handle_cast({:suspend, 4, park()}, binding)
    assert parked.parked_in_flight

    recovered = %{parked | runner_id: "ex_stale", suspended: false}

    {:noreply, after_bind} =
      Overwatch.handle_info({:migrate_task_result, self(), {:unparked, recovered}}, parked)

    assert after_bind.suspended, "the stale bound state is not adopted"
    assert ClassRegistry.parked(inst) == park()
  end

  test "whether an instance sleeps is read from its park record and its log, for either kind" do
    for kind <- [:stream, :workflow] do
      inst = instance_id()
      refute Flezha.Operative.Migration.suspended_on_disk?(inst)

      :ok = ClassRegistry.put_parked(inst, park())
      assert Flezha.Operative.Migration.suspended_on_disk?(inst), "#{kind} asleep"

      # Input that arrived while it slept outranks the record: it binds.
      {:ok, 0} = InputLog.append(inst, "arrived", %{})
      refute Flezha.Operative.Migration.suspended_on_disk?(inst), "#{kind} owes input"
    end
  end

  test "an elapsed wake deadline is pending work, as undelivered input is" do
    inst = instance_id()
    now = System.system_time(:millisecond)

    :ok = ClassRegistry.put_parked(inst, park(now + 60_000))
    assert Flezha.Operative.Migration.suspended_on_disk?(inst), "its timer is not due yet"

    :ok = ClassRegistry.put_parked(inst, park(now - 1))
    refute Flezha.Operative.Migration.suspended_on_disk?(inst), "its timer is due: it binds"
  end

  test "a sleeping stream is rediscovered asleep at boot, and its forwarded floor with it" do
    inst = instance_id()
    builder = %Builder{id: inst, factory_id: "counter", kind: :stream, opts: []}
    for n <- 0..1, do: {:ok, ^n} = InputLog.append(inst, "in-#{n}", %{})
    :ok = ClassRegistry.put_sleep_when_idle(inst, true)
    :ok = ClassRegistry.put_parked(inst, %{wake_deadline_ms: nil, consumed_offset: 1})
    fresh = %Overwatch{id: inst, namespace: "default", factory_id: "counter", builder: builder}

    {:noreply, booted, :hibernate} =
      Overwatch.handle_continue({:bind_runner, "stale-runner", builder}, fresh)

    assert booted.suspended
    assert booted.sleeps, "a message that finds it asleep is journaled"

    assert InputLog.tail(inst) == {:ok, 1, 1},
           "what it was forwarded before the sleep is not owed"
  end

  test "a stop of a sleeping workflow wakes it with the stop, and the bind sends it" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> "ex_2" end)
    :ok = ClassRegistry.put_parked(inst, park())
    asleep = state(inst, suspended: true, runner_id: nil, channel: nil)

    {:reply, :ok, waking} = Overwatch.handle_call({:wake, then: :stop}, self(), asleep)
    assert waking.migrate_task.then == :stop
    :ok = await_migrate_task(waking)
    assert ClassRegistry.parked(inst) == nil, "the bind cleared the record before the start"

    # The un-park reports its binding: the stop follows it to that runner.
    test_pid = self()
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Runner.StreamPool, :put_route, fn _, _, _ -> :ok end)

    stub(Flezha.Runner.ControlUnit, :workflow_stop, fn runner, id ->
      send(test_pid, {:workflow_stop, runner, id})
      :ok
    end)

    carrying = %{
      asleep
      | migrate_task: %{ref: make_ref(), pid: self(), mode: :unpark, then: :stop}
    }

    recovered = %{carrying | runner_id: "ex_2", suspended: false}

    {:noreply, _awake} =
      Overwatch.handle_info({:migrate_task_result, self(), {:unparked, recovered}}, carrying)

    assert_received {:workflow_stop, "ex_2", ^inst}
  end

  test "a stop finds no runner to wake on, or the instance already awake" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> nil end)
    asleep = state(inst, suspended: true, runner_id: nil, channel: nil)

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:reply, {:error, :no_runner}, still} =
               Overwatch.handle_call({:wake, then: :stop}, self(), asleep)

      assert still.suspended
    end)

    assert {:reply, {:error, :awake}, _} =
             Overwatch.handle_call({:wake, then: :stop}, self(), state(inst))
  end

  test "a suspended instance is rediscovered from durable state after a redeploy" do
    inst = instance_id()
    builder = %Builder{id: inst, factory_id: "chat", kind: :workflow, opts: []}

    # Durable state of a suspended instance: iteration 0 consumed the log's first
    # input and committed, then parked.
    {:ok, 0} = InputLog.append(inst, "turn-0", %{})
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [])
    :ok = TaskJournal.append_iter_committed(inst, 0, 0, [])
    :ok = ClassRegistry.put_parked(inst, park(System.system_time(:millisecond) + 60_000))

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
    assert Timer.armed?(booted.timers, :wake), "the recorded wake deadline is re-armed"

    # The boot seeded the floor from the journal, so the consumed input is not
    # owed. Undelivered input outranks the park: an instance with work waiting
    # binds instead of coming back suspended.
    assert InputLog.tail(inst) == {:ok, 0, 0}
    {:ok, 1} = InputLog.append(inst, "arrived-while-down", %{})

    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> nil end)

    assert {:noreply, held} =
             Overwatch.handle_continue({:bind_runner, "stale-runner", builder}, fresh),
           "it takes the normal bind path (which finds no runner here)"

    assert {:runner_not_ready, _} = held.init_error
  end

  test "whether a suspended instance owes input is answered without reading its records" do
    inst = instance_id()
    {:ok, 0} = InputLog.append(inst, "turn-0", %{})
    :ok = TaskJournal.append_iter_committed(inst, 0, 0, [])
    :ok = ClassRegistry.put_parked(inst, park())
    :ok = TaskJournal.release_input(inst)
    {:ok, log} = Flezha.State.Supervisor.lookup({inst, :input_log})

    :erlang.trace(log, true, [:receive])
    assert Flezha.Operative.Migration.suspended_on_disk?(inst)
    {:ok, 1} = InputLog.append(inst, "turn-1", %{})
    refute Flezha.Operative.Migration.suspended_on_disk?(inst)
    :erlang.trace(log, false, [:receive])

    assert_received {:trace, ^log, :receive, {:"$gen_call", _, :last_offset}}
    refute_received {:trace, ^log, :receive, {:"$gen_call", _, {:read_range, _, _}}}
  end

  test "a workflow whose state key is not its instance id is refused before it binds" do
    inst = instance_id()

    builder = %Builder{
      id: inst,
      factory_id: "chat",
      kind: :workflow,
      opts: [operative_class: "elsewhere"]
    }

    fresh = %Overwatch{id: inst, namespace: "default", factory_id: "chat", builder: builder}

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:stop, {:shutdown, {:split_state_key, ^inst, "elsewhere"}}, _} =
                 Overwatch.handle_continue({:bind_runner, "stale-runner", builder}, fresh)
      end)

    assert log =~ "its state key is its instance id"
  end

  # ---------- workflow-loop-rewind ----------

  # The rewind rides the same evict → rehydrate path a park does, so the same
  # stubs stand in for the runner side.
  defp stub_rebind() do
    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> "ex_2" end)
  end

  defp commit_turns(inst, count, cells_ran) do
    for iter <- 0..(count - 1) do
      :ok = TaskJournal.append_task_committed(inst, 0, iter, cells_ran, [])
      :ok = TaskJournal.append_iter_committed(inst, iter, iter + 1, [])
    end
  end

  # A stage whose Cell wrote a record with an output on `replies`.
  defp emitting_outbox(),
    do: [%{cell_id: "seg", first_seq: 0, seq: 0, channels: ["replies"]}]

  test "rewind journals the marker, evicts, and rehydrates" do
    inst = instance_id()
    stub_rebind()
    for n <- 0..4, do: {:ok, ^n} = InputLog.append(inst, "turn-#{n}", %{})
    commit_turns(inst, 3, [])
    # What the iteration commits' acks reported: offset 3 consumed.
    :ok = TaskJournal.release_input(inst)

    {:reply, :ok, state} = Overwatch.handle_call({:rewind, 1}, self(), state(inst))

    # The marker is durable, the epoch bumped, and the loop folded back to
    # iteration 2 with iteration 2's task outstanding as undo.
    assert TaskJournal.current_epoch(inst) == 1
    assert TaskJournal.committed_iterations(inst) == [0, 1]
    assert [%{task_idx: 0, iter: 2}] = TaskJournal.restore_state(inst).rewind_undo

    # A rewind is a rebind, not a park: the instance comes straight back up and
    # no park record was written.
    assert state.migrate_task != nil
    assert ClassRegistry.parked(inst) == nil

    # Undo-and-continue: the rebind's seed leaves the floor where the commits put
    # it, so the unwound iterations' inputs are not owed again.
    :ok = TaskJournal.release_input(inst)
    assert InputLog.tail(inst) == {:ok, 3, 4}

    :ok = await_migrate_task(state)
  end

  test "rewind to an uncommitted iteration is refused, changing nothing" do
    inst = instance_id()
    commit_turns(inst, 3, [])

    assert {:reply, {:error, {:uncommitted_iteration, [0, 1, 2]}}, _} =
             Overwatch.handle_call({:rewind, 7}, self(), state(inst))

    assert TaskJournal.current_epoch(inst) == 0
    {:ok, entries} = TaskJournal.read(inst)
    refute Enum.any?(entries, &(&1.type == :rewound))
  end

  test "rewind is refused when an unwound tasklet emitted and cannot compensate" do
    inst = instance_id()
    commit_turns(inst, 3, emitting_outbox())

    # No `compensate` hook declared on task 0 — its output cannot be undone.
    :ok = ClassRegistry.put_compensable_ops(inst, [])

    assert {:reply, {:error, {:uncovered, ["replies"]}}, _} =
             Overwatch.handle_call({:rewind, 1}, self(), state(inst))

    assert TaskJournal.current_epoch(inst) == 0, "refused before anything durable changed"
  end

  test "an emitting tasklet that declares compensate rewinds" do
    inst = instance_id()
    stub_rebind()
    commit_turns(inst, 3, emitting_outbox())
    :ok = ClassRegistry.put_compensable_ops(inst, [0])

    assert {:reply, :ok, rewound} = Overwatch.handle_call({:rewind, 1}, self(), state(inst))
    assert TaskJournal.current_epoch(inst) == 1

    :ok = await_migrate_task(rewound)
  end

  test "iterations that emitted nothing need no coverage" do
    inst = instance_id()
    stub_rebind()
    commit_turns(inst, 3, [])
    :ok = ClassRegistry.put_compensable_ops(inst, [])

    assert {:reply, :ok, rewound} = Overwatch.handle_call({:rewind, 1}, self(), state(inst))
    :ok = await_migrate_task(rewound)
  end

  describe "migration_consent/1 (processhub-deferred-migration)" do
    test "a pending correction defers" do
      inst = instance_id()

      st =
        state(inst,
          durable_workflow: false,
          corrections: %{{inst, "task", "k"} => 100}
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

      stub(Flezha.State.WriterRegistry, :live_writers, fn _class -> [self()] end)

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

      stub(Flezha.State.WriterRegistry, :live_writers, fn _class -> [self()] end)
      stub(Flezha.State.WalWriter, :outbox_status, fn _pid, _timeout -> exit(:timeout) end)

      assert Overwatch.migration_consent(st) == :ready
    end

    test "an idle stream instance consents immediately" do
      inst = instance_id()
      assert Overwatch.migration_consent(state(inst, durable_workflow: false)) == :ready
    end

    test "a stream that sleeps consents awake and asleep" do
      inst = instance_id()
      st = state(inst, durable_workflow: false, sleeps: true)
      assert Overwatch.migration_consent(st) == :ready
      assert Overwatch.migration_consent(%{st | suspended: true}) == :ready
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

      {:noreply, suspended, :hibernate} =
        Overwatch.handle_cast({:suspend, 2, park()}, state(inst))

      assert_receive {:migration_ready, hub_id, ^inst}
      assert hub_id == Identificator.hub("default")
      assert suspended.suspended
    end

    test "the last correction draining signals migration_ready; a remaining one does not" do
      inst = instance_id()
      test_pid = self()

      stub(ProcessHub.Service.Migration, :migration_ready, fn _hub_id, child_id ->
        send(test_pid, {:migration_ready, child_id})
        {:error, :not_deferred}
      end)

      # A stale debounce timer for a cell that is not pending: the pop leaves
      # `corrections` empty, which is the completion condition.
      st = state(inst, durable_workflow: false, corrections: %{})
      {:noreply, _} = Overwatch.handle_info({:run_correction, {inst, "task", "k"}}, st)
      assert_receive {:migration_ready, ^inst}

      # Another cell still pending: not complete, no signal.
      busy =
        state(inst,
          durable_workflow: false,
          corrections: %{{inst, "task", "other"} => 5}
        )

      {:noreply, _} = Overwatch.handle_info({:run_correction, {inst, "task", "gone"}}, busy)
      refute_received {:migration_ready, ^inst}
    end
  end
end

defmodule FlezhaTest.Operative.OverwatchAsyncMigrateTest do
  @moduledoc """
  Phase-3 (convergence-self-healing §3.3-3.5) async
  `:runner_migrate` task contract. Exercises the
  `stop_operative-during-in-flight-migrate` invariant: the bounded retry
  loop now runs in a `Task.Supervisor.async_nolink/3` task, so the
  Overwatch's `:stop_operative` call returns within its normal handle_call
  window even mid-retry.
  """

  # async: false: writes Application env; the live controller and its fleet; the primary Logger.

  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.Operative.Builder
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Overwatch
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "default"
  @factory "store_input"

  setup do
    test_pid = self()
    ets = :ets.new(:overwatch_async_migrate_test, [:public, :set])

    on_exit(fn -> OperativeManager.stop_all_operatives(@namespace) end)

    {:ok, test_pid: test_pid, ets: ets}
  end

  # `await_active_runner` pings once to confirm the runner is reachable
  # before the one start attempt. To reach the PARKED state these tests
  # need, the readiness gate (first ping) must pass while the START pings
  # fail — otherwise the operative is held unbound instead of parked.
  defp park_after_gate(ets) do
    fn _runner_id ->
      case :ets.update_counter(ets, :calls, {2, 1}, {:calls, 0}) do
        1 -> {:ok, :pong}
        _ -> {:error, :timeout}
      end
    end
  end

  test "stop_operative returns :ok within 1s while a parked-unpark migrate task is in flight",
       %{test_pid: test_pid, ets: ets} do
    # The async migrate path lives on the parked-unpark branch — that's
    # where the whole bind runs. Boot a parked operative (start ping fails
    # after the readiness gate) and then trigger un-park with a ping that
    # blocks for several seconds.
    Application.put_env(:flezha, :overwatch_ping_fn, park_after_gate(ets))

    operative = %Builder{id: "stop_during_migrate", factory_id: @factory, opts: []}

    {:ok, overwatch_pid} =
      Overwatch.start_link({@namespace, "stale_ce", operative})

    on_exit(fn ->
      if Process.alive?(overwatch_pid) do
        try do
          GenServer.stop(overwatch_pid, :normal, 1_000)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    # Wait for the operative to park.
    assert NodeTestHelper.wait_until(
             fn ->
               GenServer.call(overwatch_pid, :get_overwatch_state).init_error != nil
             end,
             2_000,
             25
           )

    # Replace ping with one that blocks until released, so the un-park retry
    # task is in flight for as long as the test needs.
    Application.put_env(:flezha, :overwatch_ping_fn, blocking_ping(test_pid))

    # Fire the un-park migrate. The Overwatch must return `:ok`
    # immediately (the retry task is spawned out-of-band).
    :ok = GenServer.call(overwatch_pid, {:runner_migrate, "fake_new_runner"}, 1_000)

    assert_receive {:ping_started, _}, 500

    # Now tear the operative down the way the hub stop path does (a
    # supervisor :shutdown exit). terminate/2 MUST return within 1 s even
    # though the migrate task is blocked inside the ping — it kills the
    # task instead of waiting on it. Trap: the Overwatch is linked to this
    # test, and a :shutdown stop propagates over the link.
    Process.flag(:trap_exit, true)
    started = System.monotonic_time(:millisecond)
    assert :ok = GenServer.stop(overwatch_pid, :shutdown, 1_500)
    elapsed = System.monotonic_time(:millisecond) - started

    assert elapsed < 1_000,
           "teardown returned in #{elapsed} ms; required < 1 s while migrate task is in flight"
  end

  test "starting a second parked-unpark migrate while one is in flight returns :busy",
       %{test_pid: test_pid, ets: ets} do
    Application.put_env(:flezha, :overwatch_ping_fn, park_after_gate(ets))

    operative = %Builder{id: "busy_migrate_test", factory_id: @factory, opts: []}

    {:ok, overwatch_pid} =
      Overwatch.start_link({@namespace, "stale_ce", operative})

    on_exit(fn ->
      if Process.alive?(overwatch_pid) do
        try do
          GenServer.stop(overwatch_pid, :normal, 1_000)
        catch
          :exit, _ -> :ok
        end
      end
    end)

    assert NodeTestHelper.wait_until(
             fn ->
               GenServer.call(overwatch_pid, :get_overwatch_state).init_error != nil
             end,
             2_000,
             25
           )

    # First un-park migrate: a ping that blocks until released.
    Application.put_env(:flezha, :overwatch_ping_fn, blocking_ping(test_pid))

    :ok = GenServer.call(overwatch_pid, {:runner_migrate, "first"}, 1_000)
    assert_receive {:ping_started, pinger}, 500

    # Second migrate must be rejected as :busy.
    assert {:error, :busy} =
             GenServer.call(overwatch_pid, {:runner_migrate, "second"}, 1_000)

    send(pinger, :unpark)
  end

  # fails when: the running-migrate branch commits the binding before the runner
  # answers. A cross-node heal target is unreachable by construction (the runner
  # registry is node-local), so the failed attempt would leave the operative
  # naming a LIVE runner it cannot reach — `Convergence.Diff` then reads it as
  # bound-and-healthy, drops it from `misbound_operatives`, and the wedge becomes
  # permanently invisible.
  test "a failed running-migrate leaves the binding untouched" do
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:error, :no_control_unit} end)

    state = running_state()
    :ok = ClassRegistry.put_overwatch(state.id, self(), "runner_a")
    on_exit(fn -> ClassRegistry.delete(state.id) end)

    assert {:reply, {:error, {:ping_failed, :no_control_unit}}, unchanged} =
             Overwatch.handle_call({:runner_migrate, "runner_b"}, {self(), make_ref()}, state)

    assert unchanged.runner_id == "runner_a"
    assert unchanged.channel == :chan_a

    assert ClassRegistry.owner(state.id) == {:ok, self(), "runner_a", nil},
           "a failed migrate must keep the owner and forget the identity it tried"
  end

  # fails when: a committed running-migrate keeps the snapshot it seeded the new
  # runner from, so a handover after that runner dies hard ships the older state
  # and skips the pull of every change the runner made.
  test "a committed running-migrate seeds the runner from the stored snapshot and drops it" do
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:ok, :pong} end)
    runner_id = "runner_b_#{System.unique_integer([:positive])}"
    test_pid = self()

    answer_control_unit_once(runner_id, fn {:start_operative, operative} ->
      send(test_pid, {:started_with, operative.init_values, ClassRegistry.owner(operative.id)})
      {:ok, %{}}
    end)

    state = %{running_state() | snapshot: ~s({"store_input_tasklet":{"text_inputs":["s"]}})}
    :ok = ClassRegistry.put_overwatch(state.id, self(), "runner_a")
    on_exit(fn -> ClassRegistry.delete(state.id) end)

    assert {:reply, :ok, bound} =
             Overwatch.handle_call({:runner_migrate, runner_id}, {self(), make_ref()}, state)

    assert_received {:started_with, %{"store_input_tasklet" => %{"text_inputs" => ["s"]}},
                     owner_at_start}

    assert bound.runner_id == runner_id
    assert bound.snapshot == nil

    # Both runners are accepted while the start is in flight, and only the one
    # it ended on afterwards.
    assert owner_at_start == {:ok, self(), "runner_a", runner_id}
    assert ClassRegistry.owner(state.id) == {:ok, self(), runner_id, nil}
  end

  # fails when: a handover away from a runner that is gone stops shipping the
  # final state that runner handed over, before any other runner took over.
  test "a handover away from a gone runner ships the snapshot no runner has taken over" do
    state = %{running_state() | snapshot: "final_state_of_runner_a"}

    assert {%{source_node: _}, "final_state_of_runner_a"} =
             Overwatch.prepare_handover_state(state)
  end

  # fails when: a snapshot read is kept on the Overwatch, so a handover after
  # the runner dies hard ships that read and skips the pull of every change the
  # runner made after it.
  test "a snapshot read is not kept for a later handover" do
    runner_id = "runner_read_#{System.unique_integer([:positive])}"
    state = %{running_state() | runner_id: runner_id}

    answer_control_unit_once(runner_id, fn {:send_control, {:cell_state, _}, _metadata} ->
      {:ok, "read_earlier"}
    end)

    assert {:reply, {:ok, "read_earlier"}, read} =
             Overwatch.handle_call(:create_snapshot, {self(), make_ref()}, state)

    # The runner is gone now, so the handover has nothing of its own to ship.
    assert {%{source_node: _}, nil} = Overwatch.prepare_handover_state(read)
  end

  # fails when: `alter_handover_state` marks `arrived_via_hotswap?` even for a
  # handover that carried nothing. The flag suppresses the per-Cell WAL
  # restore at bind time AND the explicit `kick_restore` repair, so a migration
  # away from a DEAD runner — where the state source is gone and the handover is
  # empty by construction — starts fresh and can never be replayed back.
  test "an empty handover does not suppress the WAL restore" do
    handover = %{
      id: "handover_guard",
      namespace: @namespace,
      factory_id: @factory,
      init_values: %{},
      plugins: []
    }

    state = running_state()

    # The struct has no reachable control unit, so the deserialization dispatch
    # logs an error it is designed to tolerate; the flag is the assertion.
    ExUnit.CaptureLog.capture_log(fn ->
      refute Overwatch.alter_handover_state(state, {handover, nil}).arrived_via_hotswap?
      refute Overwatch.alter_handover_state(state, {handover, <<>>}).arrived_via_hotswap?
      assert Overwatch.alter_handover_state(state, {handover, "sdk-state"}).arrived_via_hotswap?
    end)
  end

  defp running_state() do
    id = "migrate_bind_guard"

    %Overwatch{
      id: id,
      namespace: @namespace,
      runner_id: "runner_a",
      factory_id: @factory,
      channel: :chan_a,
      builder: %Builder{id: id, factory_id: @factory, opts: []},
      extensions: Table.new(Overwatch.__extensions_catalog__())
    }
  end

  # Stands in for `runner_id`'s control unit for one call and is then gone, as
  # a runner is once it dies. `answer` runs inside the stand-in and turns the
  # request into the reply.
  defp answer_control_unit_once(runner_id, answer) do
    test_pid = self()
    {:via, Registry, {registry, key}} = Flezha.Identificator.control_unit(runner_id)

    spawn_link(fn ->
      {:ok, _} = Registry.register(registry, key, nil)
      send(test_pid, :control_unit_registered)

      receive do
        {:"$gen_call", from, request} -> GenServer.reply(from, answer.(request))
      end
    end)

    assert_receive :control_unit_registered, 5_000
  end

  # A ping that blocks inside the migrate task until the test releases it (or
  # the task is killed) — the "several seconds into the backoff schedule"
  # scenario, without the seconds.
  defp blocking_ping(test_pid) do
    fn _runner_id ->
      send(test_pid, {:ping_started, self()})
      NodeTestHelper.park()
      {:ok, :pong}
    end
  end
end

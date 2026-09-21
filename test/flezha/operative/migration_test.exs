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
  alias Flezha.State.{TaskInbox, TaskJournal, TestHelpers}

  setup :set_mimic_global_until_exit

  setup do
    # The rehydrate drives the real (un-park) bind task, which has no runner to
    # reach in a unit test: bound its wait so the assertions about the state
    # machine do not pay the production window.
    on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 50))

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

    {:noreply, suspended, :hibernate} = Overwatch.handle_cast({:suspend, 2, nil}, state(inst))

    # Zero live runner resources; the registry entry (and this process) stay.
    assert_receive :evicted
    assert_receive {:registry_runner, nil}

    assert suspended.suspended
    assert suspended.runner_id == nil
    assert suspended.channel == nil
    refute Timer.armed?(suspended.timers, :wake), "no wake_after declared ⇒ wait for input alone"
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

    {:noreply, suspended, :hibernate} = Overwatch.handle_cast({:suspend, 2, nil}, state(inst))

    assert suspended.runner_id == nil
    assert ClassRegistry.owner(inst) == {:ok, self(), "ex_1", nil}
  end

  test "a turn that landed while the loop was parking is never stranded" do
    inst = instance_id()
    stub(Flezha.Runner.ControlUnit, :stop_operative_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :operative_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> "ex_2" end)

    # Iteration 0 consumed offset 1; offset 2 arrived while the driver was on its
    # way out and was never drained — durable, but undelivered.
    {:ok, 1} = TaskInbox.append(inst, "consumed", %{})
    {:ok, 2} = TaskInbox.append(inst, "in-flight", %{})
    :ok = TaskJournal.append_iter_committed(inst, 0, 1, [])

    {:noreply, after_park, :hibernate} = Overwatch.handle_cast({:suspend, 0, nil}, state(inst))

    assert after_park.migrate_task != nil,
           "an instance with undelivered input must rehydrate, not sit suspended " <>
             "waiting for a message that already arrived"

    :ok = await_migrate_task(after_park)

    # With nothing pending, the same park suspends and stays there.
    :ok = TaskJournal.append_iter_committed(inst, 1, 2, [])
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

    # Durable first: the input is in the inbox before anything else happens, so a
    # crash mid-rehydrate cannot lose it.
    assert [%{offset: 1, message: "hello"}] = TaskInbox.undelivered(inst)

    assert rehydrating.migrate_task != nil,
           "the rehydrate runs through the normal (un-park) bind task"

    # Racing wake sources resolve to exactly one rehydration: a second input
    # while the first is still in flight journals, and reuses the same bind.
    {:noreply, journaling} = Overwatch.handle_cast({:io_in, "second", %{}}, rehydrating)
    still_one = settle_journal(journaling)

    assert still_one.migrate_task == rehydrating.migrate_task,
           "a second wake source does not start a second rehydration"

    assert Enum.map(TaskInbox.undelivered(inst), & &1.message) == ["hello", "second"]

    :ok = await_migrate_task(rehydrating)
  end

  test "the wake deadline rehydrates with a timer tick and no input" do
    inst = instance_id()
    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> "ex_2" end)

    suspended = state(inst, suspended: true, runner_id: nil, channel: nil)
    {:noreply, rehydrating} = Overwatch.handle_info(:wake_deadline, suspended)

    assert rehydrating.migrate_task != nil
    refute Timer.armed?(rehydrating.timers, :wake)

    assert [%{offset: 1, message: "", metadata: %{"flz.wake" => "timer"}}] =
             TaskInbox.undelivered(inst),
           "the timer wake rides the inbox — one wake path, so racing sources collapse"

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
    assert [%{message: "hi"}] = TaskInbox.undelivered(inst), "the input is not lost"
  end

  # The inbox writer's answer to the append a cast issued — the host acts on it
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

  test "a suspended instance is rediscovered from durable state after a redeploy" do
    inst = instance_id()
    builder = %Builder{id: inst, factory_id: "chat", kind: :workflow, opts: []}

    # Durable state of a suspended instance: iteration 0 committed, then parked.
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [])
    :ok = TaskJournal.append_iter_committed(inst, 0, 0, [])
    :ok = TaskJournal.append_parked("default", inst, 0, System.system_time(:millisecond) + 60_000)

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
    assert Timer.armed?(booted.timers, :wake), "the journaled wake deadline is re-armed"

    # Undelivered input outranks the park: an instance with work waiting binds
    # instead of coming back suspended.
    {:ok, 1} = TaskInbox.append(inst, "arrived-while-down", %{})

    stub(Flezha.Service.OperativeManager, :assign_runner, fn _, _, _ -> nil end)

    assert {:noreply, held} =
             Overwatch.handle_continue({:bind_runner, "stale-runner", builder}, fresh),
           "it takes the normal bind path (which finds no runner here)"

    assert {:runner_not_ready, _} = held.init_error
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
      :ok = TaskJournal.append_task_committed(inst, 0, iter, [], [], outbox)
      :ok = TaskJournal.append_iter_committed(inst, iter, iter + 1, [])
    end
  end

  defp emitting_outbox(),
    do: [%{cell_id: "seg", seq_in_task: 0, channel: "replies", payload: "hi"}]

  test "rewind journals the marker, evicts, and rehydrates" do
    inst = instance_id()
    stub_rebind()
    commit_turns(inst, 3, [])

    {:reply, :ok, state} = Overwatch.handle_call({:rewind, 1}, self(), state(inst))

    # The marker is durable, the epoch bumped, and the loop folded back to
    # iteration 2 with iteration 2's task outstanding as undo.
    assert TaskJournal.current_epoch(inst) == 1
    assert TaskJournal.committed_iterations(inst) == [0, 1]
    assert [%{task_idx: 0, iter: 2}] = TaskJournal.restore_state(inst).rewind_undo

    # A rewind is a rebind, not a park: the instance comes straight back up and
    # nothing was journaled as `parked`.
    assert state.migrate_task != nil
    {:ok, entries} = TaskJournal.read(inst)
    refute Enum.any?(entries, &(&1.type == :parked))

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
    start_answering_control_unit(runner_id)
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

  # Stands in for `runner_id`'s control unit for one `:start_operative` call,
  # reporting the init values the operative was started with.
  defp start_answering_control_unit(runner_id) do
    test_pid = self()
    {:via, Registry, {registry, key}} = Flezha.Identificator.control_unit(runner_id)

    spawn_link(fn ->
      {:ok, _} = Registry.register(registry, key, nil)
      send(test_pid, :control_unit_registered)

      receive do
        {:"$gen_call", from, {:start_operative, operative}} ->
          owner = ClassRegistry.owner(operative.id)
          send(test_pid, {:started_with, operative.init_values, owner})
          GenServer.reply(from, {:ok, %{}})
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

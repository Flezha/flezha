defmodule FlezhaTest.Relay.SuspendTest do
  @moduledoc """
  `durable-suspension` lifecycle on the Overwatch: park (evict from the
  runner, keep the registry entry), rehydrate on a new input or the durable
  timer, exactly one rehydration when both wake sources race, and rediscovery of
  a suspended instance from durable state alone after a controller redeploy.

  Plus `steps-loop-rewind`, which rides the same evict → rehydrate path: the
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
  alias Flezha.Relay.{Builder, ClassRegistry, Overwatch, Suspend}
  alias Flezha.Plugin.Extension.Table
  alias Flezha.State.{InputLog, TaskJournal, TestHelpers, WalWriter}
  alias FlezhaTest.OutboxCase
  alias FlezhaTest.Helper.WireEncoders

  setup :set_mimic_global_until_exit

  setup do
    # The rehydrate drives the real (rebind) bind task, which has no runner to
    # reach in a unit test: bound its wait so the assertions about the state
    # machine do not pay the production window.
    on_exit(TestHelpers.override_env(:overwatch_runner_wait_ms, 50))

    :ok
  end

  defp instance_id(), do: "susp_#{System.unique_integer([:positive])}"

  defp park(deadline \\ nil), do: %{wake_deadline_ms: deadline}

  defp state(inst, overrides \\ []) do
    base = %Overwatch{
      id: inst,
      namespace: "default",
      runner_id: "ex_1",
      factory_id: "chat",
      # The test process stands in for the relay's StreamChannel: a
      # forwarded input arrives as an `{:io_in, ...}` cast.
      channel: self(),
      builder: %Builder{id: inst, factory_id: "chat", run: :steps, opts: []},
      extensions: Table.new(Overwatch.__extensions_catalog__()),
      durable_steps: true,
      phase: :running
    }

    struct!(base, overrides)
  end

  # fails when: a rebound instance binds onto a runner but publishes neither
  # the registry tag nor the runner's route to its pid, so every frame it produces
  # hits `stream_channel.ex`'s `[] -> :ok` drop while it still reads as healthy.
  test "un-parking publishes the registry tag and the runner route" do
    inst = instance_id()
    test_pid = self()

    stub(Flezha.Service.HubManager, :relay_set_runner, fn ns, ^inst, runner ->
      send(test_pid, {:tag_published, ns, runner})
      :ok
    end)

    stub(Flezha.Runner.StreamPool, :put_route, fn runner, ^inst, pid ->
      send(test_pid, {:route_published, runner, pid})
      :ok
    end)

    parked = state(inst)
    parked = %{parked | bind_task: %{ref: make_ref(), pid: self(), extensions: parked.extensions}}
    recovered = %{parked | runner_id: "ex_woken", phase: :running}

    {:noreply, unparked} =
      Overwatch.handle_info({:bind_task_result, self(), {:bound, recovered}}, parked)

    assert unparked.bind_task == nil
    assert_receive {:tag_published, "default", "ex_woken"}
    assert_receive {:route_published, "ex_woken", ^test_pid}
  end

  # fails when: a rebind that binds a runner keeps a snapshot stored before it,
  # so a handover after that runner dies hard ships the older state and skips
  # the pull of every change the runner made.
  test "un-parking onto a runner drops the snapshot stored before it" do
    inst = instance_id()
    stub(Flezha.Service.HubManager, :relay_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Runner.StreamPool, :put_route, fn _, _, _ -> :ok end)

    parked = state(inst, snapshot: "stored_before_the_park")
    parked = %{parked | bind_task: %{ref: make_ref(), pid: self(), extensions: parked.extensions}}

    recovered = %{parked | runner_id: "ex_woken", phase: :running}

    {:noreply, unparked} =
      Overwatch.handle_info({:bind_task_result, self(), {:bound, recovered}}, parked)

    assert unparked.runner_id == "ex_woken"
    assert unparked.snapshot == nil
  end

  test "park evicts the instance from its runner and keeps it registered" do
    inst = instance_id()
    test_pid = self()

    stub(Flezha.Runner.ControlUnit, :stop_relay_async, fn "ex_1", ^inst ->
      send(test_pid, :evicted)
      :ok
    end)

    stub(Flezha.Service.HubManager, :relay_set_runner, fn _ns, ^inst, runner ->
      send(test_pid, {:registry_runner, runner})
      :ok
    end)

    {:noreply, suspended, :hibernate} = Overwatch.handle_cast({:suspend, 2, park()}, state(inst))

    # Zero live runner resources; the registry entry (and this process) stay.
    assert_receive :evicted
    assert_receive {:registry_runner, nil}

    assert Overwatch.phase(suspended) == :suspended
    assert suspended.runner_id == nil
    assert suspended.channel == nil
    refute Timer.armed?(suspended.timers, :wake), "no pending timer ⇒ wait for input alone"
  end

  # fails when: a park clears the ownership record, so a cell frame the
  # parking runner sent before it parked, still in flight on the data stream, is
  # refused and never reaches the WAL a rehydrate restores from.
  test "a park keeps the ownership record naming the runner that parked" do
    inst = instance_id()
    stub(Flezha.Runner.ControlUnit, :stop_relay_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :relay_set_runner, fn _, _, _ -> :ok end)
    :ok = ClassRegistry.put_overwatch(inst, self(), "ex_1")
    on_exit(fn -> ClassRegistry.delete(inst) end)

    {:noreply, suspended, :hibernate} = Overwatch.handle_cast({:suspend, 2, park()}, state(inst))

    assert suspended.runner_id == nil
    assert ClassRegistry.owner(inst) == {:ok, self(), "ex_1"}
  end

  test "a turn that landed while the loop was parking is never stranded" do
    inst = instance_id()
    stub(Flezha.Runner.ControlUnit, :stop_relay_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :relay_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, _ -> "ex_2" end)

    # Iteration 0 consumed offset 0; offset 1 arrived while the driver was on its
    # way out and was never drained — durable, but undelivered. The commit's ack
    # reported offset 0 into the log's watermark.
    {:ok, 0} = InputLog.append(inst, "consumed", %{})
    {:ok, 1} = InputLog.append(inst, "in-flight", %{})
    :ok = TaskJournal.append_iter_committed(inst, 0, 0, [])
    :ok = TaskJournal.release_input(inst, 0)

    {:noreply, after_park, :hibernate} = Overwatch.handle_cast({:suspend, 0, park()}, state(inst))

    assert after_park.bind_task != nil,
           "an instance with undelivered input must rehydrate, not sit suspended " <>
             "waiting for a message that already arrived"

    :ok = await_bind_task(after_park)

    # With nothing pending, the same park suspends and stays there.
    :ok = TaskJournal.append_iter_committed(inst, 1, 1, [])
    :ok = TaskJournal.release_input(inst, 1)
    {:noreply, idle, :hibernate} = Overwatch.handle_cast({:suspend, 1, park()}, state(inst))
    assert Overwatch.phase(idle) == :suspended
    assert idle.bind_task == nil
  end

  test "a park with a wake deadline arms the durable timer" do
    inst = instance_id()
    stub(Flezha.Runner.ControlUnit, :stop_relay_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :relay_set_runner, fn _, _, _ -> :ok end)

    deadline = System.system_time(:millisecond) + 50_000

    {:noreply, suspended, :hibernate} =
      Overwatch.handle_cast({:suspend, 1, park(deadline)}, state(inst))

    assert Timer.armed?(suspended.timers, :wake)
    assert Process.read_timer(elem(Map.fetch!(suspended.timers, :wake), 0)) > 40_000
  end

  test "input for a suspended instance is journaled and rehydrates it" do
    inst = instance_id()
    stub(Flezha.Service.RelayManager, :assign_runner, fn _ns, "chat", ^inst -> "ex_2" end)

    suspended = state(inst, phase: :suspended, runner_id: nil, channel: nil)

    {:noreply, journaling} =
      Overwatch.handle_cast({:io_in, "hello", %{"channel" => "chat"}}, suspended)

    assert journaling.bind_task == nil, "nothing wakes before the input is durable"
    rehydrating = settle_journal(journaling)

    # Durable first: the input is in the log before anything else happens, so a
    # crash mid-rehydrate cannot lose it.
    assert [{0, "hello", _metadata}] = InputLog.undelivered(inst)

    assert rehydrating.bind_task != nil,
           "the rehydrate runs through the normal (un-park) bind task"

    # Racing wake sources resolve to exactly one rehydration: a second input
    # while the first is still in flight journals, and reuses the same bind.
    {:noreply, journaling} = Overwatch.handle_cast({:io_in, "second", %{}}, rehydrating)
    still_one = settle_journal(journaling)

    assert still_one.bind_task == rehydrating.bind_task,
           "a second wake source does not start a second rehydration"

    assert Enum.map(InputLog.undelivered(inst), &elem(&1, 1)) == ["hello", "second"]

    :ok = await_bind_task(rehydrating)
  end

  test "the wake deadline rehydrates and appends nothing" do
    inst = instance_id()
    stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, _ -> "ex_2" end)

    suspended = state(inst, phase: :suspended, runner_id: nil, channel: nil)
    {:noreply, rehydrating} = Overwatch.handle_info(:wake_deadline, suspended)

    assert rehydrating.bind_task != nil
    refute Timer.armed?(rehydrating.timers, :wake)

    assert InputLog.undelivered(inst) == [],
           "the restored Cells' own timer fires; nothing rides the input log"

    :ok = await_bind_task(rehydrating)
  end

  test "a runner connecting wakes a sleeper that owes input and leaves one that owes nothing" do
    inst = instance_id()
    stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, _ -> "ex_2" end)
    suspended = state(inst, phase: :suspended, runner_id: nil, channel: nil)

    {:noreply, idle, :hibernate} = Overwatch.handle_cast(:rehydrate_if_pending, suspended)
    assert idle.bind_task == nil, "a sleeper owing nothing was woken"

    {:ok, 0} = InputLog.append(inst, "kept while no runner served it", %{})
    {:noreply, waking} = Overwatch.handle_cast(:rehydrate_if_pending, suspended)
    assert waking.bind_task != nil, "a sleeper owing input stayed asleep"

    :ok = await_bind_task(waking)
  end

  test "with no runner to rehydrate onto, the instance stays suspended" do
    inst = instance_id()
    stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, _ -> nil end)

    suspended = state(inst, phase: :suspended, runner_id: nil, channel: nil)
    {:noreply, journaling} = Overwatch.handle_cast({:io_in, "hi", %{}}, suspended)
    still = settle_journal(journaling)

    assert Overwatch.phase(still) == :suspended
    assert still.bind_task == nil
    assert [{0, "hi", _metadata}] = InputLog.undelivered(inst), "the input is not lost"
  end

  test "a wake deadline with no runner to rehydrate onto is retried a convergence tick later" do
    inst = instance_id()
    stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, _ -> nil end)

    suspended = state(inst, phase: :suspended, runner_id: nil, channel: nil)
    {:noreply, still} = Overwatch.handle_info(:wake_deadline, suspended)

    assert Overwatch.phase(still) == :suspended
    assert still.bind_task == nil
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
  defp await_bind_task(%Overwatch{bind_task: %{pid: pid}}) do
    assert_receive {:bind_task_result, ^pid, {:halt, _parked, {:ping_failed, _}}}, 2_000
    :ok
  end

  test "a park writes the record before it evicts, and a record that fails leaves it bound" do
    inst = instance_id()
    test_pid = self()

    stub(Flezha.Runner.ControlUnit, :stop_relay_async, fn _, _ -> send(test_pid, :evicted) end)

    stub(Flezha.Service.HubManager, :relay_set_runner, fn _, _, _ -> :ok end)

    {:noreply, asleep, :hibernate} = Overwatch.handle_cast({:suspend, 3, park(nil)}, state(inst))
    assert Overwatch.phase(asleep) == :suspended
    assert Suspend.sleep_record(inst) == park(nil)
    assert_received :evicted

    stub(ClassRegistry, :put_record, fn _, _, _, _ -> {:error, :eio} end)
    other = instance_id()

    ExUnit.CaptureLog.capture_log(fn ->
      {:noreply, bound, :hibernate} = Overwatch.handle_cast({:suspend, 3, park()}, state(other))
      refute Overwatch.phase(bound) == :suspended
      assert bound.runner_id == "ex_1"
    end)

    refute_received :evicted
  end

  # fails when: a bind task's bound state is adopted after a park landed while
  # it was binding, a rebind's (the phase moved) or a wake's (it did not).
  test "a park landing while a bind binds keeps its record and the suspension" do
    stub(Flezha.Runner.ControlUnit, :stop_relay_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :relay_set_runner, fn _, _, _ -> :ok end)

    for from <- [:running, :suspended] do
      inst = instance_id()
      task = %{pid: self(), then: nil, from: from}
      binding = state(inst, phase: from, bind_task: task)

      {:noreply, parked, :hibernate} = Overwatch.handle_cast({:suspend, 4, park()}, binding)
      recovered = %{parked | runner_id: "ex_stale", phase: :running}

      {:noreply, after_bind} =
        Overwatch.handle_info({:bind_task_result, self(), {:bound, recovered}}, parked)

      assert Overwatch.phase(after_bind) == :suspended,
             "a bind from #{from} adopted its stale state"

      assert after_bind.bind_task == nil
      assert Suspend.sleep_record(inst) == park()
    end
  end

  # fails when: a halt that landed while a bind was binding is overwritten by
  # the bind's bound state: a start's or a rebind's (the phase moved), a wake's
  # or a halted relay's rebind (it did not).
  test "a halt landing while a bind binds stands" do
    for {from, kept} <- [running: :halted, suspended: :suspended, halted: :halted] do
      task = %{pid: self(), then: nil, from: from}
      binding = state(instance_id(), phase: from, bind_task: task)

      {:noreply, halted} = Overwatch.handle_info({:restore_refused, :boom}, binding)
      recovered = %{binding | runner_id: "ex_stale", phase: :running, bind_task: nil}

      {:noreply, after_bind} =
        Overwatch.handle_info({:bind_task_result, self(), {:bound, recovered}}, halted)

      assert Overwatch.phase(after_bind) == kept, "a bind from #{from} adopted its stale state"
      assert after_bind.halt_reason == {:restore_refused, :boom}
      assert after_bind.runner_id == "ex_1"
    end
  end

  test "whether an instance sleeps is read from its park record and its log, for either kind" do
    for kind <- [:flow, :steps] do
      inst = instance_id()
      refute Suspend.suspended_on_disk?(inst)

      :ok = Suspend.put_sleep(inst, park())
      assert Suspend.suspended_on_disk?(inst), "#{kind} asleep"

      # Input that arrived while it slept outranks the record: it binds.
      {:ok, 0} = InputLog.append(inst, "arrived", %{})
      refute Suspend.suspended_on_disk?(inst), "#{kind} owes input"
    end
  end

  test "an elapsed wake deadline is pending work, as undelivered input is" do
    inst = instance_id()
    now = System.system_time(:millisecond)

    :ok = Suspend.put_sleep(inst, park(now + 60_000))
    assert Suspend.suspended_on_disk?(inst), "its timer is not due yet"

    :ok = Suspend.put_sleep(inst, park(now - 1))
    refute Suspend.suspended_on_disk?(inst), "its timer is due: it binds"
  end

  # A restart lost the Cell rows; a sleeper boots no Cell writer, so its
  # floor is read from its Cells' own records.
  test "a sleeping stream is rediscovered asleep at boot, its floor read from its Cells" do
    inst = instance_id()
    builder = %Builder{id: inst, factory_id: "counter", run: :flow, opts: []}
    for n <- 0..1, do: {:ok, ^n} = InputLog.append(inst, "in-#{n}", %{})
    :ok = ClassRegistry.put_sleep_after(inst, 2_000)
    :ok = Suspend.put_sleep(inst, park())

    cell_id = {inst, "counter_tasklet", "a"}
    {:ok, writer} = Flezha.State.Supervisor.find_or_start(cell_id)
    :ok = WalWriter.append(writer, 0, OutboxCase.effect_header(), OutboxCase.effect_body([], 1))
    WalWriter.close(writer)
    :ets.match_delete(InputLog.watermark_table(), {{inst, :_}, :_})

    fresh = %Overwatch{id: inst, namespace: "default", factory_id: "counter", builder: builder}

    {:noreply, booted, :hibernate} =
      Overwatch.handle_continue({:bind_runner, "stale-runner", builder}, fresh)

    assert Overwatch.phase(booted) == :suspended
    assert booted.sleeps, "a message that finds it asleep is journaled"
    assert InputLog.tail(inst) == {:ok, 1, 1}, "what its Cell consumed is not owed"
  end

  # Off the tier a producer's record carrying its offset keeps its edge values:
  # the bind delivers them ahead of the tail, since the producer skips the input.
  test "a plain stream's bind delivers the edge values its Cells kept, then the tail" do
    inst = instance_id()
    for n <- 0..1, do: {:ok, ^n} = InputLog.append(inst, "in-#{n}", %{})
    keep_edges(inst, 1, [%{channel: 0, consumer: 1, origin: 1, payload: "kept"}])

    reforward_restored(inst, 1)

    assert_received {:delivered, {"kept", %{"flz.edge" => "0:1", "flz.offset" => "1"}}}
    assert_received {:delivered, {"in-0", %{"flz.offset" => "0"}}}
    assert_received {:delivered, {"in-1", %{"flz.offset" => "1"}}}
  end

  # fails when: a re-forward with no restore since, a live session's lost
  # stream, delivers again an edge value its consumer took inside the runner,
  # which counts it twice since edges move no Cell's floor.
  test "only a restore's re-forward delivers edge values, up to its last offset" do
    inst = instance_id()
    for n <- 0..2, do: {:ok, ^n} = InputLog.append(inst, "in-#{n}", %{})

    keep_edges(inst, 2, [
      %{channel: 0, consumer: 1, origin: 1, payload: "restored"},
      %{channel: 0, consumer: 1, origin: 2, payload: "live"}
    ])

    reforward_restored(inst, nil)

    assert_received {:delivered, {"in-2", %{"flz.offset" => "2"}}}
    refute_received {:delivered, {"restored", _}}

    reforward_restored(inst, 1)

    assert_received {:delivered, {"restored", %{"flz.edge" => "0:1", "flz.offset" => "1"}}}
    refute_received {:delivered, {"live", _}}
  end

  # fails when: a restore's edge values stay owed once its re-forward ran, and a
  # later one, a lost stream's, delivers them again to consumers that took them.
  test "a restore's edge values are owed to its own re-forward only" do
    inst = instance_id()
    for n <- 0..1, do: {:ok, ^n} = InputLog.append(inst, "in-#{n}", %{})
    keep_edges(inst, 1, [%{channel: 0, consumer: 1, origin: 1, payload: "kept"}])

    state = reforward_restored(inst, 1)
    assert_received {:delivered, {"kept", _}}
    assert_received {:delivered, {"in-0", _}}
    assert_received {:delivered, {"in-1", _}}
    assert state.edges_owed_through == nil

    Flezha.Relay.Replay.reforward_undelivered_tail(state)

    assert_received {:delivered, {"in-1", %{"flz.offset" => "1"}}}
    refute_received {:delivered, {"kept", _}}
  end

  # fails when: an edge value a restore delivers again loses the call its origin
  # input carried, so the downstream answer never reaches the waiting caller.
  test "a re-delivered edge value carries the call its origin input carried" do
    inst = instance_id()
    {:ok, 0} = InputLog.append(inst, "in-0", %{})
    {:ok, 1} = InputLog.append(inst, "in-1", %{"flz.reply_to" => "w:caller"})

    keep_edges(inst, 1, [
      %{channel: 0, consumer: 1, origin: 0, payload: "plain"},
      %{channel: 0, consumer: 1, origin: 1, payload: "called"}
    ])

    reforward_restored(inst, 1)

    assert_received {:delivered, {"plain", plain}}
    refute Map.has_key?(plain, "flz.reply_to")
    assert_received {:delivered, {"called", %{"flz.edge" => "0:1", "flz.reply_to" => "w:caller"}}}
  end

  # A producer Cell's one record, consuming `offset` and keeping `edges`.
  defp keep_edges(inst, offset, edges) do
    body =
      WireEncoders.encode_effect_body(%{
        changes: [],
        outputs: [],
        consumed_offset: offset,
        edges: edges
      })

    {:ok, writer} = Flezha.State.Supervisor.find_or_start({inst, "forward", ""})
    :ok = WalWriter.append(writer, 0, OutboxCase.effect_header(), body)
    WalWriter.close(writer)
  end

  # The flow relay's re-forward, every frame it sends reported to the test;
  # `through` is the last offset its last restore owed edge values up to.
  defp reforward_restored(inst, through) do
    test_pid = self()

    stub(Flezha.Runner.StreamChannel, :io_in, fn _channel, _id, payload ->
      send(test_pid, {:delivered, payload})
      :ok
    end)

    Flezha.Relay.Replay.reforward_undelivered_tail(
      state(inst, durable_steps: false, edges_owed_through: through)
    )
  end

  test "a stop of a sleeping steps relay wakes it with the stop, and the bind sends it" do
    inst = instance_id()
    stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, _ -> "ex_2" end)
    :ok = Suspend.put_sleep(inst, park())
    asleep = state(inst, phase: :suspended, runner_id: nil, channel: nil)

    {:reply, :ok, waking} = Overwatch.handle_call({:wake, then: :stop}, self(), asleep)
    assert waking.bind_task.then == :stop
    :ok = await_bind_task(waking)
    assert Suspend.sleep_record(inst) == nil, "the bind cleared the record before the start"

    # The rebind reports its binding: the stop follows it to that runner.
    test_pid = self()
    stub(Flezha.Service.HubManager, :relay_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Runner.StreamPool, :put_route, fn _, _, _ -> :ok end)

    stub(Flezha.Runner.ControlUnit, :steps_stop, fn runner, id ->
      send(test_pid, {:steps_stop, runner, id})
      :ok
    end)

    carrying = %{
      asleep
      | bind_task: %{ref: make_ref(), pid: self(), then: :stop, extensions: asleep.extensions}
    }

    recovered = %{carrying | runner_id: "ex_2", phase: :running}

    {:noreply, _awake} =
      Overwatch.handle_info({:bind_task_result, self(), {:bound, recovered}}, carrying)

    assert_received {:steps_stop, "ex_2", ^inst}
  end

  test "a stop finds no runner to wake on, or the instance already awake" do
    inst = instance_id()
    stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, _ -> nil end)
    asleep = state(inst, phase: :suspended, runner_id: nil, channel: nil)

    ExUnit.CaptureLog.capture_log(fn ->
      assert {:reply, {:error, :no_runner}, still} =
               Overwatch.handle_call({:wake, then: :stop}, self(), asleep)

      assert Overwatch.phase(still) == :suspended
    end)

    assert {:reply, {:error, :awake}, _} =
             Overwatch.handle_call({:wake, then: :stop}, self(), state(inst))
  end

  test "a suspended instance is rediscovered from durable state after a redeploy" do
    inst = instance_id()
    builder = %Builder{id: inst, factory_id: "chat", run: :steps, opts: []}

    # Durable state of a suspended instance: iteration 0 consumed the log's first
    # input and committed, then parked.
    {:ok, 0} = InputLog.append(inst, "turn-0", %{})
    :ok = TaskJournal.append_task_committed(inst, 0, 0, [], [])
    :ok = TaskJournal.append_iter_committed(inst, 0, 0, [])
    :ok = Suspend.put_sleep(inst, park(System.system_time(:millisecond) + 60_000))

    fresh = %Overwatch{
      id: inst,
      namespace: "default",
      factory_id: "chat",
      builder: builder
    }

    {:noreply, booted, :hibernate} =
      Overwatch.handle_continue({:bind_runner, "stale-runner", builder}, fresh)

    assert Overwatch.phase(booted) == :suspended,
           "the boot never binds a suspended instance to a runner"

    assert booted.runner_id == nil
    assert Timer.armed?(booted.timers, :wake), "the recorded wake deadline is re-armed"

    # The boot seeded the floor from the journal, so the consumed input is not
    # owed. Undelivered input outranks the park: an instance with work waiting
    # binds instead of coming back suspended.
    assert InputLog.tail(inst) == {:ok, 0, 0}
    {:ok, 1} = InputLog.append(inst, "arrived-while-down", %{})

    stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, _ -> nil end)

    assert {:noreply, held} =
             Overwatch.handle_continue({:bind_runner, "stale-runner", builder}, fresh),
           "it takes the normal bind path (which finds no runner here)"

    assert {:runner_not_ready, _} = held.halt_reason
  end

  test "whether a suspended instance owes input is answered without reading its records" do
    inst = instance_id()
    {:ok, 0} = InputLog.append(inst, "turn-0", %{})
    :ok = TaskJournal.append_iter_committed(inst, 0, 0, [])
    :ok = Suspend.put_sleep(inst, park())
    :ok = TaskJournal.release_input(inst)
    {:ok, log} = Flezha.State.Supervisor.lookup({inst, :input_log})

    :erlang.trace(log, true, [:receive])
    assert Suspend.suspended_on_disk?(inst)
    {:ok, 1} = InputLog.append(inst, "turn-1", %{})
    refute Suspend.suspended_on_disk?(inst)
    :erlang.trace(log, false, [:receive])

    assert_received {:trace, ^log, :receive, {:"$gen_call", _, :last_offset}}
    refute_received {:trace, ^log, :receive, {:"$gen_call", _, {:read_range, _, _}}}
  end

  test "a steps relay whose state key is not its instance id is refused before it binds" do
    inst = instance_id()

    builder = %Builder{
      id: inst,
      factory_id: "chat",
      run: :steps,
      opts: [relay_class: "elsewhere"]
    }

    fresh = %Overwatch{id: inst, namespace: "default", factory_id: "chat", builder: builder}

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:stop, {:shutdown, {:split_state_key, ^inst, "elsewhere"}}, _} =
                 Overwatch.handle_continue({:bind_runner, "stale-runner", builder}, fresh)
      end)

    assert log =~ "its state key is its instance id"
  end

  # ---------- steps-loop-rewind ----------

  # The rewind rides the same evict → rehydrate path a park does, so the same
  # stubs stand in for the runner side.
  defp stub_rebind() do
    stub(Flezha.Runner.ControlUnit, :stop_relay_async, fn _, _ -> :ok end)
    stub(Flezha.Service.HubManager, :relay_set_runner, fn _, _, _ -> :ok end)
    stub(Flezha.Service.RelayManager, :assign_runner, fn _, _, _ -> "ex_2" end)
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
    assert state.bind_task != nil
    assert Suspend.sleep_record(inst) == nil

    # Undo-and-continue: the rebind's seed leaves the floor where the commits put
    # it, so the unwound iterations' inputs are not owed again.
    :ok = TaskJournal.release_input(inst)
    assert InputLog.tail(inst) == {:ok, 3, 4}

    :ok = await_bind_task(state)
  end

  test "rewind to an uncommitted iteration is refused, changing nothing" do
    inst = instance_id()
    commit_turns(inst, 3, [])

    assert {:reply, {:error, {:uncommitted_iteration, 7, [0, 1, 2]}}, _} =
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

    assert {:reply, {:error, {:uncovered, 1, ["replies"]}}, _} =
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

    :ok = await_bind_task(rewound)
  end

  test "iterations that emitted nothing need no coverage" do
    inst = instance_id()
    stub_rebind()
    commit_turns(inst, 3, [])
    :ok = ClassRegistry.put_compensable_ops(inst, [])

    assert {:reply, :ok, rewound} = Overwatch.handle_call({:rewind, 1}, self(), state(inst))
    :ok = await_bind_task(rewound)
  end

  describe "migration_consent/1 (processhub-deferred-migration)" do
    test "a pending correction defers" do
      inst = instance_id()

      st =
        state(inst,
          durable_steps: false,
          corrections: %{{inst, "task", "k"} => 100}
        )

      assert Overwatch.migration_consent(st) == :defer
    end

    test "an unsuspended durable steps relay defers; a suspended one consents" do
      inst = instance_id()
      assert Overwatch.migration_consent(state(inst)) == :defer
      assert Overwatch.migration_consent(state(inst, phase: :suspended)) == :ready
    end

    test "an effectively-once instance with pending outbox defers, drained consents" do
      inst = instance_id()
      st = state(inst, durable_steps: false, effectively_once: true)

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
      st = state(inst, durable_steps: false, effectively_once: true)

      stub(Flezha.State.WriterRegistry, :live_writers, fn _class -> [self()] end)
      stub(Flezha.State.WalWriter, :outbox_status, fn _pid, _timeout -> exit(:timeout) end)

      assert Overwatch.migration_consent(st) == :ready
    end

    test "an idle flow instance consents immediately" do
      inst = instance_id()
      assert Overwatch.migration_consent(state(inst, durable_steps: false)) == :ready
    end

    test "a stream that sleeps consents awake and asleep" do
      inst = instance_id()
      st = state(inst, durable_steps: false, sleeps: true)
      assert Overwatch.migration_consent(st) == :ready
      assert Overwatch.migration_consent(%{st | phase: :suspended}) == :ready
    end
  end

  describe "migration_ready barriers (processhub-deferred-migration)" do
    test "suspension signals migration_ready; :not_deferred leaves the park intact" do
      inst = instance_id()
      test_pid = self()
      stub(Flezha.Runner.ControlUnit, :stop_relay_async, fn _, _ -> :ok end)
      stub(Flezha.Service.HubManager, :relay_set_runner, fn _, _, _ -> :ok end)

      stub(ProcessHub.Service.Migration, :migration_ready, fn hub_id, child_id ->
        send(test_pid, {:migration_ready, hub_id, child_id})
        {:error, :not_deferred}
      end)

      {:noreply, suspended, :hibernate} =
        Overwatch.handle_cast({:suspend, 2, park()}, state(inst))

      assert_receive {:migration_ready, hub_id, ^inst}
      assert hub_id == Identificator.hub("default")
      assert Overwatch.phase(suspended) == :suspended
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
      st = state(inst, durable_steps: false, corrections: %{})
      {:noreply, _} = Overwatch.handle_info({:run_correction, {inst, "task", "k"}}, st)
      assert_receive {:migration_ready, ^inst}

      # Another cell still pending: not complete, no signal.
      busy =
        state(inst,
          durable_steps: false,
          corrections: %{{inst, "task", "other"} => 5}
        )

      {:noreply, _} = Overwatch.handle_info({:run_correction, {inst, "task", "gone"}}, busy)
      refute_received {:migration_ready, ^inst}
    end
  end
end

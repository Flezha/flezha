defmodule FlezhaTest.Relay.RebindTest do
  @moduledoc """
  The `:rebind` bind task contract: one bind at a time, linked to its
  Overwatch, the full bind on its target, and a reconnect's session swap. The
  teardown of a bind in flight is in `lifecycle_test.exs`.
  """

  # async: false: writes Application env; the live controller and its fleet; the primary Logger.

  use FlezhaTest.Case, fixtures: [:env], async: false
  use Mimic

  alias Flezha.Plugin
  alias Flezha.Relay.Builder
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Lifecycle
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Rebind
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Service.{PluginManager, RelayManager}
  alias Flezha.State.{Outbox, WriterRegistry}
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "default"
  @factory "store_input"

  setup do
    test_pid = self()
    ets = :ets.new(:overwatch_async_migrate_test, [:public, :set])

    on_exit(fn -> RelayManager.stop_all_relays(@namespace) end)

    {:ok, test_pid: test_pid, ets: ets}
  end

  # `await_active_runner` pings once to confirm the runner is reachable
  # before the one start attempt. To reach the PARKED state these tests
  # need, the readiness gate (first ping) must pass while the START pings
  # fail — otherwise the relay is held unbound instead of parked.
  defp park_after_gate(ets) do
    fn _runner_id ->
      case :ets.update_counter(ets, :calls, {2, 1}, {:calls, 0}) do
        1 -> {:ok, :pong}
        _ -> {:error, :timeout}
      end
    end
  end

  # A bind belongs to its Overwatch: killed, it takes the bind with it, so a
  # restarted Overwatch's own bind is the only one that completes.
  test "a killed Overwatch leaves no bind running", %{test_pid: test_pid, ets: ets} do
    Application.put_env(:flezha, :overwatch_ping_fn, park_after_gate(ets))
    relay = %Builder{id: "killed_during_migrate", factory_id: @factory, opts: []}
    {:ok, overwatch_pid} = Overwatch.start_link({@namespace, "stale_ce", relay})
    Process.unlink(overwatch_pid)

    assert NodeTestHelper.wait_until(
             fn ->
               GenServer.call(overwatch_pid, :get_overwatch_state).halt_reason != nil
             end,
             2_000,
             25
           )

    Application.put_env(:flezha, :overwatch_ping_fn, blocking_ping(test_pid))
    :ok = GenServer.call(overwatch_pid, {:rebind, "fake_new_runner"}, 1_000)
    assert_receive {:ping_started, bind_task}, 500

    task_ref = Process.monitor(bind_task)
    Process.exit(overwatch_pid, :kill)
    assert_receive {:DOWN, ^task_ref, :process, ^bind_task, :killed}, 1_000
  end

  test "starting a second parked-unpark migrate while one is in flight returns :busy",
       %{test_pid: test_pid, ets: ets} do
    Application.put_env(:flezha, :overwatch_ping_fn, park_after_gate(ets))

    relay = %Builder{id: "busy_migrate_test", factory_id: @factory, opts: []}

    {:ok, overwatch_pid} =
      Overwatch.start_link({@namespace, "stale_ce", relay})

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
               GenServer.call(overwatch_pid, :get_overwatch_state).halt_reason != nil
             end,
             2_000,
             25
           )

    # First rebind: a ping that blocks until released.
    Application.put_env(:flezha, :overwatch_ping_fn, blocking_ping(test_pid))

    :ok = GenServer.call(overwatch_pid, {:rebind, "first"}, 1_000)
    assert_receive {:ping_started, pinger}, 500

    # Second migrate must be rejected as :busy.
    assert {:error, :busy} =
             GenServer.call(overwatch_pid, {:rebind, "second"}, 1_000)

    send(pinger, :unpark)
  end

  # fails when: a running migrate starts the relay with only its init
  # values, so a durable instance whose runner died starts again from nothing
  # (no journal, no restore gate, no input tail) instead of resuming; or the
  # bind places it on a runner other than the one the move chose.
  test "a running migrate is the full bind on its target, run in the bind task" do
    test_pid = self()

    Application.put_env(:flezha, :overwatch_ping_fn, fn runner_id ->
      send(test_pid, {:pinged, runner_id})
      {:error, :no_control_unit}
    end)

    state = running_state()
    :ok = ClassRegistry.put_overwatch(state.id, self(), "runner_a")
    on_exit(fn -> ClassRegistry.delete(state.id) end)

    assert {:reply, :ok, %Overwatch{bind_task: %{pid: task}} = moving} =
             Overwatch.handle_call({:rebind, "runner_b"}, {self(), make_ref()}, state)

    assert moving.runner_id == "runner_a", "the binding moves only once the bind lands"

    assert_receive {:bind_task_result, ^task, {:halt, _parked, {:ping_failed, :no_control_unit}}},
                   2_000

    assert_received {:pinged, "runner_b"}
  end

  # fails when: an instance moved while running holds its output until the
  # bind reports, so a run that completes first is torn down holding it.
  test "only a woken or un-parked bind holds the instance's output" do
    frame = {:io_out, "out", %{}}
    binding = %{running_state() | bind_task: %{ref: make_ref(), pid: self()}}

    assert {:noreply, %{held_outputs: [^frame]}} =
             Overwatch.handle_cast(frame, %{binding | phase: :suspended})

    assert {:noreply, %{held_outputs: [^frame]}} =
             Overwatch.handle_cast(frame, %{binding | phase: :halted, halt_reason: :timeout})

    assert {:noreply, %{held_outputs: []}} = Overwatch.handle_cast(frame, binding)
  end

  # fails when: an output held through a bind that failed, or dispatched again
  # after it, meets a relay whose plugins never came up and is settled, so its
  # Cell writer stops owing an output no sink took.
  test "a failed bind leaves an output nothing took owed to its Cell writer" do
    output = owed_output({"failed_bind_owed", "t", "k"}, 7)

    halted = %{
      running_state()
      | phase: :halted,
        halt_reason: :runner_gone,
        bind_task: %{pid: self(), then: nil, from: :halted, journaled: nil},
        held_outputs: [{:io_out_batch, [output]}]
    }

    {failed, _log} =
      ExUnit.CaptureLog.with_log(fn -> Rebind.bind_task_exited(halted, :killed) end)

    assert %{held_outputs: [], bind_task: nil, owes_outputs: true} = failed
    assert {:noreply, ^failed} = Overwatch.handle_cast({:io_out_batch, [output]}, failed)
    refute_received {:"$gen_cast", {:outbox_settled_many, _}}

    # A bound relay, or a halted one whose plugins came up, settles it.
    up = %{failed | extensions: %{failed.extensions | active_mask: 1}}

    for state <- [running_state(), up] do
      assert {:noreply, _} = Overwatch.handle_cast({:io_out_batch, [output]}, state)
      assert_received {:"$gen_cast", {:outbox_settled_many, [{{:cursor, 7}, 0}]}}
    end
  end

  # fails when: a bind that fails after preparing its plugins hands the halted
  # relay hooks no started plugin runs behind, so an output it owes is settled
  # through them.
  test "a bind that fails after preparing its plugins leaves its outputs owed" do
    stub(PluginManager, :resolve, fn _namespace, binding, _entry -> {:ok, binding} end)
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:error, :no_control_unit} end)
    halted = %{running_state() | phase: :halted, halt_reason: :runner_gone}
    on_exit(fn -> ClassRegistry.delete(halted.id) end)

    sink =
      Plugin.new(
        id: "sink",
        provider: Plugin.Provider.BeamMsgForward,
        opts: %{recv_pid: self()},
        namespace: @namespace,
        relay_id: halted.id,
        hooks: [{:on_output, []}]
      )

    relay = %{halted.builder | opts: [bindings: [sink]]}

    {{:error, reason, copy}, _log} =
      ExUnit.CaptureLog.with_log(fn ->
        Lifecycle.complete_init(halted, "runner_b", relay, self(), :pinned)
      end)

    output = owed_output({"prepared_bind_owed", "t", "k"}, 3)
    binding = %{halted | bind_task: %{pid: self(), then: nil, from: :halted, journaled: nil}}

    {failed, _log} =
      ExUnit.CaptureLog.with_log(fn ->
        binding
        |> Map.put(:held_outputs, [{:io_out_batch, [output]}])
        |> Rebind.bind_task_reported({:halt, copy, reason})
      end)

    refute_received {:"$gen_cast", {:outbox_settled_many, _}}
    assert %{bind_task: nil, owes_outputs: true} = failed
    assert Table.phase(failed.extensions, "sink") == :unknown
  end

  # fails when: a move stops seeding the runner from the final state a
  # graceful stop shipped, or drops the partition identity and kind the
  # builder carries.
  test "a move seeds the bind from a stored snapshot, else from the builder" do
    state = running_state()
    assert Rebind.bind_relay(state) == state.builder

    seeded = %{state | snapshot: ~s({"store_input_tasklet":{"text_inputs":["s"]}})}

    assert Rebind.bind_relay(seeded) == %Builder{
             state.builder
             | init_values: %{"store_input_tasklet" => %{"text_inputs" => ["s"]}}
           }
  end

  # fails when: a runner process that restarted with its stable identity only
  # has the binding re-pointed at it, so it never runs the relay again; or a
  # reconnect of the process that still runs it is bound afresh.
  test "a restarted runner is bound afresh, a reconnected one only re-pointed" do
    Application.put_env(:flezha, :overwatch_ping_fn, fn _ -> {:error, :no_control_unit} end)

    state = running_state()
    :ok = ClassRegistry.put_overwatch(state.id, self(), "runner_a")
    on_exit(fn -> ClassRegistry.delete(state.id) end)

    ExUnit.CaptureLog.capture_log(fn ->
      reconnected = Rebind.session_replaced(state, "runner_a", "runner_b", :reconnected)
      assert reconnected.runner_id == "runner_b"
      assert reconnected.bind_task == nil
    end)

    restarted = Rebind.session_replaced(state, "runner_a", "runner_b", :restarted)
    assert %{pid: task} = restarted.bind_task
    assert_receive {:bind_task_result, ^task, {:halt, _, {:ping_failed, _}}}, 2_000

    assert Rebind.session_replaced(state, "runner_x", "runner_b", :restarted) == state
    assert Rebind.session_replaced(restarted, "runner_a", "runner_c", :restarted) == restarted
  end

  # fails when: a handover away from a runner that is gone stops shipping the
  # final state that runner handed over, before any other runner took over.
  test "a handover away from a gone runner ships the snapshot no runner has taken over" do
    state = %{running_state() | snapshot: "final_state_of_runner_a"}

    assert {%{id: _}, "final_state_of_runner_a"} =
             Overwatch.prepare_handover_state(state)
  end

  # fails when: a tier relay's handover leaves before its cut's records are
  # durable on this node, so the destination pulls a floor below the inputs the
  # state holds; or a class whose bind re-forwards nothing waits on them too.
  test "a handover waits on its cut only where the bind re-forwards above the Cells' floor" do
    on_exit(NodeTestHelper.override_log_level(:warning))
    # An identity no writer answers for: the cut can never be shown durable.
    moved = Jason.encode!(%{"t" => 3, "$cells" => [%{identity: [0], timers: [], next_seq: 2}]})

    for {tier?, expected} <- [{true, nil}, {false, moved}] do
      runner_id = "runner_cut_#{System.unique_integer([:positive])}"
      state = %{running_state() | runner_id: runner_id, effectively_once: tier?}

      answer_control_unit_once(runner_id, fn
        {:send_control, {:migration_serialization, _}, _metadata} -> {:ok, moved}
      end)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {%{id: _}, ^expected} = Overwatch.prepare_handover_state(state)
        end)

      assert String.contains?(log, "the runner's cut is not durable") == tier?
    end
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
    assert {%{id: _}, nil} = Overwatch.prepare_handover_state(read)
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
      for empty <- [nil, <<>>] do
        assert %{arrived_via_hotswap?: false, cause: nil} =
                 Overwatch.alter_handover_state(state, {handover, empty})
      end

      assert %{arrived_via_hotswap?: true, cause: :arrive} =
               Overwatch.alter_handover_state(state, {handover, "sdk-state"})
    end)
  end

  # A durable output of `cell`, whose writer this process stands in for.
  defp owed_output(cell, seq) do
    {:ok, _} = Registry.register(WriterRegistry.name(), cell, nil)
    identity = %{cell: cell, seq: Integer.to_string(seq), epoch: 0, channel: "out"}
    Outbox.stamp("out", Map.merge(identity, %{owner: cell, entry: {:cursor, seq}}))
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
      extensions: Table.new(Overwatch.__extensions_catalog__()),
      phase: :running
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

  # A ping that blocks inside the bind task until the test releases it (or
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

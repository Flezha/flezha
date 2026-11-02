defmodule Flezha.Shutdown.CoordinatorTest do
  # async: false — exercises the singleton Coordinator GenServer and the
  # named ETS table. Tests reset state between cases.
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use FlezhaTest.Case, fixtures: [:env], async: false

  use Mimic
  import FlezhaTest.Helper.MimicGlobal
  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0, collect: 1]

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Facts
  alias Flezha.Logs.Writer
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown
  alias Flezha.Shutdown.Coordinator
  alias Flezha.State.FsyncBatcher
  alias Flezha.Transport.Server, as: TransportServer
  alias FlezhaTest.Helper.NodeTestHelper

  @finalise_rows [
    {FsyncBatcher, :flush_step, :fsync},
    {Writer, :close_step, :logs},
    {HubManager.Lifecycle, :hubs_step, :hubs},
    {TransportServer, :transports_step, :transports}
  ]

  setup :set_mimic_global_until_exit
  setup :verify_on_exit!

  setup do
    # The application boots a Coordinator under `Flezha.Supervisor`;
    # each test starts from `:idle`.
    reset_coordinator!()
    # A finished shutdown keeps admission closed, so no test may leave one behind.
    on_exit(&reset_coordinator!/0)
    # The hubs here are stand-ins: a drain's wait for the moved children's
    # deliveries registers on none.
    stub(ProcessHub, :register_hook_handlers, fn _hub, _hook, _handlers -> :ok end)
    stub(ProcessHub, :cancel_hook_handlers, fn _hub, _hook, _ids -> :ok end)
    :ok
  end

  describe "status/0" do
    test "reports idle phase and configured knobs" do
      Application.put_env(:flezha, :graceful_shutdown_timeout_ms, 4_321)

      status = Coordinator.status()

      assert status.shutdown_in_progress == false
      assert status.shutdown_phase == nil
      assert status.signal_handler_status in [:ok, :degraded]
      assert status.graceful_shutdown_timeout_ms == 4_321
    end
  end

  describe "Shutdown.quiesced?/2" do
    test "false at idle, for the node and for any namespace" do
      refute Shutdown.quiesced?(:node, nil)
      refute Shutdown.quiesced?(:namespace, "default")
    end

    test "a finished shutdown keeps admission closed until the node stops" do
      stub(HubManager, :get_running_namespaces, fn -> [] end)
      stub_finalise_rows()

      assert :ok = Coordinator.shutdown(:cli, force: true, timeout_ms: 5_000)

      assert %{shutdown_in_progress: false} = Coordinator.status()
      assert Shutdown.quiesced?(:node, nil)
      assert Shutdown.admit(:namespace, "x") == {:error, :shutting_down}
    end
  end

  describe "shutdown/2 with force: true" do
    test "skips quiesce + drain, runs only finalise, returns :ok" do
      stub(HubManager, :get_running_namespaces, fn -> [] end)
      stub_finalise_rows()

      events = attach_telemetry()

      assert :ok = Coordinator.shutdown(:cli, force: true, timeout_ms: 5_000)

      detach_telemetry(events)
      received = drain_telemetry()

      # Quiesce + drain are skipped on force; finalise + complete fire.
      assert Enum.any?(received, fn {ev, _, meta} ->
               ev == [:flezha, :shutdown, :phase_started] and meta.phase == :finalise
             end)

      assert Enum.any?(received, fn {ev, _, meta} ->
               ev == [:flezha, :shutdown, :complete] and meta.outcome == :forced
             end)
    end
  end

  describe "shutdown/2 with no peers and no running namespaces" do
    test "runs through all phases cleanly" do
      stub(HubManager, :get_running_namespaces, fn -> [] end)
      stub_finalise_rows()

      events = attach_telemetry()

      assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000)

      detach_telemetry(events)
      received = drain_telemetry()

      phases =
        received
        |> Enum.filter(fn {ev, _, _} -> ev == [:flezha, :shutdown, :phase_started] end)
        |> Enum.map(fn {_, _, meta} -> meta.phase end)

      assert phases == [:quiesce, :drain, :finalise]

      assert Enum.any?(received, fn {ev, _, meta} ->
               ev == [:flezha, :shutdown, :complete] and meta.outcome == :clean
             end)
    end
  end

  describe "shutdown/2 with rows deferred" do
    # A stop that reports runs the transports after its report, which travels
    # over them to a `flz` on the control socket.
    test "finalise leaves the deferred rows to its caller" do
      stub(HubManager, :get_running_namespaces, fn -> [] end)
      stub_finalise_rows()

      assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000, defer: [:transports])

      assert received_rows() == [:fsync, :logs, :hubs]
    end
  end

  describe "idempotent re-entry" do
    test "second call during in-flight shutdown joins and receives same outcome" do
      # Make drain take long enough that two callers race into it.
      test_pid = self()

      stub(HubManager, :get_running_namespaces, fn ->
        # The drain holds until the second caller has queued behind it.
        send(test_pid, {:hub_called, self()})
        NodeTestHelper.park()
        []
      end)

      stub_finalise_rows()

      caller_a =
        Task.async(fn ->
          Coordinator.shutdown(:cli, timeout_ms: 5_000)
        end)

      # Wait until the first caller has entered drain, then issue
      # a second shutdown — it should join.
      assert_receive {:hub_called, drainer}, 1_000
      # A failure below must not leave the coordinator parked mid-shutdown for
      # every later test; this runs before the setup's reset.
      on_exit(fn -> send(drainer, :unpark) end)

      # Release the drain once the second call is queued behind it.
      :erlang.trace(drainer, true, [:receive])

      caller_b =
        Task.async(fn ->
          Coordinator.shutdown(:cli, timeout_ms: 5_000)
        end)

      caller_b_pid = caller_b.pid

      assert_receive {:trace, ^drainer, :receive,
                      {:"$gen_call", {^caller_b_pid, _}, {:shutdown, :cli, _}}},
                     5_000

      :erlang.trace(drainer, false, [:receive])
      send(drainer, :unpark)

      assert :ok = Task.await(caller_a, 5_000)
      assert :ok = Task.await(caller_b, 5_000)
    end
  end

  describe "drain via Migration.drain/2 (processhub-deferred-migration)" do
    defp stub_local_relays() do
      stub(HubManager, :get_running_namespaces, fn -> ["default"] end)
      stub(ProcessHub, :is_alive?, fn _hub -> true end)

      # First call (capture) reports the hosted relay; later calls (the
      # post-drain locally-empty wait) fall through to the stub: gone.
      expect(ProcessHub, :process_list, fn _hub, :local -> [{"op-1", self()}] end)
      stub(ProcessHub, :process_list, fn _hub, :local -> [] end)

      stub_finalise_rows()
    end

    test "the hub drains before its supervisor stops, and the data rows run before that" do
      test_pid = self()
      stub_local_relays()

      stub(ProcessHub.Service.Migration, :drain, fn _hub, opts ->
        assert Keyword.fetch!(opts, :timeout) > 0
        assert Keyword.fetch!(opts, :when_busy) == :wait
        send(test_pid, {:row, :drain})
        {:ok, %{migrated: 1, forced: 0}}
      end)

      assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000)

      assert received_rows() == [:drain, :fsync, :logs, :hubs, :transports]
    end

    test "solo node (:no_target_nodes) skips migration and completes clean" do
      stub_local_relays()
      stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts -> {:error, :no_target_nodes} end)

      events = attach_telemetry()
      assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000)
      detach_telemetry(events)
      received = drain_telemetry()

      assert Enum.any?(received, fn {ev, m, meta} ->
               ev == [:flezha, :shutdown, :drain_progress] and m.migrated == 0 and
                 m.forced == 0 and m.remaining_hubs == 0 and is_integer(m.elapsed_ms) and
                 meta.refused == :no_target_nodes
             end)

      assert Enum.any?(received, fn {ev, _, meta} ->
               ev == [:flezha, :shutdown, :complete] and meta.outcome == :clean
             end)
    end

    # Fails when: the refused drain reports as one that had nothing to move.
    test "a drain precondition failure degrades instead of wedging" do
      on_exit(NodeTestHelper.override_log_level(:warning))
      stub_local_relays()
      stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts -> {:error, :locked} end)
      events = attach_telemetry()

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000)
        end)

      detach_telemetry(events)
      assert log =~ "degraded (:locked)"

      assert Enum.any?(drain_telemetry(), fn {ev, _m, meta} ->
               ev == [:flezha, :shutdown, :drain_progress] and meta.refused == :locked
             end)
    end

    test "forced migrations are counted and logged, outcome stays clean" do
      on_exit(NodeTestHelper.override_log_level(:warning))
      stub_local_relays()

      # `migrated` counts the delivered handovers less the forced ones: three
      # delivered, two of them forced.
      stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts ->
        send(self(), %{child_ids: ["op-1", "op-2", "op-3"], target_node: node()})
        {:ok, %{migrated: 3, forced: 2}}
      end)

      events = attach_telemetry()

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000)
        end)

      detach_telemetry(events)
      received = drain_telemetry()

      assert log =~ "force-migrated 2"

      assert Enum.any?(received, fn {ev, m, meta} ->
               ev == [:flezha, :shutdown, :drain_progress] and m.migrated == 1 and
                 m.forced == 2 and meta.refused == nil
             end)

      assert Enum.any?(received, fn {ev, _, meta} ->
               ev == [:flezha, :shutdown, :complete] and meta.outcome == :clean
             end)
    end

    test "a drain exceeding the budget times out to {:partial, still_local}" do
      stub_local_relays()
      stub_parked_drain()
      {task, _walk} = start_parked_shutdown()

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          send(Coordinator, :phase_timeout)
          assert {:partial, ["op-1"]} = Task.await(task)
        end)

      # The user's only signal that the budget was blown is this line.
      assert log =~ "timeout phase=draining"
      assert log =~ "still_local=1"

      # The timeout path still stops the hub, in finalise.
      assert received_rows() == [:fsync, :logs, :hubs, :transports]
    end

    test "a plugin home is neither locally hosted nor still local" do
      on_exit(NodeTestHelper.override_log_level(:info))
      stub(HubManager, :get_running_namespaces, fn -> ["default"] end)
      stub(ProcessHub, :is_alive?, fn _hub -> true end)

      stub(ProcessHub, :process_list, fn _hub, :local ->
        [{"op-1", self()}, {{:plugin, "p"}, self()}]
      end)

      stub_finalise_rows()
      stub_parked_drain()

      log =
        ExUnit.CaptureLog.capture_log([level: :info], fn ->
          {task, _walk} = start_parked_shutdown()
          send(Coordinator, :phase_timeout)
          assert {:partial, ["op-1"]} = Task.await(task)
        end)

      assert log =~ "locally_hosted=1"
      assert log =~ "still_local=1"
    end

    test "a timed-out drain leaves finalise its own budget, and a hub stop that never returns ends it" do
      test_pid = self()
      stub_local_relays()

      stub_parked_drain()

      stub_finalise_rows(%{
        hubs: fn _target, budget ->
          send(test_pid, {:hubs, budget})
          NodeTestHelper.park()
        end
      })

      {task, _walk} = start_parked_shutdown()
      %{timers: %{phase_timeout: {drain_timer, _}}} = :sys.get_state(Coordinator)

      budget =
        Durations.get(:orchestrator_grace_ms) - Durations.get(:graceful_shutdown_timeout_ms)

      armed = (budget - 1_000)..(budget + Durations.get(:drain_budget_slack_ms))

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          send(Coordinator, :phase_timeout)
          assert_receive {:hubs, hubs_budget}, 5_000
          assert hubs_budget > budget - 1_000

          assert %{phase: :finalising, timers: %{phase_timeout: {timer, _}}} =
                   :sys.get_state(Coordinator)

          assert timer != drain_timer

          assert Process.read_timer(timer) in armed

          send(Coordinator, :phase_timeout)
          assert {:partial, ["op-1"]} = Task.await(task)
        end)

      assert log =~ "timeout phase=finalising"
      assert received_rows() == [:fsync, :logs]
    end

    test "a failed row is logged and the rows after it still run" do
      on_exit(NodeTestHelper.override_log_level(:warning))
      stub(HubManager, :get_running_namespaces, fn -> [] end)
      stub_finalise_rows(%{fsync: fn _target, _budget -> exit(:disk_gone) end})

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok = Coordinator.shutdown(:cli, force: true, timeout_ms: 5_000)
        end)

      assert log =~ "[shutdown] step node/fsync failed: {:exit, :disk_gone}"
      assert received_rows() == [:logs, :hubs, :transports]
    end

    test "the drain walk ends with its shutdown, and an old walk's report is never read" do
      stub_local_relays()
      expect(ProcessHub, :process_list, fn _hub, :local -> [{"op-1", self()}] end)

      stub_parked_drain()

      # The phase timeout is fired by hand, so the walk is known to be inside
      # its drain when the budget runs out.
      {first, walk} = start_parked_shutdown()
      draining = :sys.get_state(Coordinator)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          send(Coordinator, :phase_timeout)
          assert {:partial, ["op-1"]} = Task.await(first)
        end)

      assert log =~ "timeout phase=draining"
      assert_receive {:DOWN, _, :process, ^walk, :killed}, 5_000
      assert :hubs in received_rows()

      reset_coordinator!()
      {second, walk} = start_parked_shutdown()
      {_pid, old_ref} = draining.walk
      send(Coordinator, {:walk_done, old_ref, %{results: %{drain: %{migrated: 0, forced: 0}}}})

      assert %{shutdown_phase: :drain} = Coordinator.status()
      refute_received {:row, :hubs}

      reset_coordinator!()
      assert_receive {:DOWN, _, :process, ^walk, :killed}, 5_000
      Task.shutdown(second, :brutal_kill)
    end
  end

  describe "the hub and transport rows" do
    # A stand-in for `Flezha.Supervisor`, so the suite keeps its own hubs and transports.
    test "a stopped hub supervisor and stopped transports stay stopped" do
      ids = [HubManager | Enum.map(Flezha.Config.get(:transports), &{TransportServer, &1})]
      children = Enum.map(ids, &Supervisor.child_spec({Agent, fn -> :ok end}, id: &1))

      sup =
        start_supervised!(%{
          id: :stand_in,
          start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]},
          type: :supervisor
        })

      refs = for {_id, pid, _, _} <- Supervisor.which_children(sup), do: Process.monitor(pid)
      rows = Enum.filter(Shutdown.steps(:node), &(&1.name in [:hubs, :transports]))

      assert {:ok, %{ran: [:hubs, :transports]}} =
               Shutdown.run(:node, %{supervisor: sup}, 5_000, rows, on_error: :continue)

      for ref <- refs, do: assert_receive({:DOWN, ^ref, :process, _, _})
      # Every exit has reached the supervisor before it is read.
      _ = :sys.get_state(sup)
      assert Enum.all?(Supervisor.which_children(sup), &match?({_id, :undefined, _, _}, &1))
    end
  end

  describe "Application.prep_stop/1" do
    test "with the kill switch off it walks the finalise rows itself" do
      Application.put_env(:flezha, :graceful_shutdown_enabled, false)
      stub_finalise_rows()

      assert Flezha.Application.prep_stop(:state) == :state
      assert received_rows() == [:fsync, :logs, :hubs, :transports]
      refute Shutdown.quiesced?(:node, nil)
    end

    test "with the coordinator unreachable it walks the finalise rows itself" do
      on_exit(NodeTestHelper.override_log_level(:warning))
      Application.put_env(:flezha, :graceful_shutdown_enabled, true)
      stub(Coordinator, :shutdown, fn :prep_stop -> exit(:noproc) end)
      stub_finalise_rows()

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert Flezha.Application.prep_stop(:state) == :state
        end)

      assert log =~ "[shutdown] coordinator unavailable in prep_stop: :noproc"
      assert received_rows() == [:fsync, :logs, :hubs, :transports]
    end
  end

  # ---------- helpers ----------

  # The one remedy-shaped action outside the heal primitives: an administration verb
  # the loop is told about and never chooses.
  describe "the drain is declared to the convergence loop" do
    test "the drain declares itself at quiesce, runs as the node walk's step, and clears with finalise" do
      become_loop()
      stub_local_relays()

      test_pid = self()

      stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts ->
        send(test_pid, {:drain_ran, Shutdown.quiesced?(:node, nil)})
        {:ok, %{migrated: 1, forced: 0}}
      end)

      assert Enum.map(Shutdown.steps(:node), & &1.name) ==
               [:drain, :fsync, :logs, :hubs, :transports]

      assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000)

      node = node()
      assert_receive {:declare, :node_draining, ^node, %{deadline: _}}
      assert_receive {:drain_ran, true}
      assert_receive {:declare, :node_drained, ^node, %{}}

      # The counter-fact clears the fact the loop holds.
      {facts, :new} = Facts.put(%{}, :node_draining, node, %{deadline: 0}, 0)
      {facts, :cleared} = Facts.put(facts, :node_drained, node, %{}, 1)
      assert facts == %{}
    end
  end

  # The finalise rows stand in for the node's own, so no test stops the suite's
  # hubs or transports; each reports its run as `{:row, name}`.
  defp stub_finalise_rows(overrides \\ %{}) do
    test_pid = self()

    for {module, function, name} <- @finalise_rows do
      stub(
        module,
        function,
        Map.get(overrides, name, fn _target, _budget ->
          send(test_pid, {:row, name})
          :ok
        end)
      )
    end
  end

  defp received_rows() do
    collect do
      {:row, name} -> name
    end
  end

  defp reset_coordinator!() do
    Coordinator.__reset_for_tests__()
  end

  # A drain that never returns, reporting the walk it parks.
  defp stub_parked_drain() do
    test_pid = self()

    stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts ->
      send(test_pid, {:walk, hd(Process.get(:"$callers"))})
      NodeTestHelper.park()
    end)
  end

  # A shutdown in flight with its walk parked inside the drain; the walk is monitored.
  defp start_parked_shutdown() do
    task = Task.async(fn -> Coordinator.shutdown(:cli, timeout_ms: 60_000) end)
    assert_receive {:walk, walk}, 5_000
    Process.monitor(walk)
    {task, walk}
  end

  defp attach_telemetry() do
    handler_id = {__MODULE__, make_ref()}
    test_pid = self()

    :ok =
      :telemetry.attach_many(
        handler_id,
        [
          [:flezha, :shutdown, :requested],
          [:flezha, :shutdown, :phase_started],
          [:flezha, :shutdown, :phase_completed],
          [:flezha, :shutdown, :drain_progress],
          [:flezha, :shutdown, :timeout],
          [:flezha, :shutdown, :complete]
        ],
        fn event, measurements, metadata, _ ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

    handler_id
  end

  defp detach_telemetry(handler_id), do: :telemetry.detach(handler_id)

  defp drain_telemetry() do
    collect do
      {:telemetry, ev, m, md} -> {ev, m, md}
    end
  end
end

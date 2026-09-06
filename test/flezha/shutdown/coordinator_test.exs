defmodule Flezha.Shutdown.CoordinatorTest do
  # async: false — exercises the singleton Coordinator GenServer and the
  # named ETS table. Tests reset state between cases.
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use FlezhaTest.Case, fixtures: [:env], async: false

  use Mimic
  import FlezhaTest.Helper.MimicGlobal
  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]

  alias Flezha.Convergence.Facts
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown
  alias Flezha.Shutdown.Coordinator
  alias Flezha.State.FsyncBatcher
  alias Flezha.Transport.Server, as: TransportServer
  alias FlezhaTest.Helper.NodeTestHelper

  setup :set_mimic_global_until_exit
  setup :verify_on_exit!

  setup do
    # The application boots a Coordinator under `Flezha.Supervisor`.
    # Each test resets it to `:idle` by killing the process — the
    # supervisor restarts it via :one_for_one.
    reset_coordinator!()
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
  end

  describe "shutdown/2 with force: true" do
    test "skips quiesce + drain, runs only finalise, returns :ok" do
      stub(HubManager, :get_running_namespaces, fn -> [] end)
      stub(HubManager, :stop_hub_manager, fn -> :ok end)
      stub(FsyncBatcher, :flush_all, fn -> :ok end)
      stub(TransportServer, :stop_all, fn -> :ok end)

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
      stub(HubManager, :stop_hub_manager, fn -> :ok end)
      stub(FsyncBatcher, :flush_all, fn -> :ok end)
      stub(TransportServer, :stop_all, fn -> :ok end)

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

      stub(HubManager, :stop_hub_manager, fn -> :ok end)
      stub(FsyncBatcher, :flush_all, fn -> :ok end)
      stub(TransportServer, :stop_all, fn -> :ok end)

      caller_a =
        Task.async(fn ->
          Coordinator.shutdown(:cli, timeout_ms: 5_000)
        end)

      # Wait until the first caller has entered drain, then issue
      # a second shutdown — it should join.
      assert_receive {:hub_called, drainer}, 1_000

      caller_b =
        Task.async(fn ->
          Coordinator.shutdown(:cli, timeout_ms: 5_000)
        end)

      # Release the drain once the second call is queued behind it.
      coordinator = Process.whereis(Coordinator)

      assert NodeTestHelper.wait_until(
               fn -> Process.info(coordinator, :message_queue_len) > {:message_queue_len, 0} end,
               1_000
             )

      send(drainer, :unpark)

      assert :ok = Task.await(caller_a, 5_000)
      assert :ok = Task.await(caller_b, 5_000)
    end
  end

  describe "drain via Migration.drain/2 (processhub-deferred-migration)" do
    defp stub_local_operatives() do
      stub(HubManager, :get_running_namespaces, fn -> ["default"] end)
      stub(ProcessHub, :is_alive?, fn _hub -> true end)

      # First call (capture) reports the hosted operative; later calls (the
      # post-drain locally-empty wait) fall through to the stub: gone.
      expect(ProcessHub, :process_list, fn _hub, :local -> [{"op-1", self()}] end)
      stub(ProcessHub, :process_list, fn _hub, :local -> [] end)

      stub(FsyncBatcher, :flush_all, fn -> :ok end)
      stub(TransportServer, :stop_all, fn -> :ok end)
    end

    test "the hub drains before its supervisor stops" do
      test_pid = self()
      stub_local_operatives()

      stub(ProcessHub.Service.Migration, :drain, fn _hub, opts ->
        assert Keyword.fetch!(opts, :timeout) > 0
        send(test_pid, :drain_called)
        {:ok, %{migrated: 1, forced: 0}}
      end)

      stub(HubManager, :stop_hub_manager, fn ->
        send(test_pid, :hub_stopped)
        :ok
      end)

      assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000)

      assert_receive msg_a, 2_000
      assert_receive msg_b, 2_000
      assert [msg_a, msg_b] == [:drain_called, :hub_stopped]
    end

    test "solo node (:no_target_nodes) skips migration and completes clean" do
      stub_local_operatives()
      stub(HubManager, :stop_hub_manager, fn -> :ok end)
      stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts -> {:error, :no_target_nodes} end)

      events = attach_telemetry()
      assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000)
      detach_telemetry(events)
      received = drain_telemetry()

      assert Enum.any?(received, fn {ev, m, _} ->
               ev == [:flezha, :shutdown, :drain_progress] and m.migrated == 0 and
                 m.forced == 0 and m.remaining_hubs == 0 and is_integer(m.elapsed_ms)
             end)

      assert Enum.any?(received, fn {ev, _, meta} ->
               ev == [:flezha, :shutdown, :complete] and meta.outcome == :clean
             end)
    end

    test "a drain precondition failure degrades instead of wedging" do
      on_exit(NodeTestHelper.override_log_level(:warning))
      stub_local_operatives()
      stub(HubManager, :stop_hub_manager, fn -> :ok end)
      stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts -> {:error, :locked} end)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000)
        end)

      assert log =~ "degraded (:locked)"
    end

    test "forced migrations are counted and logged, outcome stays clean" do
      on_exit(NodeTestHelper.override_log_level(:warning))
      stub_local_operatives()
      stub(HubManager, :stop_hub_manager, fn -> :ok end)

      stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts ->
        {:ok, %{migrated: 1, forced: 2}}
      end)

      events = attach_telemetry()

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert :ok = Coordinator.shutdown(:cli, timeout_ms: 5_000)
        end)

      detach_telemetry(events)
      received = drain_telemetry()

      assert log =~ "force-migrated 2"

      assert Enum.any?(received, fn {ev, m, _} ->
               ev == [:flezha, :shutdown, :drain_progress] and m.migrated == 1 and m.forced == 2
             end)

      assert Enum.any?(received, fn {ev, _, meta} ->
               ev == [:flezha, :shutdown, :complete] and meta.outcome == :clean
             end)
    end

    test "a drain exceeding the budget times out to {:partial, still_local}" do
      test_pid = self()
      stub_local_operatives()

      # A drain that never returns inside the budget.
      stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts ->
        NodeTestHelper.park()
        {:ok, %{migrated: 1, forced: 0}}
      end)

      stub(HubManager, :stop_hub_manager, fn ->
        send(test_pid, :hub_stopped)
        :ok
      end)

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:partial, ["op-1"]} = Coordinator.shutdown(:cli, timeout_ms: 300)
        end)

      # The operator's only signal that the budget was blown is this line.
      assert log =~ "timeout phase=draining"
      assert log =~ "still_local=1"

      # The timeout path still stops the hub before finalise.
      assert_receive :hub_stopped, 1_000
    end
  end

  # ---------- helpers ----------

  # The one remedy-shaped action outside the heal primitives: an operator verb
  # the loop is told about and never chooses.
  describe "the drain is declared to the convergence loop" do
    test "the drain declares itself at quiesce, runs as the node walk's step, and clears with finalise" do
      become_loop()
      stub_local_operatives()
      stub(HubManager, :stop_hub_manager, fn -> :ok end)

      test_pid = self()

      stub(ProcessHub.Service.Migration, :drain, fn _hub, _opts ->
        send(test_pid, {:drain_ran, Shutdown.quiesced?(:node, nil)})
        {:ok, %{migrated: 1, forced: 0}}
      end)

      assert [%{name: :drain}] = Shutdown.steps(:node)
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

  defp reset_coordinator!() do
    Coordinator.__reset_for_tests__()
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

  defp drain_telemetry(acc \\ []) do
    receive do
      {:telemetry, ev, m, md} -> drain_telemetry([{ev, m, md} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end

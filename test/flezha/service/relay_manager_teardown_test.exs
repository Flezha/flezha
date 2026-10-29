defmodule FlezhaTest.Service.RelayManagerTeardownTest do
  @moduledoc """
  A relay stop's steps: the `:state_dir` step driven on its own (what a stop
  owes the instance's durable writers whether its files are deleted or kept for
  a rescale to move), the steps a failed start's stop runs, and the namespace
  stop's flush.
  """
  # async: false: a shared on-disk root and the node-wide writer registry.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false
  use Mimic

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias FlezhaTest.Helper.NodeTestHelper

  alias Flezha.AuthZ.Quotas
  alias Flezha.Runner.StreamPool
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.RelayManager.Teardown
  alias Flezha.Shutdown
  alias Flezha.State.{InputLog, Paths, Supervisor, TaskJournal, TestHelpers, WalWriter}
  alias FlezhaTest.Helper.Telemetry

  defp state_dir_step(), do: Enum.filter(Teardown.shutdown_steps(), &(&1.name == :state_dir))

  # `overwatch` only names the node the step reaches.
  defp stop(instance, preserve?, overwatch \\ self()) do
    target = %{
      namespace: "_system",
      relay_id: instance,
      overwatch: overwatch,
      runner_id: nil,
      preserve_state?: preserve?,
      cause: :stop,
      cell_count: 0
    }

    assert {:ok, _report} = Shutdown.run(:relay, target, 5_000, state_dir_step())
  end

  # A registration outlives its process until the registry handles the exit.
  defp closed?(instance) do
    wait_until(
      fn ->
        Supervisor.live_writers(instance) == [] and
          Supervisor.lookup({instance, :input_log}) == :error
      end,
      2_000
    )
  end

  # fails when: a preserving stop skips the writer close, so a member
  # restarted under the same id after a rescale moved its files appends
  # through the descriptor of the file that moved.
  test "a stop that keeps the files closes every durable writer of the instance" do
    instance = "teardown_keep_#{System.unique_integer([:positive])}"
    assert :ok = TaskJournal.append_iter_committed(instance, 0, 0, [])
    assert {:ok, 0} = InputLog.append(instance, "m", %{})

    stop(instance, true)

    assert closed?(instance)
    assert File.exists?(Paths.journal_path(instance))
    assert File.exists?(Paths.input_log_path(instance))

    File.rename!(Paths.journal_path(instance), Paths.journal_path(instance) <> ".moved")
    assert :ok = TaskJournal.append_iter_committed(instance, 1, 1, [])

    assert {:ok, [%{iter: 1}]} = TaskJournal.read(instance)
  end

  # fails when: an Overwatch that ends on this node, a move away among them,
  # leaves its writers open, and the relay's later return finds its own
  # stale copy held open.
  test "a relay no longer bound here closes its writers and keeps its files" do
    instance = "teardown_moved_#{System.unique_integer([:positive])}"
    assert :ok = TaskJournal.append_iter_committed(instance, 0, 0, [])
    assert {:ok, 0} = InputLog.append(instance, "m", %{})

    state = %Flezha.Relay.Overwatch{
      id: instance,
      namespace: "_system",
      builder: %Flezha.Relay.Builder{id: instance, factory_id: "f", opts: []}
    }

    steps = Enum.filter(Flezha.Relay.Lifecycle.shutdown_steps(), &(&1.name == :writers))
    assert {:ok, _report} = Shutdown.run(:process, {state, :shutdown}, 5_000, steps)

    assert closed?(instance)
    assert File.exists?(Paths.input_log_path(instance))
  end

  test "a stop that deletes the files closes the writers and removes the tree" do
    instance = "teardown_drop_#{System.unique_integer([:positive])}"
    assert :ok = TaskJournal.append_iter_committed(instance, 0, 0, [])
    assert {:ok, 0} = InputLog.append(instance, "m", %{})

    stop(instance, false)

    assert closed?(instance)
    refute File.exists?(Paths.class_dir(instance))
  end

  # fails when: a writer a late record started while the stop deletes its relay's
  # tree crashes making its directory. A delete running under `mkdir_p` makes it
  # answer `:enotdir`; a file where the Cell's directory goes answers the same.
  test "a writer booting while its Cell is deleted stops without a crash" do
    instance = "teardown_boot_#{System.unique_integer([:positive])}"
    cell_id = {instance, "t", "k1"}
    File.mkdir_p!(Paths.class_dir(instance))
    File.write!(Path.join(Paths.class_dir(instance), "t"), "")

    {:ok, writer} = Supervisor.find_or_start(cell_id)
    ref = Process.monitor(writer)

    assert_receive {:DOWN, ^ref, :process, ^writer, :normal}
    File.rm_rf!(Paths.class_dir(instance))
  end

  # fails when: a writer a late record started while the stop deleted its
  # relay's tree crashes on its next record, or keeps that record's stream credit.
  test "a writer whose Cell the stop deleted under it refuses its next record and stops" do
    instance = "teardown_late_#{System.unique_integer([:positive])}"
    cell_id = {instance, "t", "k1"}
    {:ok, writer} = Supervisor.find_or_start(cell_id)
    _booted = :sys.get_state(writer)
    :ok = Paths.delete_class_dir(instance)
    ref = Process.monitor(writer)

    credit = %{channel: self(), session: :s, wire_size: 9}
    WalWriter.append_async(writer, 0, TestHelpers.stub_header(), "x", nil, credit)

    assert_receive {:DOWN, ^ref, :process, ^writer, :normal}, 5_000
    assert_receive {:credit_drained, :s, 9, ^cell_id}
    refute File.exists?(Paths.class_dir(instance))
  end

  # fails when: a relay still waiting for a runner holds its supervisor's stop
  # unread until the shutdown bound kills it, skipping its teardown.
  test "a stop reaches a relay still waiting for its runner between polls" do
    test_pid = self()

    waiter =
      :proc_lib.spawn(fn ->
        Process.flag(:trap_exit, true)
        send(test_pid, :waiting)
        RelayManager.await_runner("teardown_no_runners", "f", "r", 30_000, fn _ -> false end)
      end)

    ref = Process.monitor(waiter)
    assert_receive :waiting
    Process.exit(waiter, :shutdown)

    assert_receive {:DOWN, ^ref, :process, ^waiter, :shutdown}, 1_000
  end

  # fails when: the step runs on the node that issued the stop, which for a
  # rescale run from another controller is not where the member's writers are.
  @tag :multi_node
  test "a stop reaches the writers on the node the relay ran on" do
    instance = "teardown_peer_#{System.unique_integer([:positive])}"

    {peer_pid, peer_node} =
      NodeTestHelper.start_peer_node(:teardown1, state_root: Paths.state_root())

    :ok = :erpc.call(peer_node, TaskJournal, :append_iter_committed, [instance, 0, 0, []])
    assert [_writer] = :erpc.call(peer_node, Supervisor, :live_writers, [instance])

    stop(instance, true, Node.spawn(peer_node, :erlang, :node, []))

    assert wait_until(
             fn -> :erpc.call(peer_node, Supervisor, :live_writers, [instance]) == [] end,
             2_000
           )

    assert :erpc.call(peer_node, File, :exists?, [Paths.journal_path(instance)])

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # The hub stop seam: every stop pipes `ProcessHub.stop_children` into
  # `Future.await`.
  defp stub_hub_stop(test_pid) do
    stub(ProcessHub, :stop_children, fn _hub, ids, _opts ->
      send(test_pid, {:hub_stop, ids})
      {:stopped, ids}
    end)

    stub(ProcessHub.Future, :await, fn {:stopped, ids} ->
      %ProcessHub.StopResult{status: :ok, stopped: Enum.map(ids, &{&1, [node()]}), errors: []}
    end)
  end

  # fails when: the stop of a start that failed flushes, releases a quota the
  # start never took, deletes state the next start owns, or announces a stop.
  test "a failed start's stop names its cause and runs the hub stop alone" do
    test_pid = self()
    instance = "teardown_failed_start_#{System.unique_integer([:positive])}"
    assert :ok = TaskJournal.append_iter_committed(instance, 0, 0, [])
    stub_hub_stop(test_pid)
    stub(Quotas, :record_deallocation, fn _ns, kind, _n -> send(test_pid, {:dealloc, kind}) end)
    stub(StreamPool, :flush_channels, fn runner, _ms -> send(test_pid, {:flushed, runner}) end)
    Telemetry.capture([:flezha, :relay, :stopped])

    # A bound relay with Cells: every step it skips would have acted.
    overwatch =
      spawn_link(fn ->
        receive do
          {:"$gen_call", from, {:note, %{cause: cause}}} ->
            send(test_pid, {:told, cause})
            GenServer.reply(from, %{builder: %{cells: ["a"]}, runner_id: "runner-1"})
        end
      end)

    assert :ok =
             Teardown.stop_relay("_system", instance, cause: :failed_start, overwatch: overwatch)

    assert_received {:told, :failed_start}
    assert_received {:hub_stop, [^instance]}
    refute_received {:flushed, _}
    refute_received {:dealloc, _}
    refute_received {:telemetry, _, _, _}
    assert File.exists?(Paths.journal_path(instance))
  end

  # fails when: a namespace stop takes its relays down with frames still queued
  # in their runners' stream channels.
  test "stop_all flushes each runner's stream channels before its one hub call" do
    test_pid = self()
    namespace = "teardown_all_#{System.unique_integer([:positive])}"
    ids = for i <- 1..3, do: "#{namespace}_#{i}"
    runners = %{Enum.at(ids, 0) => "runner-1", Enum.at(ids, 1) => "runner-1"}

    stub(HubManager, :get_all_relays, fn ^namespace -> Enum.map(ids, &{&1, self()}) end)
    stub(HubManager, :relay_runner_tag, fn ^namespace, id -> runners[id] end)
    stub(StreamPool, :flush_channels, fn runner, _ms -> send(test_pid, {:flushed, runner}) end)
    stub_hub_stop(test_pid)

    assert :ok = Teardown.stop_all(namespace, 5_000)

    assert {:messages, [{:flushed, "runner-1"}, {:hub_stop, ^ids}]} =
             Process.info(self(), :messages)
  end

  # fails when: a stop that got no answer is reported failed although an ask
  # inside the same budget finds the child gone, or a retry is handed the whole
  # budget again.
  test "a stop that got no answer is asked again within the caller's budget" do
    test_pid = self()

    stub(ProcessHub, :stop_children, fn _hub, ids, opts ->
      send(test_pid, {:hub_stop, ids, opts[:timeout]})
      ids
    end)

    stub(ProcessHub.Future, :await, fn [id] ->
      reason = if Process.put(:asked, true), do: :not_found, else: :no_response
      %ProcessHub.StopResult{status: :error, stopped: [], errors: [{id, {:error, reason}}]}
    end)

    assert :ok = Teardown.hub_stop_children("_system", ["silent"], 5_000)
    assert_received {:hub_stop, ["silent"], first}
    assert_received {:hub_stop, ["silent"], second}
    assert second < first and first <= 5_000
  end
end

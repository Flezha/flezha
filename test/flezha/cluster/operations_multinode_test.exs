defmodule Flezha.Cluster.OperationsMultiNodeTest do
  @moduledoc """
  The operational reads answered for the whole cluster: dead letters,
  schedules, observability signals and switches, quota usage and each
  controller's own facts, asked of a controller that holds only part of what
  they report, and the bound that keeps a wedged controller from holding one up.
  """

  # async: false: peer BEAMs, the live controller, the auth stores, the DLQ root.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_ops_reads", enabled: false}, :isolated_dlq_dir],
    async: false

  import ExUnit.CaptureLog

  alias Flezha.AuthZ.QuotaCounters
  alias Flezha.Config.Durations
  alias Flezha.Convergence.Snapshot
  alias Flezha.Dlq.Entry
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Observability.{Bus, Registry, RingBufferSink, Signal}
  alias Flezha.Observability.TestSinks
  alias Flezha.Plugin.Providers
  alias Flezha.Relay.Builder
  alias Flezha.Schedule.Store, as: ScheduleStore
  alias Flezha.Service.{Controller, Dlq, Fleet, Namespace, Observability, RelayManager, Schedule}
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ScheduleTestHelper
  alias FlezhaTest.Helper.StepsResumeHarness

  @moduletag :multi_node
  @moduletag timeout: 180_000

  @factory "chat_loop_steps"
  @context %{source: :ui, principal: "console@_system"}

  setup do
    TestHelpers.reset_all_cells()
    Snapshot.reset_recovery_states()
    %{ns: "ops_ns_#{System.unique_integer([:positive])}"}
  end

  test "dead letters, quota usage and controller facts answer for both controllers", %{ns: ns} do
    {peer_pid, peer} = NodeTestHelper.start_peer_node(:ops_dlq)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)
    own_dlq_dir(peer)

    _runner = NodeTestHelper.start_runner_remote(peer, ns)
    :erpc.call(peer, NodeTestHelper, :await_serving, [peer, ns, @factory])
    id = start_on(peer, ns)

    # A first turn the relay commits, so the replay below is not the input that
    # races the runner-side bind.
    StepsResumeHarness.send_turn(ns, id, "hello", "in")
    assert NodeTestHelper.wait_until(fn -> committed(peer, id) != [] end, 30_000)

    local_ref = park(node(), id, "here")
    peer_ref = park(peer, id, "there")
    dropped_ref = park(peer, id, "dropped")

    assert {:ok, %{rows: rows, unreachable: []}} = Dlq.list(id, @context)

    assert Enum.sort(Enum.map(rows, &{entry_ref(&1), &1.node})) ==
             Enum.sort([{local_ref, node()}, {peer_ref, peer}, {dropped_ref, peer}])

    assert {:ok, %{rows: stats}} = Dlq.stats(@context)
    assert %{entries: 2} = Enum.find(stats, &(&1.relay_class == id and &1.node == peer))
    assert {:ok, %{rows: tasklets}} = Dlq.tasklets(id, @context)
    assert Enum.sort(Enum.map(tasklets, & &1.node)) == Enum.sort([node(), peer])

    assert {:ok, %{"payload_bytes" => 7}, "dropped"} = Dlq.get(dropped_ref, @context)
    assert :ok = Dlq.drop(dropped_ref, @context)
    assert {:error, :not_found} = :erpc.call(peer, DlqStore, :get, [dropped_ref])

    # The replay re-enters the relay's ordinary input path on the peer.
    before = length(committed(peer, id))
    assert {:ok, %{outcome: :removed}} = Dlq.replay(peer_ref, @context)
    assert {:error, :not_found} = :erpc.call(peer, DlqStore, :get, [peer_ref])

    assert NodeTestHelper.wait_until(
             fn -> length(committed(peer, id)) > before end,
             30_000
           )

    auth_dir = NodeTestHelper.claim_tmp_dir("flz_ops_auth_peer")
    on_exit(fn -> File.rm_rf(auth_dir) end)
    authz = Keyword.put(Application.get_env(:flezha, :authn_authz), :auth_dir, auth_dir)
    :ok = :erpc.call(peer, Application, :put_env, [:flezha, :authn_authz, authz])
    {:ok, _} = :erpc.call(peer, GenServer, :start, [QuotaCounters, [], [name: QuotaCounters]])

    QuotaCounters.incr(ns, :runners, 2)
    :ok = :erpc.call(peer, QuotaCounters, :incr, [ns, :runners, 1])

    assert NodeTestHelper.wait_until(
             fn ->
               match?({:ok, %{total: 3}}, Namespace.quota(ns, :runners, @context))
             end,
             10_000
           )

    assert {:ok, %{by_node: %{} = by_node, unreachable: []}} =
             Namespace.quota(ns, :runners, @context)

    assert by_node[node()] == 2 and by_node[peer] == 1

    assert {:ok, %{total: 1, by_node: %{^peer => 1}}} = Namespace.quota(ns, :relays, @context)

    assert {:ok, %{rows: [%{node: first}, %{node: second}], unreachable: []}} =
             Controller.status(@context)

    assert [first, second] == [node(), peer]

    :ok = :erpc.call(peer, Providers, :add, ["ops_peer_only", Flezha.Plugin.Provider.StdOut])
    assert {:ok, %{rows: providers, refused: refused}} = Fleet.providers(@context)
    assert %{nodes: [^peer]} = Enum.find(providers, &(&1.id == "ops_peer_only"))
    assert %{nodes: [_, _]} = Enum.find(providers, &(&1.id == "relay_stdout"))
    assert Map.keys(refused) |> Enum.sort() == Enum.sort([node(), peer])
    assert {:ok, %{provider: "ops_peer_only"}} = Fleet.provider("ops_peer_only", @context)

    NodeTestHelper.stop_namespace(ns)
  end

  test "schedules are listed, run and removed where they are held; a name is cluster-unique" do
    ScheduleTestHelper.isolated_store_setup()
    ScheduleTestHelper.schedule_hub_setup()
    {peer_pid, peer} = NodeTestHelper.start_peer_node(:ops_sched)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)

    peer_dir = NodeTestHelper.claim_tmp_dir("flz_ops_sched")
    on_exit(fn -> File.rm_rf(peer_dir) end)
    schedules = Keyword.put(Application.get_env(:flezha, :schedules, []), :dir, peer_dir)
    :ok = :erpc.call(peer, Application, :put_env, [:flezha, :schedules, schedules])
    {:ok, _} = :erpc.call(peer, GenServer, :start, [ScheduleStore, [], [name: ScheduleStore]])
    {:ok, _} = :erpc.call(peer, Flezha.Service.HubManager, :start_schedule_hub, [])

    nightly = ScheduleTestHelper.entry_fields(%{name: "nightly", cron: "0 0 1 1 *"})
    assert {:ok, _} = :erpc.call(peer, Schedule, :add, [nightly, @context])

    assert {:ok, %{rows: [%{name: "nightly", node: ^peer} = row], unreachable: []}} =
             Schedule.list(@context)

    assert Map.has_key?(row, :fires_on)

    assert {:error, %Flezha.Error{code: "schedule.already_exists", details: details}} =
             Schedule.add(nightly, @context)

    assert details["node"] == to_string(peer)
    assert ScheduleStore.list() == []

    # Found and fired on the peer, where no runner serves its factory.
    log =
      capture_log(fn ->
        assert {:error, %Flezha.Error{code: "relay.factory_unknown"}} =
                 Schedule.run("nightly", @context)
      end)

    assert log =~ "spawn_failed" and log =~ "nightly@manual-"

    assert :ok = Schedule.remove("nightly", @context)
    assert :error = :erpc.call(peer, ScheduleStore, :get, ["nightly"])
    assert {:ok, %{rows: [], unreachable: []}} = Schedule.list(@context)
  end

  test "signals carry their node and wall-clock time; sources switch on every controller" do
    {peer_pid, peer} = NodeTestHelper.start_peer_node(:ops_obs)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)

    src = :"ops_obs_src_#{System.unique_integer([:positive])}"
    source = %{name: src, kinds: [:event], flavour: :declarative}
    :ok = Registry.register_source(source)
    on_exit(fn -> Registry.unregister_source(src) end)
    :ok = :erpc.call(peer, Registry, :register_source, [source])

    since_us = System.os_time(:microsecond)
    Bus.emit(Signal.new(%{kind: :event, source: src, name: :first}))
    TestSinks.settle(RingBufferSink)
    :ok = :erpc.call(peer, Bus, :emit, [Signal.new(%{kind: :event, source: src, name: :second})])
    :erpc.call(peer, TestSinks, :settle, [RingBufferSink])

    assert {:ok, %{rows: [first, second], unreachable: []}} =
             Observability.query(%{source: src, since_us: since_us}, @context)

    assert %{name: :first, node: here} = first
    assert %{name: :second, node: ^peer} = second
    assert here == node()
    assert first.at_us <= second.at_us and first.at_us >= since_us

    assert {:ok, %{rows: [%{node: ^peer}]}} =
             Observability.query(%{source: src, since_us: since_us, limit: 1}, @context)

    assert {:ok, %{rows: sources}} = Observability.list_sources(@context)

    assert %{statuses: statuses, recent: %{signals: signals}} =
             Enum.find(sources, &(&1.name == src))

    assert Map.keys(statuses) |> Enum.sort() == Enum.sort([node(), peer])
    assert %{first: %{count: 1}, second: %{count: 1}} = signals

    assert {:ok, %{rows: sinks}} = Observability.list_sinks(@context)
    assert Enum.sort(Enum.uniq(Enum.map(sinks, & &1.node))) == Enum.sort([node(), peer])

    assert {:ok, %{applied: applied, unreachable: []}} =
             Observability.disable_source(src, @context)

    assert Enum.sort(applied) == Enum.sort([node(), peer])
    assert {:ok, %{status: :disabled}} = :erpc.call(peer, Registry, :lookup_source, [src])
    assert {:ok, %{status: :disabled}} = Registry.lookup_source(src)
  end

  test "a tail opened on one controller hands on another controller's signals" do
    {peer_pid, peer} = NodeTestHelper.start_peer_node(:ops_tail)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)
    :ok = TestSinks.claim_pubsub_sink()
    observability = Application.get_env(:flezha, :observability)
    :ok = :erpc.call(peer, Application, :put_env, [:flezha, :observability, observability])
    # A peer boots its PubSub sink from the host's environment.
    assert :erpc.call(peer, Process, :whereis, [Flezha.Observability.PubSubSink])

    src = :"ops_tail_src_#{System.unique_integer([:positive])}"
    source = %{name: src, kinds: [:event], flavour: :declarative}
    :ok = Registry.register_source(source)
    on_exit(fn -> Registry.unregister_source(src) end)
    :ok = :erpc.call(peer, Registry, :register_source, [source])

    parent = self()
    context = Map.merge(@context, %{emit: parent, window_ms: 10_000})
    tail = Task.async(fn -> Observability.tail(src, context) end)

    assert NodeTestHelper.wait_until(
             fn ->
               :ok =
                 :erpc.call(peer, Bus, :emit, [Signal.new(%{kind: :event, source: src, name: :x})])

               receive do
                 {:flezha_emit, %Signal{} = signal} -> located_on?(signal, peer)
               after
                 100 -> false
               end
             end,
             10_000
           )

    Task.shutdown(tail, :brutal_kill)
  end

  test "a wedged controller costs each read one bound and is named in unreachable" do
    {peer_pid, peer} = NodeTestHelper.start_peer_node(:ops_wedged)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)
    own_dlq_dir(peer)

    src = :"ops_wedged_src_#{System.unique_integer([:positive])}"
    :ok = Registry.register_source(%{name: src, kinds: [:event], flavour: :declarative})
    on_exit(fn -> Registry.unregister_source(src) end)

    # Each read's local half on the peer waits on a server that never answers.
    servers = [DlqStore, Registry, Flezha.Shutdown.Coordinator]
    for server <- servers, do: :ok = :erpc.call(peer, :sys, :suspend, [server])
    on_exit(fn -> for server <- servers, do: :rpc.call(peer, :sys, :resume, [server]) end)

    assert {:ok, %{rows: [], unreachable: [^peer]}} =
             within_one_bound(fn -> Dlq.list(nil, @context) end)

    assert {:ok, %{applied: [here], unreachable: [^peer]}} =
             within_one_bound(fn -> Observability.disable_source(src, @context) end)

    assert {:ok, %{rows: [%{node: ^here}], unreachable: [^peer]}} =
             within_one_bound(fn -> Controller.status(@context) end)

    assert here == node()
  end

  # A wedged peer costs a read one `peer_read_ms` bound, never one per call it
  # would have made.
  defp within_one_bound(read) do
    started = System.monotonic_time(:millisecond)
    answer = read.()
    assert System.monotonic_time(:millisecond) - started < Durations.get(:peer_read_ms) + 2_000
    answer
  end

  # Read as a map: a signal is located only where it leaves its node.
  defp located_on?(signal, node),
    do: Map.get(signal, :node) == node and is_integer(Map.get(signal, :at_us))

  defp own_dlq_dir(peer) do
    dir = NodeTestHelper.claim_tmp_dir("flz_ops_dlq_peer")
    on_exit(fn -> File.rm_rf(dir) end)
    config = Keyword.put(Application.get_env(:flezha, :dlq, []), :dir, dir)
    :ok = :erpc.call(peer, Application, :put_env, [:flezha, :dlq, config])
  end

  defp start_on(peer, ns) do
    id = "ops_#{System.unique_integer([:positive])}"
    builder = %Builder{id: id, factory_id: @factory, run: :steps, init_values: %{}, opts: []}
    assert {:ok, %{node: ^peer}} = RelayManager.start_relay(ns, builder)
    id
  end

  defp committed(peer, id), do: StepsResumeHarness.committed_iters(id, peer)

  defp park(on, id, payload) do
    {:ok, entry} =
      Entry.new(%{
        relay_class: id,
        tasklet_id: "chat_turn",
        cell_key: payload,
        channel: "in",
        error: "parked by the test",
        payload: payload
      })

    {:ok, ref} = :erpc.call(on, DlqStore, :append, [entry])
    ref
  end

  defp entry_ref(entry), do: "#{entry["relay_class"]}/#{entry["tasklet_id"]}/#{entry["id"]}"
end

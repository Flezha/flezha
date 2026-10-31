defmodule Flezha.Cluster.SplitBehaviourMultinodeTest do
  @moduledoc """
  `FLEZHA_SPLIT_BEHAVIOUR=dynamic` on three controllers. The controller cut
  off from the other two stops its relays, tells its runner to drop them, says
  `no_quorum` on `/healthz` and refuses a relay start, while the other two run
  every relay; after the heal each relay runs once. A drained controller is not
  counted as lost, so draining them one after another leaves the last one
  running. Under `static`, a cut shorter than the grace moves nothing, and an
  effectively-once relay absorbs an acknowledged input sent again after it.

  The three controllers are peers. This node runs no runner of the namespace,
  so it is no member of its hub; it stays connected to every peer through the
  split and watches both sides.
  """

  use FlezhaTest.Case, fixtures: [:env], async: false

  @moduletag :multi_node
  @moduletag :capture_log
  @moduletag timeout: 300_000

  alias Flezha.Cluster.GroupedHashing
  alias Flezha.Identificator
  alias Flezha.Runner.ControlUnit
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.State.InputLog
  alias Flezha.State.InputLog.Sweep
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.RelayTestHelper

  @factory "store_input"
  @ops %{source: :ui, principal: "op@_system", role: :ops, namespace: "_system"}

  setup context do
    namespace = "split-#{System.unique_integer([:positive])}"

    app_env =
      Keyword.merge(
        [
          split_behaviour: :dynamic,
          healthz_enabled: true,
          healthz_port: 0,
          convergence_health_enabled: true,
          convergence_tick_ms: 500
        ],
        Map.get(context, :app_env, [])
      )

    peers =
      for name <- [:split_p1, :split_p2, :split_p3] do
        {_pid, peer} =
          NodeTestHelper.start_peer_node(name,
            args: NodeTestHelper.split_tolerant_args(),
            app_env: app_env
          )

        peer
      end

    # Each runner is owned by a process on its own peer, not by the caller.
    peers
    |> Enum.map(&Task.async(fn -> NodeTestHelper.start_runner_remote(&1, namespace) end))
    |> Task.await_many(60_000)

    assert wait(fn -> Enum.all?(peers, &(group(&1, namespace) == Enum.sort(peers))) end, 30_000),
           "the three controllers never served #{@factory} together"

    %{namespace: namespace, peers: peers, hub_id: Identificator.hub(namespace)}
  end

  test "the cut-off controller stops, the other two run every relay, the heal runs each once",
       %{namespace: ns, peers: [p1, p2, p3] = peers, hub_id: hub_id} do
    relays = start_until_on(p3, peers, ns, hub_id, start_relays(p1, ns))
    cut_off = running(p3, hub_id)

    :ok =
      :erpc.call(p3, NodeTestHelper, :report_calls, [ControlUnit, :stop_relay, 4, self()])

    NodeTestHelper.split([p3], [p1, p2])

    assert wait(fn -> :erpc.call(p3, HubManager, :quorum, [hub_id]) == {:error, :no_quorum} end),
           "the cut-off controller kept its quorum"

    assert wait(fn -> running(p3, hub_id) == [] end), "its relays kept running"

    for id <- cut_off,
        do:
          assert_receive(
            {:called, ControlUnit, :stop_relay, [_runner, ^id, _timeout, _drain]},
            10_000
          )

    # `default` loses quorum here too, so the status alone can come from a tick
    # that judged this namespace before its own hub lost quorum.
    assert wait(fn -> ns in no_quorum_namespaces(p3) end),
           "/healthz said #{inspect(healthz(p3))}"

    assert {:error, %Flezha.Error{code: "controller.no_quorum"}} =
             :erpc.call(p3, Flezha.Service.Relay, :start, [
               ns,
               [factory_id: @factory, id: "refused_on_minority"],
               @ops
             ])

    assert wait(fn -> counts([p1, p2], hub_id, relays) == ones(relays) end, 30_000),
           "the other two do not run every relay once: #{inspect(counts([p1, p2], hub_id, relays))}"

    NodeTestHelper.heal([p3], [p1, p2])

    assert wait(fn -> :erpc.call(p3, HubManager, :quorum, [hub_id]) == :ok end, 30_000)

    assert wait(fn -> counts(peers, hub_id, relays) == ones(relays) end, 30_000),
           "after the heal a relay runs other than once: #{inspect(counts(peers, hub_id, relays))}"
  end

  test "draining controllers one after another leaves the last one running",
       %{namespace: ns, peers: [p1, p2, p3], hub_id: hub_id} do
    relays = start_relays(p1, ns)

    for peer <- [p3, p2] do
      assert :ok = shutdown(peer)
      assert wait(fn -> peer not in :erpc.call(p1, HubManager, :get_hub_nodes, [ns]) end, 30_000)
    end

    assert :erpc.call(p1, HubManager, :quorum, [hub_id]) == :ok
    assert wait(fn -> counts([p1], hub_id, relays) == ones(relays) end, 30_000)
  end

  describe "static quorum, a cut shorter than the grace" do
    # The cut-off controller keeps its relays, as when detection plus the grace
    # outlasts the cut. Any consumed prefix is cut, by the sweep the test runs.
    @describetag app_env: [
                   split_behaviour: :static,
                   quorum_nodes: 2,
                   process_hub_nodedown_grace_ms: 120_000,
                   record_log_cut_after_bytes: 0,
                   time_travel_retention_check_interval_ms: 0
                 ]

    test "an acknowledged msg_id sent again after a sweep in the cut is absorbed",
         %{namespace: ns, peers: [p1, p2, p3] = peers, hub_id: hub_id} do
      start_until_on(p3, peers, ns, hub_id, start_relays(p1, ns, "tier_chain"), "tier_chain")
      [relay | _] = running(p3, hub_id)
      m1 = "#{relay}:m1"

      assert :ok = ingest(ns, relay, "k1:1", m1)
      assert :ok = ingest(ns, relay, "k1:2", "#{relay}:m2")
      acked = :erpc.call(p3, InputLog, :msg_offset, [relay, m1])

      # Consumed: only the hold keeps the acknowledged record from the sweep.
      assert wait(fn -> (:erpc.call(p3, InputLog, :watermark, [relay]) || -1) > acked end, 30_000)

      NodeTestHelper.split([p3], [p1, p2])
      :ok = :erpc.call(p3, Sweep, :run, [relay])
      NodeTestHelper.heal([p3], [p1, p2])
      assert relay in running(p3, hub_id), "the relay moved during a cut shorter than the grace"

      # The sender never saw its acknowledgement and sends again.
      assert {:ok, :absorbed} = ingest(ns, relay, "k1:1", m1)

      # A later input is consumed behind anything journaled before it, so once
      # the watermark passes it, an input counted twice shows in the count.
      m3 = "#{relay}:m3"
      assert :ok = ingest(ns, relay, "k1:3", m3)
      last = :erpc.call(p3, InputLog, :msg_offset, [relay, m3])
      assert wait(fn -> (:erpc.call(p3, InputLog, :watermark, [relay]) || -1) >= last end, 30_000)
      assert count(ns, relay, "tier_pass", "k1") == 3
    end
  end

  defp start_relays(at, ns, factory \\ @factory),
    do: for(i <- 1..9, do: start_relay(at, ns, i, factory))

  defp start_relay(at, ns, i, factory) do
    id = "split_#{i}"

    {:ok, _} =
      :erpc.call(at, RelayManager, :start_relay, [ns, RelayFixture.get(id, factory)], 30_000)

    id
  end

  # Placement hashes each id, so nine relays can all miss one controller: more
  # are started until one runs on `target`. A start returns once the relay is
  # placed, and its process starts after that.
  defp start_until_on(target, [at | _] = peers, ns, hub_id, relays, factory \\ @factory) do
    assert wait(fn ->
             Enum.sum(Enum.map(peers, &length(running(&1, hub_id)))) == length(relays)
           end),
           "the relays never all ran"

    cond do
      running(target, hub_id) != [] ->
        relays

      length(relays) >= 30 ->
        flunk("no relay landed on the controller that is cut off")

      true ->
        start_until_on(
          target,
          peers,
          ns,
          hub_id,
          relays ++ [start_relay(at, ns, length(relays) + 1, factory)],
          factory
        )
    end
  end

  # The chaos driver's path: the relay resolved from this node, which the cut
  # never reaches, asked for its verdict.
  defp ingest(ns, relay, body, msg_id) do
    deadline = System.monotonic_time(:millisecond) + 15_000
    RelayTestHelper.send_confirmed(relay, ns, body, %{"flz.msg_id" => msg_id}, deadline)
  end

  defp count(ns, relay, tasklet, key) do
    with {:ok, json, _seq} <-
           RelayManager.get_cell_state(ns, relay, %{tasklet: tasklet, cell_key: key}),
         [_, value] <- Regex.run(~r/"value"\s*:\s*(-?\d+)/, json),
         do: String.to_integer(value),
         else: (unread -> unread)
  end

  defp group(at, ns),
    do: Enum.sort(:erpc.call(at, GroupedHashing, :group_nodes, [Identificator.hub(ns), @factory]))

  defp shutdown(peer) do
    :erpc.call(peer, Flezha.Shutdown.Coordinator, :shutdown, [:cli, [timeout_ms: 30_000]], 45_000)
  end

  # The relays a controller runs: the processes under its hub's distributed
  # supervisor, which a hub without quorum has terminated.
  defp running(at, hub_id) do
    case :erpc.call(at, ProcessHub.Hub, :get, [hub_id]) do
      nil ->
        []

      hub ->
        for {id, pid, _type, _modules} <-
              :erpc.call(at, Supervisor, :which_children, [hub.procs.dist_sup]),
            is_binary(id) and is_pid(pid),
            do: id
    end
  catch
    _kind, _reason -> []
  end

  defp counts(nodes, hub_id, relays) do
    seen = nodes |> Enum.flat_map(&running(&1, hub_id)) |> Enum.frequencies()
    Map.new(relays, &{&1, Map.get(seen, &1, 0)})
  end

  defp ones(relays), do: Map.new(relays, &{&1, 1})

  defp no_quorum_namespaces(node) do
    case healthz(node) do
      {200, %{"status" => "no_quorum", "namespaces" => listed}} -> listed
      _other -> []
    end
  end

  defp healthz(at) do
    {:ok, port} = :erpc.call(at, Flezha.Healthz.Server, :listen_port, [])
    {:ok, conn} = Flezha.Http.Client.connect(:http, "127.0.0.1", port)

    {:ok, conn, status, _headers, body} =
      Flezha.Http.Client.request(conn, "GET", "/healthz", [], "", 5_000)

    Flezha.Http.Client.close(conn)
    {status, Jason.decode!(body)}
  end

  defp wait(fun, timeout_ms \\ 15_000), do: NodeTestHelper.wait_until(fun, timeout_ms, 100)
end

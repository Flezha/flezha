defmodule Flezha.Logs.LogsMultiNodeTest do
  @moduledoc """
  The relay history read as one cluster from two controllers, each writing its
  own files: a read merges newest first and pages across both, a tail on one
  hears a record written on the other, a controller that does not answer is
  named while the other's records are read, the spawn lookup finds a key the
  other holds, and a relay that moves from one to the other ends with the first
  start its origin controller stamped.
  """

  # async: false: peer BEAMs and the live controller.
  use FlezhaTest.Case, async: false

  alias Flezha.Logs.{Kind, RelayHistory}
  alias Flezha.Relay.Builder
  alias Flezha.Service.{Logs, RelayManager}
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :multi_node
  @moduletag timeout: 180_000

  @ctx %{source: :internal}

  setup do
    TestHelpers.reset_all_cells()
    {peer_pid, peer} = NodeTestHelper.start_peer_node(:relay_history_reads)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)
    %{peer: peer, ns: "history_ns_#{System.unique_integer([:positive])}"}
  end

  test "a read merges both controllers newest first and pages across them", %{peer: peer, ns: ns} do
    for n <- 1..6 do
      on = if rem(n, 2) == 0, do: peer, else: node()
      :ok = :erpc.call(on, RelayHistory, :started, [ns, builder("run-#{n}")])
    end

    {:ok, %{records: first, next: next, unreachable: []}} = read(ns, %{limit: 4})
    {:ok, %{records: rest, next: nil}} = read(ns, %{limit: 4, before: next})

    assert Enum.map(first ++ rest, & &1["relay"]) == for(n <- 6..1//-1, do: "run-#{n}")
    assert Enum.map(first, & &1["node"]) == Enum.map([peer, node(), peer, node()], &to_string/1)

    assert {:ok, %{controllers: rows, unreachable: []}} =
             Logs.stats(%{kind: "relay_history"}, @ctx)

    assert Enum.sort(Enum.map(rows, & &1.node)) == Enum.sort([node(), peer])
    assert {:started, "run-2", _ts} = RelayHistory.seen(ns, {:relay, "run-2"})
  end

  test "a tail on one controller hears a record the other writes", %{peer: peer, ns: ns} do
    test = self()

    emit = fn record ->
      send(test, {:tailed, record})
      :ok
    end

    tail =
      Task.async(fn ->
        Logs.tail(%{kind: "relay_history", filters: %{namespace: ns}}, Map.put(@ctx, :emit, emit))
      end)

    topic = Flezha.Logs.Writer.topic(:relay_history)

    assert NodeTestHelper.wait_until(
             fn -> Elixir.Registry.lookup(Flezha.Observability.PubSub, topic) != [] end,
             5_000
           )

    :ok = :erpc.call(peer, RelayHistory, :started, [ns, builder("over-there")])
    assert_receive {:tailed, %{"relay" => "over-there", "node" => node}}, 5_000
    assert node == to_string(peer)
    Task.shutdown(tail, :brutal_kill)
  end

  test "a controller that does not answer is named, and the other's records are read",
       %{peer: peer, ns: ns} do
    :ok = RelayHistory.started(ns, builder("here"))
    writer = :erpc.call(peer, GenServer, :whereis, [Kind.name(:relay_history, :writer)])
    :ok = :erpc.call(peer, :sys, :suspend, [writer])
    assert {:ok, %{records: [%{"relay" => "here"}], unreachable: [^peer]}} = read(ns, %{})
    :ok = :erpc.call(peer, :sys, :resume, [writer])
  end

  test "a relay that moves ends where it lands, with the start its origin stamped",
       %{peer: peer} do
    relay_id = RelayFixture.start_on(peer, "history_move", "store_input")
    {:ok, %{records: [started]}} = read("default", %{filters: %{relay: relay_id}})

    assert :ok =
             :erpc.call(
               peer,
               Flezha.Shutdown.Coordinator,
               :shutdown,
               [:cli, [timeout_ms: 30_000]],
               45_000
             )

    assert NodeTestHelper.wait_until(
             fn ->
               match?(
                 {:ok, pid} when node(pid) == node(),
                 RelayManager.get_relay_pid("default", relay_id)
               )
             end,
             15_000
           )

    :ok = RelayManager.stop_relay("default", relay_id)

    assert NodeTestHelper.wait_until(fn -> length(local(relay_id)) == 1 end, 10_000)
    [ended] = local(relay_id)
    assert %{"event" => "ended", "outcome" => "stopped"} = ended
    assert ended["node"] == to_string(node())
    assert ended["first_started_at"] == started["first_started_at"]

    assert {:ok, %{records: [^started]}} =
             read("default", %{filters: %{relay: relay_id, event: "started"}})
  end

  defp builder(id), do: %Builder{id: id, factory_id: "f", run: :steps}

  defp read(ns, params),
    do: Logs.read(Map.merge(%{kind: "relay_history", filters: %{namespace: ns}}, params), @ctx)

  # This controller's own records of `relay_id`: the peer it started on is gone.
  defp local(relay_id) do
    {:ok, %{records: records}} =
      Logs.read(%{kind: "relay_history", filters: %{relay: relay_id, event: "ended"}}, @ctx)

    records
  end
end

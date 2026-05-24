defmodule Flezha.Reconciler.ProbeTest do
  @moduledoc """
  Liveness probes (self-healing-control-plane, Phase 5 / 5.1). Each dimension is
  exercised against a synthetic registry entry plus stub relay/channel GenServers,
  so no real executor or peer node is required.
  """
  use ExUnit.Case, async: false

  alias Flezha.Reconciler.{Activity, Probe}
  alias Flezha.Relay.Overwatch

  # Minimal GenServer that replies to a call with a pre-seeded value.
  defmodule Stub do
    use GenServer
    def start(replies), do: GenServer.start(__MODULE__, replies)
    @impl true
    def init(replies), do: {:ok, replies}
    @impl true
    def handle_call(msg, _from, replies), do: {:reply, Map.get(replies, msg, :unknown), replies}
  end

  defp stub(replies) do
    {:ok, pid} = Stub.start(replies)
    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    pid
  end

  defp dead_pid do
    pid = spawn(fn -> :ok end)
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000
    pid
  end

  defp entry(node_pids, metadata \\ %{tag: "ce1"}) do
    relay = %{id: "r1", factory_id: "f1", opts: []}
    cspec = %{id: "r1", start: {Overwatch, :start_link, [{"ns", relay}]}}
    {cspec, node_pids, metadata}
  end

  defp ctx(active \\ ["ce1"], factory_available? \\ true) do
    %{active_executors: MapSet.new(active), factory_available?: fn _ -> factory_available? end}
  end

  test "live local relay, not parked, stream bound, executor reachable + available" do
    channel = stub(%{stream_ref_status: :bound_ref})

    relay =
      stub(%{
        get_overwatch_state: %Overwatch{init_error: nil, channel: channel, executor_id: "ce1"}
      })

    p = Probe.probe("ns", "r1", entry([{node(), relay}]), ctx())

    assert p.relay_alive?
    assert p.local?
    refute p.parked?
    assert p.stream_bound? == true
    assert p.executor_reachable?
    assert p.executor_available?
    assert p.factory_id == "f1"
    assert p.dead_node_entries == []
  end

  test "parked relay surfaces park reason" do
    relay =
      stub(%{
        get_overwatch_state: %Overwatch{init_error: :boom, channel: nil, executor_id: "ce1"}
      })

    p = Probe.probe("ns", "r1", entry([{node(), relay}]), ctx())

    assert p.parked?
    assert p.park_reason == :boom
  end

  test "unbound data stream is reported (channel stream_ref nil)" do
    channel = stub(%{stream_ref_status: nil})

    relay =
      stub(%{
        get_overwatch_state: %Overwatch{init_error: nil, channel: channel, executor_id: "ce1"}
      })

    p = Probe.probe("ns", "r1", entry([{node(), relay}]), ctx())

    assert p.stream_bound? == false
  end

  test "nil channel counts as unbound" do
    relay =
      stub(%{get_overwatch_state: %Overwatch{init_error: nil, channel: nil, executor_id: "ce1"}})

    p = Probe.probe("ns", "r1", entry([{node(), relay}]), ctx())
    assert p.stream_bound? == false
  end

  test "dead-node-only entry is not alive and lists the dead entry" do
    dead = dead_pid()
    p = Probe.probe("ns", "r1", entry([{:dead@nohost, dead}]), ctx())

    refute p.relay_alive?
    refute p.local?
    assert p.dead_node_entries == [{:dead@nohost, dead}]
  end

  test "mixed entry: live local pid wins, dead entry flagged for purge" do
    relay =
      stub(%{get_overwatch_state: %Overwatch{init_error: nil, channel: nil, executor_id: "ce1"}})

    dead = dead_pid()

    p = Probe.probe("ns", "r1", entry([{:dead@nohost, dead}, {node(), relay}]), ctx())

    assert p.relay_alive?
    assert p.relay_pid == relay
    assert p.dead_node_entries == [{:dead@nohost, dead}]
  end

  test "executor not in the active set is unreachable" do
    relay =
      stub(%{get_overwatch_state: %Overwatch{init_error: nil, channel: nil, executor_id: "ce1"}})

    p = Probe.probe("ns", "r1", entry([{node(), relay}]), ctx([], false))

    refute p.executor_reachable?
    refute p.executor_available?
  end

  test "wedged relay (call times out) degrades to unknown, not a crash" do
    # A relay pid that never replies — the bounded call returns :error so the
    # probe leaves stream state :unknown rather than blocking the sweep.
    relay = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(relay, :kill) end)

    p = Probe.probe("ns", "r1", entry([{node(), relay}]), ctx())

    assert p.relay_alive?
    refute p.parked?
    assert p.stream_bound? == :unknown
  end

  test "last-output age is read from the activity table" do
    start_supervised!(Activity)
    Activity.record_output("r1")

    relay =
      stub(%{get_overwatch_state: %Overwatch{init_error: nil, channel: nil, executor_id: "ce1"}})

    p = Probe.probe("ns", "r1", entry([{node(), relay}]), ctx())

    assert is_integer(p.last_output_age_ms)
    assert p.last_output_age_ms >= 0
  end
end

defmodule Flezha.Reconciler.ActionsTest do
  @moduledoc """
  Idempotent convergence actions (self-healing-control-plane, Phase 5 / 5.3):
  no-op on a healthy relay, leader-gating of cluster-wide actions, and that each
  action drives the right existing primitive.
  """
  use ExUnit.Case, async: false
  use Mimic

  alias Flezha.Executor.FleetManager
  alias Flezha.Reconciler.{Actions, Probe}
  alias Flezha.Service.{HubManager, Identificator, RelayManager}
  alias ProcessHub.Service.ProcessRegistry

  # Replies :ok to any call and forwards it to a test pid for assertions.
  defmodule Echo do
    use GenServer
    def start(test_pid), do: GenServer.start(__MODULE__, test_pid)
    @impl true
    def init(t), do: {:ok, t}
    @impl true
    def handle_call(msg, _from, t) do
      send(t, {:called, msg})
      {:reply, :ok, t}
    end
  end

  defp probe(fields), do: struct(%Probe{namespace: "ns", relay_id: "r1"}, fields)

  test "healthy relay → no actions (idempotent no-op)" do
    p =
      probe(
        relay_alive?: true,
        local?: true,
        parked?: false,
        stream_bound?: true,
        executor_reachable?: true,
        executor_available?: true
      )

    assert Actions.converge(p, :healthy, true) == []
    assert Actions.converge(p, :healthy, false) == []
  end

  describe "purge_dead_node (leader-gated, cluster-wide)" do
    @ns "actions_purge_test"

    setup do
      {:ok, _} = ensure_hub(@ns)
      on_exit(fn -> HubManager.stop_hub(@ns) end)
      :ok
    end

    test "leader purges the dead-node entry, keeping the live one" do
      hub_id = Identificator.hub(@ns)
      dead = spawn(fn -> :ok end)
      inject(@ns, "r1", [{:dead@nohost, dead}, {node(), self()}])

      p =
        probe(
          namespace: @ns,
          relay_alive?: true,
          local?: true,
          dead_node_entries: [{:dead@nohost, dead}]
        )

      assert Actions.converge(p, :degraded, true) == [:purge_dead_node]

      assert {_cspec, [{n, _pid}], _meta} = ProcessRegistry.dump(hub_id)["r1"]
      assert n == node()
    end

    test "non-leader does not purge" do
      hub_id = Identificator.hub(@ns)
      dead = spawn(fn -> :ok end)
      inject(@ns, "r1", [{:dead@nohost, dead}, {node(), self()}])

      p =
        probe(
          namespace: @ns,
          relay_alive?: true,
          local?: true,
          dead_node_entries: [{:dead@nohost, dead}]
        )

      assert Actions.converge(p, :degraded, false) == []
      assert {_cspec, node_pids, _meta} = ProcessRegistry.dump(hub_id)["r1"]
      assert length(node_pids) == 2
    end
  end

  test "replace is leader-gated" do
    stub(ProcessHub, :start_child, fn _hub, _cspec, _opts -> {:ok, :future} end)

    cspec = %{
      id: "r1",
      start: {Flezha.Relay.Overwatch, :start_link, [{"ns", %{factory_id: "f1"}}]}
    }

    p = probe(relay_alive?: false, executor_available?: true, cspec: cspec)

    assert Actions.converge(p, :degraded, false) == []
    assert Actions.converge(p, :degraded, true) == [:replace]
  end

  test "unpark re-issues executor_migrate to a freshly-resolved executor (local, any node)" do
    {:ok, echo} = Echo.start(self())
    on_exit(fn -> if Process.alive?(echo), do: GenServer.stop(echo) end)

    stub(RelayManager, :assign_executor, fn "ns", "f1" -> "ce_new" end)

    p =
      probe(
        parked?: true,
        local?: true,
        relay_alive?: true,
        executor_available?: true,
        factory_id: "f1",
        relay_pid: echo
      )

    assert Actions.converge(p, :degraded, false) == [:unpark]
    assert_receive {:called, {:executor_migrate, "ce_new"}}
  end

  test "rebind_stream resets the control channel (local, any node)" do
    stub(FleetManager, :reset_control_channel, fn "ce1" -> :ok end)

    p =
      probe(
        stream_bound?: false,
        parked?: false,
        local?: true,
        relay_alive?: true,
        executor_reachable?: true,
        assigned_executor: "ce1"
      )

    assert Actions.converge(p, :degraded, false) == [:rebind_stream]
  end

  test "local actions do not run for a relay whose pid is on another node" do
    # local? false → un-park / rebind are left to the owning node's local pass.
    p =
      probe(
        parked?: true,
        local?: false,
        relay_alive?: true,
        executor_available?: true,
        factory_id: "f1"
      )

    assert Actions.converge(p, :degraded, false) == []
  end

  defp ensure_hub(ns) do
    case HubManager.start_hub(ns) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _info} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  defp inject(ns, relay_id, node_pids) do
    hub_id = Identificator.hub(ns)
    cspec = %{id: relay_id, start: {Flezha.Relay.Overwatch, :start_link, [{ns, relay_id}]}}
    :ok = ProcessRegistry.bulk_insert(hub_id, %{relay_id => {cspec, node_pids, %{}}})
  end
end

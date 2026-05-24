defmodule Flezha.Reconciler.LoopTest do
  @moduledoc """
  Reconciliation loop integration (self-healing-control-plane, Phase 5 / 5.4, 5.6).

  Exercises the loop end-to-end against a real hub with synthetic registry
  entries: a registered-but-broken relay (a stale dead-node entry) is detected and
  driven back toward desired state with no operator action — purged, health
  persisted, and observable via telemetry — plus the event-driven trigger and the
  `:reconciler_enabled` kill-switch. The full "Kafka offsets advance" producing
  path needs a real executor and is verified in Docker (§13.1/§13.4).
  """
  use ExUnit.Case, async: false

  alias Flezha.Cluster.Orchestrator
  alias Flezha.Reconciler.{Activity, Loop}
  alias Flezha.Relay.Overwatch
  alias Flezha.Service.{HubManager, Identificator}
  alias ProcessHub.Service.ProcessRegistry

  defmodule StubRelay do
    use GenServer
    def start(state), do: GenServer.start(__MODULE__, state)
    @impl true
    def init(state), do: {:ok, state}
    @impl true
    def handle_call(:get_overwatch_state, _from, state), do: {:reply, state, state}
  end

  test "kill-switch: with :reconciler_enabled false the supervisor starts no children" do
    # :test config sets reconciler_enabled: false, so the global subsystem boots
    # but runs nothing (pre-reconciler code paths).
    assert Supervisor.which_children(Flezha.Reconciler.Supervisor) == []
  end

  describe "reconcile" do
    @ns "reconciler_loop_test"

    setup do
      assert eventually(fn -> Orchestrator.cluster_ready?() end)

      start_supervised!(Activity)
      # Non-zero (but long) sweep so schedule_sweep/1's timer path runs at init
      # without the auto-sweep firing during these millisecond-scale tests; the
      # reconcile tests drive reconciliation explicitly.
      start_supervised!({Loop, [sweep_interval_ms: 60_000, debounce_ms: 10]})

      {:ok, _} = ensure_hub(@ns)
      on_exit(fn -> HubManager.stop_hub(@ns) end)
      :ok
    end

    test "detects a stale dead-node relay and purges it, persisting health + telemetry" do
      hub_id = Identificator.hub(@ns)
      attach_telemetry([:flezha, :reconcile, :action], :action)
      attach_telemetry([:flezha, :reconcile, :health], :health)

      relay = stub_relay()
      dead = spawn(fn -> :ok end)
      inject(@ns, "r1", [{:dead@nohost, dead}, {node(), relay}])

      results = Loop.reconcile_namespace(@ns)

      assert {"r1", :degraded, [:purge_dead_node]} =
               Enum.find(results, fn {id, _, _} -> id == "r1" end)

      # Dead-node entry purged; the live local entry remains.
      assert {_cspec, [{n, _pid}], metadata} = ProcessRegistry.dump(hub_id)["r1"]
      assert n == node()
      assert metadata[:health] == :degraded

      assert_receive {:telemetry, :action, %{count: 1},
                      %{relay_id: "r1", action: :purge_dead_node}}

      assert_receive {:telemetry, :health, %{count: 1}, %{relay_id: "r1", health: :degraded}}
    end

    test "a fully-purged second pass is a no-op (idempotent)" do
      relay = stub_relay()
      dead = spawn(fn -> :ok end)
      inject(@ns, "r1", [{:dead@nohost, dead}, {node(), relay}])

      assert [{"r1", :degraded, [:purge_dead_node]}] =
               Enum.filter(Loop.reconcile_namespace(@ns), fn {id, _, _} -> id == "r1" end)

      # The dead entry is gone; the next pass takes no purge action.
      assert [{"r1", _health, actions}] =
               Enum.filter(Loop.reconcile_namespace(@ns), fn {id, _, _} -> id == "r1" end)

      refute :purge_dead_node in actions
    end

    test "an executor_connected_event drives a targeted reconcile" do
      attach_telemetry([:flezha, :reconcile, :action], :action)

      relay = stub_relay()
      dead = spawn(fn -> :ok end)
      inject(@ns, "r1", [{:dead@nohost, dead}, {node(), relay}])

      # Real Blockade dispatch — the loop subscribed in init.
      Flezha.Service.EventManager.dispatch(:executor_connected_event, {@ns, "ce_x"}, %{
        members: :local
      })

      assert_receive {:telemetry, :action, _m, %{relay_id: "r1", action: :purge_dead_node}}, 2_000
    end

    test "a ProcessHub hook callback drives a debounced reconcile" do
      attach_telemetry([:flezha, :reconcile, :action], :action)

      relay = stub_relay()
      dead = spawn(fn -> :ok end)
      inject(@ns, "r1", [{:dead@nohost, dead}, {node(), relay}])

      # The MFA ProcessHub would invoke on child_registered/node_leave/etc.
      :ok = Loop.hook_event(Identificator.hub(@ns), %{})

      assert_receive {:telemetry, :action, _m, %{relay_id: "r1", action: :purge_dead_node}}, 2_000
    end

    test "the periodic sweep reconciles running namespaces" do
      attach_telemetry([:flezha, :reconcile, :action], :action)

      relay = stub_relay()
      dead = spawn(fn -> :ok end)
      inject(@ns, "r1", [{:dead@nohost, dead}, {node(), relay}])

      # Drive the backstop sweep directly (exercises handle_info(:sweep) +
      # reschedule) rather than waiting on the 60 s timer.
      send(Process.whereis(Loop), :sweep)

      assert_receive {:telemetry, :action, _m, %{relay_id: "r1", action: :purge_dead_node}}, 2_000
    end
  end

  # ---------- helpers ----------

  defp stub_relay do
    {:ok, pid} =
      StubRelay.start(%Overwatch{init_error: nil, channel: nil, executor_id: "ce_old"})

    on_exit(fn -> if Process.alive?(pid), do: GenServer.stop(pid) end)
    pid
  end

  defp attach_telemetry(event, tag) do
    test_pid = self()
    id = "loop-test-#{tag}-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      id,
      event,
      fn _e, m, md, _ -> send(test_pid, {:telemetry, tag, m, md}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(id) end)
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
    relay = %{id: relay_id, factory_id: "f1", opts: []}
    cspec = %{id: relay_id, start: {Flezha.Relay.Overwatch, :start_link, [{ns, relay}]}}
    :ok = ProcessRegistry.bulk_insert(hub_id, %{relay_id => {cspec, node_pids, %{}}})
  end

  defp eventually(fun, attempts \\ 50) do
    cond do
      fun.() ->
        true

      attempts <= 0 ->
        false

      true ->
        Process.sleep(50)
        eventually(fun, attempts - 1)
    end
  end
end

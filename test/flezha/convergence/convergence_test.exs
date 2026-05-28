defmodule Flezha.ConvergenceTest do
  @moduledoc """
  Unit coverage for the declarative-reconciler seams introduced by
  `unified-convergence-loop` session 2: the pure `Backoff`, `Diff`,
  `Planner` functions, the `SafeCall` wrapper, and the `Snapshot`
  publish point. Live healing + the Loop's executor takeover are
  covered by the `executor disconnect preserve` integration test.
  """

  use ExUnit.Case, async: false

  alias Flezha.Convergence.{Backoff, Diff, Heal, Intent, Planner, SafeCall, Snapshot}
  alias Flezha.Plugin
  alias Flezha.Service.HubManager

  defp plugin(id) do
    Plugin.new(
      id: id,
      provider: __MODULE__.NoopProvider,
      opts: %{},
      subscriptions: [{:on_output, []}],
      namespace: "default",
      relay_id: "r"
    )
  end

  describe "Backoff" do
    test "fresh keys are eligible; failures arm an exponential cooldown" do
      b = Backoff.new()
      key = {:rebind_relay, "btc"}
      now = 1_000

      assert Backoff.eligible?(b, key, now)

      b = Backoff.update(b, key, :error, now)
      refute Backoff.eligible?(b, key, now)
      # first step is 5s
      assert Backoff.eligible?(b, key, now + 5_000)

      # a success clears the entry
      b = Backoff.update(b, key, :ok, now)
      assert Backoff.eligible?(b, key, now)
    end

    test "filter drops ops still in cooldown, keeps the rest" do
      now = 0
      b = Backoff.update(Backoff.new(), {:rebind_relay, "a"}, :error, now)

      ops = [
        %{primitive: :rebind_relay, target: "a"},
        %{primitive: :rebind_relay, target: "b"}
      ]

      kept = Backoff.filter(ops, b, &Planner.key/1, now)
      assert kept == [%{primitive: :rebind_relay, target: "b"}]
    end
  end

  describe "Diff.compute/2" do
    test "classifies unbound and misbound relays against active executors" do
      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "unbound", factory_id: nil, plugins: []},
          %{namespace: :ns, relay_id: "ok", factory_id: nil, plugins: []},
          %{namespace: :ns, relay_id: "stale", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e_live"])},
        relays: %{
          {:ns, "unbound"} => %{bound_executor: nil},
          {:ns, "ok"} => %{bound_executor: "e_live"},
          {:ns, "stale"} => %{bound_executor: "e_dead"}
        },
        stalled_relays: MapSet.new()
      }

      delta = Diff.compute(intent, reality)

      assert delta.unbound_relays == [{:ns, "unbound"}]
      assert delta.misbound_relays == [{{:ns, "stale"}, "e_dead"}]
      refute Diff.empty?(delta)
    end

    test "empty delta when every relay is bound to a live executor" do
      intent = %Intent{relays: [%{namespace: :ns, relay_id: "ok", factory_id: nil, plugins: []}]}

      reality = %{
        active_executors: %{ns: MapSet.new(["e_live"])},
        relays: %{{:ns, "ok"} => %{bound_executor: "e_live"}},
        stalled_relays: MapSet.new()
      }

      assert Diff.empty?(Diff.compute(intent, reality))
    end

    test "I2: classifies missing and extra plugins on a live relay" do
      p = plugin("declared")

      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: nil, plugins: [{"declared", p}]}]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e"])},
        relays: %{
          {:ns, "r"} => %{bound_executor: "e", pid: self(), running_plugins: ["stray"]}
        },
        stalled_relays: MapSet.new()
      }

      delta = Diff.compute(intent, reality)

      assert delta.missing_plugins == [{{:ns, "r"}, "declared", p}]
      assert delta.extra_plugins == [{{:ns, "r"}, "stray", self()}]
    end

    test "I2: no plugin divergence when running set matches declared" do
      p = plugin("p1")

      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: nil, plugins: [{"p1", p}]}]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e"])},
        relays: %{{:ns, "r"} => %{bound_executor: "e", pid: self(), running_plugins: ["p1"]}},
        stalled_relays: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
    end

    test "I2: skips plugin diff for relays with no live pid" do
      p = plugin("declared")

      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: nil, plugins: [{"declared", p}]}]
      }

      reality = %{
        active_executors: %{ns: MapSet.new()},
        relays: %{{:ns, "r"} => %{bound_executor: nil, pid: nil, running_plugins: []}},
        stalled_relays: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.missing_plugins == []
    end

    test "I4: passes wedged channels through from reality" do
      reality = %{
        active_executors: %{},
        relays: %{},
        stalled_relays: MapSet.new(),
        wedged_channels: [{self(), :stream_channel, nil}]
      }

      delta = Diff.compute(%Intent{relays: []}, reality)
      assert delta.wedged_channels == [{self(), :stream_channel, nil}]
    end
  end

  describe "Planner.plan/3" do
    test "orders eviction before rebind for misbound relays" do
      delta = %Diff{misbound_relays: [{{:ns, "r"}, "e_dead"}]}
      plan = Planner.plan(delta, Backoff.new())

      assert [%{primitive: :evict_executor, target: "e_dead"}, %{primitive: :rebind_relay} = rebind] =
               plan

      assert rebind.namespace == :ns
      assert rebind.target == "r"
    end

    test "caps ops per tick at max_ops" do
      delta = %Diff{unbound_relays: for(i <- 1..30, do: {:ns, "r#{i}"})}
      assert length(Planner.plan(delta, Backoff.new(), max_ops: 5)) == 5
    end

    test "wedged channels heal via reconnect_stream, ordered before binds" do
      delta = %Diff{
        wedged_channels: [{self(), :stream_channel, nil}],
        unbound_relays: [{:ns, "r"}]
      }

      assert [first | _] = Planner.plan(delta, Backoff.new())
      assert %{primitive: :reconnect_stream, target: pid, args: %{kind: :stream_channel}} = first
      assert pid == self()
    end

    test "missing plugins emit start_plugin carrying the full %Plugin{}" do
      p = plugin("p1")
      delta = %Diff{missing_plugins: [{{:ns, "r"}, "p1", p}]}

      assert [%{primitive: :start_plugin, namespace: :ns, target: "r", args: args}] =
               Planner.plan(delta, Backoff.new())

      assert args.plugin_id == "p1"
      assert args.plugin == p
    end
  end

  describe "SafeCall.bounded/3" do
    test "returns {:ok, reply} for a healthy call" do
      {:ok, pid} = Agent.start_link(fn -> :state end)
      assert {:ok, :state} = SafeCall.bounded(pid, {:get, & &1}, 1_000)
      Agent.stop(pid)
    end

    test "returns {:error, :noproc} for a dead process" do
      {:ok, pid} = Agent.start_link(fn -> :state end)
      Agent.stop(pid)
      assert {:error, :noproc} = SafeCall.bounded(pid, {:get, & &1}, 100)
    end
  end

  describe "Snapshot" do
    test "publish/current round-trips" do
      Snapshot.publish(%{last_tick_at: 42, stalled_relays: ["x"], bound_relays: 1, producing_relays: 1})
      assert Snapshot.current().last_tick_at == 42
      assert Snapshot.current().stalled_relays == ["x"]
      # reset for other suites
      Snapshot.publish(%{last_tick_at: nil, stalled_relays: [], bound_relays: 0, producing_relays: 0})
    end

    test "observe_recovery caches per-hub state" do
      hub = :"conv_snap_#{System.unique_integer([:positive])}"
      assert Snapshot.recovery_state(hub) == :unknown
      Snapshot.observe_recovery(hub, :normal)
      assert Snapshot.recovery_state(hub) == :normal
      assert Map.get(Snapshot.recovery_states(), hub) == :normal
    end
  end

  describe "Heal" do
    test "stubbed primitives are dispatchable no-ops" do
      # Real primitives (`rebind_relay`, `start_plugin`, `stop_plugin`,
      # `evict_executor`, channel `reconnect_stream`) are covered below /
      # by the `executor disconnect preserve` integration test; the rest
      # remain observable no-ops pending later phases.
      stubbed = [:start_relay, :stop_relay, :restart_plugin, :replay_state, :compact_wal]

      for primitive <- stubbed do
        op = %{primitive: primitive, namespace: nil, target: :noop, args: %{}}
        assert Heal.execute(op) == :ok
      end
    end

    test "reconnect_stream kills a wedged stream channel and returns :ok" do
      victim = spawn(fn -> Process.sleep(:infinity) end)
      ref = Process.monitor(victim)

      op = %{primitive: :reconnect_stream, namespace: nil, target: victim, args: %{kind: :stream_channel}}
      assert Heal.execute(op) == :ok

      assert_receive {:DOWN, ^ref, :process, ^victim, :killed}, 1_000
    end

    test "evict_executor on an unknown id is an idempotent :ok" do
      op = %{primitive: :evict_executor, namespace: nil, target: "ghost_executor", args: %{}}
      assert Heal.execute(op) == :ok
    end
  end

  describe "HubManager channel-health watchdog" do
    test "wedged_channel_pids honours the strike threshold and clear" do
      a = spawn(fn -> Process.sleep(:infinity) end)
      b = spawn(fn -> Process.sleep(:infinity) end)
      now = System.monotonic_time(:millisecond)

      # a: 3 strikes inside the window → wedged. b: 2 strikes → not yet.
      :ets.insert(:flezha_channel_health, {a, {3, now}})
      :ets.insert(:flezha_channel_health, {b, {2, now}})

      wedged = HubManager.wedged_channel_pids()
      assert a in wedged
      refute b in wedged

      HubManager.clear_channel_timeout(a)
      refute a in HubManager.wedged_channel_pids()

      :ets.delete(:flezha_channel_health, b)
      Process.exit(a, :kill)
      Process.exit(b, :kill)
    end

    test "a strike older than the window does not count as wedged" do
      stale = spawn(fn -> Process.sleep(:infinity) end)
      old = System.monotonic_time(:millisecond) - 60_000
      :ets.insert(:flezha_channel_health, {stale, {5, old}})

      refute stale in HubManager.wedged_channel_pids()
      :ets.delete(:flezha_channel_health, stale)
      Process.exit(stale, :kill)
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end
end

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
    test "every stubbed primitive is a dispatchable no-op" do
      # `:rebind_relay` is the one real primitive (it calls into the hub)
      # and is covered by the `executor disconnect preserve` integration
      # test; the rest are observable no-ops this session.
      for primitive <- Heal.primitives() -- [:rebind_relay] do
        op = %{primitive: primitive, namespace: nil, target: :noop, args: %{}}
        assert Heal.execute(op) == :ok
      end
    end
  end
end

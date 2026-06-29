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

  defp plugin(id) do
    Plugin.new(
      id: id,
      provider: __MODULE__.NoopProvider,
      opts: %{},
      subscriptions: [{:on_output, []}],
      namespace: "default",
      operative_id: "r"
    )
  end

  describe "Backoff" do
    test "fresh keys are eligible; failures arm an exponential cooldown" do
      b = Backoff.new()
      key = {:rebind_operative, "btc"}
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
      b = Backoff.update(Backoff.new(), {:rebind_operative, "a"}, :error, now)

      ops = [
        %{primitive: :rebind_operative, target: "a"},
        %{primitive: :rebind_operative, target: "b"}
      ]

      kept = Backoff.filter(ops, b, &Planner.key/1, now)
      assert kept == [%{primitive: :rebind_operative, target: "b"}]
    end
  end

  describe "Diff.compute/2" do
    test "classifies unbound and misbound operatives against active executors" do
      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "unbound", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "ok", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "stale", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e_live"])},
        operatives: %{
          {:ns, "unbound"} => %{bound_executor: nil},
          {:ns, "ok"} => %{bound_executor: "e_live"},
          {:ns, "stale"} => %{bound_executor: "e_dead"}
        },
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)

      assert delta.unbound_operatives == [{:ns, "unbound"}]
      assert delta.misbound_operatives == [{{:ns, "stale"}, "e_dead"}]
      refute Diff.empty?(delta)
    end

    test "empty delta when every operative is bound to a live executor" do
      intent = %Intent{operatives: [%{namespace: :ns, operative_id: "ok", factory_id: nil, plugins: []}]}

      reality = %{
        active_executors: %{ns: MapSet.new(["e_live"])},
        operatives: %{{:ns, "ok"} => %{bound_executor: "e_live"}},
        stalled_operatives: MapSet.new()
      }

      assert Diff.empty?(Diff.compute(intent, reality))
    end

    test "I2: classifies missing and extra plugins on a live operative" do
      p = plugin("declared")

      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"declared", p}]}]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{bound_executor: "e", pid: self(), running_plugins: ["stray"]}
        },
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)

      assert delta.missing_plugins == [{{:ns, "r"}, "declared", p}]
      assert delta.extra_plugins == [{{:ns, "r"}, "stray", self()}]
    end

    test "I2: no plugin divergence when running set matches declared" do
      p = plugin("p1")

      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"p1", p}]}]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e"])},
        operatives: %{{:ns, "r"} => %{bound_executor: "e", pid: self(), running_plugins: ["p1"]}},
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
    end

    # fails when: a operative whose declared plugin set reads empty (cspec lag or
    # read failure) has its entire running plugin set torn down every tick,
    # causing a stop/re-subscribe storm
    test "I2: empty declared plugin set never marks running plugins as extra" do
      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{
            bound_executor: "e",
            pid: self(),
            running_plugins: ["ws_in", "kafka_out"]
          }
        },
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.missing_plugins == []
      assert delta.extra_plugins == []
    end

    test "I3: a subscribed declared plugin self-reporting unhealthy diffs as unhealthy" do
      p = plugin("ws_in")

      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"ws_in", p}]}]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{
            bound_executor: "e",
            pid: self(),
            running_plugins: ["ws_in"],
            plugin_health: %{"ws_in" => {:unhealthy, :connection_idle}}
          }
        },
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.unhealthy_plugins == [{{:ns, "r"}, "ws_in", p, :connection_idle}]
      assert delta.missing_plugins == []
      refute Diff.empty?(delta)
    end

    test "I3: missing and unhealthy buckets are disjoint; uncertain health is healthy" do
      missing = plugin("gone")
      shaky = plugin("shaky")
      fine = plugin("fine")

      intent = %Intent{
        operatives: [
          %{
            namespace: :ns,
            operative_id: "r",
            factory_id: nil,
            plugins: [{"gone", missing}, {"shaky", shaky}, {"fine", fine}]
          }
        ]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{
            bound_executor: "e",
            pid: self(),
            running_plugins: ["shaky", "fine"],
            # "gone" is absent from health (unsubscribed); "fine" has no
            # verdict at all (uncertain probe) — both must not be unhealthy.
            plugin_health: %{"shaky" => {:unhealthy, :produce_stalled}}
          }
        },
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.missing_plugins == [{{:ns, "r"}, "gone", missing}]
      assert delta.unhealthy_plugins == [{{:ns, "r"}, "shaky", shaky, :produce_stalled}]
    end

    test "I2: skips plugin diff for operatives with no live pid" do
      p = plugin("declared")

      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"declared", p}]}]
      }

      reality = %{
        active_executors: %{ns: MapSet.new()},
        operatives: %{{:ns, "r"} => %{bound_executor: nil, pid: nil, running_plugins: []}},
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.missing_plugins == []
    end

    test "skips misbound classification when the namespace's live-executor read is :unknown" do
      # C1 regression: a failed FleetManager read (Probe records `:unknown`)
      # must NOT make every bound operative misbound — that mass-migrates a whole
      # namespace on a transient read race.
      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "a", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "b", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_executors: %{ns: :unknown},
        operatives: %{
          {:ns, "a"} => %{bound_executor: "e1"},
          {:ns, "b"} => %{bound_executor: "e2"}
        },
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.misbound_operatives == []
      assert delta.unbound_operatives == []
    end

    test "skips a operative whose own state read failed (bound_executor :unknown)" do
      intent = %Intent{operatives: [%{namespace: :ns, operative_id: "r", factory_id: nil, plugins: []}]}

      reality = %{
        active_executors: %{ns: MapSet.new(["e_live"])},
        operatives: %{{:ns, "r"} => %{bound_executor: :unknown, pid: nil, running_plugins: []}},
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.misbound_operatives == []
      assert delta.unbound_operatives == []
      assert delta.missing_plugins == []
    end
  end

  describe "Planner.plan/3" do
    test "orders eviction before rebind for misbound operatives" do
      delta = %Diff{misbound_operatives: [{{:ns, "r"}, "e_dead"}]}
      plan = Planner.plan(delta, Backoff.new())

      assert [
               %{primitive: :evict_executor, target: "e_dead"},
               %{primitive: :rebind_operative} = rebind
             ] =
               plan

      assert rebind.namespace == :ns
      assert rebind.target == "r"
    end

    test "caps ops per tick at max_ops" do
      delta = %Diff{unbound_operatives: for(i <- 1..30, do: {:ns, "r#{i}"})}
      assert length(Planner.plan(delta, Backoff.new(), max_ops: 5)) == 5
    end

    test "unbound operatives bind via rebind_operative (un-park), not a no-op" do
      delta = %Diff{unbound_operatives: [{:ns, "parked"}]}

      assert [%{primitive: :rebind_operative, namespace: :ns, target: "parked"}] =
               Planner.plan(delta, Backoff.new())
    end

    test "missing plugins emit start_plugin carrying the full %Plugin{}" do
      p = plugin("p1")
      delta = %Diff{missing_plugins: [{{:ns, "r"}, "p1", p}]}

      assert [%{primitive: :start_plugin, namespace: :ns, target: "r", args: args}] =
               Planner.plan(delta, Backoff.new())

      assert args.plugin_id == "p1"
      assert args.plugin == p
    end

    test "unhealthy plugins emit restart_plugin carrying the %Plugin{} and reason" do
      p = plugin("p1")
      delta = %Diff{unhealthy_plugins: [{{:ns, "r"}, "p1", p, :produce_stalled}]}

      assert [%{primitive: :restart_plugin, namespace: :ns, target: "r", args: args}] =
               Planner.plan(delta, Backoff.new())

      assert args.plugin_id == "p1"
      assert args.plugin == p
      assert args.reason == :produce_stalled
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

    test "on timeout, probes the wedged target's mailbox depth and current function" do
      prev_level = Logger.level()
      Logger.configure(level: :warning)
      on_exit(fn -> Logger.configure(level: prev_level) end)

      {:ok, pid} = GenServer.start_link(__MODULE__.WedgeServer, nil)

      # Wedge it: a blocking call leaves the server stuck inside handle_call.
      spawn(fn -> catch_exit(GenServer.call(pid, :block, :infinity)) end)
      Process.sleep(20)
      # Pile messages behind the blocked call so the probe sees a backlog.
      for _ <- 1..5, do: GenServer.cast(pid, :noop)

      handler_ids =
        :telemetry_test.attach_event_handlers(self(), [[:flezha, :convergence, :safe_call]])

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, :timeout} = SafeCall.bounded(pid, :ping, 50)
        end)

      assert log =~ "target_queue_len="
      assert log =~ "target_current_function="

      assert_receive {[:flezha, :convergence, :safe_call], _ref, %{count: 1},
                      %{result: :timeout, target_queue_len: qlen, target_current_function: cf}}

      assert qlen >= 5
      assert is_tuple(cf)

      handler_ids |> List.wrap() |> Enum.each(&:telemetry.detach/1)
      Process.exit(pid, :kill)
    end

    test "on timeout against a remote pid, reports :remote instead of crashing the caller" do
      # Regression: `Process.info/2` raises `ArgumentError` on a remote pid.
      # That raise is an :error, not an :exit, so the catch in bounded/3 never
      # covered it — it propagated out of Convergence.Loop and took the node down.
      prev_level = Logger.level()
      Logger.configure(level: :warning)
      on_exit(fn -> Logger.configure(level: prev_level) end)

      args = ~c"-loader inet -hosts 127.0.0.1 -setcookie \"#{:erlang.get_cookie()}\""

      {:ok, peer, peer_node} =
        :peer.start_link(%{host: ~c"127.0.0.1", name: :safecall_remote_probe, args: [args]})

      on_exit(fn ->
        if Process.alive?(peer), do: catch_exit(:peer.stop(peer))
      end)

      # A bare remote process that never answers, so the call times out and the
      # timeout branch probes a genuinely remote pid.
      remote_pid = Node.spawn(peer_node, :timer, :sleep, [:infinity])
      assert node(remote_pid) == peer_node

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          assert {:error, :timeout} = SafeCall.bounded(remote_pid, :ping, 50)
        end)

      assert log =~ "target_status: :remote"
    end
  end

  describe "Snapshot" do
    test "publish/current round-trips" do
      Snapshot.publish(%{
        last_tick_at: 42,
        stalled_operatives: ["x"],
        bound_operatives: 1,
        producing_operatives: 1
      })

      assert Snapshot.current().last_tick_at == 42
      assert Snapshot.current().stalled_operatives == ["x"]
      # reset for other suites
      Snapshot.publish(%{
        last_tick_at: nil,
        stalled_operatives: [],
        bound_operatives: 0,
        producing_operatives: 0
      })
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
    test "the heal surface is exactly the five reachable primitives" do
      assert Enum.sort(Heal.primitives()) ==
               Enum.sort([
                 :rebind_operative,
                 :start_plugin,
                 :stop_plugin,
                 :restart_plugin,
                 :evict_executor
               ])
    end

    test "evict_executor on an unknown id is an idempotent :ok" do
      op = %{primitive: :evict_executor, namespace: nil, target: "ghost_executor", args: %{}}
      assert Heal.execute(op) == :ok
    end

    test "a deleted/unknown primitive is no longer dispatchable" do
      # `start_operative`, `replay_state`, `compact_wal` were never planned and
      # did nothing — they are gone, not silent no-ops.
      for primitive <- [:start_operative, :replay_state, :compact_wal] do
        op = %{primitive: primitive, namespace: nil, target: :noop, args: %{}}
        assert Heal.execute(op) == :error
      end
    end

    test "a malformed restart_plugin op (no args) is a failed heal, not a crash" do
      op = %{primitive: :restart_plugin, namespace: nil, target: :noop}
      assert Heal.execute(op) == :error
    end
  end

  describe "Loop output-stall rebind injection" do
    alias Flezha.Convergence.{Backoff, Intent, Loop}

    test "stalled operative not in plan gets a rebind_operative op injected with reason: :output_stall" do
      ns = "stall_ns_#{System.unique_integer([:positive])}"
      operative_id = "operative_stalled"

      intent = %Intent{
        operatives: [%{namespace: ns, operative_id: operative_id, factory_id: nil, plugins: []}]
      }

      ready = MapSet.new([ns])
      stalled = %{operative_id => System.monotonic_time(:millisecond)}

      injected = Loop.inject_stall_rebinds_for_test([], stalled, intent, ready, Backoff.new())

      assert [%{primitive: :rebind_operative, target: ^operative_id, args: %{reason: :output_stall}}] =
               injected
    end

    test "stalled operative already in plan is not duplicated" do
      ns = "stall_ns2_#{System.unique_integer([:positive])}"
      operative_id = "operative_dup"

      intent = %Intent{
        operatives: [%{namespace: ns, operative_id: operative_id, factory_id: nil, plugins: []}]
      }

      ready = MapSet.new([ns])
      stalled = %{operative_id => System.monotonic_time(:millisecond)}
      existing_op = %{primitive: :rebind_operative, namespace: ns, target: operative_id, args: %{}}

      result =
        Loop.inject_stall_rebinds_for_test([existing_op], stalled, intent, ready, Backoff.new())

      assert length(result) == 1
      assert hd(result) == existing_op
    end

    test "stalled operative in backoff cooldown is filtered out" do
      ns = "stall_ns3_#{System.unique_integer([:positive])}"
      operative_id = "operative_cooldown"

      intent = %Intent{
        operatives: [%{namespace: ns, operative_id: operative_id, factory_id: nil, plugins: []}]
      }

      ready = MapSet.new([ns])
      stalled = %{operative_id => System.monotonic_time(:millisecond)}
      now = System.monotonic_time(:millisecond)
      backoff = Backoff.update(Backoff.new(), {:rebind_operative, operative_id}, :error, now)

      result = Loop.inject_stall_rebinds_for_test([], stalled, intent, ready, backoff)
      assert result == []
    end
  end

  describe "Loop.apply_misbound_streak/2 (B5)" do
    alias Flezha.Convergence.Loop

    test "a operative misbound for one tick is not yet migrated; two ticks releases it" do
      delta = %Diff{misbound_operatives: [{{:ns, "r"}, "e_dead"}]}

      {streaks, tick1} = Loop.apply_misbound_streak(%{}, delta)
      assert tick1.misbound_operatives == []

      {_streaks, tick2} = Loop.apply_misbound_streak(streaks, delta)
      assert tick2.misbound_operatives == [{{:ns, "r"}, "e_dead"}]
    end

    test "a operative seen bound again resets its streak" do
      delta = %Diff{misbound_operatives: [{{:ns, "r"}, "e_dead"}]}
      {streaks, _} = Loop.apply_misbound_streak(%{}, delta)

      # Bound again this tick → drops out → streak resets.
      {streaks, _} = Loop.apply_misbound_streak(streaks, %Diff{misbound_operatives: []})
      {_streaks, tick} = Loop.apply_misbound_streak(streaks, delta)
      assert tick.misbound_operatives == []
    end
  end

  describe "Loop.operative_health_map/3 (operator health surface)" do
    alias Flezha.Convergence.{Intent, Loop}

    defp intent_operative(ns, rid),
      do: %{namespace: ns, operative_id: rid, factory_id: nil, plugins: []}

    defp reality(ns, observed_operatives, active),
      do: %{operatives: observed_operatives, active_executors: %{ns => active}}

    test "a bound operative on a live executor is :healthy" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}

      reality =
        reality(ns, %{{ns, "r"} => %{bound_executor: "e1", pid: self()}}, MapSet.new(["e1"]))

      health = Loop.operative_health_map(intent.operatives, reality, %Loop{})
      assert health == %{{ns, "r"} => :healthy}
    end

    test "a operative bound to no executor is :unbound" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}
      reality = reality(ns, %{{ns, "r"} => %{bound_executor: nil}}, MapSet.new(["e1"]))

      assert Loop.operative_health_map(intent.operatives, reality, %Loop{}) == %{{ns, "r"} => :unbound}
    end

    test "a operative bound to a dead executor is :failed" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}
      reality = reality(ns, %{{ns, "r"} => %{bound_executor: "gone"}}, MapSet.new(["e1"]))

      assert Loop.operative_health_map(intent.operatives, reality, %Loop{}) == %{{ns, "r"} => :failed}
    end

    test "a bound-but-stalled operative is :degraded" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}

      reality =
        reality(ns, %{{ns, "r"} => %{bound_executor: "e1", pid: self()}}, MapSet.new(["e1"]))

      state = %Loop{stalled_operatives: %{"r" => System.monotonic_time(:millisecond)}}
      assert Loop.operative_health_map(intent.operatives, reality, state) == %{{ns, "r"} => :degraded}
    end

    test "a bound operative with a degraded plugin is :degraded" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}

      reality =
        reality(ns, %{{ns, "r"} => %{bound_executor: "e1", pid: self()}}, MapSet.new(["e1"]))

      assert Loop.operative_health_map(intent.operatives, reality, %Loop{plugin_degraded: ["r"]}) ==
               %{{ns, "r"} => :degraded}
    end

    test "an unobservable operative (failed read) is :unknown for the tick, not classified" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}
      reality = reality(ns, %{{ns, "r"} => %{bound_executor: :unknown}}, MapSet.new(["e1"]))

      assert Loop.operative_health_map(intent.operatives, reality, %Loop{}) == %{{ns, "r"} => :unknown}
    end

    test "a operative whose namespace live-executor read failed is :unknown, not :failed" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}
      reality = reality(ns, %{{ns, "r"} => %{bound_executor: "e1"}}, :unknown)

      assert Loop.operative_health_map(intent.operatives, reality, %Loop{}) == %{{ns, "r"} => :unknown}
    end
  end

  describe "OperativeManager.pick_executor/2 — deterministic HRW placement (C1p)" do
    alias Flezha.Service.OperativeManager

    test "empty candidate set yields nil" do
      assert OperativeManager.pick_executor([], "r") == nil
    end

    test "same (operative_id, set) always picks the same executor" do
      set = ["e1", "e2", "e3"]
      pick = OperativeManager.pick_executor(set, "btc")
      assert pick in set
      assert OperativeManager.pick_executor(Enum.shuffle(set), "btc") == pick
    end

    test "removing only the chosen executor moves only that operative (stable)" do
      set = ["e1", "e2", "e3", "e4"]
      operatives = for i <- 1..40, do: "operative_#{i}"
      before = Map.new(operatives, fn r -> {r, OperativeManager.pick_executor(set, r)} end)

      victim = "e2"
      after_set = set -- [victim]

      for r <- operatives do
        new_pick = OperativeManager.pick_executor(after_set, r)

        if before[r] == victim do
          assert new_pick in after_set
        else
          # operatives not on the removed executor keep their placement
          assert new_pick == before[r]
        end
      end
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end

  # A GenServer that wedges inside `handle_call(:block, ...)` so the SafeCall
  # probe has a live-but-stuck target to introspect.
  defmodule WedgeServer do
    @moduledoc false
    use GenServer

    @impl true
    def init(_), do: {:ok, nil}

    @impl true
    def handle_call(:block, _from, state) do
      receive do
        :never -> {:reply, :ok, state}
      end
    end

    def handle_call(_other, _from, state), do: {:reply, :ok, state}

    @impl true
    def handle_cast(_msg, state), do: {:noreply, state}
  end
end

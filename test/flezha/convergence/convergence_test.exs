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

    # fails when: a relay whose declared plugin set reads empty (cspec lag or
    # read failure) has its entire running plugin set torn down every tick,
    # causing a stop/re-subscribe storm
    test "I2: empty declared plugin set never marks running plugins as extra" do
      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "r", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e"])},
        relays: %{
          {:ns, "r"} => %{
            bound_executor: "e",
            pid: self(),
            running_plugins: ["ws_in", "kafka_out"]
          }
        },
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

    test "I3: resolves stalled instance ids to {ns, rid} with age, drops unknown ids" do
      intent = %Intent{
        relays: [%{namespace: :ns, relay_id: "live", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_executors: %{ns: MapSet.new(["e"])},
        relays: %{{:ns, "live"} => %{bound_executor: "e"}},
        tick_at: 1_000,
        # "live" is running → resolved with age 600; "ghost" has no live
        # relay → dropped (nothing to rebind).
        stalled_relays: %{"live" => 400, "ghost" => 200}
      }

      delta = Diff.compute(intent, reality)
      assert delta.stalled_relays == [{{:ns, "live"}, 600}]
    end
  end

  describe "Planner.plan/3" do
    test "orders eviction before rebind for misbound relays" do
      delta = %Diff{misbound_relays: [{{:ns, "r"}, "e_dead"}]}
      plan = Planner.plan(delta, Backoff.new())

      assert [
               %{primitive: :evict_executor, target: "e_dead"},
               %{primitive: :rebind_relay} = rebind
             ] =
               plan

      assert rebind.namespace == :ns
      assert rebind.target == "r"
    end

    test "caps ops per tick at max_ops" do
      delta = %Diff{unbound_relays: for(i <- 1..30, do: {:ns, "r#{i}"})}
      assert length(Planner.plan(delta, Backoff.new(), max_ops: 5)) == 5
    end

    test "stalled relays emit a rebind targeting the instance id" do
      delta = %Diff{stalled_relays: [{{:ns, "live"}, 600}]}

      assert [%{primitive: :rebind_relay, namespace: :ns, target: "live", args: args}] =
               Planner.plan(delta, Backoff.new())

      assert args.reason == :output_stall
      assert args.output_age_ms == 600
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
  end

  describe "Snapshot" do
    test "publish/current round-trips" do
      Snapshot.publish(%{
        last_tick_at: 42,
        stalled_relays: ["x"],
        bound_relays: 1,
        producing_relays: 1
      })

      assert Snapshot.current().last_tick_at == 42
      assert Snapshot.current().stalled_relays == ["x"]
      # reset for other suites
      Snapshot.publish(%{
        last_tick_at: nil,
        stalled_relays: [],
        bound_relays: 0,
        producing_relays: 0
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
    test "stubbed primitives are dispatchable no-ops" do
      # Real primitives (`rebind_relay`, `start_plugin`, `stop_plugin`,
      # `evict_executor`) are covered below / by the `executor disconnect
      # preserve` integration test; the rest remain observable no-ops.
      stubbed = [:start_relay, :stop_relay, :restart_plugin, :replay_state, :compact_wal]

      for primitive <- stubbed do
        op = %{primitive: primitive, namespace: nil, target: :noop, args: %{}}
        assert Heal.execute(op) == :ok
      end
    end

    test "evict_executor on an unknown id is an idempotent :ok" do
      op = %{primitive: :evict_executor, namespace: nil, target: "ghost_executor", args: %{}}
      assert Heal.execute(op) == :ok
    end

    test "restart_plugin for an absent producer is an idempotent :ok" do
      op = %{
        primitive: :restart_plugin,
        namespace: :ns,
        target: "r",
        args: %{plugin_id: "no-such-plugin-#{System.unique_integer([:positive])}"}
      }

      assert Heal.execute(op) == :ok
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

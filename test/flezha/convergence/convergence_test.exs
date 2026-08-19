defmodule Flezha.ConvergenceTest do
  @moduledoc """
  Unit coverage for the declarative-reconciler seams introduced by
  `unified-convergence-loop` session 2: the pure `Backoff`, `Diff`,
  `Planner` functions, the `SafeCall` wrapper, and the `Snapshot`
  publish point. Live healing + the Loop's runner takeover are
  covered by the `runner disconnect preserve` integration test.
  """

  use ExUnit.Case, async: false
  use Mimic

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
    test "classifies unbound and misbound operatives against active runners" do
      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "unbound", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "ok", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "stale", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        operatives: %{
          {:ns, "unbound"} => %{bound_runner: nil},
          {:ns, "ok"} => %{bound_runner: "e_live"},
          {:ns, "stale"} => %{bound_runner: "e_dead"}
        },
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)

      assert delta.unbound_operatives == [{:ns, "unbound"}]
      assert delta.misbound_operatives == [{{:ns, "stale"}, "e_dead"}]
      refute Diff.empty?(delta)
    end

    test "empty delta when every operative is bound to a live runner" do
      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "ok", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        operatives: %{{:ns, "ok"} => %{bound_runner: "e_live"}},
        stalled_operatives: MapSet.new()
      }

      assert Diff.empty?(Diff.compute(intent, reality))
    end

    test "a declared operative with no observation is the reconcile's to start, not a heal's" do
      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "declared_only", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        operatives: %{},
        stalled_operatives: MapSet.new()
      }

      assert Diff.empty?(Diff.compute(intent, reality))
    end

    test "re-places instead of rebinding when the factory is only served elsewhere" do
      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "movable", factory_id: "f", plugins: []},
          %{namespace: :ns, operative_id: "stale_movable", factory_id: "f", plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_other"])},
        operatives: %{
          {:ns, "movable"} => %{bound_runner: nil, factory_id: "f"},
          {:ns, "stale_movable"} => %{bound_runner: "e_dead", factory_id: "f"}
        },
        placement: %{{:ns, "f"} => %{local: [], remote_nodes: [:peer@host]}},
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)

      assert delta.unbound_operatives == []
      assert delta.misbound_operatives == []

      assert delta.replace_operatives == [
               {{:ns, "movable"}, nil},
               {{:ns, "stale_movable"}, "e_dead"}
             ]
    end

    test "an unknown local placement read falls back to an ordinary rebind" do
      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: "f", plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new()},
        operatives: %{{:ns, "r"} => %{bound_runner: nil, factory_id: "f"}},
        placement: %{{:ns, "f"} => %{local: :unknown, remote_nodes: [:peer@host]}},
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.unbound_operatives == [{:ns, "r"}]
      assert delta.replace_operatives == []
    end

    test "no eligible fleet anywhere stays an ordinary rebind — degraded, no re-place churn" do
      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: "f", plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new()},
        operatives: %{{:ns, "r"} => %{bound_runner: nil, factory_id: "f"}},
        placement: %{{:ns, "f"} => %{local: [], remote_nodes: []}},
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.unbound_operatives == [{:ns, "r"}]
      assert delta.replace_operatives == []
    end

    test "I2: classifies missing and extra plugins on a live operative" do
      p = plugin("declared")

      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"declared", p}]}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{bound_runner: "e", pid: self(), running_plugins: ["stray"]}
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
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{{:ns, "r"} => %{bound_runner: "e", pid: self(), running_plugins: ["p1"]}},
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
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{
            bound_runner: "e",
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
        operatives: [
          %{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"ws_in", p}]}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{
            bound_runner: "e",
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
        active_runners: %{ns: MapSet.new(["e"])},
        operatives: %{
          {:ns, "r"} => %{
            bound_runner: "e",
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
        operatives: [
          %{namespace: :ns, operative_id: "r", factory_id: nil, plugins: [{"declared", p}]}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new()},
        operatives: %{{:ns, "r"} => %{bound_runner: nil, pid: nil, running_plugins: []}},
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.missing_plugins == []
    end

    test "skips misbound classification when the namespace's live-runner read is :unknown" do
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
        active_runners: %{ns: :unknown},
        operatives: %{
          {:ns, "a"} => %{bound_runner: "e1"},
          {:ns, "b"} => %{bound_runner: "e2"}
        },
        stalled_operatives: MapSet.new()
      }

      delta = Diff.compute(intent, reality)
      assert delta.misbound_operatives == []
      assert delta.unbound_operatives == []
    end

    test "skips a operative whose own state read failed (bound_runner :unknown)" do
      intent = %Intent{
        operatives: [%{namespace: :ns, operative_id: "r", factory_id: nil, plugins: []}]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        operatives: %{{:ns, "r"} => %{bound_runner: :unknown, pid: nil, running_plugins: []}},
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
               %{primitive: :evict_runner, target: "e_dead"},
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

    test "a re-place evicts its dead runner first and keys its own backoff" do
      delta = %Diff{replace_operatives: [{{:ns, "r"}, "e_dead"}]}

      assert [
               %{primitive: :evict_runner, target: "e_dead"},
               %{primitive: :replace_operative, namespace: :ns, target: "r"} = replace
             ] = Planner.plan(delta, Backoff.new())

      assert Planner.key(replace) == {:replace_operative, "r"}
    end

    test "an unbound re-place plans no eviction" do
      delta = %Diff{replace_operatives: [{{:ns, "r"}, nil}]}

      assert [%{primitive: :replace_operative, namespace: :ns, target: "r"}] =
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

  describe "data-plane verdict → heal op (D3)" do
    # One healthy operative bound to the runner every verdict below names, so
    # the resolution is exercised against the same probed reality the other
    # invariants are classified from.
    defp wedge_reality() do
      %{
        active_runners: %{ns: MapSet.new(["e_live", "e_second"])},
        operatives: %{
          {:ns, "rc_a"} => %{bound_runner: "e_live"},
          {:ns, "rc_b"} => %{bound_runner: "e_live"},
          {:ns, "rc_other"} => %{bound_runner: "e_second"}
        }
      }
    end

    defp wedge_intent() do
      %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "rc_a", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "rc_b", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "rc_other", factory_id: nil, plugins: []}
        ]
      }
    end

    defp plan_for(verdict) do
      wedge_intent()
      |> Diff.compute(wedge_reality(), %{verdict: verdict})
      |> Planner.plan(Backoff.new())
    end

    test "a wedged runner pool is evicted" do
      assert [%{primitive: :evict_runner, target: "e_live", args: args}] =
               plan_for({:runner_pool_wedged, %{runner_id: "e_live", dispatches: 7}})

      assert args == %{reason: :data_plane_wedge, layer: :runner_pool_wedged}
    end

    test "a runner that stopped consuming is evicted, resolved through its data transport" do
      verdict =
        {:runner_not_consuming, %{transport: {:uds_data, "e_live", :primary}, stream_id: 1}}

      assert [%{primitive: :evict_runner, target: "e_live"}] = plan_for(verdict)
    end

    test "a wedged channel resyncs every operative on that runner, in place" do
      verdict = {:channel_not_sending, %{runner_id: "e_live", channel: "c1"}}

      assert [
               %{primitive: :resync_runner, namespace: :ns, target: "rc_a"},
               %{primitive: :resync_runner, namespace: :ns, target: "rc_b"}
             ] = plan_for(verdict)
    end

    test "a wedged operative is rebound, named by the verdict" do
      assert [%{primitive: :rebind_operative, namespace: :ns, target: "rc_other", args: args}] =
               plan_for({:operative_not_forwarding, %{operative_id: "rc_other"}})

      assert args.reason == :data_plane_wedge
    end

    test "a fabric verdict plans nothing — no BEAM-level heal reaches it" do
      for layer <- [:driver_loop_frozen, :nif_queue_backing_up] do
        assert plan_for({layer, %{transport: {:uds_data, "e_live", :primary}}}) == []
      end
    end

    test "a verdict naming nothing the probe observed plans nothing" do
      assert plan_for({:runner_pool_wedged, %{runner_id: "e_never_seen"}}) == []
      assert plan_for({:operative_not_forwarding, %{operative_id: "rc_never_seen"}}) == []
      assert plan_for({:channel_not_sending, %{runner_id: "e_never_seen", channel: "c"}}) == []
    end

    test "an :ok verdict clears the recorded wedge" do
      delta = Diff.compute(wedge_intent(), wedge_reality(), %{verdict: :ok})
      assert delta.data_plane_wedge == nil
      assert delta.data_plane_layer == nil
      assert Diff.empty?(delta)
    end

    test "wedge ops ride the standard per-target backoff" do
      verdict = {:runner_pool_wedged, %{runner_id: "e_live"}}
      now = System.monotonic_time(:millisecond)
      backoff = Backoff.update(Backoff.new(), {:evict_runner, "e_live"}, :error, now)

      assert wedge_intent()
             |> Diff.compute(wedge_reality(), %{verdict: verdict})
             |> Planner.plan(backoff) == []
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
      on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))

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
      # That raise is an :error, not an :exit, so the catch in workflow/3 never
      # covered it — it propagated out of Convergence.Loop and took the node down.
      on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))

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
    test "the heal surface is exactly the seven reachable primitives" do
      assert Enum.sort(Heal.primitives()) ==
               Enum.sort([
                 :rebind_operative,
                 :replace_operative,
                 :resync_runner,
                 :start_plugin,
                 :stop_plugin,
                 :restart_plugin,
                 :evict_runner
               ])
    end

    test "replace_operative against an unknown operative is a failed heal, not a crash" do
      op = %{
        primitive: :replace_operative,
        namespace: "ghost_ns",
        target: "ghost_operative",
        args: %{}
      }

      assert Heal.execute(op) == :error
    end

    test "resync_runner against an unknown operative is a failed heal, not a crash" do
      op = %{
        primitive: :resync_runner,
        namespace: "ghost_ns",
        target: "ghost_operative",
        args: %{reason: :output_stall}
      }

      assert Heal.execute(op) == :error
    end

    test "evict_runner on an unknown id is an idempotent :ok" do
      op = %{primitive: :evict_runner, namespace: nil, target: "ghost_runner", args: %{}}
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

  describe "output-stall ladder (D1)" do
    @stall_ns :stall_ns

    defp stalled_diff(operative_id, extra \\ []) do
      intent = %Intent{
        operatives: [
          %{namespace: @stall_ns, operative_id: operative_id, factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{@stall_ns => MapSet.new(["e_live"])},
        operatives: %{{@stall_ns, operative_id} => Enum.into(extra, %{bound_runner: "e_live"})}
      }

      Diff.compute(intent, reality, %{stalled: [operative_id]})
    end

    test "a flagged instance resolves to a stalled divergence carrying its namespace" do
      assert %Diff{stalled_operatives: [{@stall_ns, "op_a"}]} = stalled_diff("op_a")
    end

    test "a flag for an operative this tick is not healing resolves to nothing" do
      # Not declared, or declared in a namespace still mid-recovery — the
      # caller passes only the recovery-ready operatives.
      delta = Diff.compute(%Intent{operatives: []}, %{}, %{stalled: ["ghost"]})
      assert delta.stalled_operatives == []
      assert Diff.empty?(delta)
    end

    test "the first eligible attempt resyncs the existing binding, not a migration" do
      assert [%{primitive: :resync_runner, target: "op_a", args: %{reason: :output_stall}}] =
               Planner.plan(stalled_diff("op_a"), Backoff.new())
    end

    test "a stall that survived the resync escalates to rebind_operative" do
      now = System.monotonic_time(:millisecond)

      # One ladder attempt recorded and its cooldown already elapsed: the same
      # stall is still flagged, so the next rung is the migration.
      backoff =
        Backoff.update(Backoff.new(), Planner.ladder_key("op_a"), :error, now - 10_000)

      assert [%{primitive: :rebind_operative, target: "op_a", args: %{reason: :output_stall}}] =
               Planner.plan(stalled_diff("op_a"), backoff)
    end

    test "both rungs share one cooldown key, so escalating does not restart the throttle" do
      for primitive <- [:resync_runner, :rebind_operative] do
        assert Planner.key(%{primitive: primitive, target: "r", args: %{reason: :output_stall}}) ==
                 Planner.ladder_key("r")
      end
    end

    test "a stalled operative already covered by the plan is not laddered too" do
      # Also unbound, so the structural bind op already names it.
      delta = stalled_diff("op_dup", bound_runner: nil)

      assert [%{primitive: :rebind_operative, target: "op_dup", args: args}] =
               Planner.plan(delta, Backoff.new())

      refute Map.has_key?(args, :reason)
    end

    test "a stalled operative in backoff cooldown is filtered out" do
      now = System.monotonic_time(:millisecond)
      backoff = Backoff.update(Backoff.new(), Planner.ladder_key("op_cool"), :error, now)

      assert Planner.plan(stalled_diff("op_cool"), backoff) == []
    end

    test "the ladder is not starved by a plan that filled the per-tick budget" do
      delta = %Diff{stalled_diff("op_a") | stalled_operatives: [{@stall_ns, "op_a"}]}

      assert [%{primitive: :resync_runner, target: "op_a"}] =
               Planner.plan(delta, Backoff.new(), max_ops: 0)
    end

    test "a ladder rung is recognisable as one; a structural op is not" do
      assert Planner.ladder?(%{
               primitive: :resync_runner,
               target: "r",
               args: %{reason: :output_stall}
             })

      refute Planner.ladder?(%{primitive: :rebind_operative, target: "r", args: %{}})
    end
  end

  describe "reconcile-round cooperation (D7)" do
    alias Flezha.Convergence.Loop

    test "a round that started orphans triggers one immediate tick" do
      assert :ok =
               Loop.handle_telemetry(
                 [:flezha, :boot, :reconcile_round],
                 %{started: 2, duplicates: 0, elapsed_ms: 4},
                 %{first_round: false},
                 self()
               )

      assert_receive :reconcile_started, 500
    end

    test "a quiet round triggers nothing — the Loop's cadence is unchanged" do
      assert :ok =
               Loop.handle_telemetry(
                 [:flezha, :boot, :reconcile_round],
                 %{started: 0, duplicates: 0, elapsed_ms: 1},
                 %{first_round: false},
                 self()
               )

      refute_receive :reconcile_started, 50
    end

    test "the post-round plan adopts what the round started rather than fighting it" do
      # The reconcile started two operatives; by the time the completion event
      # reaches the Loop they probe as bound to a live runner, so the Diff sees
      # no divergence and the plan names neither of them.
      intent = %Intent{
        operatives: [
          %{namespace: :ns, operative_id: "adopted_a", factory_id: nil, plugins: []},
          %{namespace: :ns, operative_id: "adopted_b", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        operatives: %{
          {:ns, "adopted_a"} => %{bound_runner: "e_live"},
          {:ns, "adopted_b"} => %{bound_runner: "e_live"}
        }
      }

      assert Planner.plan(Diff.compute(intent, reality), Backoff.new()) == []
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
      do: %{operatives: observed_operatives, active_runners: %{ns => active}}

    test "a bound operative on a live runner is :healthy" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}

      reality =
        reality(ns, %{{ns, "r"} => %{bound_runner: "e1", pid: self()}}, MapSet.new(["e1"]))

      health = Loop.operative_health_map(intent.operatives, reality, %Loop{})
      assert health == %{{ns, "r"} => :healthy}
    end

    test "a operative bound to no runner is :unbound" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}
      reality = reality(ns, %{{ns, "r"} => %{bound_runner: nil}}, MapSet.new(["e1"]))

      assert Loop.operative_health_map(intent.operatives, reality, %Loop{}) == %{
               {ns, "r"} => :unbound
             }
    end

    test "a operative bound to a dead runner is :failed" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}
      reality = reality(ns, %{{ns, "r"} => %{bound_runner: "gone"}}, MapSet.new(["e1"]))

      assert Loop.operative_health_map(intent.operatives, reality, %Loop{}) == %{
               {ns, "r"} => :failed
             }
    end

    test "a bound-but-stalled operative is :degraded" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}

      reality =
        reality(ns, %{{ns, "r"} => %{bound_runner: "e1", pid: self()}}, MapSet.new(["e1"]))

      state = %Loop{stalled_operatives: %{"r" => System.monotonic_time(:millisecond)}}

      assert Loop.operative_health_map(intent.operatives, reality, state) == %{
               {ns, "r"} => :degraded
             }
    end

    test "a bound operative with a degraded plugin is :degraded" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}

      reality =
        reality(ns, %{{ns, "r"} => %{bound_runner: "e1", pid: self()}}, MapSet.new(["e1"]))

      assert Loop.operative_health_map(intent.operatives, reality, %Loop{plugin_degraded: ["r"]}) ==
               %{{ns, "r"} => :degraded}
    end

    test "an unobservable operative (failed read) is :unknown for the tick, not classified" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}
      reality = reality(ns, %{{ns, "r"} => %{bound_runner: :unknown}}, MapSet.new(["e1"]))

      assert Loop.operative_health_map(intent.operatives, reality, %Loop{}) == %{
               {ns, "r"} => :unknown
             }
    end

    test "a operative whose namespace live-runner read failed is :unknown, not :failed" do
      ns = "ns"
      intent = %Intent{operatives: [intent_operative(ns, "r")]}
      reality = reality(ns, %{{ns, "r"} => %{bound_runner: "e1"}}, :unknown)

      assert Loop.operative_health_map(intent.operatives, reality, %Loop{}) == %{
               {ns, "r"} => :unknown
             }
    end
  end

  describe "OperativeManager.pick_runner/2 — deterministic HRW placement (C1p)" do
    alias Flezha.Service.OperativeManager

    test "empty candidate set yields nil" do
      assert OperativeManager.pick_runner([], "r") == nil
    end

    test "same (operative_id, set) always picks the same runner" do
      set = ["e1", "e2", "e3"]
      pick = OperativeManager.pick_runner(set, "btc")
      assert pick in set
      assert OperativeManager.pick_runner(Enum.shuffle(set), "btc") == pick
    end

    test "removing only the chosen runner moves only that operative (stable)" do
      set = ["e1", "e2", "e3", "e4"]
      operatives = for i <- 1..40, do: "operative_#{i}"
      before = Map.new(operatives, fn r -> {r, OperativeManager.pick_runner(set, r)} end)

      victim = "e2"
      after_set = set -- [victim]

      for r <- operatives do
        new_pick = OperativeManager.pick_runner(after_set, r)

        if before[r] == victim do
          assert new_pick in after_set
        else
          # operatives not on the removed runner keep their placement
          assert new_pick == before[r]
        end
      end
    end
  end

  describe "Loop.intent_mismatch/2 — durable evidence vs live intent" do
    alias Flezha.Convergence.{Intent, Loop}
    alias Flezha.Service.HubManager

    setup do
      root =
        Path.join(
          System.tmp_dir!(),
          "flezha_intent_mismatch_#{System.unique_integer([:positive])}"
        )

      File.mkdir_p!(root)
      prev = Application.get_env(:flezha, :state_root)
      Application.put_env(:flezha, :state_root, root)

      # Other suites leak hubs into the global registry; pin the durable-
      # registry evidence to empty so only the on-disk dirs drive these tests.
      stub(HubManager, :durable_operative_ids, fn -> MapSet.new() end)

      on_exit(fn ->
        File.rm_rf!(root)

        if prev,
          do: Application.put_env(:flezha, :state_root, prev),
          else: Application.delete_env(:flezha, :state_root)
      end)

      {:ok, root: root}
    end

    defp seed_entity(root, operative, procedure, key) do
      dir = Path.join([root, "_system", operative, procedure])
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "#{key}.wal"), <<1, 2, 3>>)
    end

    defp intent_for(ids) do
      %Intent{
        operatives:
          Enum.map(ids, fn id ->
            %{namespace: :ns, operative_id: id, factory_id: nil, plugins: []}
          end)
      }
    end

    test "fresh install with no durable evidence → empty mismatch" do
      assert {[], _cache} = Loop.intent_mismatch(intent_for([]), nil)
    end

    test "wiped registry with on-disk evidence → mismatch names the operatives", %{root: root} do
      seed_entity(root, "btc_kraken", "op", "k1")
      seed_entity(root, "eth_kraken", "op", "k1")

      assert {["btc_kraken", "eth_kraken"], cache} = Loop.intent_mismatch(intent_for([]), nil)

      # Live intent covering one operative clears exactly that entry.
      assert {["eth_kraken"], _} = Loop.intent_mismatch(intent_for(["btc_kraken"]), cache)
    end

    test "durable-registry rows without live intent also count as evidence" do
      stub(HubManager, :durable_operative_ids, fn -> MapSet.new(["ghost_operative"]) end)

      assert {["ghost_operative"], _} = Loop.intent_mismatch(intent_for([]), nil)
    end

    test "flz state delete clears the entry", %{root: root} do
      seed_entity(root, "btc_kraken", "op", "k1")
      assert {["btc_kraken"], cache} = Loop.intent_mismatch(intent_for([]), nil)

      # The CLI `flz state delete btc_kraken/op/k1` path.
      {:ok, _} = Flezha.State.Supervisor.delete_entity({"btc_kraken", "op", "k1"})

      assert {[], _} = Loop.intent_mismatch(intent_for([]), cache)
    end
  end

  describe "Loop.progress_markers/0 — the tier cursors reach the classifier" do
    alias Flezha.Convergence.{Loop, StallClassifier}
    alias Flezha.Operative.ClassRegistry
    alias Flezha.State.{FsyncBatcher, InputLog, Supervisor, TestHelpers, WalWriter, Wire}

    setup do
      base = TestHelpers.tmp_app_data_dir()
      ClassRegistry.ensure_table()

      for child <- [
            {Elixir.Registry, keys: :unique, name: Supervisor.registry_name()},
            Supervisor,
            FsyncBatcher,
            Flezha.State.Watchdog
          ] do
        case start_supervised(child) do
          {:ok, _} -> :ok
          {:error, {:already_started, _}} -> :ok
        end
      end

      on_exit(fn -> File.rm_rf(base) end)
      :ok
    end

    defp effect(outputs, offset) do
      Wire.encode_effect_body(%{
        changes: [<<1>>],
        outputs: outputs,
        consumed_offset: offset,
        dlq: nil
      })
    end

    defp effect_hdr(), do: <<1, 0, 0, 0, 1, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0>>

    test "a tier-on Entity's markers carry its OWN consumed offset and the log tail" do
      rc = "tiered-#{System.unique_integer([:positive])}"
      seg = {rc, "op", "k1"}
      ClassRegistry.put_overwatch(rc, self())

      {:ok, 0} = InputLog.append(rc, "i0", %{})
      {:ok, 1} = InputLog.append(rc, "i1", %{})

      {:ok, pid} = Supervisor.find_or_start(seg)
      :ok = WalWriter.append(pid, 0, effect_hdr(), effect([], 0))
      :ok = WalWriter.fsync(pid)

      marker = Enum.find(Loop.progress_markers(), &(&1.operative_id == rc))

      assert marker.entity == {"op", "k1"}
      assert marker.input_log_tail == 1
      assert marker.consumed_offset == 0, "the Entity's own offset, not the class-wide min"

      # The record was dispatched (its consumed offset landed), so the flush cursor
      # advanced past it — but it bore no outputs, so out_pending_seq did NOT move.
      # That asymmetry is precisely what keeps `outbox_flat?` from firing on a
      # Entity that persists state and emits nothing.
      assert marker.out_pending_seq == -1
      assert marker.outbox_flushed_seq == 0

      :ok = WalWriter.close(pid)
    end

    test "a tier-off Entity gets no cursor fields and no input log is created" do
      rc = "plain-#{System.unique_integer([:positive])}"
      seg = {rc, "op", "k1"}

      {:ok, pid} = Supervisor.find_or_start(seg)
      :ok = WalWriter.append(pid, 0, <<1, 0, 0, 0, 1, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0>>, "change")
      :ok = WalWriter.fsync(pid)

      marker = Enum.find(Loop.progress_markers(), &(&1.operative_id == rc))

      refute Map.has_key?(marker, :input_log_tail)
      refute Map.has_key?(marker, :consumed_offset)

      assert Supervisor.lookup({rc, :input_log}) == :error,
             "polling markers must never conjure an input log for a tier-off operative"

      :ok = WalWriter.close(pid)
    end

    test "an Entity that stops consuming while the log grows classifies STUCK, naming it" do
      rc = "stalling-#{System.unique_integer([:positive])}"
      seg = {rc, "op", "poisoned"}
      ClassRegistry.put_overwatch(rc, self())

      {:ok, 0} = InputLog.append(rc, "i0", %{})
      {:ok, pid} = Supervisor.find_or_start(seg)
      :ok = WalWriter.append(pid, 0, effect_hdr(), effect([], 0))
      :ok = WalWriter.fsync(pid)

      baseline = Enum.filter(Loop.progress_markers(), &(&1.operative_id == rc))

      # The source keeps feeding, but this Entity never consumes another offset —
      # a poison payload mid-retry, or a procedure wedged on it.
      {:ok, 1} = InputLog.append(rc, "i1", %{})
      {:ok, 2} = InputLog.append(rc, "i2", %{})

      current = Enum.filter(Loop.progress_markers(), &(&1.operative_id == rc))

      assert %{^rc => {:stuck, :watermark_frozen, %{entity: {"op", "poisoned"}}}} =
               StallClassifier.classify_producing(current, baseline, %{
                 now: System.monotonic_time(:millisecond),
                 grace_ms: 60_000
               })

      :ok = WalWriter.close(pid)
    end
  end

  describe "Loop.rotate_progress_history/4 — grace-window cursor baselines" do
    alias Flezha.Convergence.Loop

    test "baseline is nil until a snapshot has aged one grace window, then rotates" do
      {baseline, h} = Loop.rotate_progress_history([], [:a], 0, 100)
      assert baseline == nil

      {baseline, h} = Loop.rotate_progress_history(h, [:b], 50, 100)
      assert baseline == nil

      {baseline, h} = Loop.rotate_progress_history(h, [:c], 100, 100)
      assert baseline == [:a]

      # Older-than-baseline entries are pruned; the newest aged one wins.
      {baseline, h} = Loop.rotate_progress_history(h, [:d], 160, 100)
      assert baseline == [:b]
      refute Enum.any?(h, fn {_at, snap} -> snap == [:a] end)
    end
  end

  describe "Loop stuck / intent-mismatch transition telemetry" do
    alias Flezha.Convergence.Loop

    setup do
      parent = self()
      ref = make_ref()
      handler_id = {__MODULE__, ref}

      :telemetry.attach_many(
        handler_id,
        [
          [:flezha, :convergence, :operative_stuck],
          [:flezha, :convergence, :intent_mismatch]
        ],
        fn event, measurements, metadata, _ ->
          send(parent, {ref, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)
      {:ok, ref: ref}
    end

    test "freshly-stuck operatives emit operative_stuck once, not on steady state", %{ref: ref} do
      stuck = %{"btc" => %{detail: :wal_frozen, entity: "op/k1"}}

      Loop.note_stuck_transitions(%{}, stuck)

      assert_receive {^ref, [:flezha, :convergence, :operative_stuck], %{count: 1},
                      %{operative_id: "btc", detail: :wal_frozen, entity: "op/k1"}}

      Loop.note_stuck_transitions(stuck, stuck)
      refute_receive {^ref, [:flezha, :convergence, :operative_stuck], _, _}, 50
    end

    test "intent-mismatch transitions emit with the named operatives", %{ref: ref} do
      Loop.note_intent_mismatch_transition([], ["btc", "eth"])

      assert_receive {^ref, [:flezha, :convergence, :intent_mismatch], %{count: 2},
                      %{operatives: ["btc", "eth"]}}

      # Unchanged and cleared transitions do not re-emit.
      Loop.note_intent_mismatch_transition(["btc", "eth"], ["btc", "eth"])
      Loop.note_intent_mismatch_transition(["btc", "eth"], [])
      refute_receive {^ref, [:flezha, :convergence, :intent_mismatch], _, _}, 50
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

defmodule Flezha.Convergence.HealTest do
  @moduledoc """
  Healing: the data-plane verdicts that become heal operations, the
  `SafeCall` wrapper that bounds one, the `Snapshot` publish point, `Heal`
  itself, and the output-stall and reconcile-round ladders above them.
  """

  # async: false: writes Application env; Mimic stubs; a shared on-disk root.

  use ExUnit.Case, async: false

  import FlezhaTest.Helper.ConvergenceFixtures, only: [diff: 2, diff: 3]
  use Mimic

  alias Flezha.Config.Backoff
  alias Flezha.Convergence.{Diff, Facts, Heal, Intent, Planner, Snapshot}
  alias Flezha.Operator.Overwatch
  alias Flezha.Service.{HubManager, OperatorManager}
  alias Flezha.SDK.SafeCall
  alias FlezhaTest.Helper.NodeTestHelper

  describe "data-plane verdict → heal op (D3)" do
    # One healthy operator bound to the runner every verdict below names, so
    # the resolution is exercised against the same probed reality the other
    # invariants are classified from.
    defp wedge_reality() do
      %{
        active_runners: %{ns: MapSet.new(["e_live", "e_second"])},
        operators: %{
          {:ns, "rc_a"} => %{bound_runner: "e_live"},
          {:ns, "rc_b"} => %{bound_runner: "e_live"},
          {:ns, "rc_other"} => %{bound_runner: "e_second"}
        }
      }
    end

    defp wedge_intent() do
      %Intent{
        operators: [
          %{namespace: :ns, operator_id: "rc_a", factory_id: nil, plugins: []},
          %{namespace: :ns, operator_id: "rc_b", factory_id: nil, plugins: []},
          %{namespace: :ns, operator_id: "rc_other", factory_id: nil, plugins: []}
        ]
      }
    end

    defp plan_for(verdict) do
      wedge_intent()
      |> diff(wedge_reality(), %{stall_verdict: verdict})
      |> Planner.plan(Backoff.new(:heal))
    end

    test "a wedged runner pool is restarted terminally" do
      assert [%{primitive: :restart_runner, target: "e_live", args: args}] =
               plan_for({:runner_pool_wedged, %{runner_id: "e_live", dispatches: 7}})

      assert args == %{reason: :data_plane_wedge, layer: :runner_pool_wedged}
    end

    test "a runner that stopped consuming is restarted, resolved through its data transport" do
      verdict =
        {:runner_not_consuming, %{transport: {:uds_data, "e_live", :primary}, stream_id: 1}}

      assert [%{primitive: :restart_runner, target: "e_live"}] = plan_for(verdict)
    end

    test "a wedged channel resyncs every operator on that runner, in place" do
      verdict = {:channel_not_sending, %{runner_id: "e_live", channel: "c1"}}

      assert [
               %{primitive: :resync_runner, namespace: :ns, target: "rc_a"},
               %{primitive: :resync_runner, namespace: :ns, target: "rc_b"}
             ] = plan_for(verdict)
    end

    test "a wedged operator is rebound, named by the verdict" do
      assert [%{primitive: :rebind_operator, namespace: :ns, target: "rc_other", args: args}] =
               plan_for({:operator_not_forwarding, %{operator_id: "rc_other"}})

      assert args.reason == :data_plane_wedge
    end

    test "a fabric verdict plans nothing — no BEAM-level heal reaches it" do
      for layer <- [:driver_loop_frozen, :nif_queue_backing_up] do
        assert plan_for({layer, %{transport: {:uds_data, "e_live", :primary}}}) == []
      end
    end

    test "a verdict naming nothing the probe observed plans nothing" do
      assert plan_for({:runner_pool_wedged, %{runner_id: "e_never_seen"}}) == []
      assert plan_for({:operator_not_forwarding, %{operator_id: "rc_never_seen"}}) == []
      assert plan_for({:channel_not_sending, %{runner_id: "e_never_seen", channel: "c"}}) == []
    end

    test "an :ok verdict clears the recorded wedge" do
      delta = diff(wedge_intent(), wedge_reality(), %{stall_verdict: :ok})
      assert delta.data_plane_wedge == nil
      assert delta.data_plane_layer == nil
      assert Diff.empty?(delta)
    end

    test "wedge ops ride the standard per-target backoff" do
      verdict = {:channel_not_sending, %{runner_id: "e_live", channel: "c1"}}
      now = System.monotonic_time(:millisecond)

      backoff =
        Backoff.update(
          Backoff.new(:heal),
          Planner.ladder_key(:data_plane_wedge, "rc_a"),
          :error,
          now
        )
        |> Backoff.update(Planner.ladder_key(:data_plane_wedge, "rc_b"), :error, now)

      assert wedge_intent()
             |> diff(wedge_reality(), %{stall_verdict: verdict})
             |> Planner.plan(backoff) == []
    end

    # The terminal order goes to the session the verdict named: eviction ends
    # that session, the same process reconnects under a fresh id — a target no
    # ladder keyed on the session could reach again — and is placeable once
    # more. Eviction is the rung above, for a process that did not exit.
    test "a proved-wedged runner is restarted terminally first, and evicted only if it survives" do
      verdict = {:runner_pool_wedged, %{runner_id: "e_live", dispatches: 7}}
      now = System.monotonic_time(:millisecond)

      assert [%{primitive: :restart_runner, namespace: :ns, target: "e_live", args: args}] =
               plan_for(verdict)

      assert args == %{reason: :data_plane_wedge, layer: :runner_pool_wedged}
      assert Planner.proved_wedged?(%{args: args}), "the restart rung is gated on proved evidence"

      # The rung is read off the wedge ladder's own cooldown attempt count, so
      # the tick that applied the restart is what reaches the eviction.
      backoff =
        Backoff.update(
          Backoff.new(:heal),
          Planner.ladder_key(:data_plane_wedge, "e_live"),
          :error,
          now - 60_000
        )

      assert [%{primitive: :evict_runner, namespace: :ns, target: "e_live", args: ^args}] =
               wedge_intent()
               |> diff(wedge_reality(), %{stall_verdict: verdict})
               |> Planner.plan(backoff)
    end

    test "a runner with no wedge verdict is never restarted" do
      for verdict <- [:ok, {:operator_not_forwarding, %{operator_id: "rc_other"}}] do
        refute Enum.any?(plan_for(verdict), &(&1.primitive == :restart_runner))
      end
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
      on_exit(NodeTestHelper.override_log_level(:warning))

      {:ok, pid} = GenServer.start_link(__MODULE__.WedgeServer, nil)

      # Wedge it: a blocking call leaves the server stuck inside handle_call.
      spawn(fn -> catch_exit(GenServer.call(pid, :block, :infinity)) end)

      assert NodeTestHelper.wait_until(
               fn ->
                 Process.info(pid, :current_function) ==
                   {:current_function, {__MODULE__.WedgeServer, :handle_call, 3}}
               end,
               1_000
             )

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
      on_exit(NodeTestHelper.override_log_level(:warning))

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
      Snapshot.publish(%{last_tick_at: 42, stuck_operators: %{"x" => :outbox_flat}})

      assert Snapshot.current().last_tick_at == 42
      assert Snapshot.current().stuck_operators == %{"x" => :outbox_flat}
      # reset for other suites
      Snapshot.publish(%{last_tick_at: nil, stuck_operators: %{}})
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
    test "the heal surface is exactly the nine reachable primitives" do
      assert Enum.sort(Heal.primitives()) ==
               Enum.sort([
                 :rebind_operator,
                 :resync_runner,
                 :start_plugin,
                 :stop_plugin,
                 :restart_plugin,
                 :evict_runner,
                 :restart_runner,
                 :reset_control_channel,
                 :publish_binding
               ])
    end

    test "resync_runner against an unknown operator is a failed heal, not a crash" do
      op = %{
        primitive: :resync_runner,
        namespace: "ghost_ns",
        target: "ghost_operator",
        args: %{reason: :output_stall}
      }

      assert Heal.execute(op) == :error
    end

    test "evict_runner on an unknown id is an idempotent :ok" do
      op = %{primitive: :evict_runner, namespace: nil, target: "ghost_runner", args: %{}}
      assert Heal.execute(op) == :ok
    end

    test "a deleted/unknown primitive is no longer dispatchable" do
      # `start_operator`, `replay_state`, `compact_wal` were never planned and
      # did nothing — they are gone, not silent no-ops.
      for primitive <- [:start_operator, :replay_state, :compact_wal] do
        op = %{primitive: primitive, namespace: nil, target: :noop, args: %{}}
        assert Heal.execute(op) == :error
      end
    end

    test "a malformed restart_plugin op (no args) is a failed heal, not a crash" do
      op = %{primitive: :restart_plugin, namespace: nil, target: :noop}
      assert Heal.execute(op) == :error
    end

    # fails when: "no runner advertises this factory" is answered as a repair
    # that failed. The Loop reads `:unmet` as the one outcome that drives the
    # ladder neither way, so an operator woken with nothing to bind to is not
    # pushed further up its own rung by the attempt.
    test "a rebind with no runner for the factory is unmet, not a failed heal" do
      pid = overwatch_double(%Overwatch{id: "op", namespace: :ns, factory_id: "f"})

      stub(HubManager, :get_pid, fn :ns, "op" -> {:ok, pid} end)
      stub(OperatorManager, :assign_runner, fn :ns, "f", "op" -> nil end)

      op = %{primitive: :rebind_operator, namespace: :ns, target: "op", args: %{}}
      assert Heal.execute(op) == :unmet
    end

    # A target that answers the one call `Heal` reads an operator's factory
    # through, without standing up a real Overwatch tree.
    defp overwatch_double(%Overwatch{} = state) do
      {:ok, pid} = GenServer.start_link(__MODULE__.WedgeServer, state)
      pid
    end
  end

  describe "output-stall ladder (D1)" do
    @stall_ns :stall_ns

    defp stalled_diff(operator_id, extra \\ []) do
      intent = %Intent{
        operators: [
          %{namespace: @stall_ns, operator_id: operator_id, factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{@stall_ns => MapSet.new(["e_live"])},
        operators: %{{@stall_ns, operator_id} => Enum.into(extra, %{bound_runner: "e_live"})}
      }

      diff(intent, reality, %{
        facts: %{{:output_stall, operator_id} => %{at: 0, args: %{}}}
      })
    end

    test "a flagged instance resolves to a stalled divergence carrying its namespace" do
      assert %Diff{stalled_operators: [{@stall_ns, "op_a"}]} = stalled_diff("op_a")
    end

    test "a flag for an operator this tick is not healing resolves to nothing" do
      # Not declared, or declared in a namespace still mid-recovery — the
      # caller passes only the recovery-ready operators.
      delta =
        diff(%Intent{operators: []}, %{}, %{
          facts: %{{:output_stall, "ghost"} => %{at: 0, args: %{}}}
        })

      assert delta.stalled_operators == []
      assert Diff.empty?(delta)
    end

    test "the first eligible attempt resyncs the existing binding, not a migration" do
      assert [%{primitive: :resync_runner, target: "op_a", args: %{reason: :output_stall}}] =
               Planner.plan(stalled_diff("op_a"), Backoff.new(:heal))
    end

    test "a stall that survived the resync escalates to rebind_operator" do
      now = System.monotonic_time(:millisecond)

      # One ladder attempt recorded and its cooldown already elapsed: the same
      # stall is still flagged, so the next rung is the migration.
      backoff =
        Backoff.update(Backoff.new(:heal), Planner.ladder_key("op_a"), :error, now - 10_000)

      assert [%{primitive: :rebind_operator, target: "op_a", args: %{reason: :output_stall}}] =
               Planner.plan(stalled_diff("op_a"), backoff)
    end

    test "both rungs share one cooldown key, so escalating does not restart the throttle" do
      for primitive <- [:resync_runner, :rebind_operator] do
        assert Planner.key(%{primitive: primitive, target: "r", args: %{reason: :output_stall}}) ==
                 Planner.ladder_key("r")
      end
    end

    test "a stalled operator already covered by the plan is not laddered too" do
      # Also unbound, so the structural bind op already names it.
      delta = stalled_diff("op_dup", bound_runner: nil)

      assert [%{primitive: :rebind_operator, target: "op_dup", args: args}] =
               Planner.plan(delta, Backoff.new(:heal))

      refute Map.has_key?(args, :reason)
    end

    test "a stalled operator in backoff cooldown is filtered out" do
      now = System.monotonic_time(:millisecond)
      backoff = Backoff.update(Backoff.new(:heal), Planner.ladder_key("op_cool"), :error, now)

      assert Planner.plan(stalled_diff("op_cool"), backoff) == []
    end

    test "the ladder is not starved by a plan that filled the per-tick budget" do
      %Diff{} = stalled = stalled_diff("op_a")
      delta = %Diff{stalled | stalled_operators: [{@stall_ns, "op_a"}]}

      assert [%{primitive: :resync_runner, target: "op_a"}] =
               Planner.plan(delta, Backoff.new(:heal), max_ops: 0)
    end

    test "a ladder rung is recognisable as one; a structural op is not" do
      assert Planner.ladder?(%{
               primitive: :resync_runner,
               target: "r",
               args: %{reason: :output_stall}
             })

      refute Planner.ladder?(%{primitive: :rebind_operator, target: "r", args: %{}})
    end

    test "a stall a runner's stream declared is repaired once its operator is bound elsewhere" do
      facts =
        Map.new(
          [
            {"op_moved", %{runner_id: "e_gone"}},
            {"op_stayed", %{runner_id: "e_live"}},
            {"op_unread", %{runner_id: "e_gone"}},
            {"op_watchdog", %{}}
          ],
          fn {id, args} -> {{:output_stall, id}, %{at: 0, args: args}} end
        )

      reality = %{
        operators: %{
          {@stall_ns, "op_moved"} => %{bound_runner: "e_live"},
          {@stall_ns, "op_stayed"} => %{bound_runner: "e_live"},
          {@stall_ns, "op_unread"} => %{bound_runner: :unknown},
          {@stall_ns, "op_watchdog"} => %{bound_runner: "e_live"}
        }
      }

      {held, released} = Facts.settle(facts, reality, 60_000, 1_000)

      assert Enum.sort(Map.keys(held)) ==
               [
                 {:output_stall, "op_stayed"},
                 {:output_stall, "op_unread"},
                 {:output_stall, "op_watchdog"}
               ]

      assert released == [Planner.ladder_key("op_moved")]
    end
  end

  describe "reconcile-round cooperation (D7)" do
    alias Flezha.Convergence.Loop
    import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]

    test "a round that started orphans is declared to the loop, which ticks at once" do
      become_loop()

      assert :ok =
               Loop.handle_telemetry(
                 [:flezha, :boot, :reconcile_round],
                 %{started: 2, duplicates: 0, elapsed_ms: 4},
                 %{first_round: false, hub_id: :ns_hub},
                 self()
               )

      assert_receive {:declare, :reconcile_started, :ns_hub, %{}}, 500
    end

    test "a quiet round declares nothing — the Loop's cadence is unchanged" do
      become_loop()

      assert :ok =
               Loop.handle_telemetry(
                 [:flezha, :boot, :reconcile_round],
                 %{started: 0, duplicates: 0, elapsed_ms: 1},
                 %{first_round: false, hub_id: :ns_hub},
                 self()
               )

      refute_received {:declare, :reconcile_started, _, _}
    end

    test "the post-round plan adopts what the round started rather than fighting it" do
      # The reconcile started two operators; by the time the completion event
      # reaches the Loop they probe as bound to a live runner, so the Diff sees
      # no divergence and the plan names neither of them.
      intent = %Intent{
        operators: [
          %{namespace: :ns, operator_id: "adopted_a", factory_id: nil, plugins: []},
          %{namespace: :ns, operator_id: "adopted_b", factory_id: nil, plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e_live"])},
        operators: %{
          {:ns, "adopted_a"} => %{bound_runner: "e_live"},
          {:ns, "adopted_b"} => %{bound_runner: "e_live"}
        }
      }

      assert Planner.plan(diff(intent, reality), Backoff.new(:heal)) == []
    end
  end

  # A GenServer that wedges inside `handle_call(:block, ...)` so the SafeCall
  # probe has a live-but-stuck target to introspect, and answers its start
  # argument to `:get_overwatch_state` so a heal that reads an operator's
  # factory has a target that is not a real Overwatch tree.
  defmodule WedgeServer do
    @moduledoc false
    use GenServer

    @impl true
    def init(state), do: {:ok, state}

    @impl true
    def handle_call(:block, _from, state) do
      receive do
        :never -> {:reply, :ok, state}
      end
    end

    def handle_call(:get_overwatch_state, _from, state), do: {:reply, state, state}

    def handle_call(_other, _from, state), do: {:reply, :ok, state}

    @impl true
    def handle_cast(_msg, state), do: {:noreply, state}
  end
end

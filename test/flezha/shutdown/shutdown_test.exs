defmodule Flezha.ShutdownTest do
  @moduledoc """
  The shutdown plan's walk (`Flezha.Shutdown.run/5`): the steps of a scope run
  in their declared order, each once, each handed the budget still remaining; a
  step that fails halts the walk and the report names what did not run; a
  namespace reads as quiesced for exactly the walk's duration. And the half of
  the drain exception the loop owns — while a drain is declared the Planner
  plans nothing onto this node. The coordinator's half (declaring it, and
  clearing it at finalise) is `Flezha.Shutdown.CoordinatorTest`.
  """

  # async: false: Mimic global mode; the live coordinator; the loop's registered name.
  use ExUnit.Case, async: false
  use Mimic
  import ExUnit.CaptureLog, only: [capture_log: 1]
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Config.Backoff
  alias Flezha.Convergence.{Intent, Planner}
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown
  alias Flezha.Shutdown.Coordinator
  alias FlezhaTest.Helper.{AuthTestHelper, NodeTestHelper}

  setup :set_mimic_global_until_exit

  @namespace "shutdown_walk_test"

  describe "the fold" do
    # Three steps that report the order they ran in and whether the namespace
    # read as quiesced while they ran. Declared here rather than stubbed on
    # the modules that own the real ones: a step's `run` is captured from its
    # own module, so a stub on that module's public function never sees it.
    defp steps(behaviours) do
      test_pid = self()

      for {name, order, behaviour} <- behaviours do
        %{
          scope: :namespace,
          order: order,
          name: name,
          run: fn %{namespace: namespace}, budget ->
            send(test_pid, {:step, name, Shutdown.quiesced?(:namespace, namespace), budget})

            case behaviour do
              :ok -> :ok
              :raise -> raise "boom"
              {:result, value} -> {:ok, value}
            end
          end
        }
      end
    end

    defp walk(behaviours, budget \\ 5_000),
      do: Shutdown.run(:namespace, %{namespace: @namespace}, budget, steps(behaviours))

    test "runs every step in its declared order, each under the quiesced mark and a shrinking budget" do
      refute Shutdown.quiesced?(:namespace, @namespace)

      assert {:ok, report} =
               walk([
                 {:first, 10, :ok},
                 {:second, 20, {:result, %{migrated: 2}}},
                 {:third, 30, :ok}
               ])

      assert report.ran == [:first, :second, :third]
      assert report.skipped == [] and report.errors == []
      assert report.results == %{second: %{migrated: 2}}

      assert_receive {:step, :first, true, first_budget}
      assert_receive {:step, :second, true, _}
      assert_receive {:step, :third, true, third_budget}
      assert third_budget <= first_budget, "the budget must shrink as the walk spends it"

      refute Shutdown.quiesced?(:namespace, @namespace),
             "the quiesce mark is released when the walk ends"
    end

    test "a step that raises halts the walk and the report names the steps not run" do
      assert {:partial, report} =
               walk([{:first, 10, :ok}, {:second, 20, :raise}, {:third, 30, :ok}])

      assert report.ran == [:first]
      assert [{:second, {:error, %RuntimeError{}}}] = report.errors
      assert report.skipped == [:third]
      refute_received {:step, :third, _, _}
      refute Shutdown.quiesced?(:namespace, @namespace)
    end

    test "a spent budget leaves every remaining step unrun, and names them" do
      # The walk re-checks the deadline between steps, so a budget the earlier
      # steps consumed stops it rather than being spent again by every one
      # that follows.
      assert {:partial, report} = walk([{:first, 10, :ok}, {:second, 20, :ok}], 0)

      assert report.ran == []
      assert report.skipped == [:first, :second]
      assert report.errors == []
      assert {:messages, []} = Process.info(self(), :messages), "a step ran past the budget"
    end

    test "a walk told to continue runs past a failed step and names it" do
      steps = steps([{:first, 10, :ok}, {:second, 20, :raise}, {:third, 30, :ok}])

      assert {:partial, report} =
               Shutdown.run(:namespace, %{namespace: @namespace}, 5_000, steps,
                 on_error: :continue
               )

      assert report.ran == [:first, :third]
      assert [{:second, {:error, %RuntimeError{}}}] = report.errors
      assert report.skipped == []
      assert_receive {:step, :third, true, _}
    end

    test "a :start step's refusal is answered, not logged; a teardown step's failure is" do
      refusal = %{scope: :start, order: 10, name: :quota, run: fn _, _ -> {:error, :full} end}

      log =
        AuthTestHelper.capture_log_at(:warning, fn ->
          assert {:partial, %{errors: [quota: :full]}} =
                   Shutdown.run(:start, %{}, 5_000, [refusal])

          assert {:partial, _} = walk([{:second, 20, :raise}])
        end)

      assert log =~ "[shutdown] step namespace/second failed"
      refute log =~ "start/quota"
    end
  end

  describe "the namespace walk" do
    test "stops the namespace's relays, runners and hub, in that order" do
      test_pid = self()
      stub(HubManager, :get_all_relays, fn _ns -> [] end)

      stub(HubManager, :stop_hub, fn ns ->
        send(test_pid, {:effect, :hub, ns})
        :ok
      end)

      assert [:relays, :runners, :hub] = Enum.map(Shutdown.steps(:namespace), & &1.name)

      assert {:ok, report} =
               Shutdown.run(:namespace, %{namespace: @namespace, runner_ids: []}, 5_000)

      assert report.ran == [:relays, :runners, :hub]
    end
  end

  test "the node reads as quiesced through every namespace from quiesce until it stops" do
    refute Shutdown.quiesced?(:node, nil)
    on_exit(fn -> Shutdown.node_phase(:idle) end)

    for phase <- [:quiescing, :done] do
      Shutdown.node_phase(phase)
      assert Shutdown.quiesced?(:node, nil), "#{phase}"
      assert Shutdown.quiesced?(:namespace, "any"), "#{phase}"
    end
  end

  describe "boot" do
    test "a coordinator restart does not reopen admission" do
      on_exit(fn -> Shutdown.node_phase(:idle) end)
      Shutdown.node_phase(:quiescing)

      old = Process.whereis(Coordinator)
      ref = Process.monitor(old)

      log =
        capture_log(fn ->
          Process.exit(old, :kill)
          assert_receive {:DOWN, ^ref, :process, ^old, :killed}

          assert NodeTestHelper.wait_until(
                   fn -> Process.whereis(Coordinator) not in [nil, old] end,
                   5_000
                 )
        end)

      assert log =~ "Flezha.Shutdown.Coordinator"
      assert %{shutdown_in_progress: false} = Coordinator.status()
      assert Shutdown.quiesced?(:node, nil)
    end

    @tag :multi_node
    test "a booted controller has discovered its steps before any walk asks" do
      {peer_pid, peer} = NodeTestHelper.start_peer_node(:shutdown_steps_boot)
      on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, peer) end)

      cached = :erpc.call(peer, :persistent_term, :get, [{Shutdown, :steps}, nil])
      assert is_list(cached), "the steps are discovered at boot"

      assert Enum.map(cached, &{&1.scope, &1.name}) ==
               Enum.map(Shutdown.all_steps(), &{&1.scope, &1.name})
    end
  end

  describe "the drain exception" do
    defp placing_delta(facts) do
      intent = %Intent{
        relays: [
          %{namespace: :ns, relay_id: "misbound", factory_id: "f", plugins: []},
          %{namespace: :ns, relay_id: "unbound", factory_id: "f", plugins: []},
          %{namespace: :ns, relay_id: "parked", factory_id: "f", plugins: []},
          %{namespace: :ns, relay_id: "stale", factory_id: "f", plugins: []}
        ]
      }

      reality = %{
        active_runners: %{ns: MapSet.new(["e1"])},
        relays: %{
          {:ns, "misbound"} => %{pid: self(), bound_runner: "e_dead", factory_id: "f"},
          {:ns, "unbound"} => %{pid: self(), bound_runner: nil, factory_id: "f"},
          {:ns, "parked"} => %{
            pid: self(),
            bound_runner: "e1",
            factory_id: "f",
            phase: :halted,
            halt_reason: :x
          },
          {:ns, "stale"} => %{
            pid: self(),
            bound_runner: "e1",
            registry_tag: "e_old",
            factory_id: "f"
          }
        }
      }

      FlezhaTest.Helper.ConvergenceFixtures.diff(intent, reality, %{facts: facts})
    end

    test "a tick during a declared drain plans nothing local" do
      draining = %{{:node_draining, node()} => %{at: 0, args: %{deadline: 0}}}
      delta = placing_delta(draining)

      assert delta.draining
      assert delta.misbound_relays == [] and delta.unbound_relays == []
      assert delta.halted_relays == [] and delta.stale_tags == []
      assert Planner.plan(delta, Backoff.new(:heal)) == []

      # The same reality with no drain plans every binding.
      plain = placing_delta(%{})
      refute plain.draining

      planned =
        plain |> Planner.plan(Backoff.new(:heal)) |> Enum.map(& &1.primitive) |> Enum.uniq()

      assert :rebind_relay in planned and :publish_binding in planned
    end
  end
end

defmodule Flezha.Convergence.RecoveryGateTest do
  @moduledoc """
  Unit coverage for `Flezha.Convergence.RecoveryGate`.

  Tests the `ready_namespaces/1` classification (`:normal` and `:skipped`
  → included; `:recovering` and `:unknown` → excluded; `:timeout` →
  included with a Logger.warning), the `await_recovery_complete/2` timeout
  contract, and the recovery predicate this module owns for every reader —
  including the one case where the two answers deliberately differ.
  """

  # async: false: the primary Logger; registered runner channels; node-global tables.

  use ExUnit.Case, async: false

  alias Flezha.Convergence.{Intent, RecoveryGate, Snapshot}
  alias Flezha.Identificator
  alias FlezhaTest.Helper.NodeTestHelper

  describe "ready_namespaces/1" do
    test ":normal hub is included in the ready set" do
      ns = "rg_normal_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(Identificator.hub(ns), :normal)

      intent = intent_for([ns])
      ready = RecoveryGate.ready_namespaces(intent)
      assert MapSet.member?(ready, ns)
    end

    test ":skipped hub is included in the ready set" do
      ns = "rg_skipped_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(Identificator.hub(ns), :skipped)

      ready = RecoveryGate.ready_namespaces(intent_for([ns]))
      assert MapSet.member?(ready, ns)
    end

    test ":recovering hub is excluded from the ready set" do
      ns = "rg_recovering_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(Identificator.hub(ns), :recovering)

      ready = RecoveryGate.ready_namespaces(intent_for([ns]))
      refute MapSet.member?(ready, ns)
    end

    test ":unknown hub (never observed) is excluded" do
      ns = "rg_unknown_#{System.unique_integer([:positive])}"

      ready = RecoveryGate.ready_namespaces(intent_for([ns]))
      refute MapSet.member?(ready, ns)
    end

    test ":timeout hub is included and emits a Logger.warning" do
      ns = "rg_timeout_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(Identificator.hub(ns), :timeout)

      on_exit(NodeTestHelper.override_log_level(:warning))

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          ready = RecoveryGate.ready_namespaces(intent_for([ns]))
          assert MapSet.member?(ready, ns)
        end)

      assert log =~ ":timeout"
    end

    test "healthy and recovering namespaces are partitioned correctly in a mixed intent" do
      ns_ok = "rg_mix_ok_#{System.unique_integer([:positive])}"
      ns_busy = "rg_mix_busy_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(Identificator.hub(ns_ok), :normal)
      Snapshot.observe_recovery(Identificator.hub(ns_busy), :recovering)

      ready = RecoveryGate.ready_namespaces(intent_for([ns_ok, ns_busy]))
      assert MapSet.member?(ready, ns_ok)
      refute MapSet.member?(ready, ns_busy)
    end
  end

  describe "recovering?/1 and any_recovering?/0,1" do
    test "the member list is this module's, and a terminal state is not in it" do
      for state <- [:recovery_pending, :recovering, :timeout] do
        assert RecoveryGate.recovering?(state), "#{state} must count as recovering"
      end

      for state <- [:normal, :skipped, :unknown] do
        refute RecoveryGate.recovering?(state), "#{state} must not count as recovering"
      end
    end

    test "a :timeout hub is recovering AND healable — the two predicates differ" do
      ns = "rg_to_ready_#{System.unique_integer([:positive])}"
      Snapshot.reset_recovery_states()
      Snapshot.observe_recovery(Identificator.hub(ns), :timeout)

      assert MapSet.member?(RecoveryGate.ready_namespaces(intent_for([ns])), ns)
      assert RecoveryGate.any_recovering?()
    end

    test "with no argument it reads the whole observed cache" do
      Snapshot.reset_recovery_states()
      refute RecoveryGate.any_recovering?()

      hub = :"rg_any_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub, :recovery_pending)
      assert RecoveryGate.any_recovering?()

      Snapshot.observe_recovery(hub, :normal)
      refute RecoveryGate.any_recovering?()
    end

    test "with an argument it reads what the caller already sampled" do
      assert RecoveryGate.any_recovering?(%{a: :normal, b: :recovering})
      refute RecoveryGate.any_recovering?(%{a: :normal, b: :skipped})
      assert RecoveryGate.any_recovering?([:normal, :timeout])
      refute RecoveryGate.any_recovering?([])
    end

    test "recovery_summary/0 samples the states and the verdict once" do
      Snapshot.reset_recovery_states()
      hub = :"rg_sum_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub, :recovering)

      assert %{states: [:recovering], recovering?: true} = RecoveryGate.recovery_summary()
    end
  end

  describe "await_recovery_complete/2" do
    test "returns :ok immediately when hub is already :normal" do
      hub_id = :"rg_await_normal_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :normal)
      assert :ok = RecoveryGate.await_recovery_complete([hub_id], 200)
    end

    test "returns :ok immediately when hub is already :skipped" do
      hub_id = :"rg_await_skipped_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :skipped)
      assert :ok = RecoveryGate.await_recovery_complete([hub_id], 200)
    end

    test "returns error when hub stays :recovering past the timeout" do
      hub_id = :"rg_await_timeout_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :recovering)

      assert {:error, {:recovery_timeout, [^hub_id]}} =
               RecoveryGate.await_recovery_complete([hub_id], 100)
    end

    test "unblocks when recovery telemetry transitions hub to :normal" do
      hub_id = :"rg_await_unblock_#{System.unique_integer([:positive])}"
      Snapshot.observe_recovery(hub_id, :recovering)
      test_pid = self()

      task =
        Task.async(fn ->
          send(test_pid, :await_started)
          RecoveryGate.await_recovery_complete([hub_id], 3_000)
        end)

      assert_receive :await_started, 1_000

      # Simulate the upstream recovery_state_changed hook reaching :normal.
      Snapshot.handle_recovery_transition(hub_id, %{to: :normal, reason: :replay_complete})

      assert :ok = Task.await(task, 3_000)
    end
  end

  defp intent_for(namespaces) do
    operators =
      Enum.map(namespaces, fn ns ->
        %{namespace: ns, operator_id: "r_#{ns}", factory_id: nil, plugins: []}
      end)

    %Intent{operators: operators}
  end
end

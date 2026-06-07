defmodule Flezha.Convergence.RecoveryGateTest do
  @moduledoc """
  Unit coverage for `Flezha.Convergence.RecoveryGate`.

  Tests the `ready_namespaces/1` classification (`:normal` and `:skipped`
  → included; `:recovering` and `:unknown` → excluded; `:timeout` →
  included with a Logger.warning) and the `await_recovery_complete/2`
  timeout contract.
  """

  use ExUnit.Case, async: false

  alias Flezha.Convergence.{Intent, RecoveryGate, Snapshot}
  alias Flezha.Service.Identificator

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

      prev_level = Logger.level()
      Logger.configure(level: :warning)
      on_exit(fn -> Logger.configure(level: prev_level) end)

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

      :telemetry.execute(
        [:process_hub, :recovery, :complete],
        %{succeeded: 0, failed: 0, skipped: 0, elapsed_ms: 0, cspec_count: 0},
        %{hub_id: hub_id}
      )

      assert :ok = Task.await(task, 3_000)
    end
  end

  defp intent_for(namespaces) do
    relays =
      Enum.map(namespaces, fn ns ->
        %{namespace: ns, relay_id: "r_#{ns}", factory_id: nil, plugins: []}
      end)

    %Intent{relays: relays}
  end
end

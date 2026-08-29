defmodule Flezha.Cluster.OrchestratorTest do
  @moduledoc """
  Cluster-formation gate (self-healing-control-plane, Phase 4 / D3).

  `cluster_ready?/0` is the lock-free gate the on-demand `start_hub` path consults
  so a namespace hub is never created on a not-yet-clustered node. These tests
  drive the running singleton (which boots solo-ready under the test config) and
  restart it under a synthetic unreachable-peer config to exercise the
  degrade-to-solo path.
  """
  # async: false: writes Application env.
  use ExUnit.Case, async: false

  alias Flezha.Cluster.Orchestrator

  describe "cluster_ready? (solo / running app)" do
    test "a node with no discovered peers is ready and await returns :ok" do
      # Test config uses manual_discovery with no initial_nodes → solo-ready.
      assert Orchestrator.cluster_ready?() == true
      assert Orchestrator.await_cluster_ready(100) == :ok
    end
  end

  describe "degrade-to-solo when a discovered peer is unreachable" do
    setup do
      saved = %{
        cluster_discovery: Application.fetch_env!(:flezha, :cluster_discovery),
        initial_nodes: Application.fetch_env!(:flezha, :initial_nodes),
        cluster_formation_timeout_ms:
          Application.fetch_env!(:flezha, :cluster_formation_timeout_ms),
        cluster_update_interval: Application.fetch_env!(:flezha, :cluster_update_interval)
      }

      on_exit(fn ->
        Enum.each(saved, fn {k, v} -> Application.put_env(:flezha, k, v) end)
        restart_orchestrator()
        # Leave the singleton solo-ready for the rest of the suite.
        assert Orchestrator.await_cluster_ready(3_000) == :ok
      end)

      :ok
    end

    test "stays not-ready until the formation timeout, then degrades to ready" do
      Application.put_env(:flezha, :cluster_discovery, "manual_discovery")
      Application.put_env(:flezha, :initial_nodes, [:"ghost@127.0.0.1"])
      # Degrade window well below the periodic sweep so the degrade timer (not
      # the sweep's discovery reset) is what flips readiness.
      Application.put_env(:flezha, :cluster_formation_timeout_ms, 500)
      Application.put_env(:flezha, :cluster_update_interval, 60_000)
      restart_orchestrator()

      # Unreachable peer → not ready yet; the on-demand hub gate would hold.
      refute Orchestrator.cluster_ready?()
      assert Orchestrator.await_cluster_ready(200) == :timeout

      # After the degrade-to-solo window elapses, it declares ready so a
      # single-node deployment still boots.
      assert Orchestrator.await_cluster_ready(3_000) == :ok
      assert Orchestrator.cluster_ready?()
    end
  end

  defp restart_orchestrator() do
    :ok = Supervisor.terminate_child(Flezha.Supervisor, Orchestrator)

    case Supervisor.restart_child(Flezha.Supervisor, Orchestrator) do
      {:ok, _} -> :ok
      {:ok, _, _} -> :ok
    end
  end
end

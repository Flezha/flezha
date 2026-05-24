defmodule Flezha.Reconciler.HealthTest do
  @moduledoc """
  Health classification (self-healing-control-plane, Phase 5 / 5.2). Pure-function
  tests over constructed `Probe` snapshots — every branch of `classify/1`.
  """
  use ExUnit.Case, async: true

  alias Flezha.Reconciler.{Health, Probe}

  defp probe(fields), do: struct(%Probe{namespace: "ns", relay_id: "r1"}, fields)

  describe "classify/1" do
    test "healthy: alive, not parked, stream bound, executor reachable + available" do
      p =
        probe(
          relay_alive?: true,
          local?: true,
          parked?: false,
          stream_bound?: true,
          executor_reachable?: true,
          executor_available?: true
        )

      assert Health.classify(p) == :healthy
    end

    test "failed: dead pid and no executor advertises the factory" do
      p = probe(relay_alive?: false, executor_available?: false)
      assert Health.classify(p) == :failed
    end

    test "failed: parked and no executor advertises the factory" do
      p = probe(relay_alive?: true, parked?: true, executor_available?: false)
      assert Health.classify(p) == :failed
    end

    test "degraded: dead pid but an executor is available to re-place onto" do
      p = probe(relay_alive?: false, executor_available?: true)
      assert Health.classify(p) == :degraded
    end

    test "degraded: parked but an executor is available to un-park onto" do
      p =
        probe(relay_alive?: true, parked?: true, executor_available?: true)

      assert Health.classify(p) == :degraded
    end

    test "degraded: running but data stream unbound" do
      p =
        probe(
          relay_alive?: true,
          parked?: false,
          stream_bound?: false,
          executor_reachable?: true,
          executor_available?: true
        )

      assert Health.classify(p) == :degraded
    end

    test "degraded: stale dead-node entries to purge" do
      p =
        probe(
          relay_alive?: true,
          stream_bound?: true,
          executor_reachable?: true,
          executor_available?: true,
          dead_node_entries: [{:dead@nohost, self()}]
        )

      assert Health.classify(p) == :degraded
    end

    test "degraded: assigned executor no longer reachable" do
      p =
        probe(
          relay_alive?: true,
          stream_bound?: true,
          executor_reachable?: false,
          executor_available?: true
        )

      assert Health.classify(p) == :degraded
    end

    test "unknown stream state does not by itself degrade an otherwise-healthy relay" do
      # A wedged relay whose :stream_ref_status probe timed out (:unknown) is not
      # treated as unbound — only an explicit `false` triggers a rebind.
      p =
        probe(
          relay_alive?: true,
          parked?: false,
          stream_bound?: :unknown,
          executor_reachable?: true,
          executor_available?: true
        )

      assert Health.classify(p) == :healthy
    end
  end
end

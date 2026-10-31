defmodule FlezhaTest.Convergence.SnapshotTest do
  @moduledoc """
  The published snapshot stores one row per verdict, so reading one subject
  costs the same whatever the fleet size, and a namespace's health is read in
  one pass.
  """

  # async: false: the snapshot table is node-global.
  use FlezhaTest.Case, fixtures: [:env], async: false

  import FlezhaTest.Helper.ConvergenceFixtures, only: [verdicts: 1]

  alias Flezha.Convergence.{Snapshot, Verdict}

  setup do
    published = Snapshot.current()
    on_exit(fn -> Snapshot.publish(published) end)
    :ok
  end

  defp relay(ns, id), do: {:relay, {ns, id}}

  defp publish(entries, extra \\ %{}),
    do: Snapshot.publish(Map.put(extra, :relay_health, verdicts(entries)))

  describe "reads of one subject" do
    test "answer the published verdict, worded as before" do
      publish(
        [
          {relay("ns", "ok"), :healthy},
          {relay("ns", "down"), :failed},
          {{:runner, "r1"}, :degraded}
        ],
        %{unmet_intent_ms: 0}
      )

      assert %Verdict{state: :healthy} = Snapshot.verdict("ns", "ok")
      assert %Verdict{state: :degraded} = Snapshot.record({:runner, "r1"})
      assert Snapshot.relay_health("ns", "ok") == :healthy
      assert Snapshot.relay_health("ns", "down") == :recovering
      assert Snapshot.relay_health("ns", "never") == :unknown
      assert Snapshot.verdict("ns", "never") == nil
    end

    test "a subject dropped from the next tick is gone" do
      publish([{relay("ns", "a"), :healthy}, {relay("ns", "b"), :healthy}])
      publish([{relay("ns", "b"), :degraded}])

      assert Snapshot.verdict("ns", "a") == nil
      assert Snapshot.relay_health("ns", "b") == :degraded
      assert Map.keys(Snapshot.current().relay_health) == [relay("ns", "b")]
    end
  end

  describe "relay_health_of/1" do
    test "answers only its namespace's relays, each as relay_health/2 does" do
      publish(
        [
          {relay("ns", "a"), :healthy},
          {relay("ns", "b"), :failed},
          {relay("other", "c"), :healthy},
          {{:plugin, {"ns", "a", "p"}}, :degraded}
        ],
        %{unmet_intent_ms: 0}
      )

      health = Snapshot.relay_health_of("ns")

      assert health == %{"a" => :healthy, "b" => :recovering}
      for {id, listed} <- health, do: assert(Snapshot.relay_health("ns", id) == listed)
    end
  end

  describe "the whole-fleet read" do
    test "folds every published relay, failed ones included" do
      publish([
        {relay("ns", "a"), :healthy},
        {relay("ns", "b"), :healthy},
        {relay("ns", "c"), :failed}
      ])

      assert %{intended: 3, bound: 2} = Verdict.counts(Snapshot.current().relay_health)
    end
  end

  describe "cost" do
    defp fleet(n), do: for(i <- 1..n, do: {relay("scale", "r#{i}"), :healthy})

    test "one lookup does not grow with the fleet, and publishing stays cheap" do
      small_fleet = verdicts(fleet(1_000))
      large_fleet = verdicts(fleet(8_000))
      subjects = for i <- 1..1_000, do: relay("scale", "r#{i}")
      lookups = fn -> for subject <- subjects, do: Snapshot.record(subject) end

      # The two sizes take turns, so a stretch of load on the box lands on
      # both; the best round of each is its cost with the load left out.
      rounds =
        for _ <- 1..7 do
          Snapshot.publish(%{relay_health: small_fleet})
          {small, _} = :timer.tc(lookups)
          {publish_us, _} = :timer.tc(fn -> Snapshot.publish(%{relay_health: large_fleet}) end)
          {large, _} = :timer.tc(lookups)
          {small, large, publish_us}
        end

      {small, large, publish_us} =
        Enum.reduce(rounds, fn {s, l, p}, {bs, bl, bp} -> {min(s, bs), min(l, bl), min(p, bp)} end)

      assert large / max(small, 1) < 2,
             "1,000 lookups took #{small} µs at 1,000 subjects and #{large} µs at 8,000"

      assert publish_us < 50_000, "publishing 8,000 verdicts took #{publish_us} µs"
    end
  end
end

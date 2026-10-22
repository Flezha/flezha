defmodule Mix.Tasks.Bench.TallyTest do
  @moduledoc """
  The bench's shared accounting: the stamp an input carries back, the pacing
  rule every paced sender uses, and the warm-up prime that keeps the first
  seqs out of the percentiles.
  """

  use ExUnit.Case, async: true

  alias Mix.Tasks.Bench.Tally

  describe "parse_stamp/1" do
    test "a bare stamp" do
      assert Tally.parse_stamp("12,345") == {12, 345}
    end

    test "a keyed input and a key=value output parse like a bare one" do
      assert Tally.parse_stamp("k3:12,345") == {12, 345}
      assert Tally.parse_stamp("k3=12,345") == {12, 345}
    end

    test "padding after the stamp is ignored" do
      assert Tally.parse_stamp("k3:12,345," <> String.duplicate("x", 4096)) == {12, 345}
    end

    test "a body without a stamp is an error" do
      assert Tally.parse_stamp("test_msg_flow_test_1_1") == :error
      assert Tally.parse_stamp("k3=7") == :error
      assert Tally.parse_stamp("12,") == :error
      assert Tally.parse_stamp("") == :error
    end

    test "a negative monotonic send time parses" do
      assert Tally.parse_stamp("k1:3,-576460751234,xx") == {3, -576_460_751_234}
    end

    test "a key is only looked for in the first 64 bytes" do
      assert Tally.parse_stamp(String.duplicate("a", 63) <> ":1,2") == {1, 2}
      assert Tally.parse_stamp(String.duplicate("a", 64) <> ":1,2") == :error
    end

    test "the steps reply body still parses" do
      assert Tally.parse_stamp("7,1700000000000") == {7, 1_700_000_000_000}
    end
  end

  describe "due/3" do
    test "nothing is due at the start of a schedule" do
      assert Tally.due(System.monotonic_time(:nanosecond) + 1_000_000_000, 1000, 0) == 0
    end

    test "elapsed time times the rate, less what was issued" do
      t0 = System.monotonic_time(:nanosecond) - 1_000_000_000
      due = Tally.due(t0, 1000, 0)
      assert due >= 1000 and due < 1100
      assert Tally.due(t0, 1000, due) in 0..100
    end

    test "never negative when ahead of schedule" do
      assert Tally.due(System.monotonic_time(:nanosecond), 10, 1_000) == 0
    end
  end

  describe "percentiles/2" do
    test "never exceed the largest sample" do
      tally = Tally.record({0, 461_000}, Tally.new(1))
      assert Tally.percentiles(tally, [0.999])[0.999] == 461_000
      assert %{p99_9: 0.46, max: 0.46} = Tally.latency_ms(tally)
    end

    test "take the nearest rank" do
      ms = fn n ->
        Enum.reduce(1..n, Tally.new(n), &Tally.record({&1 - 1, &1 * 1_000_000}, &2))
      end

      # 0.95 * 12 = 11.4: rank 12, the largest sample, not rank 11.
      assert Tally.percentiles(ms.(12), [0.95])[0.95] == 12_000_000
      assert Tally.percentiles(ms.(100), [0.95])[0.95] == 95_005_000
    end
  end

  describe "persisted/2" do
    test "sums the run's relays only, and gives the growth since a reading" do
      relay = "bench_tally_test_#{System.unique_integer([:positive])}"
      other = Flezha.State.Metrics.for_cell({relay <> "_other", "t", "k"})
      mine = Flezha.State.Metrics.for_cell({relay, "t", "k"})
      Flezha.State.Metrics.add(mine, :persisted_changes_total, 3)
      Flezha.State.Metrics.add(other, :persisted_changes_total, 100)

      before = Tally.persisted(MapSet.new([relay]))
      assert before.changes == 3

      Flezha.State.Metrics.add(mine, :persisted_changes_total, 2)
      Flezha.State.Metrics.add(mine, :persisted_bytes_total, 64)
      Flezha.State.Metrics.add(mine, :fsync_total, 1)

      assert Tally.persisted(MapSet.new([relay]), before) == %{changes: 2, bytes: 64, fsyncs: 1}
    end
  end

  describe "a prime count" do
    test "keeps warm-up seqs counted but out of the percentiles" do
      tally =
        Enum.reduce(
          [{0, 900_000_000}, {1, 900_000_000}, {2, 50_000}, {3, 50_000}],
          Tally.new(4, 2),
          &Tally.record/2
        )

      assert tally.seen == 4
      assert tally.received == 2
      assert Tally.percentiles(tally, [0.99])[0.99] < 100_000
      assert tally.max_ns == 50_000
    end
  end
end

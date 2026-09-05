defmodule Flezha.PartitionTest do
  @moduledoc """
  The Elixir half of a three-implementation contract. The controller decides
  which member owns a key; the SDK refuses a key it does not own. If the two
  hashes disagree by one bit, every operative in that group parks its traffic —
  so the fixture vectors here are the SAME ones `determinism.rs` and
  `StableIdTest.java` assert.
  """
  use ExUnit.Case, async: true

  alias Flezha.Partition

  # `StableId::derive(identity, seq, ordinal)` in both SDKs: FNV-1a-128 over
  # identity, then the big-endian seq, then the big-endian ordinal.
  defp stable_id(identity, seq, ordinal) do
    Partition.fnv1a128(identity)
    |> Partition.fnv1a128(<<seq::unsigned-big-64>>)
    |> Partition.fnv1a128(<<ordinal::unsigned-big-32>>)
    |> Integer.to_string(16)
    |> String.downcase()
    |> String.pad_leading(32, "0")
  end

  describe "fnv1a128/1,2" do
    test "reproduces the cross-language StableId fixture vectors" do
      assert stable_id("instance/op/key", 42, 0) == "bbf81fd4955afd1724203700a240e822"
      assert stable_id("instance/op/key", 42, 1) == "bbf81fd4965afd1724203700a240e95d"
      assert stable_id("", 0, 0) == "15915375576d2f389a938dc3a9940c9d"
    end

    test "the empty input is the bare offset basis" do
      assert Partition.fnv1a128("") == 0x6C62272E07BB014262B821756295C58D
    end
  end

  describe "group_of/1" do
    test "reproduces the cross-language key-group fixture" do
      # The twin assertions live in `flz_core`'s `key_group_matches_the_cross_language_fixture`
      # and Java's `keyGroupMatchesTheCrossLanguageFixture`.
      assert Partition.group_of("BTC-USD") == 129
      assert Partition.group_of("ETH-USD") == 213
      assert Partition.group_of("SOL-USD") == 60
      assert Partition.group_of("") == 141
      assert Partition.group_of("a") == 100
    end

    test "is the low byte of the digest and stays in range" do
      for key <- ["BTC-USD", "ETH-USD", "", "a", String.duplicate("x", 512)] do
        assert Partition.group_of(key) == rem(Partition.fnv1a128(key), 256)
        assert Partition.group_of(key) in 0..255
      end
    end

    test "is stable across calls" do
      assert Partition.group_of("BTC-USD") == Partition.group_of("BTC-USD")
    end
  end

  describe "ranges/1" do
    test "an even split covers 0..255 with no gap and no overlap" do
      for n <- [1, 2, 3, 5, 7, 16, 256] do
        ranges = Partition.ranges(n)
        assert length(ranges) == n

        assert :ok ==
                 Partition.coverage(Enum.with_index(ranges, fn range, i -> {i, range} end))
      end
    end

    test "three partitions split as documented" do
      assert Partition.ranges(3) == [{0, 84}, {85, 169}, {170, 255}]
    end

    test "one partition owns everything" do
      assert Partition.ranges(1) == [{0, 255}]
    end
  end

  describe "owner/2" do
    test "resolves every group to exactly the range containing it" do
      ranges = Partition.ranges(4)

      for group <- 0..255 do
        idx = Partition.owner(ranges, group)
        {from, to} = Enum.at(ranges, idx)
        assert group >= from and group <= to
      end
    end

    test "is nil when no range covers the group" do
      assert Partition.owner([{0, 10}], 11) == nil
    end
  end

  describe "coverage/1" do
    test "reports a gap left by a missing member" do
      [_p0, p1, p2] = Partition.ranges(3)

      assert {:error, %{gaps: [{0, 84}], overlaps: []}} =
               Partition.coverage([{"p1", p1}, {"p2", p2}])
    end

    test "reports an overlap naming both members" do
      assert {:error, %{gaps: gaps, overlaps: overlaps}} =
               Partition.coverage([{"p0", {0, 128}}, {"p1", {128, 255}}])

      assert gaps == []
      assert overlaps == [{128, ["p0", "p1"]}]
    end
  end
end

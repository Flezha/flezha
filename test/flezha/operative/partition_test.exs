defmodule Flezha.PartitionTest do
  @moduledoc """
  How the controller splits the key-group space among a group's members and
  reports its coverage. The hash itself is `Flezha.SDK.KeyGroup`'s, held to the
  cross-language vectors in that package's own suite.
  """
  use ExUnit.Case, async: true

  alias Flezha.Partition

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

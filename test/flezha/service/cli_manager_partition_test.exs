defmodule Flezha.Service.CliManagerPartitionTest do
  @moduledoc """
  The operator's view of a partition group: one row in `list`, per-member
  detail plus a read-time coverage check in `info`, a stop that reaches every
  member, and a keyed `state get` that resolves the owner by hash.

  Coverage is recomputed from the member rows on every read — there is no
  stored group record — so a missing or overlapping member has to show up as a
  warning rather than as a quietly short list. That is what these pin.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import Mimic

  alias Flezha.Partition
  alias Flezha.Service.CliManager
  alias Flezha.Operative.PartitionGroup
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager

  setup :set_mimic_global

  defp member_row(group, index, count, opts \\ []) do
    %{
      id: Partition.member_id(group, index),
      health: Keyword.get(opts, :health, :healthy),
      runner: Keyword.get(opts, :runner, "runner-#{index}"),
      factory: "candles",
      partition: %{
        group: group,
        index: index,
        count: count,
        range: Keyword.get(opts, :range, Enum.at(Partition.ranges(count), index))
      },
      node: node()
    }
  end

  defp plain_row(id) do
    %{id: id, health: :healthy, runner: "runner-9", factory: "f", partition: nil, node: node()}
  end

  describe "group_partition_rows/1" do
    test "collapses a group's members into one row with per-member placement" do
      rows = for index <- 0..2, do: member_row("ohlc", index, 3)

      assert [row] = CliManager.group_partition_rows(rows)
      assert row.id == "ohlc"
      assert row.partitions == "3/3"
      assert row.runner == "p0→runner-0, p1→runner-1, p2→runner-2"
      assert row.health == :healthy
      assert length(row.members) == 3
    end

    test "shows a short group as such" do
      rows = [member_row("ohlc", 0, 3), member_row("ohlc", 2, 3)]
      assert [row] = CliManager.group_partition_rows(rows)
      assert row.partitions == "2/3"
    end

    test "is exactly as healthy as its sickest member" do
      rows = [
        member_row("ohlc", 0, 3),
        member_row("ohlc", 1, 3, health: :degraded),
        member_row("ohlc", 2, 3, health: :failed)
      ]

      assert [%{health: :failed}] = CliManager.group_partition_rows(rows)
    end

    test "leaves ungrouped operatives untouched" do
      rows = [plain_row("plain"), member_row("ohlc", 0, 2), member_row("ohlc", 1, 2)]

      assert [group, plain] = CliManager.group_partition_rows(rows)
      assert group.id == "ohlc"
      assert plain == plain_row("plain")
      refute Map.has_key?(plain, :partitions)
    end
  end

  describe "operative info" do
    defp info_output(rows) do
      stub(HubManager, :operative_rows, fn _ns -> rows end)
      stub(OperativeManager, :get_operative_ids, fn _ns -> {:ok, Enum.map(rows, & &1.id)} end)

      stub(OperativeManager, :get_overwatch_state, fn _ns, _id ->
        {:error, :not_running}
      end)

      capture_io(fn -> CliManager.operative_info("default") end)
    end

    test "renders each member's range and reports clean coverage" do
      out = info_output(for index <- 0..2, do: member_row("ohlc", index, 3))

      assert out =~ "Partition group: ohlc (3/3)"
      assert out =~ "p0  ohlc#p0"
      assert out =~ "key groups 0-84"
      assert out =~ "key groups 170-255"
      assert out =~ "all 256 key groups owned exactly once"
    end

    test "a missing member is reported as the gap it is" do
      out = info_output([member_row("ohlc", 0, 3), member_row("ohlc", 2, 3)])

      assert out =~ "WARNING: key-group coverage is broken"
      assert out =~ "gap: key groups 85-169 are owned by no member"
    end

    test "two members claiming one key group is reported as an overlap" do
      rows = [
        member_row("ohlc", 0, 2, range: {0, 128}),
        member_row("ohlc", 1, 2, range: {128, 255})
      ]

      out = info_output(rows)

      assert out =~ "WARNING: key-group coverage is broken"
      assert out =~ "overlap: key group 128 is claimed by ohlc#p0, ohlc#p1"
    end
  end

  describe "operative stop" do
    test "a group id stops every member" do
      members = for index <- 0..2, do: member_row("ohlc", index, 3)
      parent = self()

      stub(PartitionGroup, :members, fn _ns, id ->
        if id == "ohlc", do: Enum.map(members, &%{id: &1.id, partition: &1.partition}), else: []
      end)

      stub(OperativeManager, :request_stop, fn _ns, id ->
        send(parent, {:stopped, id})
        :ok
      end)

      out = capture_io(fn -> CliManager.operative_stop("default", "ohlc") end)

      assert out =~ "Operative group ohlc stopped (3 partitions)"
      assert_receive {:stopped, "ohlc#p0"}, 100
      assert_receive {:stopped, "ohlc#p1"}, 100
      assert_receive {:stopped, "ohlc#p2"}, 100
    end

    test "an ordinary operative takes the single-stop path" do
      stub(PartitionGroup, :members, fn _ns, _id -> [] end)
      stub(OperativeManager, :request_stop, fn _ns, "plain" -> :ok end)

      assert capture_io(fn -> CliManager.operative_stop("default", "plain") end) =~
               "Operative has been stopped"
    end

    test "a failed member is named, never swallowed" do
      members = for index <- 0..1, do: member_row("ohlc", index, 2)

      stub(PartitionGroup, :members, fn _ns, "ohlc" ->
        Enum.map(members, &%{id: &1.id, partition: &1.partition})
      end)

      stub(OperativeManager, :request_stop, fn _ns, id ->
        if id == "ohlc#p1", do: {:error, :wedged}, else: :ok
      end)

      out =
        capture_io(fn ->
          try do
            CliManager.operative_stop("default", "ohlc")
          catch
            :exit, _ -> :ok
          end
        end)

      assert out =~ "ohlc#p1"
      assert out =~ "wedged"
    end
  end

  describe "state get against a group" do
    setup do
      members = for index <- 0..2, do: member_row("ohlc", index, 3)

      stub(OperativeManager, :namespace_of, fn _id -> :error end)
      stub(OperativeManager, :namespace_of_group, fn "ohlc" -> {:ok, "trading"} end)

      stub(PartitionGroup, :members, fn _ns, "ohlc" ->
        Enum.map(members, &%{id: &1.id, partition: &1.partition})
      end)

      :ok
    end

    defp run_state_get(path) do
      capture_io(:stderr, fn ->
        printed =
          capture_io(fn ->
            try do
              CliManager.state_get(path)
            catch
              :exit, _ -> :ok
            end
          end)

        send(self(), {:stdout, printed})
      end)

      receive do
        {:stdout, printed} -> printed
      end
    end

    test "a keyed read goes to exactly the member owning the key's group" do
      parent = self()

      stub(OperativeManager, :get_entity_state, fn "trading", id, _selector ->
        send(parent, {:read, id})
        {:ok, ~s({"candle":{"BTC-USD":{"high":42}}}), 7}
      end)

      out = run_state_get("ohlc/candle/BTC-USD")

      # BTC-USD hashes to key group 129, which p1 owns under ranges(3).
      assert_receive {:read, "ohlc#p1"}
      refute_receive {:read, "ohlc#p0"}, 50
      assert out =~ "BTC-USD"
    end

    test "a procedure-level read fans out and merges the members' entities" do
      stub(OperativeManager, :get_entity_state, fn "trading", id, _selector ->
        case id do
          "ohlc#p0" -> {:ok, ~s({"candle":{"SOL-USD":{"high":1}}}), 3}
          "ohlc#p1" -> {:ok, ~s({"candle":{"BTC-USD":{"high":2}}}), 9}
          "ohlc#p2" -> {:ok, ~s({"candle":{"ETH-USD":{"high":3}}}), 5}
        end
      end)

      out = run_state_get("ohlc/candle")

      assert out =~ "SOL-USD"
      assert out =~ "BTC-USD"
      assert out =~ "ETH-USD"
      # The seq reported is the freshest of the members read.
      assert out =~ "9"
    end
  end
end

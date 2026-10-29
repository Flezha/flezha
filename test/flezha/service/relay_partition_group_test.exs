defmodule Flezha.Service.Cli.PartitionTest do
  @moduledoc """
  The caller's view of a partition group: one row in `list`, per-member
  detail plus a read-time coverage check in `info`, a stop that reaches every
  member, and a keyed `state get` that resolves the owner by hash.

  Coverage is recomputed from the member rows on every read — there is no
  stored group record — so a missing or overlapping member has to show up as a
  warning rather than as a quietly short list. That is what these pin.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Convergence.Snapshot
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Cluster.Directory
  alias Flezha.Partition
  alias Flezha.Service.Cli
  alias Flezha.Service.HubManager
  alias Flezha.Service.Relay
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.ConvergenceFixtures
  alias FlezhaTest.Helper.CliCapture

  setup :set_mimic_global_until_exit

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

  # The rows `flz relay list` prints: a partition group collapsed into one,
  # each member's health its published verdict (`unmet_ms` past its intent).
  defp grouped(rows, unmet_ms \\ 500_000) do
    ConvergenceFixtures.stub_registry_rows("default", rows, unmet_ms)
    {:ok, %{rows: listed, unreachable: []}} = Relay.list("default", %{source: :cli})
    listed
  end

  describe "a group in the relay list" do
    setup do
      prev = Snapshot.current()
      on_exit(fn -> Snapshot.publish(prev) end)
    end

    test "collapses a group's members into one row with per-member placement" do
      rows = for index <- 0..2, do: member_row("ohlc", index, 3)

      assert [row] = grouped(rows)
      assert row.id == "ohlc"
      assert row.partitions == "3/3"
      assert row.runner == "p0→runner-0, p1→runner-1, p2→runner-2"
      assert row.health == :healthy
      assert length(row.members) == 3
    end

    test "shows a short group as such" do
      rows = [member_row("ohlc", 0, 3), member_row("ohlc", 2, 3)]
      assert [row] = grouped(rows)
      assert row.partitions == "2/3"
    end

    test "is exactly as healthy as its sickest member" do
      rows = [
        member_row("ohlc", 0, 3),
        member_row("ohlc", 1, 3, health: :degraded),
        member_row("ohlc", 2, 3, health: :failed)
      ]

      assert [%{health: :failed}] = grouped(rows)
    end

    test "ranks a recovering member between degraded and parked" do
      # A failed member inside the unmet-intent grace reads recovering.
      group = fn healths ->
        [%{health: health}] =
          healths
          |> Enum.map(&if(&1 == :recovering, do: :failed, else: &1))
          |> Enum.with_index(fn h, i -> member_row("ohlc", i, length(healths), health: h) end)
          |> grouped(2_000)

        health
      end

      assert group.([:healthy, :recovering]) == :recovering
      assert group.([:degraded, :recovering]) == :recovering
      assert group.([:recovering, :halted]) == :halted
    end

    test "leaves ungrouped relays untouched" do
      rows = [plain_row("plain"), member_row("ohlc", 0, 2), member_row("ohlc", 1, 2)]

      assert [group, plain] = grouped(rows)
      assert group.id == "ohlc"
      assert plain == plain_row("plain")
      refute Map.has_key?(plain, :partitions)
    end
  end

  describe "relay info" do
    defp info_output(rows) do
      stub(HubManager, :relay_rows, fn _ns -> rows end)
      stub(RelayManager, :get_relay_ids, fn _ns -> {:ok, Enum.map(rows, & &1.id)} end)

      stub(RelayManager, :get_overwatch_state, fn ns, id ->
        {:error, {:relay_not_found, ns, id}}
      end)

      capture_io(fn -> Cli.call("Relay.info", Jason.encode!(["default"]), []) end)
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

  describe "health while a group moves off a dead runner" do
    setup do
      prev = Snapshot.current()
      on_exit(fn -> Snapshot.publish(prev) end)

      rows =
        for(index <- 0..2, do: member_row("prices", index, 3, health: :failed)) ++
          [plain_row("quotes")]

      stub(RelayManager, :get_relay_ids, fn _ns -> {:ok, Enum.map(rows, & &1.id)} end)

      stub(RelayManager, :get_overwatch_state, fn ns, _id ->
        {:ok,
         %{
           runner_id: "r2",
           namespace: ns,
           extensions: %{},
           default_input_type: "bytes",
           calls: %Flezha.Relay.Calls.Inbound{}
         }}
      end)

      stub(Directory, :hub_nodes, fn _ns -> [node()] end)
      stub(HubManager, :recovery_state, fn _hub_id -> {:ok, :normal} end)
      %{rows: rows}
    end

    test "info reads recovering for the group's members and each relay", %{rows: rows} do
      ConvergenceFixtures.stub_registry_rows("trading", rows, 2_000)

      text = capture_io(fn -> Cli.call("Relay.info", Jason.encode!(["trading"]), []) end)
      assert text =~ ~r/p0  prices#p0 .*runner-0  recovering\n/
      assert length(Regex.scan(~r/Health: recovering\n/, text)) == 3
      assert text =~ "Health: healthy"
      refute text =~ "failed"

      json =
        Jason.decode!(
          capture_io(fn ->
            Cli.call("Relay.info", Jason.encode!(["trading"]), output: "json")
          end)
        )

      [%{"members" => members}] = json["groups"]
      assert Enum.map(members, & &1["health"]) == List.duplicate("recovering", 3)

      assert Map.new(json["relays"], &{&1["id"], &1["health"]}) ==
               %{
                 "prices#p0" => "recovering",
                 "prices#p1" => "recovering",
                 "prices#p2" => "recovering",
                 "quotes" => "healthy"
               }
    end

    test "cluster status counts recovering inside the grace", %{rows: rows} do
      ConvergenceFixtures.stub_registry_rows("trading", rows, 2_000)

      json =
        Jason.decode!(
          capture_io(fn ->
            Cli.call("Controller.cluster_status", Jason.encode!(["trading"]), output: "json")
          end)
        )

      assert json["relay_health"] == %{"recovering" => 3, "healthy" => 1}

      text =
        capture_io(fn ->
          Cli.call("Controller.cluster_status", Jason.encode!(["trading"]), [])
        end)

      assert text =~ "recovering=3"
      refute text =~ "failed"
    end

    test "cluster status and info read failed past the grace", %{rows: rows} do
      ConvergenceFixtures.stub_registry_rows("trading", rows, 500_000)

      json =
        Jason.decode!(
          capture_io(fn ->
            Cli.call("Controller.cluster_status", Jason.encode!(["trading"]), output: "json")
          end)
        )

      assert json["relay_health"] == %{"failed" => 3, "healthy" => 1}

      text = capture_io(fn -> Cli.call("Relay.info", Jason.encode!(["trading"]), []) end)
      assert length(Regex.scan(~r/Health: failed\n/, text)) == 3
      refute text =~ "recovering"
    end
  end

  describe "relay stop" do
    test "a group id stops every member" do
      members = for index <- 0..2, do: member_row("ohlc", index, 3)
      parent = self()

      stub(PartitionGroup, :members, fn _ns, id ->
        if id == "ohlc", do: Enum.map(members, &%{id: &1.id, partition: &1.partition}), else: []
      end)

      stub(RelayManager, :request_stop, fn _ns, id ->
        send(parent, {:stopped, id})
        :ok
      end)

      out =
        capture_io(fn -> Cli.call("Relay.stop", Jason.encode!(["default", "ohlc"]), []) end)

      assert out =~ "Relay group ohlc stopped (3 partitions)"
      assert_receive {:stopped, "ohlc#p0"}, 100
      assert_receive {:stopped, "ohlc#p1"}, 100
      assert_receive {:stopped, "ohlc#p2"}, 100
    end

    test "an ordinary relay takes the single-stop path" do
      stub(PartitionGroup, :members, fn _ns, _id -> [] end)
      stub(RelayManager, :request_stop, fn _ns, "plain" -> :ok end)

      assert capture_io(fn ->
               Cli.call("Relay.stop", Jason.encode!(["default", "plain"]), [])
             end) =~
               "Relay has been stopped"
    end

    test "a failed member is named, never swallowed" do
      members = for index <- 0..1, do: member_row("ohlc", index, 2)

      stub(PartitionGroup, :members, fn _ns, "ohlc" ->
        Enum.map(members, &%{id: &1.id, partition: &1.partition})
      end)

      stub(RelayManager, :request_stop, fn _ns, id ->
        if id == "ohlc#p1", do: {:error, {:relay_not_running, id}}, else: :ok
      end)

      out =
        capture_io(:stderr, fn ->
          try do
            Cli.call("Relay.stop", Jason.encode!(["default", "ohlc"]), [])
          catch
            :exit, _ -> :ok
          end
        end)

      assert out =~ "ohlc#p1 (relay 'ohlc#p1' is not running"
      assert out =~ "(relay.group_stop_partial)"
    end
  end

  describe "state get against a group" do
    setup do
      members = for index <- 0..2, do: member_row("ohlc", index, 3)

      stub(RelayManager, :namespace_of, fn _id -> :error end)
      stub(RelayManager, :namespace_of_group, fn "ohlc" -> {:ok, "trading"} end)

      stub(PartitionGroup, :members, fn _ns, "ohlc" ->
        Enum.map(members, &%{id: &1.id, partition: &1.partition})
      end)

      :ok
    end

    defp run_state_get(path) do
      {_outcome, out, _err} =
        CliCapture.run(fn -> Cli.call("Steps.state_get", Jason.encode!([path]), []) end)

      out
    end

    test "a keyed read goes to exactly the member owning the key's group" do
      parent = self()

      stub(RelayManager, :get_cell_state, fn "trading", id, _selector ->
        send(parent, {:read, id})
        {:ok, ~s({"candle":{"BTC-USD":{"high":42}}}), 7}
      end)

      out = run_state_get("ohlc/candle/BTC-USD")

      # BTC-USD hashes to key group 129, which p1 owns under ranges(3).
      assert_receive {:read, "ohlc#p1"}
      refute_received {:read, "ohlc#p0"}
      assert out =~ "BTC-USD"
    end

    test "a tasklet-level read fans out and merges the members' cells" do
      stub(RelayManager, :get_cell_state, fn "trading", id, _selector ->
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

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
  alias Flezha.Observability.RingBufferSink
  alias Flezha.Observability.TestSinks
  alias Flezha.Operator.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Service.Cli
  alias Flezha.Service.HubManager
  alias Flezha.Service.Operator
  alias Flezha.Service.OperatorManager
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

  # The rows `flz operator list` prints: a partition group collapsed into one.
  defp grouped(rows) do
    stub(HubManager, :operator_rows, fn _ns -> rows end)
    {:ok, listed} = Operator.list("default", %{source: :cli})
    listed
  end

  describe "a group in the operator list" do
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
      group = fn healths ->
        [%{health: health}] =
          healths
          |> Enum.with_index(fn h, i -> member_row("ohlc", i, length(healths), health: h) end)
          |> grouped()

        health
      end

      assert group.([:healthy, :recovering]) == :recovering
      assert group.([:degraded, :recovering]) == :recovering
      assert group.([:recovering, :parked]) == :parked
    end

    test "leaves ungrouped operators untouched" do
      rows = [plain_row("plain"), member_row("ohlc", 0, 2), member_row("ohlc", 1, 2)]

      assert [group, plain] = grouped(rows)
      assert group.id == "ohlc"
      assert plain == plain_row("plain")
      refute Map.has_key?(plain, :partitions)
    end
  end

  describe "operator info" do
    defp info_output(rows) do
      stub(HubManager, :operator_rows, fn _ns -> rows end)
      stub(OperatorManager, :get_operator_ids, fn _ns -> {:ok, Enum.map(rows, & &1.id)} end)

      stub(OperatorManager, :get_overwatch_state, fn ns, id ->
        {:error, {:operator_not_found, ns, id}}
      end)

      capture_io(fn -> Cli.call("Operator.info", Jason.encode!(["default"]), []) end)
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

      stub(OperatorManager, :get_operator_ids, fn _ns -> {:ok, Enum.map(rows, & &1.id)} end)

      stub(OperatorManager, :get_overwatch_state, fn ns, _id ->
        {:ok,
         %{
           runner_id: "r2",
           namespace: ns,
           extensions: %{},
           default_input_type: "bytes",
           calls: %Flezha.Operator.Calls{}
         }}
      end)

      stub(HubManager, :get_hub_nodes, fn _ns, true -> [node()] end)
      stub(HubManager, :recovery_state, fn _hub_id -> {:ok, :normal} end)
      %{rows: rows}
    end

    test "info reads recovering for the group's members and each operator", %{rows: rows} do
      ConvergenceFixtures.stub_registry_rows("trading", rows, 2_000)

      text = capture_io(fn -> Cli.call("Operator.info", Jason.encode!(["trading"]), []) end)
      assert text =~ ~r/p0  prices#p0 .*runner-0  recovering\n/
      assert length(Regex.scan(~r/Health: recovering\n/, text)) == 3
      assert text =~ "Health: healthy"
      refute text =~ "failed"

      json =
        Jason.decode!(
          capture_io(fn ->
            Cli.call("Operator.info", Jason.encode!(["trading"]), output: "json")
          end)
        )

      [%{"members" => members}] = json["groups"]
      assert Enum.map(members, & &1["health"]) == List.duplicate("recovering", 3)

      assert Map.new(json["operators"], &{&1["id"], &1["health"]}) ==
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

      assert json["operator_health"] == %{"recovering" => 3, "healthy" => 1}

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

      assert json["operator_health"] == %{"failed" => 3, "healthy" => 1}

      text = capture_io(fn -> Cli.call("Operator.info", Jason.encode!(["trading"]), []) end)
      assert length(Regex.scan(~r/Health: failed\n/, text)) == 3
      refute text =~ "recovering"
    end
  end

  describe "operator start rendering" do
    defp start(format, fields) do
      operator = Map.new([factory_id: "candles"] ++ fields)
      Cli.call("Operator.start", Jason.encode!(["default", operator]), output: format)
    end

    defp started_as(test_pid) do
      stub(OperatorManager, :start_operator, fn _ns, op, opts ->
        send(test_pid, {:started, op, opts})
        {:ok, %{operator_id: op.id, runner_id: "r1", node: node(), pid: self()}}
      end)
    end

    test "a start names the id it runs under" do
      started_as(self())

      assert capture_io(fn -> start("text", id: "demo") end) == "Started demo\n"
      assert_received {:started, %{id: "demo"}, []}
    end

    test "a first message rides the start as the instance's seed input" do
      started_as(self())

      capture_io(fn -> start("text", id: "demo", text: "hi", channel: "order") end)

      assert_received {:started, %{id: "demo", opts: opts}, _}
      assert opts[:seed_input] == {"hi", %{"channel" => "order"}}
    end

    test "a channel without a first message is refused before anything starts" do
      started_as(self())

      err = capture_io(:stderr, fn -> catch_exit(start("text", id: "demo", channel: "order")) end)

      assert err =~ "and no --text was given (operator.channel_without_text)"
      refute_received {:started, _, _}
    end

    test "a start without an id prints the id the controller generated" do
      started_as(self())

      out = capture_io(fn -> start("text", []) end)

      assert [_, generated] = Regex.run(~r/\AStarted (candles-[a-z2-7]{16})\n\z/, out)
      assert_received {:started, %{id: ^generated}, [generated: true]}
    end

    test "JSON output is the one object carrying the id and the outcome" do
      started_as(self())

      assert capture_io(fn -> start("json", id: "demo") end) ==
               ~s({"id":"demo","outcome":"started"}\n)
    end

    test "a live duplicate start reports a no-op instead of erroring" do
      stub(OperatorManager, :start_operator, fn _ns, _op, _opts -> :duplicate end)

      assert capture_io(fn -> start("text", id: "demo") end) ==
               "demo is already running, left untouched\n"

      assert capture_io(fn -> start("json", id: "demo") end) ==
               ~s({"id":"demo","outcome":"already_running"}\n)
    end

    test "the start's audit record names the id it ran under" do
      started_as(self())
      boundary = System.monotonic_time(:microsecond)

      out = capture_io(fn -> start("text", []) end)
      "Started " <> generated = String.trim(out)

      TestSinks.settle(RingBufferSink)

      assert Enum.any?(RingBufferSink.query(:operator_service, boundary), fn signal ->
               signal.name == :start and signal.metadata[:operator] == generated
             end)
    end

    test "a stale-row refusal still exits as an error" do
      stub(OperatorManager, :start_operator, fn _ns, _op, _opts ->
        {:error, {:id_in_use, "demo"}}
      end)

      out =
        capture_io(:stderr, fn ->
          assert catch_exit(start("text", id: "demo")) == {:shutdown, 6}
        end)

      assert out =~ "demo"
      assert out =~ "(operator.already_running)"
    end

    test "a converge reports started and already-running members distinctly" do
      stub(PartitionGroup, :start, fn _ns, _op, 3, _json ->
        {:ok,
         %{
           group: "ohlc",
           members: ["ohlc#p2"],
           already_running: ["ohlc#p0", "ohlc#p1"]
         }}
      end)

      out = capture_io(fn -> start("text", partitions: 3, id: "ohlc") end)

      assert out =~ "converged: started 1 missing member(s), 2 already running"
      assert out =~ "started ohlc#p2"
      assert out =~ "already running ohlc#p0"
    end

    test "a fully-present group re-run reports the no-op" do
      stub(PartitionGroup, :start, fn _ns, _op, 3, _json ->
        {:ok,
         %{
           group: "ohlc",
           members: [],
           already_running: ["ohlc#p0", "ohlc#p1", "ohlc#p2"]
         }}
      end)

      out = capture_io(fn -> start("text", partitions: 3, id: "ohlc") end)

      assert out =~ "already running with 3 partitions"
      assert out =~ "left untouched"
    end

    test "a fresh group start keeps its message" do
      stub(PartitionGroup, :start, fn _ns, _op, 3, _json ->
        {:ok, %{group: "ohlc", members: ["ohlc#p0", "ohlc#p1", "ohlc#p2"]}}
      end)

      out = capture_io(fn -> start("text", partitions: 3, id: "ohlc") end)

      assert out =~ "Operator ohlc started with 3 partitions:"
      assert out =~ "  ohlc#p0"
    end

    test "a group started without an id names the base it generated" do
      stub(PartitionGroup, :start, fn _ns, %{id: nil}, 3, _json ->
        {:ok,
         %{
           group: "candles-k7m2qzxw4pd3fn2a",
           members: Enum.map(0..2, &Partition.member_id("candles-k7m2qzxw4pd3fn2a", &1))
         }}
      end)

      out = capture_io(fn -> start("text", partitions: 3) end)
      assert out =~ "Operator candles-k7m2qzxw4pd3fn2a started with 3 partitions:"

      json = capture_io(fn -> start("json", partitions: 3) end)
      assert %{"id" => "candles-k7m2qzxw4pd3fn2a", "outcome" => "started"} = Jason.decode!(json)
    end
  end

  describe "operator scale rendering" do
    # The summary line reads the rescale result key by key, so a key the
    # producer stops returning raises on the success path rather than being
    # rendered blank.
    test "a rescale prints the summary the result can actually answer" do
      stub(PartitionGroup, :rescale, fn _ns, "ohlc", 3, _opts ->
        {:ok,
         %{
           group: "ohlc",
           from: 2,
           to: 3,
           cells_moved: 1,
           vacated: ["ohlc#p2"],
           sealed: [],
           members: ["ohlc#p0", "ohlc#p1", "ohlc#p2"]
         }}
      end)

      out =
        capture_io(fn ->
          Cli.call("Steps.rescale", Jason.encode!(["default", "ohlc", 3, %{}]), [])
        end)

      assert out =~ "Operator ohlc rescaled 2 -> 3"
      assert out =~ "1 cells moved, 1 trees removed"
      assert out =~ "  ohlc#p0"
      refute out =~ "Sealed"
    end

    test "a steps group's rescale names the members it sealed" do
      stub(PartitionGroup, :rescale, fn _ns, "orders", 3, _opts ->
        {:ok,
         %{
           group: "orders",
           from: 2,
           to: 3,
           cells_moved: 2,
           vacated: [],
           sealed: ["orders#p0", "orders#p1"],
           members: ["orders#p0", "orders#p1", "orders#p2"]
         }}
      end)

      out =
        capture_io(fn ->
          Cli.call("Steps.rescale", Jason.encode!(["default", "orders", 3, %{}]), [])
        end)

      assert out =~ "Sealed orders#p0, orders#p1 at their latest committed iteration"
      assert out =~ "no rewind or rollback reaches a pass committed before this rescale"
    end
  end

  describe "operator stop" do
    test "a group id stops every member" do
      members = for index <- 0..2, do: member_row("ohlc", index, 3)
      parent = self()

      stub(PartitionGroup, :members, fn _ns, id ->
        if id == "ohlc", do: Enum.map(members, &%{id: &1.id, partition: &1.partition}), else: []
      end)

      stub(OperatorManager, :request_stop, fn _ns, id ->
        send(parent, {:stopped, id})
        :ok
      end)

      out =
        capture_io(fn -> Cli.call("Operator.stop", Jason.encode!(["default", "ohlc"]), []) end)

      assert out =~ "Operator group ohlc stopped (3 partitions)"
      assert_receive {:stopped, "ohlc#p0"}, 100
      assert_receive {:stopped, "ohlc#p1"}, 100
      assert_receive {:stopped, "ohlc#p2"}, 100
    end

    test "an ordinary operator takes the single-stop path" do
      stub(PartitionGroup, :members, fn _ns, _id -> [] end)
      stub(OperatorManager, :request_stop, fn _ns, "plain" -> :ok end)

      assert capture_io(fn ->
               Cli.call("Operator.stop", Jason.encode!(["default", "plain"]), [])
             end) =~
               "Operator has been stopped"
    end

    test "a failed member is named, never swallowed" do
      members = for index <- 0..1, do: member_row("ohlc", index, 2)

      stub(PartitionGroup, :members, fn _ns, "ohlc" ->
        Enum.map(members, &%{id: &1.id, partition: &1.partition})
      end)

      stub(OperatorManager, :request_stop, fn _ns, id ->
        if id == "ohlc#p1", do: {:error, {:operator_not_running, id}}, else: :ok
      end)

      out =
        capture_io(:stderr, fn ->
          try do
            Cli.call("Operator.stop", Jason.encode!(["default", "ohlc"]), [])
          catch
            :exit, _ -> :ok
          end
        end)

      assert out =~ "ohlc#p1 (operator 'ohlc#p1' is not running"
      assert out =~ "(operator.group_stop_partial)"
    end
  end

  describe "state get against a group" do
    setup do
      members = for index <- 0..2, do: member_row("ohlc", index, 3)

      stub(OperatorManager, :namespace_of, fn _id -> :error end)
      stub(OperatorManager, :namespace_of_group, fn "ohlc" -> {:ok, "trading"} end)

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

      stub(OperatorManager, :get_cell_state, fn "trading", id, _selector ->
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
      stub(OperatorManager, :get_cell_state, fn "trading", id, _selector ->
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

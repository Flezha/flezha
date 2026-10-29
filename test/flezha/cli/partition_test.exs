defmodule Flezha.Service.Cli.PartitionRenderTest do
  @moduledoc """
  How `flz relay start` and `flz relay scale` render a partition group's
  outcome. The group behaviour these verbs reach is pinned in
  `test/flezha/service/relay_partition_group_test.exs`.
  """
  # async: false: Mimic global mode; Mimic stubs; the live controller and its fleet.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO
  import Mimic
  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Observability.RingBufferSink
  alias Flezha.Observability.TestSinks
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Partition
  alias Flezha.Service.Cli
  alias Flezha.Service.RelayManager

  setup :set_mimic_global_until_exit

  describe "relay start rendering" do
    defp start(format, fields) do
      relay = Map.new([factory_id: "candles"] ++ fields)
      Cli.call("Relay.start", Jason.encode!(["default", relay]), output: format)
    end

    defp started_as(test_pid) do
      stub(RelayManager, :start_relay, fn _ns, op, opts ->
        send(test_pid, {:started, op, opts})
        {:ok, %{relay_id: op.id, runner_id: "r1", node: node(), pid: self()}}
      end)
    end

    test "a start names the id it runs under" do
      started_as(self())

      assert capture_io(fn -> start("text", id: "demo") end) == "Started demo\n"
      assert_received {:started, %{id: "demo"}, [origin: %{kind: "operator"}]}
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

      assert err =~ "and no --text was given (relay.channel_without_text)"
      refute_received {:started, _, _}
    end

    test "a start without an id prints the id the controller generated" do
      started_as(self())

      out = capture_io(fn -> start("text", []) end)

      assert [_, generated] = Regex.run(~r/\AStarted (candles-[a-z2-7]{16})\n\z/, out)

      assert_received {:started, %{id: ^generated},
                       [generated: true, origin: %{kind: "operator"}]}
    end

    test "JSON output is the one object carrying the id and the outcome" do
      started_as(self())

      assert capture_io(fn -> start("json", id: "demo") end) ==
               ~s({"id":"demo","outcome":"started"}\n)
    end

    test "a live duplicate start reports a no-op instead of erroring" do
      stub(RelayManager, :start_relay, fn _ns, _op, _opts -> :duplicate end)

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

      assert Enum.any?(RingBufferSink.query(:relay_service, boundary), fn signal ->
               signal.name == :start and signal.metadata[:relay] == generated
             end)
    end

    test "a stale-row refusal still exits as an error" do
      stub(RelayManager, :start_relay, fn _ns, _op, _opts ->
        {:error, {:id_in_use, "demo"}}
      end)

      out =
        capture_io(:stderr, fn ->
          assert catch_exit(start("text", id: "demo")) == {:shutdown, 6}
        end)

      assert out =~ "demo"
      assert out =~ "(relay.already_running)"
    end

    test "a converge reports started and already-running members distinctly" do
      stub(PartitionGroup, :start, fn _ns, _op, 3, _json, [origin: %{kind: "group"}] ->
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
      stub(PartitionGroup, :start, fn _ns, _op, 3, _json, [origin: %{kind: "group"}] ->
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
      stub(PartitionGroup, :start, fn _ns, _op, 3, _json, [origin: %{kind: "group"}] ->
        {:ok, %{group: "ohlc", members: ["ohlc#p0", "ohlc#p1", "ohlc#p2"]}}
      end)

      out = capture_io(fn -> start("text", partitions: 3, id: "ohlc") end)

      assert out =~ "Relay ohlc started with 3 partitions:"
      assert out =~ "  ohlc#p0"
    end

    test "a group started without an id names the base it generated" do
      stub(PartitionGroup, :start, fn _ns, %{id: nil}, 3, _json, [origin: %{kind: "group"}] ->
        {:ok,
         %{
           group: "candles-k7m2qzxw4pd3fn2a",
           members: Enum.map(0..2, &Partition.member_id("candles-k7m2qzxw4pd3fn2a", &1))
         }}
      end)

      out = capture_io(fn -> start("text", partitions: 3) end)
      assert out =~ "Relay candles-k7m2qzxw4pd3fn2a started with 3 partitions:"

      json = capture_io(fn -> start("json", partitions: 3) end)
      assert %{"id" => "candles-k7m2qzxw4pd3fn2a", "outcome" => "started"} = Jason.decode!(json)
    end
  end

  describe "relay scale rendering" do
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

      assert out =~ "Relay ohlc rescaled 2 -> 3"
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
end

defmodule Flezha.SDK.RecordHoldTest do
  @moduledoc """
  Every frame the SDK writes to a secondary stream that no stream will take: an
  Entity's records, the delete an evicted one ships, and the workflow driver's
  writes the runner relays. Each is held and replayed on the next bound stream,
  in the order it was written, still ahead of the outputs of its own dispatch —
  and the credit window is debited by the write rather than by the hold.
  """
  # async: false: the reason WAS `FLEZHA_RECONNECT_RING_CAPACITY`, and the cap
  # tests now state their bound instead of writing it, so that reason is gone
  # and no other has been established. The module keeps its own Operatives,
  # Entities, counters and streams, and its Mimic stub is private to the test
  # process, so nothing here is known to be shared. Flipping it is a deliberate
  # change to the async budget and is nobody's side effect.
  use ExUnit.Case, async: false
  use Mimic

  import FlezhaTest.Helper.SdkOperativeHarness

  alias Flezha.SDK.{Codec, Counters, Credit, Entity, Hold, Record, RunnerServer}
  alias Flezha.SDK.Operative.Inbox
  alias Flezha.SDK.RunnerServer.State, as: ServerState
  alias Flezha.SDK.Wire
  alias Flezha.SDK.LocalStream
  alias FlezhaTest.Helper.SdkOperativeHarness
  alias FlezhaTest.Helper.SdkPersistenceFixtures.{CounterOp, OptedOutCounterOp, TimerOp}

  # The saga marker the workflow driver ships on the record wire: a real payload
  # id, because the class table refuses one it does not declare.
  @saga_marker Wire.payload_id_compensation_intent()

  # One input into `CounterOp` is one dispatch that writes a change record and
  # then a sink output — the two frames whose order the spec governs.
  defp feed(op, n), do: Inbox.input(op, "in", Integer.to_string(n), %{})

  defp kinds(n), do: n |> frames() |> Enum.map(&kind/1)

  # Every dispatch is finished and every output already cast to the router: a
  # read answers only behind the queued inputs, and a cast is in the router's
  # mailbox before the dispatch that sent it returns. Without it a rebind can
  # overtake the dispatches it is meant to follow.
  defp settled(op, procedure, seq),
    do: assert({:ok, _state, ^seq} = Entity.read(entity!(op, procedure), 1_000))

  # A secondary this process owns whose stream is already gone: what a rebind
  # onto a wire that died again hands the Entity.
  defp dead_secondary do
    {sdk, _ctrl} = SdkOperativeHarness.own_secondary()
    :ok = close_secondary(sdk)
    sdk
  end

  describe "a record no stream would take" do
    test "keeps its dispatch's timer-ops record behind its change, in seq order" do
      %{op: op} = start_operative(TimerOp)
      send(op, {:secondaries, []})

      feed(op, 1)
      rebind_secondary(op)

      assert [{:change, 0, {:delta, 1}}, {:timer_ops, 1, [%{register: true}]}, {:output, _, _, _}] =
               kinds(3)
    end
  end

  test "a replayed output never arrives without the record of the change that produced it" do
    %{op: op, sdk_secondary: sdk} = start_operative(CounterOp)
    :ok = close_secondary(sdk)

    for n <- 1..3, do: feed(op, n)
    settled(op, "counter", 3)
    rebind_secondary(op)

    # Each hold drains as a run, so the records arrive as a block ahead of the
    # outputs; what the spec governs is that no dispatch's output precedes its
    # own record, and that is what this order says.
    assert [
             {:change, 0, {:delta, 1}},
             {:change, 1, {:delta, 2}},
             {:change, 2, {:delta, 3}},
             {:output, "op-1", "1", _},
             {:output, "op-1", "3", _},
             {:output, "op-1", "6", _}
           ] = kinds(6)
  end

  test "the hold is bounded, and what it sheds is counted where every drop-oldest is" do
    %{op: op, counters: counters} = start_operative(CounterOp, hold_cap: 2)
    send(op, {:secondaries, []})
    for n <- 1..5, do: feed(op, n)
    settled(op, "counter", 5)

    rebind_secondary(op)

    # Drop-oldest: the last two records survive, and the three shed are counted
    # rather than lost in silence. The Operative's own hold sheds three outputs
    # at the same cap, so the counter carries six.
    assert [{:change, 3, {:delta, 4}}, {:change, 4, {:delta, 5}} | _] = kinds(2)
    assert Counters.drops_oldest(counters) == 6
  end

  test "a cap of zero is not a cap, on the caller's path as on the variable's" do
    # `FLEZHA_RECONNECT_RING_CAPACITY=0` is refused by the knob and the default
    # stands. `cap: 0` is truthy in Elixir, so without the same fallback it
    # reached `:queue.drop/1` on an empty queue and raised on the first frame.
    hold = Hold.new(cap: 0)

    assert hold.cap == Hold.cap()
    assert hold |> Hold.push(:x) |> Hold.member?(:x)
  end

  test "the credit window is debited by the write, never by the hold" do
    counters = Counters.new()
    {:ok, credit} = Credit.start_link(counters: counters)
    %{op: op} = start_operative(CounterOp, credit: credit)

    # A window one byte wide: a record that debited on the way into the hold
    # would park the Entity here, waiting on a controller that is not there.
    Credit.arm(credit, "session-1", 1)
    assert Credit.session(credit) == "session-1"

    send(op, {:secondaries, []})
    for n <- 1..3, do: feed(op, n)
    assert :ok = Entity.rebind(entity!(op, "counter"), [])
    assert Counters.credit_parked(counters) == 0

    # A reconnect disarms the session before it re-establishes the sockets, so
    # the replay's own debit belongs to the next window, never to this one twice.
    Credit.disarm(credit)
    rebind_secondary(op)

    assert [{:change, 0, _}, {:change, 1, _}, {:change, 2, _} | _] = kinds(4)
  end

  test "compaction still ships the snapshot below its re-stated timers after a replay" do
    %{op: op} = start_operative(TimerOp)
    send(op, {:secondaries, []})

    feed(op, 1)
    rebind_secondary(op)
    assert [{:change, 0, _}, {:timer_ops, 1, _}, {:output, _, _, _}] = kinds(3)

    pid = entity!(op, "timer")
    assert {:ok, 1, _size} = Entity.snapshot(pid)
    [{"entity_state_snapshot", frame, _}, restated] = frames(2)

    # The snapshot folds every seq the replay put on the wire — `change_seq - 1`
    # — and the pending timers are re-stated above it, exactly as they are when
    # nothing was ever held.
    assert {:ok, %{seq: 1, body: body}} = Wire.decode_frame(frame)
    assert {:ok, %{value: 1, fired: []}} = Record.decode_body(body)
    assert {2, [%{kind: :processing_time, register: true}], _} = timer_ops(restated)
  end

  test "an eviction whose delete the wire refuses is replayed on the next bound stream" do
    %{op: op, sdk_secondary: sdk} = start_operative(CounterOp)
    feed(op, 1)
    settled(op, "counter", 1)
    assert [{:change, 0, {:delta, 1}}, {:output, "op-1", "1", _}] = kinds(2)

    # Nothing is held here: the dispatch went out on a live stream. What the
    # eviction cannot ship is the delete itself.
    :ok = close_secondary(sdk)
    assert Entity.evict(entity!(op, "counter")) == :evicted

    rebind_secondary(op)
    assert [{:delete, {"op-1", "counter", ""}}] = kinds(1)
  end

  test "an Entity evicted while holding hands its records and its delete on" do
    %{op: op} = start_operative(CounterOp)
    send(op, {:secondaries, []})
    feed(op, 2)
    settled(op, "counter", 1)

    entity = entity!(op, "counter")
    ref = Process.monitor(entity)
    assert Entity.evict(entity) == :evicted
    assert_receive {:DOWN, ^ref, :process, ^entity, :normal}, 1_000

    rebind_secondary(op)
    feed(op, 7)

    # The dead Entity's frames go first, so its record still precedes the
    # output of the dispatch that produced it and its delete still precedes
    # anything the replacement Entity writes.
    assert [
             {:change, 0, {:delta, 2}},
             {:delete, {"op-1", "counter", ""}},
             {:output, "op-1", "2", _},
             {:change, 0, {:delta, 7}},
             {:output, "op-1", "7", _}
           ] = kinds(5)
  end

  test "a class that persists nothing owes no delete and hands on nothing" do
    %{op: op} = start_operative(OptedOutCounterOp)
    send(op, {:secondaries, []})
    feed(op, 2)
    settled(op, "counter", 1)
    assert Entity.evict(entity!(op, "counter")) == :evicted

    rebind_secondary(op)

    # Nothing was written and no delete is owed, so the only frame the rebind
    # brings back is the output the Operative held for itself. A handed-on
    # delete would be replayed BEFORE that output, so this position is the
    # assertion that no delete exists — there is nothing to refute.
    assert [{:output, "op-1", "2", _}] = kinds(1)
  end

  describe "the workflow driver's writes, which the runner ships" do
    test "a record and an output cast with no stream bound are held and replayed" do
      s = runner_state([])

      s = cast(s, {:record, @saga_marker, <<1, 2, 3>>})
      s = cast(s, {:emit_output, "op-1", %{"channel" => "c"}, "body"})

      bind(s)

      assert [
               {@saga_marker, <<1, 2, 3>>, %{}},
               {"op-1", "body", %{"channel" => "c"}}
             ] = frames(2)
    end

    test "a write the wire refuses is held rather than discarded" do
      s = runner_state([dead_secondary()])

      s = cast(s, {:record, @saga_marker, <<4>>})
      s = cast(s, {:emit_output, "op-1", %{}, "body"})

      bind(s)

      assert [{@saga_marker, <<4>>, %{}}, {"op-1", "body", %{}}] = frames(2)
    end

    test "the hold is bounded, and what it sheds is counted where every drop-oldest is" do
      counters = Counters.new()
      s = runner_state([], counters, 2)
      s = Enum.reduce(1..5, s, &cast(&2, {:record, @saga_marker, <<&1>>}))

      bind(s)

      assert [{@saga_marker, <<4>>, %{}}, {@saga_marker, <<5>>, %{}}] =
               frames(2)

      assert Counters.drops_oldest(counters) == 3
    end
  end

  # The runner's own state, built by hand as `commands_test.exs` builds it: no
  # controller, and a control handle whose peer nothing owns, so only the data
  # frames the replay writes reach this process.
  defp runner_state(secondaries, counters \\ Counters.new(), cap \\ nil) do
    {control_handle, _peer} = LocalStream.open()

    %ServerState{
      transport: :native,
      session_token: "runner-hold",
      counters: counters,
      registry: :ets.new(:runner_hold_registry, [:public, :set]),
      control_handle: control_handle,
      secondaries: secondaries,
      held: Hold.new(cap: cap, counters: counters),
      control: Hold.new(cap: cap, counters: counters)
    }
  end

  defp cast(s, message) do
    {:noreply, s} = RunnerServer.handle_cast(message, s)
    s
  end

  # The controller's `ConnectSocket`, which is what binds fresh secondaries on
  # a reconnect. The bind is stubbed to hand this process the controller side
  # of each pair, so the replay's frames arrive here.
  defp bind(s) do
    me = self()

    stub(Flezha.Transport.Server, :bind_data_stream, fn _runner_id, type, handle ->
      :ok = Flezha.Transport.Stream.controlling_process(handle, me)
      :ok = Flezha.Transport.Stream.set_active(handle)
      {:ok, if(type == 1, do: :primary, else: :secondary), "s"}
    end)

    connect = %RunnerCommands.ConnectSocket{sockets_count: 1}
    command = Codec.encode_request("ref-1", {:connect_socket, connect})

    {:noreply, s} = RunnerServer.handle_info({:flz_data, 0, "control_command", command, %{}}, s)
    s
  end
end

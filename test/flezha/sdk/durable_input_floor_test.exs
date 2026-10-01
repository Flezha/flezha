defmodule Flezha.SDK.DurableInputFloorTest do
  @moduledoc """
  The durable input floor in the SDK's own core: the offset a Cell off the
  tier keeps in its record, the floor it drops a re-forward at, the
  processed-prefix marker its Operator reports behind what it settled, the
  inter-tasklet values a tier record keeps for a restore to deliver again, and
  the identity a tasklet's spawn carries across a re-run of its input.
  """
  use ExUnit.Case, async: true

  import FlezhaTest.Helper.SdkOperatorHarness

  alias Flezha.SDK.{Cell, Channels, Ctx, Record, Spawn}
  alias Flezha.SDK.Operator.Inbox
  alias FlezhaTest.Helper.SdkSpawnFixtures.SpawnerOp

  defmodule Store do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "store"
    @impl true
    def init_state(_), do: %{}

    defchange set(s, k, v) do
      Map.put(s, k, v)
    end

    # A `hold:<pid>` tells that test process it is inside `run`, then blocks
    # on its own mailbox until the test releases it; the timeout is the
    # fixture's safety net.
    @impl true
    def run("hold:" <> test, s, _ctx) do
      send(:erlang.list_to_pid(String.to_charlist(test)), {:holding, self()})

      receive do
        :release -> :ok
      after
        5_000 -> :ok
      end

      {:ok, s}
    end

    def run("poison", _s, _ctx), do: {:error, {:non_retryable, "poison value"}}

    def run(input, s, ctx) do
      [k, v] = String.split(input, "=", parts: 2)
      {:ok, set(s, ctx, k, v)}
    end
  end

  defmodule SmallStore do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "small_store", mailbox_capacity: 1
    @impl true
    def init_state(key), do: Store.init_state(key)

    defchange set(s, k, v) do
      Map.put(s, k, v)
    end

    @impl true
    def run(input, s, ctx), do: Store.run(input, s, ctx)
  end

  defmodule Splitter do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "splitter"

    @impl true
    def run(input, s, ctx) do
      for part <- String.split(input, ","), do: emit(ctx, part)
      {:ok, s}
    end
  end

  defmodule Relay do
    @moduledoc false
    use Flezha.SDK.Tasklet, name: "relay"
    @impl true
    def init_state(_), do: 0

    defchange relayed(count) do
      count + 1
    end

    @impl true
    def run(input, count, ctx) do
      emit(ctx, input)
      {:ok, relayed(count, ctx)}
    end
  end

  defmodule RelayOp do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "floor_relay"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(Relay)
        |> Channels.named("relayed")
        |> Channels.into(Store)
  end

  defmodule StoreOp do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "floor_store"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.key_by(&String.first/1)
        |> Channels.into(Store)
  end

  defmodule DropOp do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "floor_drop"

    def channels(c),
      do: c |> Channels.input("in") |> Channels.on_full(:drop_newest) |> Channels.into(SmallStore)
  end

  defmodule SplitOp do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "floor_split"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(Splitter)
        |> Channels.named("parts")
        |> Channels.into(Store)
  end

  defmodule TierSplitOp do
    @moduledoc false
    @behaviour Flezha.SDK.Operator
    def factory_id, do: "floor_tier_split"
    def run, do: {:flow, exactly: :effectively_once}

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(Splitter)
        |> Channels.named("parts")
        |> Channels.into(Store)
        |> Channels.named("stored")
  end

  defp push(op, input, offset),
    do: Inbox.input(op, "in", input, %{"flz.offset" => Integer.to_string(offset)})

  # The identity of the spawn request `ctx` casts to this process as its runner.
  defp spawned(ctx, factory, payload) do
    Ctx.spawn_steps(ctx, factory, payload)
    assert_receive {:"$gen_cast", {:spawn, _caller, %Spawn{identity: identity}, ^payload, _}}
    identity
  end

  defp ctx(origin), do: Ctx.new(Store, nil, 0, runner: self(), origin: origin)

  # Push a `hold` at `offset` and wait until its Cell is inside `run`; the Cell.
  defp hold(op, offset) do
    push(op, "hold:#{:erlang.pid_to_list(self())}", offset)
    assert_receive {:holding, cell}, 2_000
    cell
  end

  # Markers until one reports `prefix`; each one on the way reports less.
  defp marker_reaching(prefix) do
    case marker() do
      {^prefix, 1} -> :ok
      {earlier, 1} when earlier < prefix -> marker_reaching(prefix)
    end
  end

  describe "off the tier" do
    test "a Cell that records keeps each input's offset in the record of the state it produced" do
      %{op: op} = start_operator(StoreOp)
      push(op, "x=1", 3)

      assert [record] = frames(1)
      assert {0, %{consumed_offset: 3, changes: [change]}} = effect(record)
      assert {:ok, {:set, "x", "1"}} = Record.decode_body(change)
    end

    test "a re-forward of an input the Cell took is dropped, and a restore takes the floor from the records" do
      %{op: op} = start_operator(StoreOp)
      push(op, "x=1", 3)
      assert [_record] = frames(1)
      push(op, "x=2", 3)
      pid = cell!(op, "store", "x")
      assert {:ok, %{"x" => "1"}, 1} = Cell.read(pid, 1_000)

      log =
        effect_record(0, %{
          changes: [Record.encode_body({:set, "x", "seed"})],
          outputs: [],
          consumed_offset: 5
        })

      assert {:ok, 0, false} = Cell.restore(pid, <<>>, <<>>, log, 0)
      push(op, "x=3", 5)
      push(op, "x=4", 6)
      assert [record] = frames(1)
      assert {1, %{consumed_offset: 6}} = effect(record)
      assert {:ok, %{"x" => "4"}, 2} = Cell.read(pid, 1_000)
    end

    test "every value a producer emitted for one input reaches its consumer" do
      %{op: op} = start_operator(SplitOp)
      push(op, "x=1,y=2", 4)

      assert [_, _] = frames(2)
      assert {:ok, %{"x" => "1", "y" => "2"}, 2} = Cell.read(cell!(op, "store"), 1_000)
    end

    test "a producer's record keeps the values it emitted, and a re-delivered one reaches its consumer" do
      %{op: op} = start_operator(RelayOp)
      push(op, "x=1", 4)

      assert [%{origin: 4} = edge] =
               frames(2) |> Enum.map(&effect/1) |> Enum.flat_map(fn {_seq, b} -> b.edges end)

      %{op: fresh} = start_operator(RelayOp, operator_id: "op-2")
      named = %{"flz.edge" => "#{edge.channel}:#{edge.consumer}", "flz.offset" => "4"}
      Inbox.input(fresh, "in", edge.payload, named)

      assert [_record] = frames(1)
      assert {:ok, %{"x" => "1"}, 1} = Cell.read(cell!(fresh, "store"), 1_000)
      assert {:ok, 0, 0} = Cell.read(cell!(fresh, "relay"), 1_000)
    end

    test "a park rides the record with the offset of the input it parked" do
      %{op: op} = start_operator(StoreOp)
      push(op, "poison", 7)

      assert [record] = frames(1)
      assert {0, %{consumed_offset: 7, dlq: %{error: "poison value"}}} = effect(record)
    end
  end

  describe "the processed-prefix marker" do
    test "goes out behind the record its input settled into" do
      %{op: op} = start_operator(StoreOp)
      push(op, "x=1", 5)

      assert_receive {:flz_data, _id, payload_id, _frame, _meta}, 2_000
      assert payload_id == "cell_effect_record"
      assert {5, 1} = marker()
    end

    test "a pending input holds it below itself until its Cell settles the input" do
      %{op: op} = start_operator(StoreOp)
      held = hold(op, 1)
      push(op, "x=1", 2)
      assert [_record] = frames(1)
      assert {0, 1} = marker()

      send(held, :release)
      assert :ok = marker_reaching(2)
    end

    test "a dropped input releases its hold" do
      %{op: op} = start_operator(DropOp)
      pid = hold(op, 1)
      push(op, "x=1", 2)
      push(op, "x=2", 3)

      send(pid, :release)
      assert :ok = marker_reaching(3)
      assert {:ok, %{"x" => "1"}, 1} = Cell.read(pid, 1_000)
    end
  end

  describe "an inter-tasklet value under the tier" do
    test "rides its producer's record, and a re-delivered one reaches its consumer" do
      %{op: op} = start_operator(TierSplitOp)
      push(op, "x=1,y=2", 4)

      edges =
        frames(3)
        |> Enum.map(&effect/1)
        |> Enum.flat_map(fn {_seq, body} -> body.edges end)

      assert [%{origin: 4} = x, %{origin: 4} = y] = edges
      assert {:ok, "x=1"} = Record.decode_body(x.payload)
      assert {:ok, "y=2"} = Record.decode_body(y.payload)

      %{op: fresh} = start_operator(TierSplitOp, operator_id: "op-2")
      named = %{"flz.edge" => "#{x.channel}:#{x.consumer}", "flz.offset" => "4"}
      Inbox.input(fresh, "in", x.payload, named)

      assert [record] = frames(1)
      assert {0, %{consumed_offset: 4}} = effect(record)
      assert {:ok, %{"x" => "1"}, 1} = Cell.read(cell!(fresh, "store"), 1_000)
    end
  end

  describe "a tasklet's spawn identity" do
    test "names the origin, the call's position, the factory and the payload, as every core does" do
      first = ctx({"cell-a", 7})
      assert spawned(first, "worker", "p") == "ea4e9a44a7709f773c348f9751fccf29"
      second_call = spawned(first, "worker", "p")

      again = ctx({"cell-a", 7})
      assert spawned(again, "worker", "p") == "ea4e9a44a7709f773c348f9751fccf29"

      others = [
        second_call,
        spawned(ctx({"cell-a", 8}), "worker", "p"),
        spawned(ctx({"cell-a", 7}), "other", "p"),
        spawned(ctx({"cell-a", 7}), "worker", "q")
      ]

      assert Enum.uniq(others) == others
      refute "ea4e9a44a7709f773c348f9751fccf29" in others
    end

    test "is empty for an invocation that took no offset" do
      assert spawned(ctx(nil), "worker", "p") == ""
    end

    test "comes from the input the Cell's invocation took" do
      %{op: op} = start_operator(SpawnerOp)
      Inbox.input(op, "in", "unread:w1", %{"flz.offset" => "7"})
      assert_receive {:"$gen_cast", {:spawn, _, %Spawn{identity: identity}, _, _}}, 2_000
      assert identity =~ ~r/^[0-9a-f]{32}$/

      Inbox.input(op, "in", "unread:w2", %{})
      assert_receive {:"$gen_cast", {:spawn, _, %Spawn{identity: ""}, _, _}}, 2_000
    end
  end
end

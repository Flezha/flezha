defmodule Flezha.SDK.UnitTest do
  use ExUnit.Case, async: true

  alias Flezha.Identificator
  alias Flezha.SDK.{Channels, Codec, Ctx, Declarations, KeyGroup, PersistentId, Topology}
  alias Flezha.SDK.Proto.RunnerCommands
  alias Flezha.SDK.Transport
  alias FlezhaTest.Helper.SdkTierFixtures.{CheckpointBatchedOp, TierStoreOp}

  # A ctx built without a procedure has no name to refuse with; the tests
  # that build one declare the policy themselves.
  @timer_policy %{procedure: "unit", cap: 10_000, clock: :wall}

  # --- fixtures ---

  defmodule EchoProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "echo"
    def init_state(_), do: nil

    def run(input, state, ctx),
      do:
        (
          Ctx.emit(ctx, input)
          {:ok, state}
        )
  end

  defmodule EchoOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "echo_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(EchoProc)
        |> Channels.named("echo")
        |> Channels.sink()
  end

  defmodule KeyedProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "keyed"
    def init_state(_), do: 0
    def run(_input, state, _ctx), do: {:ok, state + 1}
  end

  defmodule DynamicOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "dyn_op"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.filter(fn s -> s != "drop" end)
      |> Channels.key_by(fn s -> s end)
      |> Channels.into(KeyedProc)
      |> Channels.named("k")
      |> Channels.sink()
    end
  end

  defmodule EventTimeProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "event_time_proc"
    def init_state(_), do: 0
    def clock, do: :event_time
    def event_time_of(input), do: String.to_integer(input)
    def apply_change(s, _), do: s
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule StaticProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "static"
    def init_state(_), do: nil
    def run(_i, s, _c), do: {:ok, s}
    def entity_keys, do: ["a", "b"]
  end

  defmodule StaticOp do
    @behaviour Flezha.SDK.StreamOperative
    def factory_id, do: "static_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(StaticProc)
        |> Channels.named("s")
        |> Channels.sink()
  end

  # --- entity mode derivation ---

  test "a linear echo topology compiles Single and keyless" do
    assert {:ok, topo} = Topology.build(EchoOp)
    assert topo.factory_id == "echo_op"
    assert [mode] = Map.values(topo.node_mode)
    assert mode == :single
    assert topo.keyless?
  end

  test "a key_by edge into a keyless procedure is Dynamic and not keyless" do
    assert {:ok, topo} = Topology.build(DynamicOp)
    assert [mode] = Map.values(topo.node_mode)
    assert mode == :dynamic
    refute topo.keyless?
  end

  test "a procedure declaring entity_keys is Static" do
    assert {:ok, topo} = Topology.build(StaticOp)
    assert [mode] = Map.values(topo.node_mode)
    assert mode == :static
  end

  # --- transform fusion ---

  test "transforms fuse to one closure per edge — 3 inputs, 3 predicate calls" do
    {:ok, agent} = Agent.start_link(fn -> 0 end)

    chain = [
      {:filter,
       fn _v ->
         Agent.update(agent, &(&1 + 1))
         true
       end},
      {:map, fn v -> v * 2 end}
    ]

    {transform, key_by} = Channels.fuse(chain)
    assert key_by == nil
    assert Enum.map([1, 2, 3], fn v -> transform.(v) end) == [[2], [4], [6]]
    assert Agent.get(agent, & &1) == 3
  end

  test "filter drops, flat_map expands, in one fused closure" do
    {transform, _} = Channels.fuse([{:filter, &(&1 > 0)}, {:flat_map, fn v -> [v, v] end}])
    assert transform.(5) == [5, 5]
    assert transform.(-1) == []
  end

  # --- builder refusals ---
  #
  # Only the shapes no row of `test/fixtures/reference_topologies.txt` states
  # are asserted here: the names the reference vocabulary does not carry, and
  # the ways this SDK's own surface composes. Every `{:invalid_topology,
  # [Name]}` case whose shape is a fixture row is retired — the conformance run
  # drives those rows through this builder.

  test "UnservableClock, EventTimeClockWithoutExtractor, UnknownClock — a clock the runtime cannot serve" do
    defmodule LogicalNoChange do
      @behaviour Flezha.SDK.Procedure
      def name, do: "logical_no_change"
      def clock, do: :logical
      def init_state(_), do: nil
      def run(_i, s, _ctx), do: {:ok, s}
    end

    defmodule EventTimeNoExtractor do
      @behaviour Flezha.SDK.Procedure
      def name, do: "event_time_no_extractor"
      def clock, do: :event_time
      def init_state(_), do: nil
      def run(_i, s, _ctx), do: {:ok, s}
    end

    defmodule Sundial do
      @behaviour Flezha.SDK.Procedure
      def name, do: "sundial"
      def clock, do: :sundial
      def init_state(_), do: nil
      def run(_i, s, _ctx), do: {:ok, s}
    end

    defmodule ClockOp do
      def factory_id, do: "clock_op"

      def channels(c) do
        c |> Channels.input("a") |> Channels.into(LogicalNoChange)
        c |> Channels.input("b") |> Channels.into(EventTimeNoExtractor)
        c |> Channels.input("c") |> Channels.into(Sundial)
      end
    end

    assert {:error, {:invalid_topology, issues}} = Topology.build(ClockOp)
    assert {:UnservableClock, inspect(LogicalNoChange)} in issues
    assert {:EventTimeClockWithoutExtractor, inspect(EventTimeNoExtractor)} in issues
    assert {:UnknownClock, inspect(Sundial)} in issues
    assert length(issues) == 3
  end

  test "KeyedOnEntry — an entry channel has no output of its own to stamp" do
    defmodule KeyedEntry do
      def factory_id, do: "keyed_entry"
      def channels(c), do: c |> Channels.input("in") |> Channels.keyed() |> Channels.sink()
    end

    assert {:error, {:invalid_topology, [{:KeyedOnEntry, "in"}]}} = Topology.build(KeyedEntry)
  end

  # The two names this SDK gains from the reference vocabulary here. Both are
  # rows of `test/fixtures/reference_topologies.txt`, so this test is the one
  # to retire once the conformance run drives those rows through this builder;
  # until then it is their only reader.
  test "the two refusals this SDK gains from the reference vocabulary" do
    defmodule KeyedNoWire do
      def factory_id, do: "keyed_no_wire"

      def channels(c),
        do:
          c
          |> Channels.input("in")
          |> Channels.into(EchoProc)
          |> Channels.keyed()
          |> Channels.into(KeyedProc)
    end

    defmodule SyncStream do
      def factory_id, do: "sync_stream"
      def sync_durability, do: true

      def channels(c),
        do: c |> Channels.input("in") |> Channels.into(EchoProc) |> Channels.sink()
    end

    assert {:KeyedChannelWithoutPluginWire, "echo"} = one_issue(KeyedNoWire)
    assert {:SyncDurabilityOnStream, _} = one_issue(SyncStream)
  end

  test "the ctx refuses a keyless emit where a key is required, and a key where none is allowed" do
    single =
      Ctx.new(nil, nil, 0,
        channel: "c",
        keyed_sink?: true,
        key_required?: true,
        timer_policy: @timer_policy
      )

    assert {:error, {:key_required_on_keyed_channel, "c"}} = Ctx.emit(single, :a)
    assert {:error, {:key_required_on_keyed_channel, "c"}} = Ctx.emit_with_meta(single, :a, %{})
    assert :ok = Ctx.emit_keyed(single, "k", :a)
    assert Ctx.take_emits(single) == [{:a, %{}, "k"}]

    plain = Ctx.new(nil, "k", 0, channel: "p", timer_policy: @timer_policy)
    assert {:error, {:key_not_allowed_on_non_keyed_channel, "p"}} = Ctx.emit_keyed(plain, "k", :a)
    assert :ok = Ctx.emit(plain, :a)
    assert Ctx.take_emits(plain) == [{:a, %{}, nil}]
  end

  test "a builder call into a deferred surface fails registration naming the module" do
    defmodule Deferred do
      def factory_id, do: "def"
      def channels(c), do: Channels.branch(c)
    end

    assert {:error, {:invalid_topology, [{:DeferredBuilderSurface, Deferred, _}]}} =
             Topology.build(Deferred)
  end

  # --- Ctx ---

  test "emits accumulate and flush; reserved flz. keys refuse" do
    ctx = Ctx.new(nil, "k", 123, timer_policy: @timer_policy)
    assert Ctx.now(ctx) == 123
    assert Ctx.entity_key(ctx) == "k"
    assert :ok = Ctx.emit(ctx, :a)
    assert :ok = Ctx.emit_with_meta(ctx, :b, %{"m" => "1"})

    assert {:error, {:reserved_metadata_key, "flz.seq"}} =
             Ctx.emit_with_meta(ctx, :c, %{"flz.seq" => "1"})

    assert Ctx.take_emits(ctx) == [{:a, %{}, nil}, {:b, %{"m" => "1"}, nil}]
  end

  # --- the effectively-once tier: the builder, the refusals, the declaration ---

  test "the four tier calls pipe, and the three checkpoint options compose in any order" do
    assert {:ok, tier} = Topology.build(TierStoreOp)
    assert tier.effectively_once?
    assert tier.checkpoint == nil

    assert {:ok, batched} = Topology.build(CheckpointBatchedOp)
    assert batched.effectively_once?
    assert batched.checkpoint == %{every: nil, outputs: :at_checkpoint, max_delay_ms: 1_500}

    defmodule EveryFirst do
      def factory_id, do: "every_first"

      def channels(c) do
        c
        |> Channels.effectively_once()
        |> Channels.checkpoint_every(3)
        |> Channels.outputs_at_checkpoint(50)
        |> Channels.input("in")
        |> Channels.into(KeyedProc)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
      end
    end

    defmodule DelayFirst do
      def factory_id, do: "delay_first"

      def channels(c) do
        c
        |> Channels.outputs_at_checkpoint(50)
        |> Channels.checkpoint_every(3)
        |> Channels.effectively_once()
        |> Channels.input("in")
        |> Channels.into(KeyedProc)
        |> Channels.named("sink")
        |> Channels.keyed()
        |> Channels.sink()
      end
    end

    assert {:ok, a} = Topology.build(EveryFirst)
    assert {:ok, b} = Topology.build(DelayFirst)
    assert a.checkpoint == %{every: 3, outputs: :at_checkpoint, max_delay_ms: 50}
    assert b.checkpoint == a.checkpoint
  end

  # A zero delay is the one checkpoint refusal the reference vocabulary spells
  # as free text rather than as a name, so no fixture row states it.
  test "CheckpointMaxDelayZero — an interval that closes after no time at all" do
    defmodule DelayZero do
      def factory_id, do: "delay_zero"

      def channels(c),
        do:
          c
          |> Channels.effectively_once()
          |> Channels.outputs_at_checkpoint(0)
          |> Channels.input("in")
          |> Channels.into(KeyedProc)
          |> Channels.named("sink")
          |> Channels.keyed()
          |> Channels.sink()
    end

    assert {:CheckpointMaxDelayZero, _} = one_issue(DelayZero)
  end

  # How the two correction options compose is this SDK's own rule, not a row:
  # a fate declared alone leaves the window at zero, which is refused.
  test "a beyond-window policy declared alone is a window of zero" do
    defmodule PolicyWithoutWindow do
      def factory_id, do: "policy_no_window"

      def channels(c),
        do:
          c
          |> Channels.effectively_once()
          |> Channels.beyond_window_policy(:drop)
          |> Channels.input("in")
          |> Channels.into(EventTimeProc)
          |> Channels.named("sink")
          |> Channels.keyed()
          |> Channels.sink()
    end

    assert {:CorrectionWindowZero, _} = one_issue(PolicyWithoutWindow)
  end

  # `register.rs` refuses a shedding channel under a declared window with its own
  # message; here a window requires the tier and the tier already refuses every
  # shedding edge, so the same program is refused by the check that exists.
  test "a shedding policy under a declared window is refused by the tier's own check" do
    defmodule SheddingUnderWindow do
      def factory_id, do: "shedding_window"

      def channels(c),
        do:
          c
          |> Channels.effectively_once()
          |> Channels.correction_window(600_000)
          |> Channels.input("in")
          |> Channels.on_full(:drop_oldest)
          |> Channels.into(EventTimeProc)
          |> Channels.named("sink")
          |> Channels.keyed()
          |> Channels.sink()
    end

    assert {:DroppingPolicyUnderEffectivelyOnce, "in", :drop_oldest} =
             one_issue(SheddingUnderWindow)
  end

  test "the two correction options compose in either order and neither clears the other" do
    defmodule WindowThenPolicy do
      def factory_id, do: "window_then_policy"

      # The tier declared LAST, which is still legal and is the point.
      def channels(c),
        do:
          c
          |> Channels.correction_window(600_000)
          |> Channels.beyond_window_policy(:drop)
          |> Channels.effectively_once()
          |> Channels.input("in")
          |> Channels.into(EventTimeProc)
          |> Channels.named("sink")
          |> Channels.keyed()
          |> Channels.sink()
    end

    defmodule PolicyThenWindow do
      def factory_id, do: "policy_then_window"

      def channels(c),
        do:
          c
          |> Channels.effectively_once()
          |> Channels.beyond_window_policy(:drop)
          |> Channels.correction_window(600_000)
          |> Channels.input("in")
          |> Channels.into(EventTimeProc)
          |> Channels.named("sink")
          |> Channels.keyed()
          |> Channels.sink()
    end

    assert {:ok, %{correction: %{window_ms: 600_000, beyond_window: :drop}}} =
             Topology.build(WindowThenPolicy)

    assert {:ok, %{correction: %{window_ms: 600_000, beyond_window: :drop}}} =
             Topology.build(PolicyThenWindow)
  end

  test "a correction class declares the two keys the candle golden pins" do
    defmodule CandleShape do
      def factory_id, do: "candle_shape"

      def channels(c),
        do:
          c
          |> Channels.effectively_once()
          |> Channels.correction_window(600_000)
          |> Channels.input("in")
          |> Channels.into(EventTimeProc)
          |> Channels.named("sink")
          |> Channels.keyed()
          |> Channels.sink()
    end

    assert {:ok, topo} = Topology.build(CandleShape)
    assert Declarations.for(topo) == golden("candle")
  end

  test "the tier ships two more declaration keys, and they equal the committed goldens" do
    assert {:ok, tier} = Topology.build(TierStoreOp)
    assert {:ok, batched} = Topology.build(CheckpointBatchedOp)

    assert Declarations.for(tier) == golden("tier_store")
    assert Declarations.for(batched) == golden("checkpoint_batched_store")
  end

  defp one_issue(module) do
    assert {:error, {:invalid_topology, issues}} = Topology.build(module)
    assert length(issues) == 1
    hd(issues)
  end

  defp golden(id),
    do: Jason.decode!(File.read!("test/fixtures/declarations/#{id}.json"))

  # --- the ctx under the tier ---

  test "stable_id matches the cross-language fixture and advances its ordinal" do
    ctx = Ctx.new(nil, nil, 0, identity: "instance/op/key", seq: 42, timer_policy: @timer_policy)
    assert Ctx.stable_id(ctx) == "bbf81fd4955afd1724203700a240e822"
    assert Ctx.stable_id(ctx) == "bbf81fd4965afd1724203700a240e95d"

    empty = Ctx.new(nil, nil, 0, identity: "", seq: 0, timer_policy: @timer_policy)
    assert Ctx.stable_id(empty) == "15915375576d2f389a938dc3a9940c9d"

    Ctx.discard(ctx)
    Ctx.discard(empty)
  end

  test "checkpoint/1 raises a flag the Entity reads, and discard/1 clears both tier slots" do
    ctx = Ctx.new(nil, nil, 0, identity: "i", seq: 1, timer_policy: @timer_policy)
    refute Ctx.checkpointed?(ctx)
    assert :ok = Ctx.checkpoint(ctx)
    assert Ctx.checkpointed?(ctx)
    assert Ctx.stable_id(ctx) != Ctx.stable_id(ctx)

    Ctx.discard(ctx)
    refute Ctx.checkpointed?(ctx)
    fresh = Ctx.new(nil, nil, 0, identity: "i", seq: 1, timer_policy: @timer_policy)
    assert Ctx.stable_id(ctx) == Ctx.stable_id(fresh)
  end

  # --- Codec ---

  test "codec round-trips request/response and add-operative metadata" do
    bin = Codec.encode_response("ref-1", "success", [], <<>>)

    assert {:ok, %RunnerCommands.CommandResponse{reference_id: "ref-1", status: "success"}} =
             RunnerCommands.CommandResponse.decode(bin)

    raw = Codec.encode_add_operative_response(%{"keyed" => "false"})

    assert {:ok, %RunnerCommands.AddOperativeResponse{metadata: %{"keyed" => "false"}}} =
             RunnerCommands.AddOperativeResponse.decode(raw)

    uf = Codec.encode_request("uf-1", Codec.update_factories_command([{"a", "d"}]))

    assert {:ok,
            %RunnerCommands.CommandRequest{command: {:update_factories, %{factories: [decl]}}}} =
             Codec.decode_command(uf)

    assert decl.id == "a"
  end

  # --- KeyGroup / partition ---

  test "key-group hash matches the cross-language fixtures via the SDK seam" do
    range = KeyGroup.decode(0, 2, 0, 127)
    assert {:error, msg} = KeyGroup.admit(range, "BTC-USD")
    assert msg =~ "key group 129"
    assert :ok = KeyGroup.admit(KeyGroup.decode(1, 2, 128, 255), "BTC-USD")

    assert :ok = KeyGroup.admit(KeyGroup.decode(0, 0, 0, 0), "anything")
    assert KeyGroup.owns?(KeyGroup.decode(0, 0, 0, 0), "x")
  end

  # --- PersistentId ---

  test "stable id format and memoization; persisted to disk" do
    PersistentId.reset_memo()
    tmp = Path.join(System.tmp_dir!(), "flz-id-#{System.unique_integer([:positive])}")
    System.put_env("FLEZHA_RUNNER_ID_PATH", tmp)

    on_exit(fn ->
      System.delete_env("FLEZHA_RUNNER_ID_PATH")
      File.rm(tmp)
      PersistentId.reset_memo()
    end)

    id = PersistentId.stable_id("myns", ["b", "a"])
    assert id =~ ~r/^myns-[0-9a-f]{8}-[a-z0-9]{6}$/
    assert PersistentId.profile_hash8(["a", "b"]) == PersistentId.profile_hash8(["b", "a"])
    assert PersistentId.stable_id("myns", ["x"]) == id
    assert File.read!(tmp) == id
  end

  test "unwritable id path degrades to a fresh id with no crash" do
    PersistentId.reset_memo()
    System.put_env("FLEZHA_RUNNER_ID_PATH", "/proc/nonexistent/cannot/write/id")

    on_exit(fn ->
      System.delete_env("FLEZHA_RUNNER_ID_PATH")
      PersistentId.reset_memo()
    end)

    id = PersistentId.stable_id("ns", ["a"])
    assert id =~ ~r/^ns-[0-9a-f]{8}-[a-z0-9]{6}$/
  end

  # --- the in-node data stream bind ---

  # A pool channel that takes every bind and hands the test what reaches it,
  # which is the controller side of the stream it was given.
  defmodule BindingChannel do
    use GenServer

    def start_link({test, name}), do: GenServer.start_link(__MODULE__, test, name: name)

    @impl true
    def init(test), do: {:ok, test}

    @impl true
    def handle_call({:bind, _stream}, _from, test), do: {:reply, :ok, test}

    @impl true
    def handle_info(msg, test) do
      send(test, {:channel, msg})
      {:noreply, test}
    end
  end

  test "an in-node bind with no channel to take it is answered, not raised" do
    runner_id = "sdk_unit_#{System.unique_integer([:positive])}"

    assert {:error, :no_channel} = Transport.connect_sockets(:native, runner_id, 1, self(), nil)
  end

  test "a pair refused half way closes the stream already bound" do
    runner_id = "sdk_unit_#{System.unique_integer([:positive])}"
    name = Identificator.stream_channel_primary(runner_id, 1)
    start_supervised!({BindingChannel, {self(), name}})

    # The primary binds to the one channel there is; the secondary finds none.
    assert {:error, :no_channel} = Transport.connect_sockets(:native, runner_id, 1, self(), nil)
    assert_receive {:channel, {:flz_closed, _stream, "closed"}}
  end
end

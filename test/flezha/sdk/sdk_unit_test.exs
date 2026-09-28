defmodule Flezha.SDK.UnitTest do
  use ExUnit.Case, async: true

  alias Flezha.Identificator
  alias Flezha.SDK.{Channels, Codec, Ctx, Declarations, KeyGroup, PersistentId, Tasklet, Topology}
  alias Flezha.SDK.Proto.RunnerCommands
  alias Flezha.SDK.Transport
  alias FlezhaTest.Helper.SdkTierFixtures.{CheckpointBatchedOp, TierStoreOp}

  # A ctx built without a tasklet has no name to refuse with; the tests
  # that build one declare the policy themselves.
  @timer_policy %{tasklet: "unit", cap: 10_000, clock: :wall}

  # --- fixtures ---

  defmodule EchoTask do
    use Flezha.SDK.Tasklet, name: "echo"

    @impl true
    def run(input, state, ctx),
      do:
        (
          emit(ctx, input)
          {:ok, state}
        )
  end

  defmodule EchoOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "echo_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(EchoTask)
        |> Channels.named("echo")
        |> Channels.sink()
  end

  defmodule KeyedTask do
    use Flezha.SDK.Tasklet, name: "keyed"
    @impl true
    def init_state(_), do: 0
    @impl true
    def run(_input, state, _ctx), do: {:ok, state + 1}
  end

  defmodule DynamicOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "dyn_op"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.filter(fn s -> s != "drop" end)
      |> Channels.key_by(fn s -> s end)
      |> Channels.into(KeyedTask)
      |> Channels.named("k")
      |> Channels.sink()
    end
  end

  defmodule EventTimeTask do
    use Flezha.SDK.Tasklet, name: "event_time_task"
    @impl true
    def init_state(_), do: 0
    @impl true
    def event_time(input), do: String.to_integer(input)

    defchange noop(s) do
      s
    end

    @impl true
    def run(_input, state, _ctx), do: {:ok, state}
  end

  defmodule StaticTask do
    use Flezha.SDK.Tasklet, name: "static", cell_keys: ["a", "b"]
    @impl true
    def run(_i, s, _c), do: {:ok, s}
  end

  defmodule StaticOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "static_op"

    def channels(c),
      do:
        c
        |> Channels.input("in")
        |> Channels.into(StaticTask)
        |> Channels.named("s")
        |> Channels.sink()
  end

  # --- cell mode derivation ---

  test "a linear echo topology compiles Single and keyless" do
    assert {:ok, topo} = Topology.build(EchoOp)
    assert topo.factory_id == "echo_op"
    assert [mode] = Map.values(topo.node_mode)
    assert mode == :single
    assert topo.keyless?
  end

  test "a key_by edge into a keyless tasklet is Dynamic and not keyless" do
    assert {:ok, topo} = Topology.build(DynamicOp)
    assert [mode] = Map.values(topo.node_mode)
    assert mode == :dynamic
    refute topo.keyless?
  end

  test "a tasklet declaring cell_keys is Static" do
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

  test "UnservableClock, TwoClocks — a clock the runtime cannot serve" do
    defmodule LogicalNoChange do
      use Flezha.SDK.Tasklet, name: "logical_no_change", logical_clock: true
      @impl true
      def run(_i, s, _ctx), do: {:ok, s}
    end

    # Only the hand-written form can declare both: the `use` form does not compile.
    defmodule TwoClocksByHand do
      @behaviour Flezha.SDK.Tasklet
      def name, do: "two_clocks_by_hand"
      def init_state(_key), do: 0
      def logical_clock, do: true
      def event_time(input), do: String.to_integer(input)
      def apply_change(s, _change), do: s
      def run(_i, s, _ctx), do: {:ok, s}
    end

    defmodule ClockOp do
      def factory_id, do: "clock_op"

      def channels(c) do
        c |> Channels.input("a") |> Channels.into(LogicalNoChange)
        c |> Channels.input("b") |> Channels.into(TwoClocksByHand)
      end
    end

    assert {:error, {:invalid_topology, issues}} = Topology.build(ClockOp)
    assert {:UnservableClock, inspect(LogicalNoChange)} in issues
    assert {:TwoClocks, inspect(TwoClocksByHand)} in issues
    assert length(issues) == 2

    # What each writes is the clock `now/1` serves.
    assert Tasklet.clock(LogicalNoChange) == :logical
    assert Tasklet.clock(EventTimeTask) == :event_time
    assert Tasklet.clock(StaticTask) == :wall
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
          |> Channels.into(EchoTask)
          |> Channels.keyed()
          |> Channels.into(KeyedTask)
    end

    defmodule SyncUnbounded do
      def factory_id, do: "sync_unbounded"
      def sync_durability, do: true

      def channels(c),
        do: c |> Channels.input("in") |> Channels.into(EchoTask) |> Channels.sink()
    end

    assert {:KeyedChannelWithoutPluginWire, "echo"} = one_issue(KeyedNoWire)
    assert {:SyncDurabilityOnUnbounded, _} = one_issue(SyncUnbounded)
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

  # --- branch: fan-out sugar over one handle ---

  defmodule BranchedOp do
    use Flezha.SDK.UnboundedOperator, factory_id: "branched"

    @impl true
    def channels(c) do
      c
      |> input("in")
      |> map(&String.upcase/1)
      |> branch([&into(&1, EchoTask), &into(&1, KeyedTask)])
      |> sink()
    end
  end

  defmodule ReusedHandleOp do
    @behaviour Flezha.SDK.UnboundedOperator
    def factory_id, do: "reused_handle"

    def channels(c) do
      upper = c |> Channels.input("in") |> Channels.map(&String.upcase/1)
      Channels.into(upper, EchoTask)
      Channels.into(upper, KeyedTask)
      Channels.sink(upper)
    end
  end

  test "branch builds the topology the same handle wired twice builds, and answers that handle" do
    assert {:ok, branched} = Topology.build(BranchedOp)
    assert {:ok, reused} = Topology.build(ReusedHandleOp)

    assert fan_out(branched) == [
             {{:sink, nil}, ["A"]},
             {EchoTask, ["A"]},
             {KeyedTask, ["A"]}
           ]

    assert fan_out(branched) == fan_out(reused)
  end

  test "a branch of no arms is refused like a branch of one" do
    defmodule NoArms do
      def factory_id, do: "no_arms"

      def channels(c),
        do: c |> Channels.input("in") |> Channels.branch([]) |> Channels.into(EchoTask)
    end

    assert {:BranchTooFewArms, 0} = one_issue(NoArms)
  end

  # Every route off the `in` entry: what it reaches and what it makes of "a".
  defp fan_out(topology) do
    for route <- topology.routes[{:entry, "in"}] do
      target =
        case route.to do
          {:node, ref} -> Map.fetch!(topology.node_modules, ref)
          sink -> sink
        end

      {target, route.transform.("a")}
    end
  end

  test "a builder call into a deferred surface fails registration naming the module" do
    defmodule Deferred do
      def factory_id, do: "def"
      def channels(c), do: Channels.window(c)
    end

    assert {:error, {:invalid_topology, [{:DeferredBuilderSurface, Deferred, _}]}} =
             Topology.build(Deferred)
  end

  # --- Ctx ---

  test "emits accumulate and flush; reserved flz. keys refuse" do
    ctx = Ctx.new(nil, "k", 123, timer_policy: @timer_policy)
    assert Ctx.now(ctx) == 123
    assert Ctx.cell_key(ctx) == "k"
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
        |> Channels.into(KeyedTask)
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
        |> Channels.into(KeyedTask)
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
          |> Channels.into(KeyedTask)
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
          |> Channels.into(EventTimeTask)
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
          |> Channels.into(EventTimeTask)
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
          |> Channels.into(EventTimeTask)
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
          |> Channels.into(EventTimeTask)
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
          |> Channels.into(EventTimeTask)
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

  test "checkpoint/1 raises a flag the Cell reads, and discard/1 clears both tier slots" do
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

  test "codec round-trips request/response and add-operator metadata" do
    bin = Codec.encode_response("ref-1", "success", [], <<>>)

    assert {:ok, %RunnerCommands.CommandResponse{reference_id: "ref-1", status: "success"}} =
             RunnerCommands.CommandResponse.decode(bin)

    raw = Codec.encode_add_operator_response(%{"keyed" => "false"})

    assert {:ok, %RunnerCommands.AddOperatorResponse{metadata: %{"keyed" => "false"}}} =
             RunnerCommands.AddOperatorResponse.decode(raw)

    uf = Codec.encode_request("uf-1", Codec.update_factories_command([{"a", "d", "bounded"}]))

    assert {:ok,
            %RunnerCommands.CommandRequest{command: {:update_factories, %{factories: [decl]}}}} =
             Codec.decode_command(uf)

    assert {decl.id, decl.kind} == {"a", "bounded"}
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

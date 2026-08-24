defmodule Flezha.SDK.UnitTest do
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Topology, Ctx, Codec, KeyGroup, PersistentId, Channels}

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
        |> Channels.sink("out")
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
      |> Channels.sink("out")
    end
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
        |> Channels.sink("out")
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

  defp issues(module) do
    assert {:error, {:invalid_topology, issues}} = Topology.build(module)
    Enum.map(issues, &elem(&1, 0))
  end

  test "DuplicateChannel" do
    defmodule DupChan do
      def factory_id, do: "d"

      def channels(c) do
        Channels.input(c, "in")
        Channels.input(c, "in")
      end
    end

    assert :DuplicateChannel in issues(DupChan)
  end

  test "TransformAfterKeyBy" do
    defmodule TAK do
      def factory_id, do: "t"

      def channels(c),
        do:
          c
          |> Channels.input("in")
          |> Channels.key_by(& &1)
          |> Channels.map(& &1)
          |> Channels.into(KeyedProc)
    end

    assert :TransformAfterKeyBy in issues(TAK)
  end

  test "RetryMaxZero" do
    defmodule RMZ do
      def factory_id, do: "r"

      def channels(c),
        do:
          c
          |> Channels.input("in")
          |> Channels.retry_max(0)
          |> Channels.into(EchoProc)
          |> Channels.sink("o")
    end

    assert :RetryMaxZero in issues(RMZ)
  end

  test "DuplicatePluginSink" do
    defmodule DPS do
      def factory_id, do: "p"

      def channels(c) do
        s = c |> Channels.input("in") |> Channels.into(EchoProc)
        Channels.sink(s, "same")
        Channels.sink(s, "same")
      end
    end

    assert :DuplicatePluginSink in issues(DPS)
  end

  test "KeyByOnUnkeyableTarget — key_by into a Static procedure" do
    defmodule KBU do
      def factory_id, do: "k"

      def channels(c),
        do:
          c
          |> Channels.input("in")
          |> Channels.key_by(& &1)
          |> Channels.into(StaticProc)
          |> Channels.sink("o")
    end

    assert :KeyByOnUnkeyableTarget in issues(KBU)
  end

  test "NodeIdentityCollision — one name, two modules" do
    defmodule NIC do
      def factory_id, do: "n"

      def channels(c) do
        c |> Channels.input("in") |> Channels.into_named(EchoProc, "same")
        c |> Channels.input("in2") |> Channels.into_named(KeyedProc, "same")
      end
    end

    assert :NodeIdentityCollision in issues(NIC)
  end

  test "Cycle" do
    defmodule Cyc do
      def factory_id, do: "c"

      def channels(c) do
        c
        |> Channels.input("in")
        |> Channels.into(EchoProc)
        |> Channels.into(KeyedProc)
        |> Channels.into(EchoProc)
      end
    end

    assert :Cycle in issues(Cyc)
  end

  test "a builder call into a deferred surface fails registration naming the module" do
    defmodule Deferred do
      def factory_id, do: "def"
      def channels(c), do: Channels.merge(c)
    end

    assert {:error, {:invalid_topology, [{:DeferredBuilderSurface, Deferred, _}]}} =
             Topology.build(Deferred)
  end

  # --- Ctx ---

  test "emits accumulate and flush; reserved flz. keys refuse" do
    ctx = Ctx.new("k", 123)
    assert Ctx.now(ctx) == 123
    assert Ctx.entity_key(ctx) == "k"
    assert :ok = Ctx.emit(ctx, :a)
    assert :ok = Ctx.emit_with_meta(ctx, :b, %{"m" => "1"})

    assert {:error, {:reserved_metadata_key, "flz.seq"}} =
             Ctx.emit_with_meta(ctx, :c, %{"flz.seq" => "1"})

    assert Ctx.take_emits(ctx) == [{:a, %{}}, {:b, %{"m" => "1"}}]
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
end

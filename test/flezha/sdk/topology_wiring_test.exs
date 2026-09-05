defmodule Flezha.SDK.TopologyWiringTest do
  @moduledoc """
  What this SDK refuses when a plugin wire is declared twice, and the two
  repeated shapes that stay legal. The rule is not "duplicates are refused":
  one channel may reach several plugins and one plugin may be fed by several
  channels, so only the same channel wired to the same plugin is the duplicate.

  This is the one chain rule where the Elixir SDK is not the reference's:
  `flz_core` allows a channel exactly one plugin wire (the `dup_sink` row of
  `test/fixtures/reference_topologies.txt` refuses two different plugins),
  while this SDK carries a keyed wire beside a plain one on one channel —
  `FlezhaTest.Helper.SdkPersistenceFixtures.MixedSinksOp` declares it and
  `test/flezha/sdk/persistence_test.exs` pins the frames it produces. The
  repeated consumer edge, whose rule IS the reference's, is the `edge_rewired`
  and `fan_in_noop` rows.
  """
  use ExUnit.Case, async: true

  alias Flezha.SDK.{Channels, Ctx, Topology}

  defmodule EchoProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "echo"
    def init_state(_), do: nil

    def run(input, state, ctx) do
      Ctx.emit(ctx, input)
      {:ok, state}
    end
  end

  defmodule KeyedProc do
    @behaviour Flezha.SDK.Procedure
    def name, do: "keyed"
    def init_state(_), do: 0
    def run(_input, state, _ctx), do: {:ok, state + 1}
  end

  defmodule DuplicateSink do
    def factory_id, do: "p"

    def channels(c) do
      s = c |> Channels.input("in") |> Channels.into(EchoProc)
      Channels.sink(s, "same")
      Channels.sink(s, "same")
    end
  end

  defmodule ManyPlugins do
    def factory_id, do: "mp"

    def channels(c) do
      s = c |> Channels.input("in") |> Channels.into(EchoProc)
      Channels.sink(s, "a")
      Channels.sink(s, "b")
    end
  end

  defmodule OnePluginManyChannels do
    def factory_id, do: "opmc"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.into(EchoProc)
      |> Channels.named("first")
      |> Channels.sink("shared")
      |> Channels.into(KeyedProc)
      |> Channels.named("second")
      |> Channels.sink("shared")
    end
  end

  test "DuplicatePluginSink — one channel wired to one plugin twice" do
    assert {:error, {:invalid_topology, [{:DuplicatePluginSink, "echo", "same"}]}} =
             Topology.build(DuplicateSink)
  end

  test "one channel may reach several plugins, and one plugin several channels" do
    assert {:ok, _} = Topology.build(ManyPlugins)
    assert {:ok, topo} = Topology.build(OnePluginManyChannels)

    sinks = for {_from, rs} <- topo.routes, %{to: {:sink, p}} <- rs, do: p
    assert Enum.sort(sinks) == ["shared", "shared"]
  end
end

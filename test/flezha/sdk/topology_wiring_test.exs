defmodule Flezha.SDK.TopologyWiringTest do
  @moduledoc """
  How a channel is named, and what this SDK refuses when a plugin wire is
  declared twice. A sink names the channel it terminates, so `sink("x")` and
  `sink()` are the two forms, and naming one channel twice is refused.

  This is the one chain rule where the Elixir SDK is not the reference's:
  `flz_core` allows a channel exactly one plugin wire, while this SDK carries a
  keyed wire beside a plain one on one channel —
  `FlezhaTest.Helper.SdkPersistenceFixtures.MixedSinksOp` declares it and
  `test/flezha/sdk/persistence_test.exs` pins the frames it produces. Since a
  sink names the channel rather than a plugin, what tells two wires apart is
  their stamping mode, and two wires sharing it are the duplicate. The repeated
  consumer edge, whose rule IS the reference's, is the `edge_rewired` and
  `fan_in_noop` rows.
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

  defmodule TwoNamedSinks do
    def factory_id, do: "mp"

    def channels(c) do
      s = c |> Channels.input("in") |> Channels.into(EchoProc)
      Channels.sink(s, "a")
      Channels.sink(s, "b")
    end
  end

  defmodule NamedAndSinkNamed do
    def factory_id, do: "nasn"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.into(EchoProc)
      |> Channels.named("same")
      |> Channels.sink("same")
    end
  end

  defmodule KeyedBesidePlain do
    def factory_id, do: "kbp"

    def channels(c) do
      node = c |> Channels.input("in") |> Channels.into(EchoProc)
      node |> Channels.keyed() |> Channels.sink("stamped")
      node |> Channels.sink()
    end
  end

  defmodule TwoChannelsOneSink do
    def factory_id, do: "tcos"

    def channels(c) do
      c
      |> Channels.input("in")
      |> Channels.into(EchoProc)
      |> Channels.sink("shared")
      |> Channels.into(KeyedProc)
      |> Channels.sink("shared")
    end
  end

  defmodule NamedAndSinkCollide do
    def factory_id, do: "nasc"

    def channels(c) do
      s = c |> Channels.input("in") |> Channels.into(EchoProc)
      s |> Channels.named("shared") |> Channels.into(KeyedProc) |> Channels.sink("shared")
    end
  end

  defmodule SinkNamesTheChannel do
    def factory_id, do: "snc"

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(EchoProc) |> Channels.sink("items_output")
    end
  end

  defmodule NamelessSink do
    def factory_id, do: "ns"

    def channels(c) do
      c |> Channels.input("in") |> Channels.into(EchoProc) |> Channels.sink()
    end
  end

  test "DuplicatePluginSink — one channel wired twice with the same stamping mode" do
    assert {:error, {:invalid_topology, [{:DuplicatePluginSink, "same", "same"}]}} =
             Topology.build(DuplicateSink)
  end

  test "a keyed wire beside a plain one on one channel is not the duplicate" do
    assert {:ok, topo} = Topology.build(KeyedBesidePlain)

    channels = for {_from, rs} <- topo.routes, %{to: {:sink, _}, channel: c} <- rs, do: c

    assert channels == ["stamped", "stamped"],
           "both wires are on one channel, and the sink named it"
  end

  test "the sink names the channel, and sink/1 leaves the name derived" do
    assert {:ok, named} = Topology.build(SinkNamesTheChannel)
    assert sink_channels(named) == ["items_output"]

    assert {:ok, derived} = Topology.build(NamelessSink)
    assert sink_channels(derived) == ["echo"], "sink/1 derives exactly as a channel with no sink"
  end

  test "ChannelNamedTwice — named/2 beside a sink that names, even when they agree" do
    assert {:error, {:invalid_topology, issues}} = Topology.build(NamedAndSinkNamed)
    assert {:ChannelNamedTwice, "echo", "same", "same"} in issues
  end

  test "ChannelNamedTwice — two sinks naming one channel differently" do
    assert {:error, {:invalid_topology, issues}} = Topology.build(TwoNamedSinks)
    assert {:ChannelNamedTwice, "echo", "a", "b"} in issues
  end

  test "DuplicateSinkName — two channels naming one sink" do
    assert {:error, {:invalid_topology, issues}} = Topology.build(TwoChannelsOneSink)
    assert {:DuplicateSinkName, "shared"} in issues
  end

  test "a named channel colliding with a sink-named one is a bare duplicate" do
    assert {:error, {:invalid_topology, issues}} = Topology.build(NamedAndSinkCollide)

    assert {:DuplicateChannel, "shared"} in issues,
           "only two channels naming one SINK are the sink duplicate, read #{inspect(issues)}"
  end

  defp sink_channels(topo),
    do: for({_from, rs} <- topo.routes, %{to: {:sink, _}, channel: c} <- rs, do: c)
end

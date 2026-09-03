defmodule Flezha.SDK.TopologyWiringTest do
  @moduledoc """
  What the builder refuses when one thing is wired twice. Neither rule is
  "duplicates are refused": a consumer edge and a plugin sink each have one
  legal repeated shape, and refusing that shape would break topologies this
  repository's own fixtures declare.
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

  # --- consumer edges ---

  defmodule NewDeclares do
    def factory_id, do: "e1"

    def channels(c) do
      s = Channels.input(c, "in")
      Channels.into(s, EchoProc)
      s |> Channels.map(& &1) |> Channels.into(EchoProc)
    end
  end

  defmodule ExistingDeclares do
    def factory_id, do: "e2"

    def channels(c) do
      s = Channels.input(c, "in")
      s |> Channels.key_by(& &1) |> Channels.into(EchoProc)
      Channels.into(s, EchoProc)
    end
  end

  defmodule BareRedeclare do
    def factory_id, do: "e3"

    def channels(c) do
      s = Channels.input(c, "in")
      Channels.into(s, EchoProc)
      Channels.into(s, EchoProc)
    end
  end

  test "EdgeAlreadyWired — a re-declaration where either side carries a chain" do
    assert {:error, {:invalid_topology, [{:EdgeAlreadyWired, "in", proc}]}} =
             Topology.build(NewDeclares)

    assert proc =~ "EchoProc"
    assert :EdgeAlreadyWired in issues(ExistingDeclares)
  end

  test "a bare re-declaration is the fan-in no-op — one edge, no error" do
    assert {:ok, topo} = Topology.build(BareRedeclare)
    assert [routes] = Map.values(topo.routes)
    assert length(routes) == 1
  end

  # --- plugin sinks ---

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
    assert [{:DuplicatePluginSink, "echo", "same"}] = refusals(DuplicateSink)
  end

  test "one channel may reach several plugins, and one plugin several channels" do
    assert {:ok, _} = Topology.build(ManyPlugins)
    assert {:ok, topo} = Topology.build(OnePluginManyChannels)

    sinks = for {_from, rs} <- topo.routes, %{to: {:sink, p}} <- rs, do: p
    assert Enum.sort(sinks) == ["shared", "shared"]
  end

  defp issues(module), do: Enum.map(refusals(module), &elem(&1, 0))

  defp refusals(module) do
    assert {:error, {:invalid_topology, issues}} = Topology.build(module)
    issues
  end
end

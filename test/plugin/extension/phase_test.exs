defmodule FlezhaTest.Plugin.Extension.PhaseTest do
  @moduledoc """
  Covers the two-phase plugin lifecycle introduced by
  `fix-overwatch-restart-storm` (capability `operative-extensions` —
  "Two-phase plugin lifecycle gated on SDK readiness"). Plugins enter the
  `:prepared` phase on `Table.subscribe/3`; `bulk_activate/1` transitions
  every prepared plugin to `:running`, invoking the optional `activate/1`
  callback. Plugins without an `activate/1` callback transition silently.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Host

  use ExUnit.Case, async: true

  @catalog Host.build_catalog(
             [{:on_input, :transform, Ctx.IO}],
             %{file: __ENV__.file, line: __ENV__.line}
           )

  defmodule QuiescentProvider do
    @moduledoc "Provider with no activate/1 — transitions silently."
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule TrackingProvider do
    @moduledoc """
    Provider whose `init_provider/1` is fast/local and whose `activate/1`
    sends a marker to the registered reporter pid — the mirror of the
    `Plugin.Provider.Websocket` shape after the lifecycle split.
    """
    def init_provider(opts), do: {:ok, opts}

    def activate(%{report_to: pid, marker: m}) do
      send(pid, {:activated, m})
      :ok
    end

    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  defmodule RaisingProvider do
    def init_provider(opts), do: {:ok, opts}
    def activate(_), do: raise("upstream went away")
    def on_input(%Ctx.IO{} = ctx, _opts), do: {:ok, ctx}
  end

  test "5.5 plugins enter :prepared on subscribe" do
    plugin = %Plugin{
      id: "p1",
      provider: QuiescentProvider,
      subscriptions: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.subscribe(table, "p1", plugin)

    assert Table.phase(table, "p1") == :prepared
  end

  test "5.6 bulk_activate transitions prepared plugins to :running exactly once" do
    plugin = %Plugin{
      id: "p1",
      provider: TrackingProvider,
      opts: %{report_to: self(), marker: :first},
      subscriptions: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.subscribe(table, "p1", plugin)

    refute_receive {:activated, _}, 50

    table = Table.bulk_activate(table)
    assert_receive {:activated, :first}, 200
    assert Table.phase(table, "p1") == :running

    table = Table.bulk_activate(table)
    refute_receive {:activated, _}, 50
    assert Table.phase(table, "p1") == :running
  end

  test "5.6 plugins without activate/1 transition silently to :running" do
    plugin = %Plugin{
      id: "p1",
      provider: QuiescentProvider,
      subscriptions: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.subscribe(table, "p1", plugin)
    table = Table.bulk_activate(table)
    assert Table.phase(table, "p1") == :running
  end

  test "5.6 teardown handles prepared and running plugins uniformly" do
    quiescent = %Plugin{
      id: "qpro",
      provider: QuiescentProvider,
      subscriptions: [{:on_input, [position: 0]}]
    }

    tracking = %Plugin{
      id: "tpro",
      provider: TrackingProvider,
      opts: %{report_to: self(), marker: :td},
      subscriptions: [{:on_input, [position: 1]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.subscribe(table, "qpro", quiescent)
    {:ok, table} = Table.subscribe(table, "tpro", tracking)
    table = Table.bulk_activate(table)

    assert Table.phase(table, "qpro") == :running
    assert Table.phase(table, "tpro") == :running

    # Now unsubscribe one and partially tear down. Teardown should not
    # raise regardless of phase mix.
    teardown_table = Table.teardown_all(table)
    assert teardown_table.plugins == %{}
    assert teardown_table.phases == %{}
  end

  test "5.6 activate that raises leaves plugin in :prepared and logs" do
    plugin = %Plugin{
      id: "p1",
      provider: RaisingProvider,
      subscriptions: [{:on_input, [position: 0]}]
    }

    table = Table.new(@catalog)
    {:ok, table} = Table.subscribe(table, "p1", plugin)

    table =
      ExUnit.CaptureLog.with_log(fn -> Table.bulk_activate(table) end)
      |> elem(0)

    assert Table.phase(table, "p1") == :prepared
  end
end

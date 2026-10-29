defmodule FlezhaTest.Service.PluginListPageTest do
  @moduledoc """
  `PluginManager.enabled/3` with `opts`: a page of the enabled plugins, each
  row carrying a sample of its relays and how many there are, built from the
  namespace's bindings on the hub node.
  """
  # async: false: Mimic global mode for the hub reads.
  use ExUnit.Case, async: false

  import FlezhaTest.Helper.MimicGlobal

  import ExUnit.CaptureIO

  alias Flezha.Convergence.Probe
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.File.Sink, as: FileSink
  alias Flezha.Service.Cli
  alias Flezha.Service.HubManager
  alias Flezha.Service.PluginManager

  @ns "plugin_page_ns"
  @caller %{source: :cli}

  setup :set_mimic_global_until_exit

  setup do
    wide =
      for n <- 1..25, do: {"r#{String.pad_leading("#{n}", 2, "0")}", [{"wide", plugin("wide")}]}

    Mimic.stub(HubManager, :on_hub_node, fn _ns, {m, f}, args, _none -> apply(m, f, args) end)

    Mimic.stub(HubManager, :relay_bindings, fn _ns ->
      [{"r07", [{"narrow", plugin("narrow")}]} | wide]
    end)

    Mimic.stub(HubManager, :declared_bindings, fn _ns -> [] end)

    Mimic.stub(HubManager, :plugin_entries, fn _ns ->
      Enum.map(~w(wide narrow idle), &plugin/1)
    end)

    Mimic.stub(Probe, :plugins_running, fn _ns, _relays -> [] end)
    :ok
  end

  defp plugin(name), do: %Plugin{id: name, provider: FileSink}

  test "a paged row carries 10 sorted relays and the count; the unpaged row every one" do
    {:ok, %{rows: rows, total: 3, next: nil}} = PluginManager.enabled(@ns, [limit: 10], @caller)
    wide = Enum.find(rows, &(&1.name == "wide"))
    assert wide.relays == for(n <- 1..10, do: "r#{String.pad_leading("#{n}", 2, "0")}")
    assert wide.relay_count == 25

    {:ok, unpaged} = PluginManager.enabled(@ns, @caller)
    assert length(Enum.find(unpaged, &(&1.name == "wide")).relays) == 25
    refute Map.has_key?(hd(unpaged), :relay_count)
  end

  test "the relay filter answers exactly the plugins bound to that relay" do
    {:ok, page} = PluginManager.enabled(@ns, [filter: [relay: "r07"]], @caller)
    assert Enum.map(page.rows, & &1.name) == ["narrow", "wide"]

    {:ok, [%{provider_id: provider} | _]} = PluginManager.enabled(@ns, @caller)
    filter = %{"text" => "ID", "provider" => provider}
    {:ok, page} = PluginManager.enabled(@ns, [filter: filter], @caller)
    assert Enum.map(page.rows, & &1.name) == ["idle", "wide"]
  end

  test "flz plugin list --limit names the sample's full count and the page" do
    args = Jason.encode!([@ns, %{"limit" => 2}])
    text = capture_io(fn -> Cli.call("PluginManager.enabled", args, []) end)

    assert text =~ "Showing 2 of 3."
    assert [_, cursor] = Regex.run(~r/Next page: --after (\S+)/, text)

    args = Jason.encode!([@ns, %{"limit" => 2, "after" => cursor}])
    rest = capture_io(fn -> Cli.call("PluginManager.enabled", args, []) end)
    assert rest =~ "wide  " and rest =~ "r10 (25 in all)"
    assert rest =~ "Showing 1 of 3." and not (rest =~ "Next page")
  end
end

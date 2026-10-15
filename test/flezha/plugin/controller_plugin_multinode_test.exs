defmodule FlezhaTest.Plugin.ControllerPluginMultinodeTest do
  @moduledoc """
  A controller plugin across three controllers: a copy on each without
  `single` and exactly one with it; an event is delivered by the copy on the
  controller where it happened, or by the one `single` copy wherever that runs.
  """

  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.Cluster.Directory
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.HookWebhook
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.WebhookServer

  @moduletag :multi_node
  @namespace "default"
  @factory "store_input"

  setup do
    TestHelpers.reset_all_cells()
    {pid1, p1} = NodeTestHelper.start_peer_node(:hooks_p1)
    {pid2, p2} = NodeTestHelper.start_peer_node(:hooks_p2)

    on_exit(fn ->
      Enum.each(
        ["hook_local", "hook_single", "hook_late", "hook_joined"],
        &PluginManager.disable_plugin(@namespace, &1)
      )

      for {pid, peer} <- [{pid1, p1}, {pid2, p2}] do
        try do
          NodeTestHelper.stop_peer_node(pid, peer)
        catch
          _, _ -> :ok
        end
      end

      RelayManager.stop_all_relays(@namespace)
    end)

    for peer <- [p1, p2] do
      assert NodeTestHelper.wait_until(fn -> peer in hub_nodes() end, 30_000),
             "#{peer} never joined the hub cluster"
    end

    %{p1: p1, p2: p2, server: WebhookServer.start([200])}
  end

  defp hub_nodes, do: HubManager.get_hub_nodes(@namespace, false)

  defp hook(name, server, single) do
    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(name, %Plugin{
        id: name,
        provider: HookWebhook,
        namespace: @namespace,
        single: single,
        opts: %{url: "http://127.0.0.1:#{server.port}/hooks"},
        hooks: [{"on_dead_letter", []}]
      })
  end

  # A relay the hub placed on `node`: the class whose dead letter names
  # its namespace there.
  defp relay_on(node, prefix), do: RelayFixture.start_on(node, prefix, @factory, attempts: 30)

  defp dead_letter_on(node, relay) do
    entry =
      Flezha.Dlq.Entry.new!(%{
        relay_class: relay,
        tasklet_id: "t",
        cell_key: "k",
        channel: "in",
        payload: "x-#{System.unique_integer([:positive])}",
        error: "boom"
      })

    {:ok, _ref} = :erpc.call(node, Flezha.Dlq.Store, :append, [entry])
  end

  defp copies(name), do: Enum.sort(Directory.plugin_nodes(@namespace, name))

  defp received_from do
    assert_receive {:request, _line, _headers, body}, 10_000
    %{"controller" => controller} = Jason.decode!(body)
    controller
  end

  test "without single a copy runs on every controller and a dead letter reaches only its own",
       %{p2: p2, server: server} = ctx do
    hook("hook_local", server, false)
    everywhere = Enum.sort([node(), ctx.p1, p2])
    # A copy another controller runs is seen here once the directory syncs.
    assert NodeTestHelper.wait_until(fn -> copies("hook_local") == everywhere end, 5_000)

    op = relay_on(p2, "dl_local")
    dead_letter_on(p2, op)

    assert received_from() == to_string(p2)
    refute_receive {:request, _, _, _}, 500

    assert {:ok, _} = PluginManager.disable_plugin(@namespace, "hook_local")
    assert NodeTestHelper.wait_until(fn -> copies("hook_local") == [] end, 5_000)
  end

  test "a copy placed for a home that is gone does not run, and a joining controller gets one",
       %{p2: p2, server: server} = ctx do
    hook("hook_late", server, false)
    assert NodeTestHelper.wait_until(fn -> length(copies("hook_late")) == 3 end, 5_000)
    {:ok, %Plugin{} = entry} = HubManager.plugin_entry(@namespace, "hook_late")

    # A disable lands while a home's placement is still on its way to p2.
    assert {:ok, _} = PluginManager.disable_plugin(@namespace, "hook_late")
    gone = spawn(fn -> :ok end)

    :ok =
      :erpc.call(p2, Flezha.Plugin.Shared, :place, [%Plugin{entry | namespace: @namespace}, gone])

    assert NodeTestHelper.wait_until(fn -> copies("hook_late") == [] end, 5_000)

    # A controller that joins the namespace's hub gets a copy of an enabled one.
    hook("hook_joined", server, false)
    {pid3, p3} = NodeTestHelper.start_peer_node(:hooks_p3)
    on_exit(fn -> NodeTestHelper.stop_peer_node(pid3, p3) end)
    assert NodeTestHelper.wait_until(fn -> p3 in hub_nodes() end, 30_000)
    everywhere = Enum.sort([node(), ctx.p1, p2, p3])
    assert NodeTestHelper.wait_until(fn -> copies("hook_joined") == everywhere end, 10_000)

    assert {:ok, _} = PluginManager.disable_plugin(@namespace, "hook_joined")
    assert NodeTestHelper.wait_until(fn -> copies("hook_joined") == [] end, 5_000)
  end

  test "with single one copy runs, and a dead letter elsewhere reaches it",
       %{server: server} = ctx do
    hook("hook_single", server, true)
    assert NodeTestHelper.wait_until(fn -> length(copies("hook_single")) == 1 end, 5_000)
    [home] = copies("hook_single")

    elsewhere = Enum.find([node(), ctx.p1, ctx.p2], &(&1 != home))
    op = relay_on(elsewhere, "dl_single")
    dead_letter_on(elsewhere, op)

    assert received_from() == to_string(elsewhere)
    refute_receive {:request, _, _, _}, 500
    assert copies("hook_single") == [home]

    assert {:ok, _} = PluginManager.disable_plugin(@namespace, "hook_single")
    assert NodeTestHelper.wait_until(fn -> copies("hook_single") == [] end, 5_000)
  end
end

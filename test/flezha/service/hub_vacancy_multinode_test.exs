defmodule Flezha.Service.HubVacancyMultinodeTest do
  @moduledoc """
  The vacancy reclaim over two members of one namespace hub: of two members
  holding nothing here the declared list's leader keeps the hub, a member
  running only a plugin home keeps it against a leader running nothing, and a
  leave never deletes a registry directory.
  """

  use FlezhaTest.Case, fixtures: [:env, :tmp_dir], async: false

  @moduletag :multi_node
  @moduletag :capture_log

  alias Flezha.Convergence.Snapshot
  alias Flezha.Identificator
  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.File.Sink, as: FileSink
  alias Flezha.Service.HubManager
  alias Flezha.Service.HubManager.Rows
  alias Flezha.Service.PluginManager
  alias Flezha.Service.RelayManager.Init
  alias FlezhaTest.Helper.HubRegistryTestHelper
  alias FlezhaTest.Helper.HubRegistryTestHelper.StubChild
  alias FlezhaTest.Helper.NodeTestHelper
  alias ProcessHub.Service.DeclaredChildren

  # No reconcile round opens, so a declared relay stays running nowhere: the
  # settle is the hub's ceiling, longer than any test here.
  @hub_env [
    auto_restore_enabled: true,
    process_hub_cluster_settle_ms: 60_000,
    process_hub_reconcile_grace_ms: 600_000
  ]

  # One peer for the module, on its own registry root: each test runs its own
  # namespace's hub on both nodes and stops it on both.
  setup_all do
    {pid, peer} = NodeTestHelper.start_peer_node(:vacancy_p1, app_env: @hub_env)
    on_exit(fn -> NodeTestHelper.stop_peer_node(pid, peer) end)
    {:ok, peer: peer}
  end

  setup %{tmp: tmp, peer: peer} do
    Application.put_env(:flezha, :process_hub_registry_root, Path.join(tmp, "process_hub"))
    for {key, value} <- @hub_env, do: Application.put_env(:flezha, key, value)

    namespace = "vacancy-#{System.unique_integer([:positive])}"

    on_exit(fn ->
      on(peer, HubManager, :stop_hub, [namespace])
      HubManager.stop_hub(namespace)
    end)

    for at <- [node(), peer],
        do: {:ok, _} = on(at, HubRegistryTestHelper, :ensure_hub, [namespace])

    assert NodeTestHelper.wait_until(
             fn -> HubManager.get_hub_nodes(namespace) == [peer] end,
             30_000
           )

    for at <- [node(), peer],
        do: on(at, Snapshot, :observe_recovery, [Identificator.hub(namespace), :normal])

    leader = DeclaredChildren.leader(ProcessHub.Hub.get(Identificator.hub(namespace)))
    [other] = [node(), peer] -- [leader]
    %{namespace: namespace, leader: leader, other: other}
  end

  test "of two members holding nothing here, the leader keeps the hub", ctx do
    %{namespace: ns, leader: leader, other: other} = ctx
    declare(ns, leader, "declared_op")
    assert NodeTestHelper.wait_until(fn -> declares?(other, ns) end, 10_000)

    refute on(other, HubManager, :holds_anything?, [ns])
    assert on(leader, HubManager, :holds_anything?, [ns])

    for at <- [other, leader], do: :ok = on(at, HubManager, :stop_hub_if_vacant, [ns])

    refute hub_running?(other, ns)
    assert hub_running?(leader, ns)
    for at <- [other, leader], do: assert(registry_dir?(at, ns))

    # The left member's hub, started again, takes the leader's list.
    declare(ns, leader, "declared_later")
    {:ok, _} = on(other, HubRegistryTestHelper, :ensure_hub, [ns])

    assert NodeTestHelper.wait_until(
             fn -> "declared_later" in declared_ids(other, ns) end,
             10_000
           )
  end

  test "a leader running nothing and a member running only a plugin home do not both leave",
       %{tmp: tmp} = ctx do
    %{namespace: ns, leader: leader, other: other} = ctx
    name = home_on(ns, other, tmp)

    assert on(other, HubManager, :holds_anything?, [ns])
    refute on(leader, HubManager, :holds_anything?, [ns])

    for at <- [other, leader], do: :ok = on(at, HubManager, :stop_hub_if_vacant, [ns])

    refute hub_running?(leader, ns)
    assert hub_running?(other, ns)
    assert [{{:plugin, ^name}, home}] = on(other, HubManager, :local_children, [ns, :all])
    assert on(other, Process, :alive?, [home])
    for at <- [other, leader], do: assert(registry_dir?(at, ns))
  end

  # A declared relay nothing starts: the declared set's leader-side upsert
  # writes the entry and starts, stops or moves nothing.
  defp declare(namespace, leader, relay_id) do
    spec = %{
      id: relay_id,
      start: {StubChild, :start_link, [{namespace, %{id: relay_id, factory_id: "market"}}]},
      restart: :transient
    }

    :ok = on(leader, Init, :call_declared_mutate, [Identificator.hub(namespace), spec])
  end

  # A plugin's home lands where the hub hashes its name, so names are tried
  # until one lands on `host`.
  defp home_on(namespace, host, tmp, attempt \\ 1) do
    assert attempt <= 20, "no plugin name hashed onto #{host}"
    name = "home_#{attempt}"
    sink = %Plugin{id: name, provider: FileSink, namespace: namespace, opts: %{path: tmp}}
    {:ok, _} = PluginManager.enable_plugin(name, sink)

    assert NodeTestHelper.wait_until(fn -> home_node(namespace, name) != nil end, 10_000)

    if home_node(namespace, name) == host do
      name
    else
      {:ok, _} = PluginManager.disable_plugin(namespace, name)
      home_on(namespace, host, tmp, attempt + 1)
    end
  end

  defp home_node(namespace, name) do
    case ProcessHub.child_lookup(Identificator.hub(namespace), {:plugin, name}) do
      {_spec, [{host, _pid}]} -> host
      _none -> nil
    end
  end

  defp declares?(at, namespace), do: on(at, Rows, :declares_anything?, [namespace])

  defp declared_ids(at, namespace),
    do: for(%{id: id} <- on(at, HubManager, :declared_relays, [namespace]), do: id)

  defp hub_running?(at, namespace),
    do: namespace in on(at, HubManager, :get_running_namespaces, [])

  defp registry_dir?(at, namespace) do
    dir = on(at, HubRegistryTestHelper, :registry_file, [namespace]) |> Path.dirname()
    on(at, File, :dir?, [dir])
  end

  defp on(at, module, fun, args), do: :erpc.call(at, module, fun, args, 15_000)
end

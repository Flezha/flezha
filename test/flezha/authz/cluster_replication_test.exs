defmodule Flezha.AuthZ.ClusterReplicationTest do
  @moduledoc """
  One authorization state per cluster: controllers each running the full
  authorization subtree against their own auth directory honour a credential,
  a revoke, a binding, a namespace, a bootstrap claim and a runner drop made on
  another one, including one that connects after the write.
  """

  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.AuthZ.{Bootstrap, NamespaceStore, RoleStore, TokenStore}
  alias Flezha.Runner.DropMarks
  alias FlezhaTest.Helper.NodeTestHelper

  @moduletag :multi_node

  setup do
    {a, a_dir} = start_controller(:authz_a)
    {b, b_dir} = start_controller(:authz_b)

    assert NodeTestHelper.wait_until(fn -> b in :erpc.call(a, Node, :list, []) end, 30_000),
           "#{b} never connected to #{a}"

    %{a: a, b: b, a_dir: a_dir, b_dir: b_dir}
  end

  test "a token issued on one controller is verified by another", %{a: a, b: b, b_dir: b_dir} do
    admin = claim!(a)
    refute File.exists?(Path.join(b_dir, "bootstrap.token"))

    {:ok, raw, _entry} = on(a, TokenStore, :issue, ["ops", "_system", :ops, :never])

    assert {:ok, %{principal: "ops", role: :ops}} = on(b, TokenStore, :verify, [raw])
    assert {:ok, %{role: :admin}} = on(b, TokenStore, :verify, [admin])
  end

  test "a token revoked on one controller is refused by another", %{a: a, b: b} do
    claim!(a)
    {:ok, raw, entry} = on(a, TokenStore, :issue, ["ops", "_system", :ops, :never])

    {:ok, _} = on(b, TokenStore, :revoke, [String.slice(entry.token_hash, 0, 12)])

    assert {:error, %Flezha.Error{code: "authz.token_revoked"}} =
             on(a, TokenStore, :verify, [raw])
  end

  test "a binding and a namespace made on one controller are read on another", %{a: a, b: b} do
    claim!(a)
    {:ok, _} = on(a, NamespaceStore, :create, ["team-orders"])
    :ok = on(a, RoleStore, :bind, ["alice", "team-orders", :viewer])

    assert {:ok, %{name: "team-orders"}} = on(b, NamespaceStore, :get, ["team-orders"])
    assert {:ok, %{roles: [:viewer]}} = on(b, RoleStore, :lookup, ["alice"])

    :ok = on(b, RoleStore, :unbind, ["alice"])
    :ok = on(b, NamespaceStore, :delete, ["team-orders"])

    assert :error = on(a, RoleStore, :lookup, ["alice"])
    assert :error = on(a, NamespaceStore, :get, ["team-orders"])
  end

  test "the bootstrap is claimed once per cluster", %{a: a, b: b, b_dir: b_dir} do
    b_token = File.read!(Path.join(b_dir, "bootstrap.token"))
    claim!(a)

    assert {:error, :bootstrap_already_completed} = on(b, Bootstrap, :claim, [b_token])
  end

  test "a controller that connects later honours earlier writes", %{a: a} do
    claim!(a)
    {:ok, raw, _entry} = on(a, TokenStore, :issue, ["ops", "_system", :ops, :never])

    {c, c_dir} = start_controller(:authz_c)

    assert NodeTestHelper.wait_until(
             fn -> match?({:ok, _}, on(c, TokenStore, :verify, [raw])) end,
             30_000
           ),
           "#{c} never adopted the token issued before it joined"

    assert NodeTestHelper.wait_until(
             fn -> not File.exists?(Path.join(c_dir, "bootstrap.token")) end,
             5_000
           )
  end

  test "a runner drop placed on one controller is held by another, and a readmit lifts it everywhere",
       %{a: a, b: b} do
    mark = %{
      namespace: "team-orders",
      stable_id: "runner-1",
      incarnation: "process-1",
      pid: nil,
      node: a,
      dropped_at: 1,
      dropped_by: "ops"
    }

    {:ok, ^mark} = on(a, DropMarks, :put, [mark])

    assert on(b, DropMarks, :standing, ["team-orders", "runner-1"]) == mark

    {c, _c_dir} = start_controller(:authz_drop_c)

    assert NodeTestHelper.wait_until(
             fn -> on(c, DropMarks, :standing, ["team-orders", "runner-1"]) == mark end,
             30_000
           ),
           "#{c} never adopted the drop placed before it joined"

    {:ok, ^mark} = on(b, DropMarks, :clear, ["team-orders", "runner-1"])

    for node <- [a, c], do: assert(on(node, DropMarks, :list, ["team-orders"]) == [])
  end

  # A peer running the whole authorization subtree, enforced, against an auth
  # directory of its own. It launches none of the suite's runners: they carry
  # no runner credential, so an enforcing controller would refuse every one.
  defp start_controller(name) do
    dir = NodeTestHelper.claim_tmp_dir("flz_authz_#{name}")
    File.chmod!(dir, 0o700)
    auth_dir = Path.join(dir, "auth")
    on_exit(fn -> File.rm_rf(dir) end)

    authz =
      Application.get_env(:flezha, :authn_authz, [])
      |> Keyword.merge(
        enabled: true,
        application_managed: true,
        auth_dir: auth_dir,
        bootstrap_token_path: Path.join(auth_dir, "bootstrap.token")
      )

    {_pid, node} =
      NodeTestHelper.start_peer_node(name, app_env: [authn_authz: authz, external_processes: []])

    {node, auth_dir}
  end

  defp claim!(node) do
    token = node |> on(Flezha.AuthZ.Config, :bootstrap_token_path, []) |> File.read!()
    {:ok, %{minted_token: minted}} = on(node, Bootstrap, :claim, [String.trim(token)])
    minted
  end

  defp on(node, module, fun, args), do: :erpc.call(node, module, fun, args, 15_000)
end

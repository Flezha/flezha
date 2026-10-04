defmodule Flezha.Service.NamespaceListTest do
  @moduledoc """
  What a caller may be shown when it asks which namespaces there are:
  `Flezha.Service.Operator.list_namespaces/1`, which `flz namespace list` and a
  console both reach, answers the durable catalog with whether each namespace
  runs a hub here.

  The catalog is the source because reading the running hub set left a
  namespace created a moment earlier invisible until something connected to
  it, so `create` followed by `list` disagreed. The verb row is deliberately
  `:list_namespaces` (`operator.list` at `:caller` scope) rather than
  `:namespace_list` (`authz.namespace_list` at `:system` scope), which would
  take the verb away from every namespace-scoped caller who has it.

  The rule is `Flezha.AuthZ.Namespace.visible/3` with `unbound: :all`, the
  rule's fail-open branch, opt-in for that reason; the last describe pins what
  it means at the `:enforced` posture the rest of this module does not run at.
  """

  # async: false: writes Application env; the auth stores are node-global singletons.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_ns_list", enabled: false}, :env],
    async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ
  alias Flezha.AuthZ.AuthState
  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.RoleStore
  alias Flezha.AuthZ.Verbs
  alias Flezha.Service.Cli
  alias Flezha.Service.HubManager
  alias Flezha.Service.Operator

  describe "the catalog is the source" do
    test "a namespace created a moment ago is listed" do
      {:ok, _} = NamespaceStore.create("billing")

      output = capture_io(fn -> Cli.call("Operator.list_namespaces", "[]", []) end)

      assert output =~ "billing"
      assert output =~ "_system"
    end

    test "the output says which namespaces have a hub on this controller" do
      {:ok, _} = NamespaceStore.create("with-hub")
      {:ok, _} = NamespaceStore.create("without-hub")
      {:ok, _} = HubManager.start_hub("with-hub")
      on_exit(fn -> HubManager.stop_hub("with-hub") end)

      rows =
        capture_io(fn -> Cli.call("Operator.list_namespaces", "[]", output: "json") end)
        |> Jason.decode!()

      by_name = Map.new(rows, &{&1["namespace"], &1["running"]})

      assert by_name["with-hub"] == true
      assert by_name["without-hub"] == false
    end

    test "an empty answer says so rather than rendering a headerless table" do
      output = capture_io(fn -> Cli.render({:ok, []}, "Operator.list_namespaces", "[]", []) end)

      assert output =~ "No namespaces found."
      assert {:ok, []} = Operator.list_namespaces(bound("nothing-here"))
    end
  end

  describe "what a caller may see" do
    setup do
      {:ok, _} = NamespaceStore.create("team-orders")
      {:ok, _} = NamespaceStore.create("team-billing")
      :ok
    end

    test "a namespace-scoped caller sees its own namespace and no other" do
      assert {:ok, [%{namespace: "team-orders"}]} =
               Operator.list_namespaces(bound("team-orders"))
    end

    test "an admin in the system namespace sees them all" do
      assert {:ok, rows} =
               Operator.list_namespaces(%{
                 source: :cli,
                 role: :admin,
                 namespace: "_system",
                 principal: "root"
               })

      assert ["_system", "team-billing", "team-orders"] = names(rows)
    end
  end

  describe "the published read a console calls" do
    setup do
      for namespace <- ["team-orders", "team-billing"] do
        {:ok, _} = NamespaceStore.create(namespace)
        {:ok, _} = HubManager.start_hub(namespace)
      end

      on_exit(fn -> Enum.each(["team-orders", "team-billing"], &HubManager.stop_hub/1) end)
      :ok
    end

    test "a namespace-scoped caller sees its own namespace and no other" do
      assert {:ok, [%{namespace: "team-orders", running: true}]} =
               Operator.list_namespaces(%{
                 source: :ui,
                 role: :viewer,
                 namespace: "team-orders",
                 principal: "viewer-1"
               })
    end

    test "an admin in the system namespace still sees the whole cluster" do
      assert {:ok, running} =
               Operator.list_namespaces(%{
                 source: :ui,
                 role: :admin,
                 namespace: "_system",
                 principal: "root"
               })

      assert "team-orders" in names(running)
      assert "team-billing" in names(running)
    end
  end

  # Every case above runs at the `:open` posture, which is the deployment the
  # module's fixture builds. These run at `:enforced`, where `authorize/3`
  # really judges, because the open answer for a caller carrying no namespace
  # is justified by what can reach a body under enforcement and nothing else
  # pinned that.
  describe "under enforcement" do
    setup do
      previous = Application.get_env(:flezha, :authn_authz, [])
      Application.put_env(:flezha, :authn_authz, Keyword.put(previous, :enabled, true))
      :ok = AuthState.set_enabled()
      on_exit(fn -> Application.put_env(:flezha, :authn_authz, previous) end)

      assert AuthZ.enforcement_posture() == :enforced

      {:ok, _} = NamespaceStore.create("team-orders")
      {:ok, _} = NamespaceStore.create("team-billing")
      {:ok, _} = HubManager.start_hub("team-orders")
      {:ok, _} = HubManager.start_hub("team-billing")
      on_exit(fn -> Enum.each(["team-orders", "team-billing"], &HubManager.stop_hub/1) end)
      :ok
    end

    test "a tenant-bound caller is judged, runs, and sees only its own namespace" do
      assert {:ok, [%{namespace: "team-orders"}]} =
               Operator.list_namespaces(%{
                 source: :ui,
                 role: :viewer,
                 namespace: "team-orders",
                 principal: "viewer-1"
               })
    end

    test "a tenant-bound principal carried only by the role store is refused, not narrowed" do
      :ok = RoleStore.bind("tenant-alice", "team-orders", :viewer, bound_by: "root")

      assert {:error, %Flezha.Error{code: "authz.namespace_mismatch"}} =
               Operator.list_namespaces(%{source: :ui, principal: "tenant-alice"})
    end

    # The fail-open, pinned rather than left to be discovered. A principal the
    # role store binds in `_system` reaches the body with no namespace on its
    # context, and `unbound: :all` then answers the whole cluster for a viewer.
    # It crosses no tenant boundary — the case above is why — but it is wider
    # than the admin clause, so it is written down and cannot change in silence.
    test "a system-bound principal carried only by the role store gets the open answer" do
      :ok = RoleStore.bind("system-alice", "_system", :viewer, bound_by: "root")

      assert {:ok, running} =
               Operator.list_namespaces(%{source: :ui, principal: "system-alice"})

      assert "team-orders" in names(running)
      assert "team-billing" in names(running)
    end
  end

  test "the verb is judged as an operator listing in the caller's own scope" do
    assert %{action: :"operator.list", scope: :caller} = Verbs.fetch!(:list_namespaces)
  end

  defp bound(namespace), do: %{source: :cli, role: :ops, namespace: namespace, principal: "op"}

  defp names(rows), do: rows |> Enum.map(& &1.namespace) |> Enum.sort()
end

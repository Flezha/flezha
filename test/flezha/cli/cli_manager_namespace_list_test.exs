defmodule Flezha.Service.CliManagerNamespaceListTest do
  @moduledoc """
  What a caller may be shown when it asks which namespaces there are, on both
  spellings of the question: `flz namespace list`, which answers the durable
  catalog, and `Flezha.Service.Operative.list_namespaces/1`, the published
  read a console calls, which answers the running hub set.

  The catalog is the terminal's source because reading the running hub set
  left a namespace created a moment earlier invisible until something
  connected to it, so `create` followed by `list` disagreed. The verb row is
  deliberately untouched: routing the body through the published
  `Flezha.Service.Namespace.list/1` would have swapped `:list_namespaces`
  (`operative.list` at `:caller` scope) for `:namespace_list`
  (`authz.namespace_list` at `:system` scope) and taken the verb away from
  every namespace-scoped operator who has it today.

  Both spellings apply one rule, `Flezha.AuthZ.Namespace.visible/3`, so a
  scoped operator and the console it uses cannot disagree about which
  namespaces exist. Both pass `unbound: :all`, which is the rule's fail-open
  branch and is opt-in for that reason; the last describe pins what it means
  at the `:enforced` posture the rest of this module does not run at.
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
  alias Flezha.AuthZ.Verb.Call
  alias Flezha.AuthZ.Verbs
  alias Flezha.Service.Cli.Listing
  alias Flezha.Service.HubManager
  alias Flezha.Service.Operative

  describe "the catalog is the source" do
    test "a namespace created a moment ago is listed" do
      {:ok, _} = NamespaceStore.create("billing")

      output = capture_io(fn -> Listing.list_namespaces("text", unbound_call()) end)

      assert output =~ "billing"
      assert output =~ "_system"
    end

    test "the output says which namespaces have a hub on this controller" do
      {:ok, _} = NamespaceStore.create("with-hub")
      {:ok, _} = NamespaceStore.create("without-hub")
      {:ok, _} = HubManager.start_hub("with-hub")
      on_exit(fn -> HubManager.stop_hub("with-hub") end)

      %{"namespaces" => rows} =
        capture_io(fn -> Listing.list_namespaces("json", unbound_call()) end)
        |> Jason.decode!()

      by_name = Map.new(rows, &{&1["namespace"], &1["running"]})

      assert by_name["with-hub"] == true
      assert by_name["without-hub"] == false
    end

    test "an empty answer says so rather than rendering a headerless table" do
      output = capture_io(fn -> Listing.list_namespaces("text", bound_call("nothing-here")) end)

      assert output =~ "No namespaces found."
    end
  end

  describe "what a caller may see" do
    setup do
      {:ok, _} = NamespaceStore.create("team-orders")
      {:ok, _} = NamespaceStore.create("team-billing")
      :ok
    end

    test "a namespace-scoped caller sees its own namespace and no other" do
      output = capture_io(fn -> Listing.list_namespaces("text", bound_call("team-orders")) end)

      assert output =~ "team-orders"
      refute output =~ "team-billing"
      refute output =~ "_system"
    end

    test "an admin in the system namespace sees them all" do
      call = %Call{context: %{role: :admin, namespace: "_system", principal: "root"}}
      output = capture_io(fn -> Listing.list_namespaces("text", call) end)

      assert output =~ "team-orders"
      assert output =~ "team-billing"
      assert output =~ "_system"
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
      assert {:ok, ["team-orders"]} =
               Operative.list_namespaces(%{
                 source: :ui,
                 role: :viewer,
                 namespace: "team-orders",
                 principal: "viewer-1"
               })
    end

    test "an admin in the system namespace still sees the whole cluster" do
      assert {:ok, running} =
               Operative.list_namespaces(%{
                 source: :ui,
                 role: :admin,
                 namespace: "_system",
                 principal: "root"
               })

      assert "team-orders" in running
      assert "team-billing" in running
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

      {:ok, _} = HubManager.start_hub("team-orders")
      {:ok, _} = HubManager.start_hub("team-billing")
      on_exit(fn -> Enum.each(["team-orders", "team-billing"], &HubManager.stop_hub/1) end)
      :ok
    end

    test "a tenant-bound caller is judged, runs, and sees only its own namespace" do
      assert {:ok, ["team-orders"]} =
               Operative.list_namespaces(%{
                 source: :ui,
                 role: :viewer,
                 namespace: "team-orders",
                 principal: "viewer-1"
               })
    end

    test "a tenant-bound principal carried only by the role store is refused, not narrowed" do
      :ok = RoleStore.bind("tenant-alice", "team-orders", :viewer, bound_by: "root")

      assert {:error, {:forbidden, :namespace_mismatch}} =
               Operative.list_namespaces(%{source: :ui, principal: "tenant-alice"})
    end

    # The fail-open, pinned rather than left to be discovered. A principal the
    # role store binds in `_system` reaches the body with no namespace on its
    # context, and `unbound: :all` then answers the whole cluster for a viewer.
    # It crosses no tenant boundary — the case above is why — but it is wider
    # than the admin clause, so it is written down and cannot change in silence.
    test "a system-bound principal carried only by the role store gets the open answer" do
      :ok = RoleStore.bind("system-alice", "_system", :viewer, bound_by: "root")

      assert {:ok, running} =
               Operative.list_namespaces(%{source: :ui, principal: "system-alice"})

      assert "team-orders" in running
      assert "team-billing" in running
    end
  end

  # The assertion that the data source moved and the authorization did not:
  # green on both sides of the change, which is what makes it worth pinning.
  test "the verb binds the same row, action and scope it bound before" do
    assert %{action: :"operative.list", scope: :caller} = Verbs.fetch!(:list_namespaces)
  end

  # An unbound call at the `:open` posture: the deployment `unbound: :all` is
  # for, where narrowing to `_system` would hide the operator's own
  # namespaces.
  defp unbound_call(), do: %Call{context: %{source: :cli}}

  defp bound_call(namespace),
    do: %Call{context: %{role: :operator, namespace: namespace, principal: "op"}}
end

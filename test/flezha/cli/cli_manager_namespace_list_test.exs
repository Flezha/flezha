defmodule Flezha.Service.CliManagerNamespaceListTest do
  @moduledoc """
  `flz namespace list` answers from the durable catalog.

  It read the running hub set, so a namespace created a moment earlier was
  invisible until something connected to it and `create` followed by `list`
  disagreed. The verb row is deliberately untouched: routing the body through
  the published `Flezha.Service.Namespace.list/1` would have swapped
  `:list_namespaces` (`operative.list` at `:caller` scope) for
  `:namespace_list` (`authz.namespace_list` at `:system` scope) and taken the
  verb away from every namespace-scoped operator who has it today.
  """

  # async: false: writes Application env; the auth stores are node-global singletons.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_ns_list", enabled: false}, :env],
    async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.AuthZ.Verb.Call
  alias Flezha.AuthZ.Verbs
  alias Flezha.Service.Cli.Listing
  alias Flezha.Service.HubManager

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

  # The assertion that the data source moved and the authorization did not:
  # green on both sides of the change, which is what makes it worth pinning.
  test "the verb binds the same row, action and scope it bound before" do
    assert %{action: :"operative.list", scope: :caller} = Verbs.fetch!(:list_namespaces)
  end

  # Enforcement is off in this module, which is the deployment an unbound
  # caller comes from; narrowing that caller to `_system` would hide the
  # operator's own namespaces.
  defp unbound_call(), do: %Call{context: %{source: :cli}}

  defp bound_call(namespace),
    do: %Call{context: %{role: :operator, namespace: namespace, principal: "op"}}
end

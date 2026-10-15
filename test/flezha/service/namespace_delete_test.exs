defmodule Flezha.Service.NamespaceDeleteTest do
  @moduledoc """
  `Flezha.Service.Namespace.delete/2`, which `flz namespace delete`, HTTP and
  MCP all reach, refuses while the namespace still has relays or a runner
  attached, naming them, and leaves the catalog entry in place.
  """

  # async: false: the auth stores and the FleetManager are node-global singletons.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_ns_delete", enabled: false}],
    async: false

  import Mimic

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.Namespace
  alias FlezhaTest.Helper.FleetStateHelper

  @context %{source: :cli, principal: "admin@_system"}

  setup do
    fleet = :sys.get_state(FleetManager)
    on_exit(fn -> :sys.replace_state(FleetManager, fn _ -> fleet end) end)
    :ok
  end

  test "a namespace with a runner attached is refused, naming the runner" do
    {:ok, _} = NamespaceStore.create("trading")
    FleetStateHelper.seed("trading", %{"trading-runner-1" => ["do_nothing"]})

    assert {:error, %Flezha.Error{code: "namespace.in_use", class: :conflict} = error} =
             Namespace.delete("trading", @context)

    assert error.details == %{
             "namespace" => "trading",
             "relays" => [],
             "runners" => ["trading-runner-1"]
           }

    assert error.message =~ "trading-runner-1"
    assert {:ok, _} = NamespaceStore.get("trading")
  end

  test "a namespace with a relay registered is refused, naming the relay as the docs show" do
    {:ok, _} = NamespaceStore.create("trading")
    FleetStateHelper.seed("trading", %{"trading-runner-1" => ["do_nothing"]})
    stub(HubManager, :relay_rows, fn "trading" -> [%{id: "candles"}] end)

    assert {:error, %Flezha.Error{code: "namespace.in_use"} = error} =
             Namespace.delete("trading", @context)

    assert error.details["relays"] == ["candles"]

    # The text `docs/topics/core/namespaces.md` prints under "Delete a namespace".
    assert Flezha.Error.render(error, :text) ==
             "error: namespace 'trading' still has relays [candles] and runners " <>
               "[trading-runner-1] attached; stop them before deleting it (namespace.in_use)\n" <>
               "hint: flz relay list --namespace trading"

    assert {:ok, _} = NamespaceStore.get("trading")
  end

  test "a name the catalog does not hold is still not found, attached or not" do
    stub(HubManager, :relay_rows, fn "ghost" -> [%{id: "candles"}] end)

    assert {:error, %Flezha.Error{code: "namespace.not_found"}} =
             Namespace.delete("ghost", @context)
  end

  test "an empty namespace is deleted" do
    {:ok, _} = NamespaceStore.create("empty")

    assert :ok = Namespace.delete("empty", @context)
    assert :error = NamespaceStore.get("empty")
  end
end

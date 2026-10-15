defmodule Flezha.Service.ControllerRecoveryPerNodeTest do
  @moduledoc """
  `Controller.recovery_per_node` through the door: a node arrives as the JSON
  string a remote caller can write, and is still asked.
  """
  # async: false: writes Application env and a shared on-disk auth root; starts a hub.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_kb703", claim: true}],
    async: false

  alias Flezha.Service.Admin
  alias Flezha.Service.Controller
  alias FlezhaTest.Helper.HubRegistryTestHelper

  @ns "kb703-ns"

  defp viewer, do: %{source: :http, principal: "alice", namespace: @ns, role: :viewer}

  setup do
    HubRegistryTestHelper.stop_new_hubs_on_exit()
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@ns)
    :ok
  end

  # Fails when: the string never equals `node()`, the `:rpc.call` on a binary
  # raises, and the catch answers `:unreachable` for the node that answered.
  test "this node, named as a string, answers its own recovery state" do
    here = Atom.to_string(node())
    expected = Controller.local_recovery_snapshot(Flezha.Identificator.hub(@ns))

    assert {:ok, answer} =
             Admin.run("Controller.recovery_per_node", [@ns, [here]], viewer(), :given)

    assert Map.values(answer) == [expected]
    refute Map.values(answer) == [:unreachable]
  end

  # Fails when: `""` is not read as `default`, so the hub asked is the empty
  # namespace's, which no node runs, and this node answers `:unknown`.
  test "an empty namespace reads the default namespace's hub" do
    {:ok, _} = HubRegistryTestHelper.ensure_hub("default")
    expected = Controller.local_recovery_snapshot(Flezha.Identificator.hub("default"))
    caller = %{viewer() | namespace: "default"}

    assert {:ok, answer} =
             Admin.run(
               "Controller.recovery_per_node",
               ["", [Atom.to_string(node())]],
               caller,
               :given
             )

    assert Map.values(answer) == [expected]
    refute expected == :unknown
  end

  test "a node name that is no node answers unreachable and mints no atom" do
    name = "kb703-no-such-node-#{System.unique_integer([:positive])}@nowhere"

    assert {:ok, answer} =
             Admin.run("Controller.recovery_per_node", [@ns, [name]], viewer(), :given)

    assert Map.values(answer) == [:unreachable]
    assert_raise ArgumentError, fn -> String.to_existing_atom(name) end
  end
end

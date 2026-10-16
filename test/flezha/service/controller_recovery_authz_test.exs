defmodule Flezha.Service.ControllerRecoveryAuthzTest do
  @moduledoc """
  A hub's recovery state through the door is judged in the namespace the hub
  id names, not in the caller's own: a viewer of one namespace reads nothing of
  another's hub, neither by naming it nor in the boot status table.
  """
  # async: false: writes Application env and a shared on-disk auth root; starts hubs.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_kb704", claim: true}],
    async: false

  alias Flezha.Identificator
  alias Flezha.Service.Admin
  alias FlezhaTest.Helper.HubRegistryTestHelper

  @own "kb704-own"
  @other "kb704-other"

  defp viewer(namespace),
    do: %{source: :http, principal: "alice", namespace: namespace, role: :viewer}

  defp hub_of(namespace), do: namespace |> Identificator.hub() |> Atom.to_string()

  defp boot_status(ctx), do: Admin.run("Controller.boot_status", [], ctx, :given)

  defp recovery_state(hub_id, namespace),
    do: Admin.run("Controller.recovery_state", [hub_id], viewer(namespace), :given)

  setup do
    HubRegistryTestHelper.stop_new_hubs_on_exit()
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@own)
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@other)
    :ok
  end

  # Fails when: the hub id is judged at the caller's own namespace, so any
  # viewer reads any running namespace's recovery state by naming its hub.
  test "a viewer of one namespace is refused another namespace's hub recovery state" do
    assert {:error, %Flezha.Error{code: "authz.namespace_mismatch"}} =
             recovery_state(hub_of(@other), @own)
  end

  test "a viewer reads its own namespace's hub recovery state" do
    assert {:ok, state} = recovery_state(hub_of(@own), @own)
    assert is_atom(state)
  end

  test "a hub id that names no namespace is refused as no valid scope" do
    assert {:error, %Flezha.Error{code: "authz.invalid_scope"}} = recovery_state("ok", @own)
  end

  test "a hub id is never minted as an atom from the caller's text" do
    unminted = "kb704_never_minted_#{System.unique_integer([:positive])}_hub"

    assert {:error, %Flezha.Error{}} = recovery_state(unminted, @own)
    assert_raise ArgumentError, fn -> String.to_existing_atom(unminted) end
  end

  # Fails when: the boot status table lists every running namespace, so any
  # caller of the public verb learns every tenant on the node.
  test "boot status lists only the namespace a viewer is bound to" do
    assert {:ok, %{namespaces: rows}} = boot_status(viewer(@own))
    assert Enum.map(rows, & &1.namespace) == [@own]
  end

  test "boot status gives a caller with no identity the readiness verdict and no tenant" do
    assert {:ok, %{readiness: readiness, preconditions: [_ | _], namespaces: rows}} =
             boot_status(%{source: :http})

    assert is_atom(readiness)
    assert Enum.all?(rows, &(&1.namespace == "_system"))
  end

  test "boot status lists every running namespace to an admin of _system" do
    admin = %{source: :http, principal: "root", namespace: "_system", role: :admin}
    assert {:ok, %{namespaces: rows}} = boot_status(admin)
    assert MapSet.subset?(MapSet.new([@own, @other]), MapSet.new(rows, & &1.namespace))
  end
end

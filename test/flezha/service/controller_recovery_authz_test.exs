defmodule Flezha.Service.ControllerRecoveryAuthzTest do
  @moduledoc """
  A hub's recovery state through the door is judged in the namespace the hub
  id names, not in the caller's own: a viewer of one namespace reads nothing of
  another's hub.
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
end

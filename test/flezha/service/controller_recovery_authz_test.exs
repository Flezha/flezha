defmodule Flezha.Service.ControllerRecoveryAuthzTest do
  @moduledoc """
  The boot status table through the door lists only the namespaces a caller may
  see: a viewer of one namespace learns nothing of another's hub.
  """
  # async: false: writes Application env and a shared on-disk auth root; starts hubs.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_kb704", claim: true}],
    async: false

  alias Flezha.Service.Admin
  alias FlezhaTest.Helper.HubRegistryTestHelper

  @own "kb704-own"
  @other "kb704-other"

  defp viewer(namespace),
    do: %{source: :http, principal: "alice", namespace: namespace, role: :viewer}

  defp boot_status(ctx), do: Admin.run("Controller.boot_status", [], ctx, :given)

  setup do
    HubRegistryTestHelper.stop_new_hubs_on_exit()
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@own)
    {:ok, _} = HubRegistryTestHelper.ensure_hub(@other)
    :ok
  end

  # Fails when: the boot status table lists every running namespace, so any
  # caller of the public verb learns every tenant on the node.
  test "boot status lists only the namespace a viewer is bound to" do
    assert {:ok, %{rows: [%{namespaces: rows}]}} = boot_status(viewer(@own))
    assert Enum.map(rows, & &1.namespace) == [@own]
  end

  test "boot status gives a caller with no identity the readiness verdict and no tenant" do
    assert {:ok, %{rows: [%{readiness: readiness, preconditions: [_ | _], namespaces: rows}]}} =
             boot_status(%{source: :http})

    assert is_atom(readiness)
    assert Enum.all?(rows, &(&1.namespace == "_system"))
  end

  test "boot status lists every running namespace to an admin of _system" do
    admin = %{source: :http, principal: "root", namespace: "_system", role: :admin}
    assert {:ok, %{rows: [%{namespaces: rows}]}} = boot_status(admin)
    assert MapSet.subset?(MapSet.new([@own, @other]), MapSet.new(rows, & &1.namespace))
  end
end

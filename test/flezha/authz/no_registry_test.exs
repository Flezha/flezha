defmodule Flezha.AuthZ.NoRegistryTest do
  # async: false: stops the observability registry, as a boot with observability
  # disabled never starts it.
  use FlezhaTest.Case, fixtures: [{:auth_stores, stores: [], enabled: false}], async: false

  alias Flezha.AuthZ.TokenStore
  alias Flezha.Observability.TestSinks

  setup do
    :ok =
      Supervisor.terminate_child(Flezha.Observability.Supervisor, Flezha.Observability.Registry)

    on_exit(&TestSinks.restart_after_registry/0)
  end

  test "the authz supervisor starts and issues a token with no registry running" do
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        start_supervised!(Flezha.AuthZ.Supervisor)
        assert {:ok, _raw, _entry} = TokenStore.issue("no-registry", "default", :viewer, 60)
      end)

    assert log =~ "[authz] authn/authz is OFF"
  end
end

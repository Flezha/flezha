defmodule Flezha.AuthZ.NoRegistryTest do
  # async: false: stops the observability registry, as a boot with observability
  # disabled never starts it.
  use FlezhaTest.Case, fixtures: [{:auth_stores, stores: [], enabled: false}], async: false

  alias Flezha.AuthZ.TokenStore
  alias Flezha.Observability.TestSinks

  setup do
    :ok =
      Supervisor.terminate_child(Flezha.Observability.Supervisor, Flezha.Observability.Registry)

    # The opt-out's warning is the line asserted on; the test config logs errors only.
    Logger.put_module_level(Flezha.AuthZ.Supervisor, :warning)

    on_exit(fn ->
      Logger.delete_module_level(Flezha.AuthZ.Supervisor)
      TestSinks.restart_after_registry()
    end)
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

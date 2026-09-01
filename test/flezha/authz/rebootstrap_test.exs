defmodule Flezha.AuthZ.RebootstrapTest do
  @moduledoc """
  finish-platform-authn-authz §8.2: the offline break-glass marker
  invalidates a lost `admin@_system` (binding AND its `_system` admin
  tokens), and the next boot re-enters the ordinary bootstrap flow with a
  fresh token.
  """
  # async: false: a shared on-disk root; the primary Logger.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.AuthZ.{AuthState, Bootstrap, Config, NamespaceStore, RoleStore, TokenStore}
  alias Flezha.AuthZ.Bootstrap.OnBoot
  alias FlezhaTest.Helper.AuthTestHelper

  setup do
    ctx =
      AuthTestHelper.setup_auth_stores(
        prefix: "flz_rebootstrap",
        stores: [AuthState, RoleStore, NamespaceStore, TokenStore]
      )

    {:ok, Map.to_list(ctx)}
  end

  defp claim_bootstrap!() do
    :ok = Bootstrap.check_and_write()
    raw = File.read!(Config.bootstrap_token_path()) |> String.trim()
    {:ok, %{principal: principal, minted_token: minted}} = Bootstrap.claim(raw, [])
    {principal, minted}
  end

  test "recovery invalidates the old admin and re-enters bootstrap" do
    {principal, minted_admin_token} = claim_bootstrap!()
    assert RoleStore.admin_system_exists?()
    assert {:ok, _} = TokenStore.verify(minted_admin_token)
    assert AuthState.enabled?()

    # The operator (with filesystem access, controller stopped) writes the
    # marker — this is what `flz auth rebootstrap` does.
    File.write!(Bootstrap.recovery_marker_path(), "rebootstrap requested\n")

    log =
      AuthTestHelper.capture_log_at(:warning, fn ->
        assert :recovered = Bootstrap.consume_recovery_marker()
      end)

    assert log =~ "REBOOTSTRAP marker consumed"

    # The admin the marker replaced authorizes nothing: binding gone, token
    # revoked, claimed flag reset.
    refute RoleStore.admin_system_exists?()
    assert :error = RoleStore.lookup(principal)
    assert {:error, :token_revoked} = TokenStore.verify(minted_admin_token)
    refute AuthState.enabled?()
    refute File.exists?(Bootstrap.recovery_marker_path())

    # The ordinary bootstrap flow runs again with a FRESH token…
    :ok = Bootstrap.check_and_write()
    fresh = File.read!(Config.bootstrap_token_path()) |> String.trim()

    assert {:ok, %{principal: new_principal}} =
             Bootstrap.claim(fresh, principal: "recovered-admin")

    assert new_principal == "recovered-admin"
    assert RoleStore.admin_system_exists?()
    assert AuthState.enabled?()
  end

  test "a boot without the marker is a no-op" do
    {_principal, _} = claim_bootstrap!()

    assert :ok = Bootstrap.consume_recovery_marker()
    assert RoleStore.admin_system_exists?()
    assert AuthState.enabled?()
  end

  test "OnBoot consumes the marker and writes a fresh bootstrap token" do
    {_principal, _} = claim_bootstrap!()
    File.write!(Bootstrap.recovery_marker_path(), "rebootstrap requested\n")

    capture_log(fn -> assert :ignore = OnBoot.init([]) end)

    refute RoleStore.admin_system_exists?()
    assert File.exists?(Config.bootstrap_token_path())
  end
end

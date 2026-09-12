defmodule Flezha.AuthZ.RebootstrapTest do
  @moduledoc """
  finish-platform-authn-authz §8.2 and
  prove-the-namespace-and-recover-a-lost-admin §6.2: break-glass recovery for a
  lost `admin@_system`, with and without a stop.

  The offline marker invalidates the lost admin (binding AND its `_system` admin
  tokens) and the next boot re-enters the ordinary bootstrap flow with a fresh
  token. The online path needs no stop: the operator writes the same
  `bootstrap.token` file and claims it against the running controller, which
  invalidates the previous admin through the same function and audits that
  recovery occurred. A caller who wrote no file is refused naming the file it
  expected.
  """
  # async: false: a shared on-disk root; the primary Logger.
  use FlezhaTest.Case,
    fixtures: [
      {:auth_stores,
       prefix: "flz_rebootstrap",
       stores: [
         Flezha.AuthZ.AuthState,
         Flezha.AuthZ.RoleStore,
         Flezha.AuthZ.NamespaceStore,
         Flezha.AuthZ.TokenStore
       ]}
    ],
    async: false

  import ExUnit.CaptureLog

  alias Flezha.AuthZ.{AuthState, Bootstrap, Config, RoleStore, TokenStore}
  alias Flezha.AuthZ.Bootstrap.OnBoot
  alias FlezhaTest.Helper.AuthTestHelper

  # What `flz auth rebootstrap` does while the controller runs: mint a value and
  # put it where the bootstrap already reads one. `check_and_write/0` is the
  # CONTROLLER's writer and short-circuits while an admin binding exists, which
  # is exactly why recovery is the operator's write rather than a request to the
  # controller.
  defp write_bootstrap_token_as_operator!() do
    raw = :crypto.strong_rand_bytes(32) |> Base.encode16(case: :lower)
    File.write!(Config.bootstrap_token_path(), raw)
    File.chmod!(Config.bootstrap_token_path(), 0o600)
    raw
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

  describe "recovery against a RUNNING controller (§6.2)" do
    test "an admin holding other roles in _system is removed whole" do
      {lost_principal, _lost_token} = claim_bootstrap!()
      :ok = RoleStore.bind(lost_principal, "_system", :operator)
      assert {:ok, %{roles: [:operator, :admin]}} = RoleStore.lookup(lost_principal)

      written = write_bootstrap_token_as_operator!()

      capture_log(fn ->
        assert {:ok, _} = Bootstrap.claim(written, principal: "recovered-admin")
      end)

      assert :error = RoleStore.lookup(lost_principal)
    end

    test "a claim leaves its principal holding exactly admin in _system" do
      {_principal, _} = claim_bootstrap!()
      :ok = RoleStore.bind("claimant", "team-a", :operator)

      written = write_bootstrap_token_as_operator!()

      capture_log(fn ->
        assert {:ok, _} = Bootstrap.claim(written, principal: "claimant")
      end)

      assert {:ok, %{namespace: "_system", roles: [:admin]}} = RoleStore.lookup("claimant")
    end

    test "the claim succeeds once, replaces the admin, and records the recovery" do
      {lost_principal, lost_token} = claim_bootstrap!()
      since = System.monotonic_time(:microsecond)

      # The operator with filesystem access writes the same file the first
      # bootstrap read — what `flz auth rebootstrap` does against a running
      # controller. Nothing is stopped.
      written = write_bootstrap_token_as_operator!()

      log =
        AuthTestHelper.capture_log_at(:warning, fn ->
          assert {:ok, %{principal: "recovered-admin"}} =
                   Bootstrap.claim(written, principal: "recovered-admin")
        end)

      assert log =~ "recovery claim accepted against a RUNNING controller"

      # The previous admin authorizes nothing, and exactly one admin survives.
      assert :error = RoleStore.lookup(lost_principal)
      assert {:error, :token_revoked} = TokenStore.verify(lost_token)
      assert {:ok, %{namespace: "_system", roles: [:admin]}} = RoleStore.lookup("recovered-admin")
      assert AuthState.enabled?()

      # Once: the file is unlinked, so a second claim of the same value is
      # refused naming the file it expected.
      path = Config.bootstrap_token_path()
      refute File.exists?(path)
      assert {:error, {:bootstrap_token_missing, ^path}} = Bootstrap.claim(written, [])

      assert [%{metadata: %{mode: :online}} | _] =
               AuthTestHelper.await_platform_authz_events(:rebootstrap_recovery, since)
    end

    test "a claim without the file is refused naming the file it expected" do
      {_principal, _} = claim_bootstrap!()
      path = Config.bootstrap_token_path()
      refute File.exists?(path)

      assert {:error, {:bootstrap_token_missing, ^path}} = Bootstrap.claim("anything", [])
    end

    test "a wrong value does not consume the file the operator wrote" do
      {lost_principal, _} = claim_bootstrap!()
      _ = write_bootstrap_token_as_operator!()

      assert {:error, :bootstrap_token_mismatch} = Bootstrap.claim("not-the-token", [])

      assert File.exists?(Config.bootstrap_token_path())
      assert {:ok, _} = RoleStore.lookup(lost_principal)
    end
  end
end

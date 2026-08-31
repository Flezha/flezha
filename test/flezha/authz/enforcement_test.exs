defmodule Flezha.AuthZ.EnforcementTest do
  @moduledoc """
  finish-platform-authn-authz §3/§4: enforcement is the default posture,
  identity derives only from the authenticated transport (verified token /
  namespace handshake), destructive verbs are gated, and denials are audited.
  """
  # async: false: writes Application env; a shared on-disk root; the live controller and its fleet.
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{AuthState, RoleStore, Scope, TokenStore}
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.AuthTestHelper

  setup do
    ctx =
      AuthTestHelper.setup_auth_stores(
        prefix: "flz_enforcement",
        register_source: true
      )

    {:ok, Map.to_list(ctx)}
  end

  defp claim!() do
    :ok = AuthState.set_enabled()
    assert AuthZ.enforcement_posture() == :enforced
  end

  describe "enforcement posture (§4.1)" do
    test "config-enabled but unclaimed is :bootstrap_pending and refuses gated verbs" do
      assert AuthZ.enforcement_posture() == :bootstrap_pending

      assert {:error, {:forbidden, :bootstrap_pending}} =
               AuthZ.authorize(%{source: :cli}, :"state.delete", Scope.build("_system"))
    end

    test "bootstrap-pending keeps dataflow alive: internal and runner callers pass" do
      assert AuthZ.enforcement_posture() == :bootstrap_pending

      assert :ok =
               AuthZ.authorize(%{source: :internal}, :"operative.start", Scope.build("default"))

      runner_ctx = FleetManager.runner_caller_context("team-orders")
      assert :ok = AuthZ.authorize(runner_ctx, :"runner.connect", Scope.build("team-orders"))
    end

    test "the explicit opt-out is :open and allows unconditionally" do
      prev = Application.get_env(:flezha, :authn_authz, [])
      Application.put_env(:flezha, :authn_authz, Keyword.put(prev, :enabled, false))

      try do
        assert AuthZ.enforcement_posture() == :open
        assert :ok = AuthZ.authorize(%{source: :cli}, :"state.delete", Scope.build("_system"))
      after
        Application.put_env(:flezha, :authn_authz, prev)
      end
    end

    test "a claimed bootstrap flips to :enforced" do
      claim!()

      assert {:error, {:forbidden, :principal_not_bound}} =
               AuthZ.authorize(%{source: :cli}, :"state.delete", Scope.build("_system"))
    end
  end

  describe "principals from the authenticated transport (§3)" do
    test "a verified token resolves to the CLI principal" do
      claim!()
      {:ok, raw, _} = TokenStore.issue("operator-1", "team-orders", :operator, 3600)

      assert {:ok, %{principal: "operator-1", namespace: "team-orders", role: :operator}} =
               CliManager.authz_whoami(token: raw)
    end

    test "a caller-supplied principal without a token is not trusted" do
      claim!()

      # The legacy `principal:`/`role:` opts are ignored by cli_context — the
      # identity stays unbound and the gated verb refuses.
      assert {:ok, %{principal: nil}} =
               CliManager.authz_whoami(principal: "forged-admin", role: :admin)

      assert {:error, {:forbidden, :principal_not_bound}} =
               CliManager.namespace_create("forged-ns", %{},
                 principal: "forged-admin",
                 namespace: "_system",
                 role: :admin
               )
    end

    test "an invalid token leaves the identity unbound" do
      claim!()

      assert {:ok, %{principal: nil}} = CliManager.authz_whoami(token: "flzt_not_a_real_token")
    end

    test "the runner principal is bound to its connection's namespace" do
      claim!()
      ctx = FleetManager.runner_caller_context("team-orders")

      assert ctx.principal == "runner@team-orders"
      assert :ok = AuthZ.authorize(ctx, :"runner.connect", Scope.build("team-orders"))

      # ...and cannot act on another namespace.
      assert {:error, {:forbidden, :namespace_mismatch}} =
               AuthZ.authorize(ctx, :"runner.connect", Scope.build("team-other"))

      # ...nor invoke anything beyond the runner action set.
      assert {:error, {:forbidden, :role_insufficient}} =
               AuthZ.authorize(ctx, :"state.delete", Scope.build("team-orders"))
    end
  end

  describe "destructive verbs are gated and audited (§4.2/§4.3)" do
    test "an unauthorized state delete is refused and does not take effect" do
      claim!()
      {:ok, raw, _} = TokenStore.issue("viewer-1", "_system", :viewer, 3600)

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(CliManager.state_delete("op/proc/key", token: raw)) == :cli_error
        end)

      assert output =~ "refused"
    end

    test "a gated verb without any token names the credential requirement" do
      claim!()

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(CliManager.state_delete("op/proc/key", [])) == :cli_error
        end)

      assert output =~ "--token"
    end

    test "bootstrap-pending refuses a destructive verb naming the bootstrap procedure" do
      assert AuthZ.enforcement_posture() == :bootstrap_pending

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(CliManager.state_delete("op/proc/key", [])) == :cli_error
        end)

      assert output =~ "flz auth bootstrap"
    end

    test "plugin enable is gated: a viewer-role caller is refused" do
      claim!()
      {:ok, raw, _} = TokenStore.issue("viewer-1", "_system", :viewer, 3600)

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(
                   CliManager.installer_enable_plugin("some_plugin", ~s({"token": "#{raw}"}))
                 ) == :cli_error
        end)

      assert output =~ "refused"
    end

    test "an authorized destructive verb proceeds and audits the actor" do
      claim!()
      :ok = RoleStore.bind("admin-1", "_system", :admin)
      {:ok, raw, _} = TokenStore.issue("admin-1", "_system", :admin, 3600)

      boundary = System.monotonic_time(:microsecond)

      output =
        capture_io(fn ->
          CliManager.state_delete("enf_test_op/proc/key", token: raw)
        end)

      assert output =~ "deleted persisted state"

      # The success audit event names the actor and the target.
      assert [signal | _] =
               AuthTestHelper.await_platform_authz_events(
                 :"state.delete",
                 boundary
               )

      assert signal.metadata.principal == "admin-1"
      assert signal.metadata.entity == "enf_test_op/proc/key"
    end

    test "a denial is audited as :authorization_denied" do
      claim!()
      boundary = System.monotonic_time(:microsecond)

      assert {:error, {:forbidden, :principal_not_bound}} =
               AuthZ.authorize(%{source: :cli}, :"state.delete", Scope.build("_system"))

      assert [signal | _] =
               AuthTestHelper.await_platform_authz_events(
                 :authorization_denied,
                 boundary
               )

      assert signal.metadata.reason == :principal_not_bound
    end
  end
end

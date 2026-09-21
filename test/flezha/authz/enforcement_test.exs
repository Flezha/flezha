defmodule Flezha.AuthZ.EnforcementTest do
  @moduledoc """
  finish-platform-authn-authz §3/§4: enforcement is the default posture,
  identity derives only from the authenticated transport (verified token /
  namespace handshake), destructive verbs are gated, and denials are audited.
  """
  # async: false: writes Application env; a shared on-disk root; the live controller and its fleet.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_enforcement", register_source: true}],
    async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{AuthState, Config, RoleStore, Scope, TokenStore}
  alias Flezha.Observability.{RingBufferSink, TestSinks}
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.CliManager
  alias FlezhaTest.Helper.AuthTestHelper

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

    test "the bootstrap refusal names the token file, not a log line it may not have written" do
      assert AuthZ.enforcement_posture() == :bootstrap_pending

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(CliManager.state_delete("op/task/key", [])) == {:shutdown, 1}
        end)

      # The path is logged only on the boot that WRITES the file; on every later
      # boot with an unclaimed token the writer short-circuits and logs nothing,
      # so the refusal carries the path rather than promising a log line.
      assert output =~ "the bootstrap has not been claimed"
      assert output =~ Config.bootstrap_token_path()
      refute output =~ "logged at controller boot"
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
      # identity stays unbound and every gated verb, `whoami` included, refuses.
      forged = [principal: "forged-admin", namespace: "_system", role: :admin]

      for verb <- [
            fn -> CliManager.authz_render_whoami(forged) end,
            fn -> CliManager.authz_render_namespace_create("forged-ns", "{}", forged) end
          ] do
        output = capture_io(:stderr, fn -> assert catch_exit(verb.()) == {:shutdown, 1} end)
        assert output =~ "refused: no verifiable identity"
      end
    end

    test "an invalid token leaves the identity unbound" do
      claim!()

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(CliManager.authz_render_whoami(token: "flzt_not_a_real_token")) ==
                   {:shutdown, 1}
        end)

      assert output =~ "authz.whoami: refused"
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
    test "bootstrap-pending refuses a destructive verb naming the bootstrap tasklet" do
      assert AuthZ.enforcement_posture() == :bootstrap_pending

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(CliManager.state_delete("op/task/key", [])) == {:shutdown, 1}
        end)

      assert output =~ "flz auth bootstrap"
    end

    test "plugin enable is gated: a viewer-role caller is refused" do
      claim!()
      {:ok, raw, _} = TokenStore.issue("viewer-1", "_system", :viewer, 3600)

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(CliManager.installer_enable_plugin("some_plugin", token: raw)) ==
                   {:shutdown, 1}
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
          CliManager.state_delete("enf_test_op/task/key", token: raw)
        end)

      assert output =~ "deleted persisted state"

      # The success audit record names the actor and the target, on the verb's
      # own source with the action it was judged as.
      TestSinks.settle(RingBufferSink)

      assert [signal | _] =
               Enum.filter(RingBufferSink.query(:state_service, boundary), &(&1.name == :delete))

      assert signal.metadata.action == :"state.delete"
      assert signal.metadata.outcome == :ok
      assert signal.metadata.principal == "admin-1"
      assert signal.metadata.cell == "enf_test_op/task/key"
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
      assert signal.metadata.caller_roles == nil
    end

    test "a denial after resolution names the set that did not grant" do
      claim!()
      :ok = RoleStore.bind("viewer-1", "_system", :viewer)
      boundary = System.monotonic_time(:microsecond)

      assert {:error, {:forbidden, :role_insufficient}} =
               AuthZ.authorize(
                 %{source: :cli, principal: "viewer-1"},
                 :"state.delete",
                 Scope.build("_system")
               )

      assert [signal | _] =
               AuthTestHelper.await_platform_authz_events(:authorization_denied, boundary)

      assert signal.metadata.reason == :role_insufficient
      assert signal.metadata.caller_roles == [:viewer]
    end
  end
end

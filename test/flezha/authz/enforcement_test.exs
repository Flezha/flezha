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
  alias Flezha.Service.{Admin, Cli}
  alias FlezhaTest.Helper.AuthTestHelper

  defp claim!() do
    :ok = AuthState.set_enabled()
    assert AuthZ.enforcement_posture() == :enforced
  end

  describe "enforcement posture (§4.1)" do
    test "config-enabled but unclaimed is :bootstrap_pending and refuses gated verbs" do
      assert AuthZ.enforcement_posture() == :bootstrap_pending

      assert {:error, {:forbidden, :bootstrap_pending, _token_path}} =
               AuthZ.authorize(%{source: :cli}, :"state.delete", Scope.build("_system"))
    end

    test "the bootstrap refusal names the token file, not a log line it may not have written" do
      assert AuthZ.enforcement_posture() == :bootstrap_pending

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(Cli.call("Steps.delete", Jason.encode!(["op/task/key"]), [])) ==
                   {:shutdown, 4}
        end)

      # The path is logged only on the boot that WRITES the file; on every later
      # boot with an unclaimed token the writer short-circuits and logs nothing,
      # so the refusal carries the path rather than promising a log line.
      assert output =~ "no admin has claimed the bootstrap yet"
      assert output =~ "(authz.bootstrap_pending)"
      assert output =~ Config.bootstrap_token_path()
      refute output =~ "logged at controller boot"
    end

    test "bootstrap-pending keeps dataflow alive: internal and runner callers pass" do
      assert AuthZ.enforcement_posture() == :bootstrap_pending

      assert :ok =
               AuthZ.authorize(%{source: :internal}, :"relay.start", Scope.build("default"))

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
      {:ok, raw, _} = TokenStore.issue("caller-1", "team-orders", :ops, 3600)

      assert {:ok, %{principal: "caller-1", namespace: "team-orders", role: :ops}} =
               Admin.run("Namespace.whoami", "[]", Cli.Context.cli_context(token: raw), :given)
    end

    test "a caller-supplied principal without a token is not trusted" do
      claim!()

      # The legacy `principal:`/`role:` opts are ignored by cli_context — the
      # identity stays unbound and every gated verb, `whoami` included, refuses.
      forged = [principal: "forged-admin", namespace: "_system", role: :admin]

      for verb <- [
            fn -> Cli.call("Namespace.whoami", "[]", forged) end,
            fn ->
              Cli.call(
                "Namespace.create",
                Jason.encode!(["forged-ns", %{quotas: %{}}]),
                []
              )
            end
          ] do
        output = capture_io(:stderr, fn -> assert catch_exit(verb.()) == {:shutdown, 4} end)
        assert output =~ "no verifiable identity (authz.principal_not_bound)"
      end
    end

    test "an invalid token leaves the identity unbound" do
      claim!()

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(
                   Cli.call("Namespace.whoami", Jason.encode!([]), token: "flzt_not_a_real_token")
                 ) ==
                   {:shutdown, 4}
        end)

      assert output =~ "(authz.principal_not_bound)"
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
          assert catch_exit(Cli.call("Steps.delete", Jason.encode!(["op/task/key"]), [])) ==
                   {:shutdown, 4}
        end)

      assert output =~ "flz auth bootstrap"
    end

    test "plugin enable is gated: a viewer-role caller is refused" do
      claim!()
      {:ok, raw, _} = TokenStore.issue("viewer-1", "_system", :viewer, 3600)

      output =
        capture_io(:stderr, fn ->
          assert catch_exit(
                   Cli.call(
                     "PluginManager.enable",
                     Jason.encode!([
                       "some_plugin",
                       %{
                         provider: "relay_stdout",
                         namespace: "_system",
                         relay_id: "some_op"
                       }
                     ]),
                     token: raw
                   )
                 ) == {:shutdown, 4}
        end)

      assert output =~ "(authz.role_insufficient)"
    end

    test "an authorized destructive verb proceeds and audits the actor" do
      claim!()
      :ok = RoleStore.bind("admin-1", "_system", :admin)
      {:ok, raw, _} = TokenStore.issue("admin-1", "_system", :admin, 3600)

      boundary = System.monotonic_time(:microsecond)

      output =
        capture_io(fn ->
          Cli.call("Steps.delete", Jason.encode!(["enf_test_op/task/key"]), token: raw)
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
               AuthTestHelper.platform_authz_events(
                 :authorization_denied,
                 boundary
               )

      assert signal.metadata.reason == "authz.principal_not_bound"
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
               AuthTestHelper.platform_authz_events(:authorization_denied, boundary)

      assert signal.metadata.reason == "authz.role_insufficient"
      assert signal.metadata.caller_roles == [:viewer]
    end
  end

  describe "every refusal is recorded once, and allowed?/3 records none" do
    # The whole record each refusal writes, keyed by what it names besides the
    # action and reason: who, where, and the set judged (nil before resolution).
    defp denials_since(boundary) do
      :authorization_denied
      |> AuthTestHelper.platform_authz_events(boundary)
      |> Enum.map(& &1.metadata)
    end

    defp refused_once(ctx, action, scope, reason, recorded) do
      boundary = System.monotonic_time(:microsecond)
      refute AuthZ.allowed?(ctx, action, scope)
      assert denials_since(boundary) == []

      assert {:error, refusal} = AuthZ.authorize(ctx, action, scope)
      assert elem(refusal, 1) == reason
      code = "authz.#{reason}"

      assert [%{action: ^action, reason: ^code} = record] = denials_since(boundary)
      assert Map.take(record, [:principal, :scope, :caller_roles]) == recorded
    end

    test "each refusal writes its one record, the same through authorize/3 as before" do
      team_a = Scope.build("team-a")
      ops = %{source: :cli, principal: "ops-a"}
      nobody = %{principal: nil, scope: nil, caller_roles: nil}

      refused_once(ops, :"relay.list", team_a, :bootstrap_pending, %{
        nobody
        | principal: "ops-a",
          scope: team_a
      })

      claim!()
      :ok = RoleStore.bind("ops-a", "team-a", :ops)
      :ok = RoleStore.bind("viewer-a", "team-a", :viewer)
      :ok = RoleStore.bind("admin-a", "team-a", :admin)
      viewer = %{source: :cli, principal: "viewer-a"}
      admin = %{source: :cli, principal: "admin-a"}
      team_b = Scope.build("team-b")

      refused_once(ops, :"no.such_action", team_a, :unknown_action, nobody)

      refused_once(%{source: :cli}, :"relay.list", team_a, :principal_not_bound, %{
        nobody
        | scope: team_a
      })

      refused_once(
        %{source: :cli, principal: "stranger"},
        :"relay.list",
        team_a,
        :no_role_binding,
        %{nobody | principal: "stranger", scope: team_a}
      )

      refused_once(viewer, :"state.delete", team_a, :role_insufficient, %{
        principal: "viewer-a",
        scope: team_a,
        caller_roles: [:viewer]
      })

      refused_once(viewer, :"relay.list", team_b, :namespace_mismatch, %{
        principal: "viewer-a",
        scope: team_b,
        caller_roles: [:viewer]
      })

      refused_once(admin, :"authz.namespace_create", team_b, :cross_namespace_admin_denied, %{
        principal: "admin-a",
        scope: team_b,
        caller_roles: [:admin]
      })

      boundary = System.monotonic_time(:microsecond)
      refute AuthZ.allowed?(ops, :"relay.list", %{})
      assert {:error, :invalid_scope} = AuthZ.authorize(ops, :"relay.list", %{})

      assert {:error, {:forbidden, :principal_not_bound}} =
               AuthZ.require_bound_principal(%{source: :cli})

      assert [%{reason: "authz.principal_not_bound", action: nil} = record] =
               denials_since(boundary)

      assert Map.take(record, [:principal, :scope, :caller_roles]) == nobody
    end
  end
end

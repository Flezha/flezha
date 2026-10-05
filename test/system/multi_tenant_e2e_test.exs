defmodule FlezhaTest.System.MultiTenantE2ETest do
  @moduledoc """
  finish-platform-authn-authz §9.3: two namespaces against live runners with
  ENFORCEMENT ON — a runner connects over the authenticated transport and its
  namespace-bound identity holds, a token-authenticated CLI round-trip works,
  a cross-namespace verb is refused, and a quota refusal is observable.

  Tagged `:system` (boots real runner subprocesses). Run with
  `mix test --include system test/system/multi_tenant_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :runner_lifecycle

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{NamespaceStore, QuotaCounters, RoleStore, Scope, TokenStore}
  alias Flezha.Relay.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.Cli
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  @ns_a "tenant-alpha"
  @ns_b "tenant-beta"

  setup_all do
    # The harness's own on_exit (stores down, config restored) registers
    # FIRST and therefore runs LAST — after the runner/namespace teardown
    # below, which still needs the enforced posture to be consistent.
    _ =
      AuthTestHelper.setup_auth_stores(
        prefix: "flz_multi_tenant",
        claim: true
      )

    assert AuthZ.enforcement_posture() == :enforced

    {:ok, _} = NamespaceStore.create(@ns_a, quotas: %{})
    {:ok, _} = NamespaceStore.create(@ns_b, quotas: %{})

    # One user per tenant, identity carried ONLY by a verified token.
    :ok = RoleStore.bind("op-alpha", @ns_a, :ops)
    :ok = RoleStore.bind("op-beta", @ns_b, :ops)
    {:ok, token_a, _} = TokenStore.issue("op-alpha", @ns_a, :ops, 3_600)
    {:ok, token_b, _} = TokenStore.issue("op-beta", @ns_b, :ops, 3_600)

    # Live runners, one per namespace — connecting is itself the §3.1 check:
    # under enforcement a connect first proves its namespace with that
    # namespace's runner credential, then authorizes `runner.connect` for the
    # `runner@<ns>` principal.
    {secret_a, _hash} = AuthTestHelper.issue_runner_credential(@ns_a)
    {secret_b, _hash} = AuthTestHelper.issue_runner_credential(@ns_b)
    runner_a = NodeTestHelper.start_runner(@ns_a, env: [{"FLEZHA_NAMESPACE_SECRET", secret_a}])
    runner_b = NodeTestHelper.start_runner(@ns_b, env: [{"FLEZHA_NAMESPACE_SECRET", secret_b}])

    on_exit(fn ->
      for ns <- [@ns_a, @ns_b] do
        try do
          RelayManager.stop_all_relays(ns)
        catch
          _, _ -> :ok
        end

        try do
          FleetManager.stop_namespace(ns)
        catch
          _, _ -> :ok
        end
      end
    end)

    %{
      token_a: token_a,
      token_b: token_b,
      secret_a: secret_a,
      runner_a: runner_a,
      runner_b: runner_b
    }
  end

  test "authenticated runners connected under enforcement, one per tenant" do
    assert [_] = FleetManager.active_runners(@ns_a)
    assert [_] = FleetManager.active_runners(@ns_b)
  end

  test "a token-authenticated CLI round-trip resolves the tenant identity", %{token_a: token_a} do
    printed =
      ExUnit.CaptureIO.capture_io(fn ->
        Cli.call("Namespace.whoami", "[]", token: token_a, output: "json")
      end)

    assert %{"principal" => "op-alpha", "namespace" => @ns_a, "role" => "ops"} =
             Jason.decode!(printed)
  end

  test "a cross-namespace verb is refused and the tenant's own succeeds", %{
    token_a: token_a,
    token_b: token_b
  } do
    relay = %Builder{id: "alpha_op_stop_target", factory_id: "do_nothing"}
    {:ok, _info} = RelayManager.start_relay(@ns_a, relay)

    # tenant-beta's user cannot stop tenant-alpha's relay...
    output =
      ExUnit.CaptureIO.capture_io(:stderr, fn ->
        assert catch_exit(stop(token_b)) == {:shutdown, 4}
      end)

    assert output =~ "authz.namespace_mismatch"

    # ...its own user can.
    ExUnit.CaptureIO.capture_io(fn ->
      stop(token_a)
    end)
  end

  test "listings are namespace-scoped: the other tenant's rows are unreachable", %{
    token_b: token_b
  } do
    output =
      ExUnit.CaptureIO.capture_io(:stderr, fn ->
        assert catch_exit(Cli.call("Relay.list", Jason.encode!([@ns_a]), token: token_b)) ==
                 {:shutdown, 4}
      end)

    assert output =~ "authz.namespace_mismatch"
  end

  test "an over-quota namespace refuses a new relay allocation", %{token_b: _} do
    {:ok, _} = NamespaceStore.set_quotas(@ns_b, %{max_relays: 0})

    on_exit(fn -> {:ok, _} = NamespaceStore.set_quotas(@ns_b, %{}) end)

    relay = %Builder{id: "beta_quota_refused", factory_id: "do_nothing"}

    assert {:error, {:quota_exceeded, :max_relays, %{limit: 0}}} =
             RelayManager.start_relay(@ns_b, relay)
  end

  test "a runner for a namespace at max_runners is refused observably", %{secret_a: secret_a} do
    {:ok, _} = NamespaceStore.set_quotas(@ns_a, %{max_runners: 1})
    on_exit(fn -> {:ok, _} = NamespaceStore.set_quotas(@ns_a, %{}) end)

    # The live runner occupies the single slot (the counter is derived from
    # the session map on every mutation).
    assert [_] = FleetManager.active_runners(@ns_a)
    :ok = QuotaCounters.set(@ns_a, :runners, 1)

    # A proven namespace, so the refusal is the quota's and not the proof's.
    assert {:error,
            %Flezha.Error{
              code: "quota.exceeded",
              details: %{"kind" => "max_runners", "current" => 1, "limit" => 1}
            }} =
             FleetManager.connect_handler(
               make_ref(),
               @ns_a,
               %{"namespace_secret" => secret_a},
               :uds
             )
  end

  test "the runner principal cannot act outside its own namespace" do
    ctx = FleetManager.runner_caller_context(@ns_a)

    assert :ok = AuthZ.authorize(ctx, :"runner.connect", Scope.build(@ns_a))

    assert {:error, {:forbidden, :namespace_mismatch}} =
             AuthZ.authorize(ctx, :"runner.connect", Scope.build(@ns_b))
  end

  defp stop(token),
    do:
      Cli.call(
        "Relay.stop",
        Jason.encode!([@ns_a, "alpha_op_stop_target"]),
        token: token
      )
end

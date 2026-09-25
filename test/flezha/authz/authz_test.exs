defmodule Flezha.AuthZTest do
  @moduledoc """
  End-to-end tests for `Flezha.AuthZ.authorize/3`.

  These exercise the policy point with a live `AuthState` +
  `RoleStore` so role lookup hits the ETS cache the production path
  uses. Async false because of named-singleton GenServers.
  """

  # async: false: writes Application env; a shared on-disk root.

  use FlezhaTest.Case,
    fixtures: [
      {:auth_stores,
       prefix: "flz_authz",
       stores: [Flezha.AuthZ.AuthState, Flezha.AuthZ.RoleStore, Flezha.AuthZ.NamespaceStore],
       claim: true}
    ],
    async: false

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{AuthState, RoleStore, Scope}

  test "internal-source callers are pre-authorized" do
    assert :ok =
             AuthZ.authorize(
               %{source: :internal},
               :"operator.start",
               Scope.build("team-orders")
             )
  end

  test "unbound principal denied while enabled" do
    assert {:error, {:forbidden, :principal_not_bound}} =
             AuthZ.authorize(
               %{source: :cli, principal: nil},
               :"operator.start",
               Scope.build("_system")
             )
  end

  test "ops on bound namespace passes; cross-namespace denied without admin@_system" do
    :ok = RoleStore.bind("alice", "team-orders", :ops)

    ctx = %{source: :cli, principal: "alice"}

    assert :ok = AuthZ.authorize(ctx, :"operator.start", Scope.build("team-orders"))

    assert {:error, {:forbidden, :namespace_mismatch}} =
             AuthZ.authorize(ctx, :"operator.start", Scope.build("team-payments"))
  end

  test "viewer denied ops actions" do
    :ok = RoleStore.bind("eve", "_system", :viewer)
    ctx = %{source: :cli, principal: "eve"}

    assert :ok = AuthZ.authorize(ctx, :"operator.list", Scope.build("_system"))

    assert {:error, {:forbidden, :role_insufficient}} =
             AuthZ.authorize(ctx, :"operator.start", Scope.build("_system"))
  end

  test "admin@_system can mutate other namespaces' auth state" do
    :ok = RoleStore.bind("root", "_system", :admin)
    ctx = %{source: :cli, principal: "root"}

    assert :ok =
             AuthZ.authorize(ctx, :"authz.namespace_create", Scope.build("team-orders"))

    assert :ok =
             AuthZ.authorize(ctx, :"authz.token_issue", Scope.build("team-payments"))
  end

  test "admin@team-orders cannot mutate team-payments auth state" do
    :ok = RoleStore.bind("alice", "team-orders", :admin)
    ctx = %{source: :cli, principal: "alice"}

    assert :ok =
             AuthZ.authorize(ctx, :"authz.token_issue", Scope.build("team-orders"))

    assert {:error, {:forbidden, :cross_namespace_admin_denied}} =
             AuthZ.authorize(ctx, :"authz.token_issue", Scope.build("team-payments"))
  end

  test "runner role gets data-plane verbs only" do
    :ok = RoleStore.bind("exec-1", "team-orders", :runner)
    ctx = %{source: :sdk, principal: "exec-1"}

    assert :ok = AuthZ.authorize(ctx, :"runner.connect", Scope.build("team-orders"))

    assert {:error, {:forbidden, :role_insufficient}} =
             AuthZ.authorize(ctx, :"operator.start", Scope.build("team-orders"))

    assert {:error, {:forbidden, :role_insufficient}} =
             AuthZ.authorize(ctx, :"audit.read", Scope.build("team-orders"))
  end

  test "unknown action denied" do
    :ok = RoleStore.bind("root", "_system", :admin)
    ctx = %{source: :cli, principal: "root"}

    assert {:error, {:forbidden, :unknown_action}} =
             AuthZ.authorize(ctx, :"nope.nope", Scope.build("_system"))
  end

  test "invalid scope returns invalid_scope" do
    :ok = RoleStore.bind("root", "_system", :admin)
    ctx = %{source: :cli, principal: "root"}

    assert {:error, :invalid_scope} = AuthZ.authorize(ctx, :"operator.start", %{})
  end

  test "context with bound role overrides RoleStore" do
    # Even if RoleStore says viewer, an explicit (role, namespace)
    # bound onto the context (by the authn layer) wins. This is the
    # path the CLI handshake will use after binding the verified
    # token.
    :ok = RoleStore.bind("alice", "_system", :viewer)
    ctx = %{source: :cli, principal: "alice", role: :ops, namespace: "_system"}

    assert :ok = AuthZ.authorize(ctx, :"operator.start", Scope.build("_system"))
  end

  test "a stamped set grants when any role in it grants" do
    ctx = %{
      source: :sdk,
      principal: "runner@team-orders",
      namespace: "team-orders",
      roles: [:spawner, :stopper]
    }

    assert :ok = AuthZ.authorize(ctx, :"operator.start", Scope.build("team-orders"))
    assert :ok = AuthZ.authorize(ctx, :"operator.stop", Scope.build("team-orders"))

    assert {:error, {:forbidden, :role_insufficient}} =
             AuthZ.authorize(ctx, :"plugin.enable", Scope.build("team-orders"))

    assert {:error, {:forbidden, :namespace_mismatch}} =
             AuthZ.authorize(ctx, :"operator.stop", Scope.build("team-payments"))
  end

  test "a stored set grants when any role in it grants" do
    :ok = RoleStore.bind("carol", "team-orders", :spawner)
    :ok = RoleStore.bind("carol", "team-orders", :stopper)
    ctx = %{source: :cli, principal: "carol"}

    assert :ok = AuthZ.authorize(ctx, :"operator.start", Scope.build("team-orders"))
    assert :ok = AuthZ.authorize(ctx, :"operator.stop", Scope.build("team-orders"))

    assert {:error, {:forbidden, :role_insufficient}} =
             AuthZ.authorize(ctx, :"plugin.enable", Scope.build("team-orders"))
  end

  test "a credential's role is the whole of the caller's authority" do
    # `alice` holds `ops` in the store and presents a viewer token: the
    # token's role alone is judged, so the stop is refused.
    :ok = RoleStore.bind("alice", "team-orders", :ops)
    ctx = %{source: :cli, principal: "alice", role: :viewer, namespace: "team-orders"}

    assert {:error, {:forbidden, :role_insufficient}} =
             AuthZ.authorize(ctx, :"operator.stop", Scope.build("team-orders"))
  end

  test "held_roles/1 answers what each kind of caller is judged with" do
    :ok = RoleStore.bind("carol", "team-orders", :spawner)
    :ok = RoleStore.bind("carol", "team-orders", :stopper)

    assert AuthZ.held_roles(%{source: :internal}) == {:ok, [:admin], "_system"}

    assert AuthZ.held_roles(%{source: :cli, principal: "x", role: :viewer, namespace: "ns"}) ==
             {:ok, [:viewer], "ns"}

    assert AuthZ.held_roles(%{source: :sdk, principal: "x", roles: [:spawner], namespace: "ns"}) ==
             {:ok, [:spawner], "ns"}

    assert AuthZ.held_roles(%{source: :cli, principal: "carol"}) ==
             {:ok, [:spawner, :stopper], "team-orders"}

    assert AuthZ.held_roles(%{source: :cli, principal: "nobody"}) == :error
  end

  test "unclaimed enforcement is :bootstrap_pending; only the config opt-out is open" do
    :ok = AuthState.reset()
    refute AuthZ.enabled?()

    # Configured on but unclaimed refuses gated verbs (enforced-by-default).
    assert AuthZ.enforcement_posture() == :bootstrap_pending

    assert {:error, {:forbidden, :bootstrap_pending}} =
             AuthZ.authorize(
               %{source: :cli, principal: nil},
               :"operator.start",
               Scope.build("anything")
             )

    # The explicit configuration opt-out is the only open posture.
    prev = Application.get_env(:flezha, :authn_authz, [])
    Application.put_env(:flezha, :authn_authz, Keyword.put(prev, :enabled, false))

    try do
      assert AuthZ.enforcement_posture() == :open

      assert :ok =
               AuthZ.authorize(
                 %{source: :cli, principal: nil},
                 :"operator.start",
                 Scope.build("anything")
               )
    after
      Application.put_env(:flezha, :authn_authz, prev)
    end
  end
end

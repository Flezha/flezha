defmodule Flezha.AuthZTest do
  @moduledoc """
  End-to-end tests for `Flezha.AuthZ.authorize/3`.

  These exercise the policy point with a live `AuthState` +
  `RoleStore` so role lookup hits the ETS cache the production path
  uses. Async false because of named-singleton GenServers.
  """

  use ExUnit.Case, async: false

  alias Flezha.AuthZ
  alias Flezha.AuthZ.{AuthState, NamespaceStore, RoleStore, Scope}

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_authz_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)
    File.chmod!(tmp, 0o700)

    prev = Application.get_env(:flezha, :authn_authz, [])

    Application.put_env(
      :flezha,
      :authn_authz,
      prev |> Keyword.put(:auth_dir, tmp) |> Keyword.put(:enabled, true)
    )

    stop_if_running([AuthState, RoleStore, NamespaceStore])
    {:ok, _} = AuthState.start_link([])
    {:ok, _} = RoleStore.start_link([])
    {:ok, _} = NamespaceStore.start_link([])
    :ok = AuthState.set_enabled()

    on_exit(fn ->
      stop_if_running([AuthState, RoleStore, NamespaceStore])
      Application.put_env(:flezha, :authn_authz, prev)
      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "internal-source callers are pre-authorized" do
    assert :ok =
             AuthZ.authorize(
               %{source: :internal},
               :"operative.start",
               Scope.build("team-orders")
             )
  end

  test "unbound principal denied while enabled" do
    assert {:error, {:forbidden, :principal_not_bound}} =
             AuthZ.authorize(
               %{source: :cli, principal: nil},
               :"operative.start",
               Scope.build("_system")
             )
  end

  test "operator on bound namespace passes; cross-namespace denied without admin@_system" do
    :ok = RoleStore.bind("alice", "team-orders", :operator)

    ctx = %{source: :cli, principal: "alice"}

    assert :ok = AuthZ.authorize(ctx, :"operative.start", Scope.build("team-orders"))

    assert {:error, {:forbidden, :namespace_mismatch}} =
             AuthZ.authorize(ctx, :"operative.start", Scope.build("team-payments"))
  end

  test "viewer denied operator actions" do
    :ok = RoleStore.bind("eve", "_system", :viewer)
    ctx = %{source: :cli, principal: "eve"}

    assert :ok = AuthZ.authorize(ctx, :"operative.list", Scope.build("_system"))

    assert {:error, {:forbidden, :role_insufficient}} =
             AuthZ.authorize(ctx, :"operative.start", Scope.build("_system"))
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

  test "executor role gets data-plane verbs only" do
    :ok = RoleStore.bind("exec-1", "team-orders", :executor)
    ctx = %{source: :sdk, principal: "exec-1"}

    assert :ok = AuthZ.authorize(ctx, :"executor.connect", Scope.build("team-orders"))

    assert {:error, {:forbidden, :role_insufficient}} =
             AuthZ.authorize(ctx, :"operative.start", Scope.build("team-orders"))

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

    assert {:error, :invalid_scope} = AuthZ.authorize(ctx, :"operative.start", %{})
  end

  test "context with bound role overrides RoleStore" do
    # Even if RoleStore says viewer, an explicit (role, namespace)
    # bound onto the context (by the authn layer) wins. This is the
    # path the CLI handshake will use after binding the verified
    # token.
    :ok = RoleStore.bind("alice", "_system", :viewer)
    ctx = %{source: :cli, principal: "alice", role: :operator, namespace: "_system"}

    assert :ok = AuthZ.authorize(ctx, :"operative.start", Scope.build("_system"))
  end

  test "while :enabled is false, every call is :ok" do
    :ok = AuthState.reset()
    refute AuthZ.enabled?()

    assert :ok =
             AuthZ.authorize(
               %{source: :cli, principal: nil},
               :"operative.start",
               Scope.build("anything")
             )
  end

  defp stop_if_running(mods) when is_list(mods), do: Enum.each(mods, &stop_if_running/1)

  defp stop_if_running(mod) do
    case Process.whereis(mod) do
      nil ->
        :ok

      pid ->
        try do
          GenServer.stop(pid, :normal, 5_000)
        catch
          :exit, _ -> :ok
        end
    end
  end
end

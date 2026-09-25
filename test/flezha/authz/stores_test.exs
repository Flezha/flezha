defmodule Flezha.AuthZ.StoresTest do
  @moduledoc """
  Round-trip tests for the four authn/authz DETS stores
  (`AuthState`, `RoleStore`, `NamespaceStore`, `TokenStore`).
  Runs synchronously because each store is a singleton named
  GenServer + named ETS cache.
  """

  # async: false: a shared on-disk root; the primary Logger; registered singletons.

  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_authz_stores", stores: [], enabled: :unchanged}],
    async: false

  alias Flezha.AuthZ.{AuthState, NamespaceStore, RoleStore, TokenStore}
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Runner.FleetManager.Session
  alias FlezhaTest.Helper.NodeTestHelper

  @ns "team-orders"
  @runner "runner@team-orders"

  # The literal term the store wrote before it held sets, so the compatibility
  # test reads a file an older release produced rather than one this code made.
  defp old_row(principal, namespace, role) do
    %{
      principal: principal,
      namespace: namespace,
      role: role,
      bound_at: 1_757_000_000,
      bound_by: "admin-x"
    }
  end

  defp seed_old_rows! do
    seed_rows!([
      {@runner, old_row(@runner, @ns, :app)},
      {"admin@_system", old_row("admin@_system", "_system", :admin)}
    ])
  end

  # Written with `:dets` directly, before the store opens the file.
  defp seed_rows!(rows) do
    path = Layout.file(:auth, "roles.dets")
    File.mkdir_p!(Path.dirname(path))

    {:ok, table} =
      :dets.open_file(:flz_authz_roles_seed, type: :set, file: String.to_charlist(path))

    Enum.each(rows, &:dets.insert(table, &1))
    :ok = :dets.close(table)
  end

  defp stored_row(principal) do
    [{^principal, value}] = :dets.lookup(:flz_authz_roles, principal)
    value
  end

  describe "AuthState" do
    test "starts disabled by default and flips durably" do
      {:ok, pid} = AuthState.start_link([])
      refute AuthState.enabled?()
      :ok = AuthState.set_enabled()
      assert AuthState.enabled?()
      :ok = GenServer.stop(pid)

      {:ok, _pid2} = AuthState.start_link([])
      assert AuthState.enabled?(), "expected flag to survive restart"
    end
  end

  describe "RoleStore" do
    test "bind / lookup / unbind round-trip" do
      {:ok, _} = RoleStore.start_link([])
      assert :error = RoleStore.lookup("alice")

      :ok = RoleStore.bind("alice", "_system", :admin, bound_by: "bootstrap")

      assert {:ok, %{principal: "alice", namespace: "_system", roles: [:admin]}} =
               RoleStore.lookup("alice")

      assert RoleStore.admin_system_exists?()
      assert RoleStore.unbind("alice") == :ok
      assert :error = RoleStore.lookup("alice")
      refute RoleStore.admin_system_exists?()
    end

    test "bindings survive restart" do
      {:ok, pid} = RoleStore.start_link([])
      :ok = RoleStore.bind("bob", "team-orders", :ops)
      :ok = GenServer.stop(pid)

      {:ok, _} = RoleStore.start_link([])
      assert {:ok, %{roles: [:ops]}} = RoleStore.lookup("bob")
    end

    test "rejects invalid role" do
      {:ok, _} = RoleStore.start_link([])
      assert {:error, :unknown_role} = RoleStore.bind("eve", "_system", :superuser)
    end

    test "a second bind adds a role rather than replacing the first" do
      {:ok, _} = RoleStore.start_link([])
      :ok = RoleStore.bind(@runner, @ns, :stopper)
      :ok = RoleStore.bind(@runner, @ns, :spawner)

      assert {:ok, %{namespace: @ns, roles: [:spawner, :stopper]}} = RoleStore.lookup(@runner)
    end

    test "binding a role already held rewrites nothing" do
      {:ok, _} = RoleStore.start_link([])
      :ok = RoleStore.bind(@runner, @ns, :spawner, bound_by: "admin-1")
      {:ok, first} = RoleStore.lookup(@runner)

      :ok = RoleStore.bind(@runner, @ns, :spawner, bound_by: "admin-2")

      assert {:ok, ^first} = RoleStore.lookup(@runner)
      assert first.bound_by == "admin-1"
    end

    test "a bind in another namespace is refused naming the one it holds" do
      {:ok, _} = RoleStore.start_link([])
      :ok = RoleStore.bind(@runner, @ns, :app)

      assert {:error, {:bound_elsewhere, @ns}} = RoleStore.bind(@runner, "team-payments", :viewer)
      assert {:ok, %{namespace: @ns, roles: [:app]}} = RoleStore.lookup(@runner)
    end

    test "unbinding one role keeps the others, and the last removes the binding" do
      {:ok, _} = RoleStore.start_link([])
      :ok = RoleStore.bind(@runner, @ns, :spawner)
      :ok = RoleStore.bind(@runner, @ns, :stopper)

      assert :ok = RoleStore.unbind(@runner, :stopper)
      assert {:ok, %{roles: [:spawner]}} = RoleStore.lookup(@runner)

      # A role the principal does not hold changes nothing.
      assert :ok = RoleStore.unbind(@runner, :stopper)
      assert {:ok, %{roles: [:spawner]}} = RoleStore.lookup(@runner)

      assert :ok = RoleStore.unbind(@runner, :spawner)
      assert :error = RoleStore.lookup(@runner)
    end

    test "unbinding with no role removes every role" do
      {:ok, _} = RoleStore.start_link([])
      :ok = RoleStore.bind(@runner, @ns, :spawner)
      :ok = RoleStore.bind(@runner, @ns, :stopper)

      assert :ok = RoleStore.unbind(@runner)
      assert :error = RoleStore.lookup(@runner)
    end

    test "the stored term carries the set and the one-role field an older release reads" do
      {:ok, _} = RoleStore.start_link([])
      :ok = RoleStore.bind(@runner, @ns, :spawner)

      assert %{roles: [:spawner], role: :spawner} = stored_row(@runner)

      :ok = RoleStore.bind(@runner, @ns, :stopper)

      assert %{roles: [:spawner, :stopper], role: nil} = stored_row(@runner)
    end

    test "a file written before sets loads as a set of one and is not rewritten" do
      seed_old_rows!()
      {:ok, _} = RoleStore.start_link([])

      assert {:ok, %{namespace: @ns, roles: [:app], bound_at: 1_757_000_000, bound_by: "admin-x"}} =
               RoleStore.lookup(@runner)

      assert RoleStore.admin_system_exists?()
      assert Session.runner_caller_context(@ns).roles == [:app]

      # Nothing was rewritten by the load itself.
      assert stored_row(@runner) == old_row(@runner, @ns, :app)
      assert stored_row("admin@_system") == old_row("admin@_system", "_system", :admin)

      # The first change to the identity writes the new shape.
      :ok = RoleStore.bind(@runner, @ns, :stopper)
      assert %{roles: [:app, :stopper], role: nil} = stored_row(@runner)
    end

    test "a row this release cannot read grants nothing" do
      seed_rows!([{@runner, %{principal: @runner}}])
      {:ok, _} = RoleStore.start_link([])

      assert :error = RoleStore.lookup(@runner)
      assert RoleStore.list() == []
      assert Session.runner_caller_context(@ns).roles == [:runner]
    end

    test "runner_roles/2 keeps only the roles that grant the connect" do
      {:ok, _} = RoleStore.start_link([])

      assert RoleStore.runner_roles(@runner, @ns) == [:runner]

      :ok = RoleStore.bind(@runner, @ns, :spawner)
      :ok = RoleStore.bind(@runner, @ns, :admin)
      assert RoleStore.runner_roles(@runner, @ns) == [:spawner]

      # A binding in another namespace, and a set with no connect at all.
      assert RoleStore.runner_roles(@runner, "team-payments") == [:runner]
      :ok = RoleStore.unbind(@runner, :spawner)
      assert RoleStore.runner_roles(@runner, @ns) == [:runner]
    end
  end

  describe "NamespaceStore" do
    test "seeds _system on first open" do
      {:ok, _} = NamespaceStore.start_link([])

      assert {:ok, %{name: "_system", quotas: %{max_operators: 100}}} =
               NamespaceStore.get("_system")
    end

    test "create / delete / set_quotas" do
      {:ok, _} = NamespaceStore.start_link([])

      assert {:ok, %{name: "team-orders", quotas: %{max_operators: 50}}} =
               NamespaceStore.create("team-orders",
                 quotas: %{max_operators: 50},
                 created_by_principal: "alice"
               )

      assert {:ok, _} = NamespaceStore.get("team-orders")
      assert {:error, :already_exists} = NamespaceStore.create("team-orders")

      assert {:ok, %{quotas: %{max_operators: 25}}} =
               NamespaceStore.set_quotas("team-orders", %{max_operators: 25})

      assert :ok = NamespaceStore.delete("team-orders")
      assert :error = NamespaceStore.get("team-orders")

      # A delete that removed nothing says so, the way `create` refuses a name
      # that is already taken.
      assert {:error, :not_found} = NamespaceStore.delete("team-orders")
      assert {:error, :not_found} = NamespaceStore.delete("never-existed-here")

      assert {:error, :cannot_delete_system_namespace} = NamespaceStore.delete("_system")
    end
  end

  describe "TokenStore" do
    test "issue + verify round-trip" do
      {:ok, _} = TokenStore.start_link([])

      assert {:ok, raw, entry} =
               TokenStore.issue("alice", "team-orders", :ops, 3600, label: "dashboards")

      assert String.starts_with?(raw, "flzt_")
      assert entry.principal == "alice"
      assert entry.namespace == "team-orders"
      assert entry.role == :ops

      assert {:ok, %{principal: "alice", namespace: "team-orders", role: :ops}} =
               TokenStore.verify(raw)
    end

    test "expired token is rejected" do
      {:ok, _} = TokenStore.start_link([])
      {:ok, raw, %{expires_at: expires_at}} = TokenStore.issue("alice", "_system", :ops, 1)
      # Expiry is a whole-second wall-clock comparison: wait for that second.
      assert NodeTestHelper.wait_until(
               fn -> System.system_time(:second) >= expires_at end,
               2_000,
               10
             )

      assert {:error, :token_expired} = TokenStore.verify(raw)
    end

    test "revoked token is rejected" do
      {:ok, _} = TokenStore.start_link([])
      {:ok, raw, _} = TokenStore.issue("alice", "_system", :ops, 3600)
      {:ok, _} = TokenStore.revoke_by_token(raw)
      assert {:error, :token_revoked} = TokenStore.verify(raw)
    end

    test "unknown token returns :unknown" do
      {:ok, _} = TokenStore.start_link([])
      assert {:error, :unknown} = TokenStore.verify("flzt_definitelynotissued")
    end

    test "malformed token rejected pre-hash" do
      {:ok, _} = TokenStore.start_link([])
      assert {:error, :malformed} = TokenStore.verify("not-a-token")
    end

    test "list filters by namespace" do
      {:ok, _} = TokenStore.start_link([])
      {:ok, _, _} = TokenStore.issue("a", "team-orders", :ops, 3600)
      {:ok, _, _} = TokenStore.issue("b", "team-payments", :ops, 3600)
      assert length(TokenStore.list("team-orders")) == 1
      assert length(TokenStore.list("team-payments")) == 1
      assert length(TokenStore.list(:all)) == 2
    end

    test "an entry inside its warning window is announced once" do
      {:ok, _} = TokenStore.start_link([])

      # Inside the window (default one day), outside it, and one with no expiry
      # at all: only the first is announced, and only the first time.
      {:ok, _, inside} = TokenStore.issue("soon", "team-orders", :ops, 3_600)
      {:ok, _, _far} = TokenStore.issue("later", "team-orders", :ops, 30 * 86_400)
      {:ok, _, _never} = TokenStore.issue("runner@t", "team-orders", :runner, :never)

      assert TokenStore.check_expiring_now() == [inside.token_hash]
      assert TokenStore.check_expiring_now() == []
    end

    test "a revoked entry inside the window is not announced" do
      {:ok, _} = TokenStore.start_link([])
      {:ok, _, entry} = TokenStore.issue("soon", "team-orders", :ops, 3_600)
      {:ok, _} = TokenStore.revoke(String.slice(entry.token_hash, 0, 12))

      assert TokenStore.check_expiring_now() == []
    end

    test "a runner entry is refused an expiry and refused _system" do
      {:ok, _} = TokenStore.start_link([])

      assert {:error, :runner_token_cannot_expire} =
               TokenStore.issue("runner@t", "team-orders", :runner, 3_600)

      assert {:error, :runner_namespace_cannot_be_system} =
               TokenStore.issue("runner@_system", "_system", :runner, :never)

      assert {:ok, _raw, %{expires_at: :never}} =
               TokenStore.issue("runner@t", "team-orders", :runner, :never)
    end
  end

  describe "crash recovery" do
    test "corrupt DETS file is rotated and a fresh one is opened", %{auth_dir: auth_dir} do
      target = Path.join(auth_dir, "tokens.dets")
      File.mkdir_p!(auth_dir)
      File.write!(target, "this-is-not-a-dets-file")

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          {:ok, _} = TokenStore.start_link([])
        end)

      assert log =~ "corrupt DETS"

      rotated_files =
        File.ls!(auth_dir)
        |> Enum.filter(&String.starts_with?(&1, "tokens.dets.corrupt-"))

      assert length(rotated_files) == 1
      # The fresh file should be a real DETS now; issuing a token should succeed.
      assert {:ok, _raw, _entry} = TokenStore.issue("alice", "_system", :ops, 3600)
    end
  end
end

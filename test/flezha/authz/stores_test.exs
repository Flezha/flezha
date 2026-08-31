defmodule Flezha.AuthZ.StoresTest do
  @moduledoc """
  Round-trip tests for the four authn/authz DETS stores
  (`AuthState`, `RoleStore`, `NamespaceStore`, `TokenStore`).
  Runs synchronously because each store is a singleton named
  GenServer + named ETS cache.
  """

  # async: false: a shared on-disk root; the primary Logger; registered singletons.

  use ExUnit.Case, async: false

  alias Flezha.AuthZ.{AuthState, NamespaceStore, RoleStore, TokenStore}
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    %{auth_dir: auth_dir} =
      AuthTestHelper.setup_auth_stores(
        prefix: "flz_authz_stores",
        stores: [],
        enabled: :unchanged
      )

    {:ok, tmp: auth_dir}
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

      assert {:ok, %{principal: "alice", namespace: "_system", role: :admin}} =
               RoleStore.lookup("alice")

      assert RoleStore.admin_system_exists?()
      assert RoleStore.unbind("alice") == :ok
      assert :error = RoleStore.lookup("alice")
      refute RoleStore.admin_system_exists?()
    end

    test "bindings survive restart" do
      {:ok, pid} = RoleStore.start_link([])
      :ok = RoleStore.bind("bob", "team-orders", :operator)
      :ok = GenServer.stop(pid)

      {:ok, _} = RoleStore.start_link([])
      assert {:ok, %{role: :operator}} = RoleStore.lookup("bob")
    end

    test "rejects invalid role" do
      {:ok, _} = RoleStore.start_link([])
      assert {:error, :unknown_role} = RoleStore.bind("eve", "_system", :superuser)
    end
  end

  describe "NamespaceStore" do
    test "seeds _system on first open" do
      {:ok, _} = NamespaceStore.start_link([])

      assert {:ok, %{name: "_system", quotas: %{max_operatives: 100}}} =
               NamespaceStore.get("_system")
    end

    test "create / delete / set_quotas" do
      {:ok, _} = NamespaceStore.start_link([])

      assert {:ok, %{name: "team-orders", quotas: %{max_operatives: 50}}} =
               NamespaceStore.create("team-orders",
                 quotas: %{max_operatives: 50},
                 created_by_principal: "alice"
               )

      assert {:ok, _} = NamespaceStore.get("team-orders")
      assert {:error, :already_exists} = NamespaceStore.create("team-orders")

      assert {:ok, %{quotas: %{max_operatives: 25}}} =
               NamespaceStore.set_quotas("team-orders", %{max_operatives: 25})

      assert :ok = NamespaceStore.delete("team-orders")
      assert :error = NamespaceStore.get("team-orders")

      assert {:error, :cannot_delete_system_namespace} = NamespaceStore.delete("_system")
    end
  end

  describe "TokenStore" do
    test "issue + verify round-trip" do
      {:ok, _} = TokenStore.start_link([])

      assert {:ok, raw, entry} =
               TokenStore.issue("alice", "team-orders", :operator, 3600, label: "dashboards")

      assert String.starts_with?(raw, "flzt_")
      assert entry.principal == "alice"
      assert entry.namespace == "team-orders"
      assert entry.role == :operator

      assert {:ok, %{principal: "alice", namespace: "team-orders", role: :operator}} =
               TokenStore.verify(raw)
    end

    test "expired token is rejected" do
      {:ok, _} = TokenStore.start_link([])
      {:ok, raw, %{expires_at: expires_at}} = TokenStore.issue("alice", "_system", :operator, 1)
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
      {:ok, raw, _} = TokenStore.issue("alice", "_system", :operator, 3600)
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
      {:ok, _, _} = TokenStore.issue("a", "team-orders", :operator, 3600)
      {:ok, _, _} = TokenStore.issue("b", "team-payments", :operator, 3600)
      assert length(TokenStore.list("team-orders")) == 1
      assert length(TokenStore.list("team-payments")) == 1
      assert length(TokenStore.list(:all)) == 2
    end
  end

  describe "crash recovery" do
    test "corrupt DETS file is rotated and a fresh one is opened", %{tmp: tmp} do
      target = Path.join(tmp, "tokens.dets")
      File.mkdir_p!(tmp)
      File.write!(target, "this-is-not-a-dets-file")

      log =
        ExUnit.CaptureLog.capture_log(fn ->
          {:ok, _} = TokenStore.start_link([])
        end)

      assert log =~ "corrupt DETS"

      rotated_files =
        File.ls!(tmp)
        |> Enum.filter(&String.starts_with?(&1, "tokens.dets.corrupt-"))

      assert length(rotated_files) == 1
      # The fresh file should be a real DETS now; issuing a token should succeed.
      assert {:ok, _raw, _entry} = TokenStore.issue("alice", "_system", :operator, 3600)
    end
  end
end

defmodule Flezha.AuthZ.ReplicaTest do
  @moduledoc """
  The merge rules that keep a cluster's authorization state one state: a
  revoke beats an issue whatever their stamps, a delete beats an older row,
  a row never seen is adopted, and of two bootstrap claims the higher
  generation wins and, between first claims, the earlier one, whose rival
  admin is invalidated.
  """

  use FlezhaTest.Case, fixtures: [{:auth_stores, prefix: "flz_authz_replica"}], async: false

  alias Flezha.AuthZ.{AuthState, Bootstrap, Replica, RoleStore, TokenStore}

  defp later(), do: {System.os_time(:microsecond) + 60_000_000, :elsewhere@host}
  defp earlier(), do: {1, :elsewhere@host}

  describe "tokens" do
    test "a revoke held here survives a later live copy" do
      {:ok, raw, entry} = TokenStore.issue("ops", "_system", :ops, :never)
      {:ok, _} = TokenStore.revoke(String.slice(entry.token_hash, 0, 12))

      :ok = Replica.accept(TokenStore, [{entry.token_hash, entry, later()}])

      assert {:error, %Flezha.Error{code: "authz.token_revoked"}} = TokenStore.verify(raw)
    end

    test "a revoke from elsewhere wins over a live entry with a later stamp" do
      {:ok, raw, entry} = TokenStore.issue("ops", "_system", :ops, :never)
      revoked = %{entry | revoked_at: 1}

      :ok = Replica.accept(TokenStore, [{entry.token_hash, revoked, earlier()}])

      assert {:error, %Flezha.Error{code: "authz.token_revoked"}} = TokenStore.verify(raw)
    end

    test "an entry never seen here is adopted, even one written before stamps" do
      {:ok, raw, entry} = TokenStore.issue("ops", "_system", :ops, :never)
      hash = entry.token_hash
      [{^hash, row, _stamp}] = Enum.filter(Replica.snapshot(TokenStore), &(elem(&1, 0) == hash))
      :ok = :dets.delete(TokenStore.table(), hash)
      :ok = :dets.delete(Replica.table(), {TokenStore, hash})

      :ok = Replica.accept(TokenStore, [{hash, row, nil}])

      assert {:ok, %{principal: "ops"}} = TokenStore.verify(raw)
    end
  end

  describe "bindings" do
    test "a later delete removes a binding, and an older row does not bring it back" do
      :ok = RoleStore.bind("alice", "_system", :viewer)
      {:ok, binding} = RoleStore.lookup("alice")
      [{_, row, _}] = Enum.filter(Replica.snapshot(RoleStore), &(elem(&1, 0) == "alice"))

      :ok = Replica.accept(RoleStore, [{"alice", :deleted, later()}])
      assert :error = RoleStore.lookup("alice")

      :ok = Replica.accept(RoleStore, [{"alice", row, earlier()}])
      assert :error = RoleStore.lookup("alice")
      assert binding.roles == [:viewer]
    end

    test "a tombstone for a binding never held here still beats an older copy arriving later" do
      :ok = Replica.accept(RoleStore, [{"carol", :deleted, later()}])

      assert [{"carol", :deleted, _}] =
               Enum.filter(Replica.snapshot(RoleStore), &(elem(&1, 0) == "carol"))

      row = %{principal: "carol", namespace: "_system", roles: [:viewer], role: :viewer}
      :ok = Replica.accept(RoleStore, [{"carol", row, earlier()}])

      assert :error = RoleStore.lookup("carol")
    end

    test "a snapshot carries a delete as a tombstone" do
      :ok = RoleStore.bind("bob", "_system", :viewer)
      :ok = RoleStore.unbind("bob")

      assert [{"bob", :deleted, {_us, _node}}] =
               Enum.filter(Replica.snapshot(RoleStore), &(elem(&1, 0) == "bob"))
    end
  end

  describe "the bootstrap claim" do
    test "of two first claims the earlier wins and the later admin is invalidated" do
      :ok = Bootstrap.check_and_write()
      token = File.read!(Flezha.AuthZ.Config.bootstrap_token_path())
      {:ok, %{principal: late, minted_token: late_token}} = Bootstrap.claim(String.trim(token))

      :ok = RoleStore.bind("admin-early", "_system", :admin)
      early = %{enabled: true, principal: "admin-early", generation: 1, at: 1}
      :ok = Replica.accept(AuthState, [{:enabled, early, earlier()}])

      assert AuthState.enabled?()
      assert :error = RoleStore.lookup(late)
      assert {:error, %Flezha.Error{code: "authz.token_revoked"}} = TokenStore.verify(late_token)
      assert {:ok, %{roles: [:admin]}} = RoleStore.lookup("admin-early")
    end

    test "a claim of a lower generation does not replace a recovery" do
      :ok = AuthState.set_enabled("admin-first")
      :ok = AuthState.set_enabled("admin-recovered")

      stale = %{enabled: true, principal: "admin-first", generation: 1, at: 1}
      :ok = Replica.accept(AuthState, [{:enabled, stale, later()}])

      assert [{:enabled, %{principal: "admin-recovered", generation: 2}, _}] =
               Replica.snapshot(AuthState)
    end

    test "adopting a claim made elsewhere removes this controller's bootstrap token" do
      :ok = Bootstrap.check_and_write()
      assert File.exists?(Flezha.AuthZ.Config.bootstrap_token_path())

      :ok = RoleStore.bind("admin-elsewhere", "_system", :admin)
      claim = %{enabled: true, principal: "admin-elsewhere", generation: 1, at: 1}
      :ok = Replica.accept(AuthState, [{:enabled, claim, earlier()}])

      refute File.exists?(Flezha.AuthZ.Config.bootstrap_token_path())
      assert {:error, :bootstrap_already_completed} = Bootstrap.claim("anything")
    end
  end
end

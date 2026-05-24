defmodule Flezha.AuthZ.BootstrapTest do
  use ExUnit.Case, async: false

  alias Flezha.AuthZ.{AuthState, Bootstrap, NamespaceStore, RoleStore, TokenStore}

  setup do
    # We deliberately nest auth_dir one level inside the test tmp so
    # the parent-mode check in Bootstrap sees a 0700 parent (the
    # test tmp itself), not /tmp (which is 1777 on Linux and would
    # always abort the bootstrap write).
    tmp = Path.join(System.tmp_dir!(), "flz_authz_boot_#{System.unique_integer([:positive])}")
    auth_dir = Path.join(tmp, "auth")
    File.mkdir_p!(auth_dir)
    File.chmod!(tmp, 0o700)
    File.chmod!(auth_dir, 0o700)

    prev = Application.get_env(:flezha, :authn_authz, [])

    Application.put_env(
      :flezha,
      :authn_authz,
      prev
      |> Keyword.put(:auth_dir, auth_dir)
      |> Keyword.put(:bootstrap_token_path, Path.join(auth_dir, "bootstrap.token"))
      |> Keyword.put(:enabled, true)
    )

    stop_if_running([AuthState, RoleStore, NamespaceStore, TokenStore])
    {:ok, _} = AuthState.start_link([])
    {:ok, _} = RoleStore.start_link([])
    {:ok, _} = NamespaceStore.start_link([])
    {:ok, _} = TokenStore.start_link([])

    on_exit(fn ->
      stop_if_running([AuthState, RoleStore, NamespaceStore, TokenStore])
      Application.put_env(:flezha, :authn_authz, prev)
      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp}
  end

  test "first boot writes a bootstrap token (mode 0600)" do
    assert :ok = Bootstrap.check_and_write()
    path = Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path]
    assert File.exists?(path)
    {:ok, %File.Stat{mode: mode}} = File.stat(path)
    assert Bitwise.band(mode, 0o777) == 0o600
  end

  test "check_and_write/0 is a no-op once admin@_system exists" do
    :ok = RoleStore.bind("alice", "_system", :admin)
    assert :ok = Bootstrap.check_and_write()

    refute File.exists?(
             Path.join(Application.get_env(:flezha, :authn_authz)[:auth_dir], "bootstrap.token")
           )
  end

  test "claim/2 with --principal binds and flips :enabled" do
    :ok = Bootstrap.check_and_write()
    {:ok, raw} = File.read(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])

    assert {:ok, %{principal: "alice", minted_token: nil}} =
             Bootstrap.claim(raw, principal: "alice")

    assert RoleStore.admin_system_exists?()
    assert AuthState.enabled?()
    refute File.exists?(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])
  end

  test "claim/2 with cert_principal wins over --principal" do
    :ok = Bootstrap.check_and_write()
    {:ok, raw} = File.read(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])

    assert {:ok, %{principal: "spiffe://flezha.local/admins/alice"}} =
             Bootstrap.claim(raw,
               principal: "alice",
               cert_principal: "spiffe://flezha.local/admins/alice"
             )
  end

  test "claim/2 without any principal mints a token" do
    :ok = Bootstrap.check_and_write()
    {:ok, raw} = File.read(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])

    assert {:ok, %{principal: "admin-" <> _, minted_token: minted}} = Bootstrap.claim(raw)
    assert is_binary(minted)
    assert String.starts_with?(minted, "flzt_")
    assert {:ok, %{role: :admin, namespace: "_system"}} = TokenStore.verify(minted)
  end

  test "second claim attempt is rejected" do
    :ok = Bootstrap.check_and_write()
    {:ok, raw} = File.read(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])
    {:ok, _} = Bootstrap.claim(raw, principal: "alice")

    assert {:error, :bootstrap_already_completed} = Bootstrap.claim(raw, principal: "eve")
  end

  test "mismatched token is rejected" do
    :ok = Bootstrap.check_and_write()
    assert {:error, :bootstrap_token_mismatch} = Bootstrap.claim("not-the-token", principal: "x")
  end

  test "permissive parent directory aborts bootstrap write" do
    # Re-point bootstrap_token_path so its parent is /tmp (mode 1777
    # on Linux). The check_parent_mode/1 guard should refuse to
    # write and surface {:error, {:permissive_auth_parent, "/tmp"}}.
    permissive_dir =
      Path.join(System.tmp_dir!(), "flz_authz_permissive_#{System.unique_integer([:positive])}")

    File.mkdir_p!(permissive_dir)
    # Don't chmod permissive_dir's parent — /tmp is permissive on
    # purpose. The bootstrap writer must refuse.
    File.rm_rf!(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])
    :ok = RoleStore.unbind("alice")

    prev = Application.get_env(:flezha, :authn_authz)

    Application.put_env(
      :flezha,
      :authn_authz,
      Keyword.put(prev, :bootstrap_token_path, Path.join(permissive_dir, "bootstrap.token"))
    )

    on_exit(fn ->
      Application.put_env(:flezha, :authn_authz, prev)
      File.rm_rf!(permissive_dir)
    end)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, {:permissive_auth_parent, _}} = Bootstrap.check_and_write()
      end)

    assert log =~ "permissive mode"
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

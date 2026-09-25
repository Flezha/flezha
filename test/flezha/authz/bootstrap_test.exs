defmodule Flezha.AuthZ.BootstrapTest do
  # async: false: writes Application env; a shared on-disk root; the primary Logger.
  use FlezhaTest.Case,
    fixtures: [
      {:auth_stores,
       prefix: "flz_authz_boot",
       stores: [
         Flezha.AuthZ.AuthState,
         Flezha.AuthZ.RoleStore,
         Flezha.AuthZ.NamespaceStore,
         Flezha.AuthZ.TokenStore
       ]}
    ],
    async: false

  alias Flezha.AuthZ.{AuthState, Bootstrap, RoleStore, TokenStore}
  alias FlezhaTest.Helper.AuthTestHelper

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

    refute File.exists?(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])
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

  test "second claim attempt is rejected, naming the file it expected" do
    path = Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path]
    :ok = Bootstrap.check_and_write()
    {:ok, raw} = File.read(path)
    {:ok, _} = Bootstrap.claim(raw, principal: "alice")

    # The claim unlinks the file, so a second one has nothing to compare
    # against. Recovery IS a second claim, and it is the caller writing that
    # file again that admits it — so the refusal names the file rather than
    # asserting the bootstrap can never happen twice.
    assert {:error, {:bootstrap_token_missing, ^path}} = Bootstrap.claim(raw, principal: "eve")
  end

  test "mismatched token is rejected" do
    :ok = Bootstrap.check_and_write()
    assert {:error, :bootstrap_token_mismatch} = Bootstrap.claim("not-the-token", principal: "x")
  end

  test "sticky shared parent directory aborts bootstrap write" do
    # `check_parent_mode/1` inspects the GRANDPARENT of the token (the
    # dir holding the auth dir). A sticky group/other-writable root
    # (the /tmp shape) is shared by design and never tightened — the
    # guard must refuse and surface `{:error, {:permissive_auth_parent, _}}`.
    permissive_root = permissive_token_root(0o1777)

    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, {:permissive_auth_parent, _}} = Bootstrap.check_and_write()
      end)

    assert log =~ "permissive mode"
    assert {:ok, %File.Stat{mode: mode}} = File.stat(permissive_root)
    assert Bitwise.band(mode, 0o1777) == 0o1777
  end

  test "umask-inherited permissive parent is tightened in place" do
    # A 0775 root (the default-umask File.mkdir_p shape) is the app's
    # own dir; the guard strips the write bits and proceeds instead of
    # refusing every fresh install.
    permissive_root = permissive_token_root(0o775)

    log =
      AuthTestHelper.capture_log_at(:warning, fn -> assert :ok = Bootstrap.check_and_write() end)

    assert log =~ "removed group/other-write bits"
    assert {:ok, %File.Stat{mode: mode}} = File.stat(permissive_root)
    assert Bitwise.band(mode, 0o777) == 0o755
    assert File.exists?(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])
  end

  # Point the bootstrap token into a fresh root chmodded to `mode`, so
  # the parent-mode guard fires deterministically — the host's
  # `System.tmp_dir!()` mode (umask-dependent) must not decide the outcome.
  defp permissive_token_root(mode) do
    permissive_root =
      Path.join(System.tmp_dir!(), "flz_authz_permissive_#{System.unique_integer([:positive])}")

    auth_dir = Path.join(permissive_root, "auth")
    File.mkdir_p!(auth_dir)
    # File.chmod/2 masks the sticky bit, so shell out to set the mode.
    {_, 0} = System.cmd("chmod", [Integer.to_string(mode, 8), permissive_root])

    File.rm_rf!(Application.get_env(:flezha, :authn_authz)[:bootstrap_token_path])
    :ok = RoleStore.unbind("alice")

    prev = Application.get_env(:flezha, :authn_authz)

    Application.put_env(
      :flezha,
      :authn_authz,
      Keyword.put(prev, :bootstrap_token_path, Path.join(auth_dir, "bootstrap.token"))
    )

    on_exit(fn ->
      Application.put_env(:flezha, :authn_authz, prev)
      File.rm_rf!(permissive_root)
    end)

    permissive_root
  end
end

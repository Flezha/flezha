defmodule Flezha.Transport.TransportAuthTest do
  @moduledoc """
  Transport authentication posture (finish-platform-authn-authz §2):
  the UDS surface is owner-only, the wide-open opt-out is warned on every
  boot, the release refuses distribution without a provisioned cookie, and
  the cert generator emits a full mutual-TLS set.
  """
  use ExUnit.Case, async: false

  alias Flezha.Transport.Native
  alias Flezha.Transport.Server
  alias FlezhaTest.Helper.AuthTestHelper

  @tmp_base "app_data/test_transport_auth"

  setup do
    File.mkdir_p!(@tmp_base)
    on_exit(fn -> File.rm_rf!(@tmp_base) end)
    :ok
  end

  # Point the controller's socket directory at a scratch path, optionally under
  # the group mode, and put both back afterwards.
  defp with_socket_dir(name, group, fun) do
    previous = Application.fetch_env!(:flezha, :socket_files_path)
    base = Path.join(@tmp_base, name)

    Application.put_env(:flezha, :socket_files_path, base)
    if group, do: System.put_env("FLEZHA_SOCKET_DIR_GROUP", group)

    try do
      fun.(base)
    after
      System.delete_env("FLEZHA_SOCKET_DIR_GROUP")
      Application.put_env(:flezha, :socket_files_path, previous)
    end
  end

  defp mode_bits(path) do
    %File.Stat{mode: mode} = File.stat!(path)
    Bitwise.band(mode, 0o777)
  end

  describe "UDS socket permissions" do
    test "the control socket and its parent directory are owner-only" do
      dir = Path.join(@tmp_base, "sockets")
      socket_path = Path.join(dir, "auth_test.sock")

      {:ok, server_ref} = Native.start_uds_server(socket_path, self())

      try do
        assert mode_bits(dir) == 0o700
        assert mode_bits(socket_path) == 0o600
      after
        Native.stop_server(server_ref)
      end
    end

    test "the group mode widens the boundary to one named group, warns, and never further" do
      group = String.trim(elem(System.cmd("id", ["-gn"]), 0))
      gid = String.to_integer(String.trim(elem(System.cmd("id", ["-g"]), 0)))

      with_socket_dir("group_sockets", group, fn base ->
        warning =
          AuthTestHelper.capture_log_at(:warning, fn ->
            Flezha.Socket.Support.prepare_socket_dir()
          end)

        # A member of the group reaches the directory...
        assert mode_bits(base) == 0o750
        assert File.stat!(base).gid == gid
        # ...and nobody outside it does: no setting makes the directory
        # world-readable or world-traversable.
        assert Bitwise.band(mode_bits(base), 0o007) == 0
        assert warning =~ "FLEZHA_SOCKET_DIR_GROUP=#{group}"
      end)
    end

    test "an unusable group refuses the boot, naming the group" do
      with_socket_dir("bad_group_sockets", "flezha_no_such_group", fn _base ->
        assert_raise RuntimeError, ~r/flezha_no_such_group/, fn ->
          Flezha.Socket.Support.prepare_socket_dir()
        end
      end)
    end

    test "prepare_socket_dir creates the base path owner-only" do
      with_socket_dir("prepared_sockets", nil, fn base ->
        Flezha.Socket.Support.prepare_socket_dir()
        assert mode_bits(base) == 0o700
      end)
    end
  end

  describe "the attach line" do
    test "names the absolute socket directory and the assignment that reaches it" do
      with_socket_dir("attach_sockets", nil, fn base ->
        # An operator can paste the line: the directory it names is absolute
        # even when the configured value is not.
        assert Path.type(base) == :relative
        resolved = Flezha.Socket.Support.socket_dir()

        assert Path.type(resolved) == :absolute
        assert Flezha.BootLoader.attach_line() =~ "FLEZHA_SOCKET_FILES_PATH=#{resolved}"
      end)
    end

    test "names the QUIC listen address when QUIC is enabled, and nothing when neither is" do
      previous = Application.get_env(:flezha, :transports, [:uds])

      try do
        Application.put_env(:flezha, :transports, [:uds, :quic])

        assert Flezha.BootLoader.attach_line() =~
                 "QUIC listening on #{Application.get_env(:flezha, :quic_listen_address)}"

        Application.put_env(:flezha, :transports, [])
        refute Flezha.BootLoader.attach_line()
      after
        Application.put_env(:flezha, :transports, previous)
      end
    end
  end

  describe "opt-out warning" do
    test "disabling transport auth warns; the default posture does not" do
      previous = Application.get_env(:flezha, :transport_auth_required, true)

      try do
        Application.put_env(:flezha, :transport_auth_required, false)

        assert AuthTestHelper.capture_log_at(:warning, fn ->
                 Server.warn_if_transport_auth_disabled()
               end) =~ "peer authentication is DISABLED"

        Application.put_env(:flezha, :transport_auth_required, true)

        assert AuthTestHelper.capture_log_at(:warning, fn ->
                 Server.warn_if_transport_auth_disabled()
               end) == ""
      after
        Application.put_env(:flezha, :transport_auth_required, previous)
      end
    end
  end

  describe "release cookie" do
    # Renders rel/env.sh.eex the way `mix release` would and sources it in a
    # clean shell: the distribution posture must come from the script alone.
    defp release_distribution_with(env_exports) do
      script =
        EEx.eval_file("rel/env.sh.eex", assigns: [release: %{name: :flezha_controller}])

      path = Path.join(@tmp_base, "env.sh")
      File.write!(path, script)

      {out, 0} =
        System.cmd(
          "sh",
          ["-c", "#{env_exports} . #{path}; echo dist=$RELEASE_DISTRIBUTION"],
          env: [{"RELEASE_COOKIE", nil}, {"RELEASE_DISTRIBUTION", nil}],
          stderr_to_stdout: true
        )

      out
    end

    test "a missing cookie refuses to start distribution" do
      out = release_distribution_with("")
      assert out =~ "refusing to start Erlang distribution"
      assert out =~ "dist=none"
    end

    test "a provisioned cookie enables distribution without any constant fallback" do
      out = release_distribution_with("RELEASE_COOKIE=provisioned-secret;")
      refute out =~ "refusing"
      assert out =~ "dist=name"
      refute File.read!("rel/env.sh.eex") =~ "flezha_cluster_cookie"
    end
  end

  describe "cert generator" do
    test "generates a client identity beside the server pair, keys owner-only" do
      cert_dir = Path.join(@tmp_base, "certs")

      overrides = %{
        ca_cert_path: Path.join(cert_dir, "ca.pem"),
        server_cert_path: Path.join(cert_dir, "server.pem"),
        server_key_path: Path.join(cert_dir, "server.key"),
        client_cert_path: Path.join(cert_dir, "client.pem"),
        client_key_path: Path.join(cert_dir, "client.key")
      }

      previous = Enum.map(overrides, fn {k, _} -> {k, Application.get_env(:flezha, k)} end)
      Enum.each(overrides, fn {k, v} -> Application.put_env(:flezha, k, v) end)

      try do
        :ok = Flezha.Transport.CertGenerator.ensure_certs()

        Enum.each(overrides, fn {_k, path} -> assert File.exists?(path) end)
        assert mode_bits(overrides.server_key_path) == 0o600
        assert mode_bits(overrides.client_key_path) == 0o600

        # The client leaf carries the clientAuth EKU webpki verifies against.
        assert cert_eku(overrides.client_cert_path) == {1, 3, 6, 1, 5, 5, 7, 3, 2}
        assert cert_eku(overrides.server_cert_path) == {1, 3, 6, 1, 5, 5, 7, 3, 1}
      after
        Enum.each(previous, fn
          {k, nil} -> Application.delete_env(:flezha, k)
          {k, v} -> Application.put_env(:flezha, k, v)
        end)
      end
    end

    defp cert_eku(pem_path) do
      [{:Certificate, der, :not_encrypted}] = :public_key.pem_decode(File.read!(pem_path))
      {:OTPCertificate, tbs, _, _} = :public_key.pkix_decode_cert(der, :otp)
      {:OTPTBSCertificate, _, _, _, _, _, _, _, _, _, exts} = tbs

      Enum.find_value(List.wrap(exts), fn
        {:Extension, {2, 5, 29, 37}, _critical, [oid | _]} -> oid
        _ -> nil
      end)
    end
  end
end

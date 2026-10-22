defmodule Flezha.Transport.TransportAuthTest do
  @moduledoc """
  Transport authentication posture (finish-platform-authn-authz §2):
  the UDS surface is owner-only, the wide-open opt-out is warned on every
  boot, the release refuses distribution without a provisioned cookie, and
  the cert generator emits a full mutual-TLS set.
  """
  # async: false: writes Application env; the primary Logger; OS environment variables.
  use ExUnit.Case, async: false

  alias Flezha.Boot.Loader
  alias Flezha.Socket.Support
  alias Flezha.State.TestHelpers
  alias Flezha.Transport.CertGenerator
  alias Flezha.Transport.Native
  alias Flezha.Transport.Server
  alias FlezhaTest.Helper.AuthTestHelper

  setup do
    File.mkdir_p!(tmp_base())
    on_exit(fn -> File.rm_rf!(tmp_base()) end)
    :ok
  end

  # Under the temporary directory, not the checkout: a checkout may be cloned
  # anywhere, and the boot refuses a socket directory over 60 bytes.
  defp tmp_base(), do: Path.join(System.tmp_dir!(), "flz_auth_#{System.pid()}")

  # Point the controller's socket directory at a scratch path, optionally under
  # the group mode, and put both back afterwards.
  defp with_socket_dir(name, group, fun) do
    previous = Application.fetch_env!(:flezha, :socket_files_path)
    base = Path.join(tmp_base(), name)

    Application.put_env(:flezha, :socket_files_path, base)
    if group, do: Application.put_env(:flezha, :socket_dir_group, group)

    try do
      fun.(base)
    after
      Application.delete_env(:flezha, :socket_dir_group)
      Application.put_env(:flezha, :socket_files_path, previous)
    end
  end

  defp mode_bits(path) do
    %File.Stat{mode: mode} = File.stat!(path)
    Bitwise.band(mode, 0o777)
  end

  describe "UDS socket permissions" do
    test "the control socket and its parent directory are owner-only" do
      dir = Path.join(tmp_base(), "sockets")
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
            Support.prepare_socket_dir()
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
          Support.prepare_socket_dir()
        end
      end)
    end

    test "prepare_socket_dir creates the base path owner-only" do
      with_socket_dir("prepared_sockets", nil, fn base ->
        Support.prepare_socket_dir()
        assert mode_bits(base) == 0o700
      end)
    end
  end

  describe "the attach line" do
    test "names the absolute socket directory and the assignment that reaches it" do
      # A user can paste the line: the directory it names is absolute
      # even when the configured value is not.
      on_exit(TestHelpers.override_env(:socket_files_path, "storage/app/attach_sockets"))
      resolved = Support.socket_dir()

      assert Path.type(resolved) == :absolute
      assert Loader.attach_line() =~ "FLEZHA_SOCKET_FILES_PATH=#{resolved}"
    end

    test "names the QUIC listen address when QUIC is enabled, and nothing when neither is" do
      previous = Application.get_env(:flezha, :transports, [:uds])

      try do
        Application.put_env(:flezha, :transports, [:uds, :quic])

        assert Loader.attach_line() =~
                 "QUIC listening on #{Application.get_env(:flezha, :quic_listen_address)}"

        Application.put_env(:flezha, :transports, [])
        refute Loader.attach_line()
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
    defp cookie_file(), do: Path.join([tmp_base(), "state", "cookie"])

    defp home(kind),
      do: Path.join([tmp_base(), "release_tmp", "home", "cookie_test@127.0.0.1", kind])

    # rel/env.sh.eex rendered the way `mix release` renders it.
    defp env_sh() do
      script = EEx.eval_file("rel/env.sh.eex", assigns: [release: %{name: :flezha_controller}])
      path = Path.join(tmp_base(), "env.sh")
      File.write!(path, script)
      path
    end

    # Sources the script in a clean shell as the release command `command`
    # would: the distribution posture must come from the script alone.
    defp source_env_sh(command, env \\ []) do
      {out, 0} = sourced(command, env)
      [_, dist, cookie, home] = Regex.run(~r/^dist=(\S*) cookie=(\S*) home=(\S*)$/m, out)
      {out, dist, cookie, home}
    end

    defp sourced(command, env) do
      clean = %{
        "RELEASE_COMMAND" => command,
        "RELEASE_COOKIE" => nil,
        "RELEASE_DISTRIBUTION" => nil,
        "FLEZHA_STATE_DIR" => Path.dirname(cookie_file()),
        "FLEZHA_COOKIE_FILE" => cookie_file(),
        "FLEZHA_NODE_NAME" => "cookie_test",
        "FLEZHA_NODE_HOST" => "127.0.0.1",
        "RELEASE_TMP" => Path.join(tmp_base(), "release_tmp"),
        "HOME" => Path.join(tmp_base(), "user")
      }

      System.cmd(
        "sh",
        [
          "-c",
          ". #{env_sh()}; echo \"dist=$RELEASE_DISTRIBUTION cookie=$RELEASE_COOKIE home=$HOME\""
        ],
        env: Enum.to_list(Map.merge(clean, Map.new(env))),
        stderr_to_stdout: true
      )
    end

    test "a start that cannot mint a cookie refuses to start distribution" do
      # A regular file where the state directory belongs: nothing can be written under it.
      File.write!(Path.dirname(cookie_file()), "")

      {out, dist, _cookie, _home} = source_env_sh("start")
      assert out =~ "refusing to start Erlang distribution"
      assert dist == "none"
    end

    # OTP reads `$HOME/.erlang.cookie` when no command line names a cookie, so
    # the provisioned value reaches the VM there, in a directory of its own.
    test "a provisioned cookie enables distribution without any constant fallback" do
      {out, dist, cookie, home} =
        source_env_sh("start", [{"RELEASE_COOKIE", "provisioned-secret"}])

      refute out =~ "refusing"
      assert {dist, cookie, home} == {"name", "provisioned-secret", home("node")}
      assert File.read!(Path.join(home, ".erlang.cookie")) == "provisioned-secret"
      assert mode_bits(Path.join(home, ".erlang.cookie")) == 0o600
      assert mode_bits(home) == 0o700
      refute File.exists?(cookie_file())
      refute File.read!("rel/env.sh.eex") =~ "flezha_cluster_cookie"
    end

    # OTP's boot would crash on a cookie it cannot read; a directory another
    # user could have placed is never trusted with one.
    test "a cookie Erlang cannot read, or a private HOME that is a link, is refused" do
      {out, "none", _cookie, _home} =
        source_env_sh("start", [{"RELEASE_COOKIE", "cookié"}])

      assert out =~ "printable ASCII only); refusing to start Erlang distribution"

      elsewhere = Path.join(tmp_base(), "elsewhere")
      File.mkdir_p!(elsewhere)
      File.mkdir_p!(Path.dirname(home("node")))
      File.ln_s!(elsewhere, home("node"))

      {out, "none", _cookie, _home} = source_env_sh("start", [{"RELEASE_COOKIE", "fine"}])
      assert out =~ "could not be placed in #{home("node")}"
      assert File.ls!(elsewhere) == []
    end

    test "only a command that starts a node mints the node-local cookie; the rest only read it" do
      # With no cookie there is no node to reach: the call answers as an
      # unreachable one does, starts no VM and writes nothing.
      for command <- ~w(rpc remote stop restart pid) do
        assert {out, 1} = sourced(command, [])
        assert out == "--rpc-eval : RPC failed with reason :noconnection\n", command
        refute File.exists?(Path.dirname(cookie_file())), "#{command} wrote a cookie"
      end

      for command <- ~w(eval version) do
        assert {out, "none", "", home} = source_env_sh(command)
        refute out =~ "[flezha]", command
        assert home == Path.join(tmp_base(), "user"), command
        refute File.exists?(Path.dirname(cookie_file())), "#{command} wrote a cookie"
      end

      for command <- ~w(start start_iex daemon daemon_iex) do
        File.rm_rf!(Path.dirname(cookie_file()))

        {out, "name", cookie, home} = source_env_sh(command)
        assert File.read!(cookie_file()) == cookie <> "\n", command
        assert mode_bits(cookie_file()) == 0o600
        assert out =~ "using the node-local cookie in #{cookie_file()}"
        assert {home, File.read!(Path.join(home, ".erlang.cookie"))} == {home("node"), cookie}

        assert {_, "name", ^cookie, cli} = source_env_sh("rpc")
        assert {cli, File.read!(Path.join(cli, ".erlang.cookie"))} == {home("cli"), cookie}
      end

      refute File.exists?(Path.join([tmp_base(), "user", ".erlang.cookie"]))
    end

    # The window a start racing a first start falls into: it has found no
    # cookie file and is still inside the script when the other writes one. `od`
    # runs in exactly that window, so a stand-in for it runs the whole other
    # start there. Returns the cookie each start exported, outer first. A verb
    # has no such window: it never mints, so it reads a file or reaches nothing.
    defp race_a_first_start() do
      shims = Path.join(tmp_base(), "shims")
      started = Path.join(tmp_base(), "started")
      File.mkdir_p!(shims)

      File.write!(Path.join(shims, "od"), """
      #!/bin/sh
      if [ ! -e "#{started}.lock" ]; then
        : >"#{started}.lock"
        (RELEASE_COMMAND=start; . "#{env_sh()}"; printf %s "$RELEASE_COOKIE" >"#{started}") >/dev/null 2>&1
      fi
      exec #{System.find_executable("od")} "$@"
      """)

      File.chmod!(Path.join(shims, "od"), 0o755)

      {_out, "name", cookie, _home} =
        source_env_sh("start", [{"PATH", shims <> ":" <> System.get_env("PATH")}])

      # The start really ran inside the window, and minted.
      assert <<_::binary-size(64)>> = start_cookie = File.read!(started)
      {cookie, start_cookie}
    end

    # The inner start writes first and the outer one last. Either node may win
    # the name contest, so both must have exported the value the file keeps.
    test "two starts racing a first boot each export the cookie the file holds" do
      {outer_cookie, inner_cookie} = race_a_first_start()
      kept = File.read!(cookie_file())
      assert {inner_cookie <> "\n", outer_cookie <> "\n"} == {kept, kept}
      assert mode_bits(cookie_file()) == 0o600
      assert File.read!(Path.join(home("node"), ".erlang.cookie")) <> "\n" == kept
    end
  end

  describe "cert generator" do
    test "generates a client identity beside the server pair, keys owner-only" do
      cert_dir = Path.join(tmp_base(), "certs")

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
        :ok = CertGenerator.ensure_certs()

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

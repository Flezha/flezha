defmodule Flezha.Plugin.InstallerTest do
  @moduledoc """
  Install, uninstall and sync place archives in the plugin root and touch
  nothing else, from any VM; `list/0` merges what the boot decided with what the
  root holds since. The fetches run against a local HTTPS origin.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  @moduletag :capture_log

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Package
  alias Flezha.Transport.CertGenerator
  alias Flezha.TestHelpers.PluginPackage

  setup %{tmp: tmp} do
    root = Path.join(tmp, "plugins")
    configure(plugin_root: root)
    on_exit(fn -> PluginPackage.unload!(root) end)
    {:ok, root: root, example: PluginPackage.example!()}
  end

  defp configure(settings) do
    Application.put_env(
      :flezha,
      :plugins,
      Keyword.merge(Application.get_env(:flezha, :plugins, []), settings)
    )
  end

  defp root_files(root), do: root |> File.ls!() |> Enum.sort()
  defp staging_empty?(root), do: Package.files(Path.join(root, ".staging")) == []

  defp sibling(tmp, id, edits \\ []),
    do: PluginPackage.sibling(Path.join(tmp, "#{id}.fpz"), id, edits)

  describe "install/2" do
    test "places a local archive as <id>.fpz in an empty root, with no controller tree",
         %{root: root, example: example} do
      assert {:ok, placed} = Installer.install(example)

      assert placed == %{
               id: "flezha_example_plugin",
               version: "0.1.0",
               signed: false,
               root: root,
               warning: nil
             }

      assert File.read!(Path.join(root, "flezha_example_plugin.fpz")) == File.read!(example)
      assert staging_empty?(root)
    end

    test "a package built on an older Elixir minor is placed with a warning naming both versions",
         %{example: example, tmp: tmp} do
      {:ok, %Version{major: major, minor: minor}} = Version.parse(System.version())
      built = "#{major}.#{minor - 1}.4"

      older =
        PluginPackage.rewrite(example, Path.join(tmp, "older.fpz"), [
          {:manifest, &put_in(&1.built_with.elixir, built)}
        ])

      assert {:ok, %{warning: warning}} = Installer.install(older)

      assert warning =~
               "plugin package flezha_example_plugin was built on Elixir #{built} " <>
                 "and loads on Elixir #{System.version()};"
    end

    test "replaces every archive declaring the same id", %{root: root, example: example, tmp: tmp} do
      File.mkdir_p!(root)
      File.cp!(example, Path.join(root, "flezha_example_plugin-0.1.0.fpz"))

      newer =
        PluginPackage.rewrite(example, Path.join(tmp, "newer.fpz"), [
          {:manifest, &Map.put(&1, :description, "newer")}
        ])

      assert {:ok, %{id: "flezha_example_plugin"}} = Installer.install(newer)
      assert root_files(root) == [".staging", "flezha_example_plugin.fpz"]
      assert File.read!(Path.join(root, "flezha_example_plugin.fpz")) == File.read!(newer)
    end

    test "a refused install leaves the root as it was", %{root: root, example: example, tmp: tmp} do
      {:ok, _} = Installer.install(example)
      before = root_files(root)

      native =
        PluginPackage.rewrite(example, Path.join(tmp, "native.fpz"), [
          {:put, "lib/stamp_lib-0.1.0/priv/nif.so", "ELF"}
        ])

      assert {:error, {:native_code, _}} = Installer.install(native)
      assert root_files(root) == before
      assert staging_empty?(root)
    end

    test "library and provider conflicts are refused against the release and installed manifests",
         %{example: example, tmp: tmp} do
      {:ok, _} = Installer.install(example)

      other_lib = sibling(tmp, "flz_lib", [{:app_version, :stamp_lib, "0.2.0"}])

      assert {:error,
              {:library_conflict, :stamp_lib, "0.2.0", "0.1.0", "package 'flezha_example_plugin'"}} =
               Installer.install(other_lib)

      taken =
        sibling(tmp, "flz_taken", [
          {:manifest, &%{&1 | providers: %{"example_stamp" => FlzTaken.Provider}}}
        ])

      assert {:error, {:provider_id_taken, "example_stamp", "package 'flezha_example_plugin'"}} =
               Installer.install(taken)

      release =
        sibling(tmp, "flz_stdout", [
          {:manifest, &%{&1 | providers: %{"relay_stdout" => FlzStdout.Provider}}}
        ])

      assert {:error, {:provider_id_taken, "relay_stdout", "the release"}} =
               Installer.install(release)

      spec = {:application, :jason, [vsn: ~c"0.0.1", modules: []]}

      jason =
        sibling(tmp, "flz_jason", [
          {:put, "lib/jason-0.0.1/ebin/jason.app", :io_lib.format(~c"~p.~n", [spec])},
          {:manifest, &%{&1 | apps: &1.apps ++ [{:jason, "0.0.1"}]}}
        ])

      assert {:error, {:library_conflict, :jason, "0.0.1", _, "the release"}} =
               Installer.install(jason)
    end

    test "a source that is neither a path nor an https:// URL is refused, fetching nothing",
         %{root: root} do
      assert {:error, {:source_unsupported, "http://host/p.fpz"}} =
               Installer.install("http://host/p.fpz")

      assert {:error, {:source_unsupported, "ftp://host/p.fpz"}} =
               Installer.install("ftp://host/p.fpz")

      assert staging_empty?(root)
    end
  end

  describe "uninstall/2" do
    test "removes every archive declaring the id", %{root: root, example: example} do
      {:ok, _} = Installer.install(example)
      File.cp!(example, Path.join(root, "copy.fpz"))

      assert {:ok, %{id: "flezha_example_plugin", root: ^root}} =
               Installer.uninstall("flezha_example_plugin")

      assert root |> Path.join("*.fpz") |> Path.wildcard() == []
    end

    test "an id with no archive is not installed" do
      assert {:error, {:plugin_not_installed, "ghost"}} = Installer.uninstall("ghost")
    end
  end

  describe "a declared list" do
    test "refuses install and uninstall while it is set, even empty", %{
      root: root,
      example: example
    } do
      configure(list: "")

      assert {:error, {:list_declared, "FLEZHA_PLUGINS"}} = Installer.install(example)
      assert {:error, {:list_declared, "FLEZHA_PLUGINS"}} = Installer.uninstall("x")
      refute File.exists?(Path.join(root, "flezha_example_plugin.fpz"))
    end

    test "sync converges the root: a pinned present archive kept unfetched, a new one placed, the rest removed",
         %{root: root, example: example, tmp: tmp} do
      {:ok, _} = Installer.install(example)
      {:ok, _} = Installer.install(sibling(tmp, "flz_b"))
      b = Path.join(root, "flz_b.fpz")
      c = sibling(tmp, "flz_c")

      configure(list: "#{tmp}/nowhere/b.fpz#sha256=#{Package.digest(b)}, #{c}")

      assert {:ok, %{placed: [%{id: "flz_c"}], kept: ["flz_b"], removed: [removed]}} =
               Installer.sync()

      assert removed == Path.join(root, "flezha_example_plugin.fpz")
      assert root_files(root) == [".staging", "flz_b.fpz", "flz_c.fpz"]
      assert staging_empty?(root)
    end

    test "a failed entry fails the sync and changes nothing", %{root: root, example: example} do
      {:ok, _} = Installer.install(example)
      present = Path.join(root, "flezha_example_plugin.fpz")
      before = root_files(root)

      configure(list: "#{present}#sha256=#{Package.digest(present)} https://127.0.0.1:1/p.fpz")

      assert {:error, {:fetch_failed, "https://127.0.0.1:1/p.fpz", _}} = Installer.sync()
      assert root_files(root) == before
    end

    test "a pinned entry fetched with another digest is refused", %{root: root, example: example} do
      configure(list: "#{example}#sha256=#{String.duplicate("0", 64)}")

      assert {:error, {:digest_mismatch, ^example, _pin, actual}} = Installer.sync()
      assert actual == Package.digest(example)
      refute File.exists?(Path.join(root, "flezha_example_plugin.fpz"))
    end

    test "sync with no list declared is refused, naming the variable" do
      assert {:error, {:list_not_declared, "FLEZHA_PLUGINS"}} = Installer.sync()
    end
  end

  describe "fetching over HTTPS" do
    # The transport's own generator: a CA and a `localhost` certificate it signs.
    setup %{example: example, tmp: tmp} do
      dir = Path.join(tmp, "certs")

      for {key, file} <- [
            ca_cert_path: "ca.pem",
            server_cert_path: "server.pem",
            server_key_path: "server.key",
            client_cert_path: "client.pem",
            client_key_path: "client.key"
          ],
          do: Application.put_env(:flezha, key, Path.join(dir, file))

      :ok = CertGenerator.ensure_certs()
      [{:Certificate, ca, _}] = :public_key.pem_decode(File.read!(Path.join(dir, "ca.pem")))

      {port, stop} =
        serve_https(
          Path.join(dir, "server.pem"),
          Path.join(dir, "server.key"),
          File.read!(example)
        )

      on_exit(stop)
      {:ok, base: "https://localhost:#{port}", cacerts: [ca]}
    end

    test "follows an HTTPS redirect and places the archive", %{
      base: base,
      cacerts: cacerts,
      root: root
    } do
      assert {:ok, %{id: "flezha_example_plugin"}} =
               Installer.install("#{base}/redirect", cacerts: cacerts)

      assert File.exists?(Path.join(root, "flezha_example_plugin.fpz"))
    end

    test "refuses a peer the CA store does not trust", %{base: base} do
      assert {:error, {:fetch_failed, _, reason}} =
               Installer.install("#{base}/pkg.fpz", cacerts: [])

      assert reason =~ ~r/unknown ca|certificate/i
    end

    test "abandons a body past the size limit, leaving nothing in staging",
         %{base: base, cacerts: cacerts, root: root} do
      configure(max_package_bytes: 1_000_000)

      assert {:error, {:package_invalid, :too_large, %{limit: 1_000_000}}} =
               Installer.install("#{base}/big", cacerts: cacerts)

      assert staging_empty?(root)
    end
  end

  describe "list/0" do
    test "answers loaded, refused, pending and removed rows",
         %{root: root, example: example, tmp: tmp} do
      refused = sibling(tmp, "flz_refused", [{:manifest, &put_in(&1.built_with.otp, "0")}])
      PluginPackage.load!(root, [example, sibling(tmp, "flz_b"), refused])

      {:ok, _} = Installer.uninstall("flz_b")
      {:ok, _} = Installer.install(sibling(tmp, "flz_new"))

      newer =
        PluginPackage.rewrite(example, Path.join(tmp, "newer.fpz"), [
          {:app_version, :flezha_example_plugin, "0.2.0"}
        ])

      {:ok, _} = Installer.install(newer)

      rows = Map.new(Installer.list(), &{&1.id, &1})

      assert %{status: :pending, version: "0.2.0", loaded_version: "0.1.0"} =
               rows["flezha_example_plugin"]

      assert %{status: :removed, version: "0.1.0"} = rows["flz_b"]
      assert %{status: :pending, version: "0.1.0", loaded_version: nil} = rows["flz_new"]
      assert %{status: :refused, error: %{code: "plugin.runtime_mismatch"}} = rows["flz_refused"]
      assert rows["flz_new"].providers == ["flz_new_stamp"]
    end

    test "names why a provider a package declares is no row", %{root: root, tmp: tmp} do
      refused = sibling(tmp, "flz_refused", [{:manifest, &put_in(&1.built_with.otp, "0")}])
      PluginPackage.load!(root, [refused])
      {:ok, _} = Installer.install(sibling(tmp, "flz_later"))

      assert {:provider_unavailable, "flz_refused_stamp", "flz_refused", "refused",
              "plugin.runtime_mismatch"} = Installer.unavailable("flz_refused_stamp")

      assert {:provider_unavailable, "FlzLater.Provider", "flz_later", "pending", nil} =
               Installer.unavailable(FlzLater.Provider)

      assert Installer.unavailable("nobody_declares_this") == nil
    end
  end

  # One HTTPS origin: `/pkg.fpz` serves the archive, `/redirect` sends there,
  # `/big` serves two megabytes.
  defp serve_https(certfile, keyfile, archive) do
    {:ok, listen} =
      :ssl.listen(0,
        certs_keys: [%{certfile: certfile, keyfile: keyfile}],
        active: false,
        mode: :binary,
        reuseaddr: true
      )

    {:ok, {_, port}} = :ssl.sockname(listen)
    acceptor = spawn(fn -> accept_loop(listen, archive) end)

    {port,
     fn ->
       Process.exit(acceptor, :kill)
       :ssl.close(listen)
     end}
  end

  defp accept_loop(listen, archive) do
    with {:ok, socket} <- :ssl.transport_accept(listen) do
      server = spawn(fn -> receive(do: ({:serve, socket} -> serve(socket, archive))) end)
      :ok = :ssl.controlling_process(socket, server)
      send(server, {:serve, socket})
      accept_loop(listen, archive)
    end
  end

  defp serve(socket, archive) do
    with {:ok, socket} <- :ssl.handshake(socket),
         {:ok, request} <- read_request(socket, "") do
      [_method, path | _] = String.split(request, " ", parts: 3)

      response =
        case path do
          "/pkg.fpz" -> reply("200 OK", [], archive)
          "/redirect" -> reply("302 Found", [{"location", "/pkg.fpz"}], "")
          "/big" -> reply("200 OK", [], :binary.copy("x", 2_000_000))
          _ -> reply("404 Not Found", [], "")
        end

      :ssl.send(socket, response)
      :ssl.close(socket)
    end
  end

  defp read_request(socket, acc) do
    if String.contains?(acc, "\r\n\r\n") do
      {:ok, acc}
    else
      with {:ok, data} <- :ssl.recv(socket, 0, 5_000), do: read_request(socket, acc <> data)
    end
  end

  defp reply(status, headers, body) do
    head =
      Enum.map_join(
        [{"content-length", byte_size(body)}, {"connection", "close"} | headers],
        fn {k, v} -> "#{k}: #{v}\r\n" end
      )

    ["HTTP/1.1 ", status, "\r\n", head, "\r\n", body]
  end
end

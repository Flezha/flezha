defmodule Mix.Tasks.Flezha.Plugin.PackageTest do
  @moduledoc """
  "A plugin package SHALL be built by `mix flezha.plugin.package`": the example
  packaged, signed, and the task's four refusals, each on a copy of the example
  project edited in a temp directory. The copies compile into the repository's
  `_build` beside the example, so this module runs alone.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir], async: false

  alias Flezha.Plugin.Installer.Manifest
  alias Flezha.Plugin.Installer.Package
  alias Flezha.TestHelpers.PluginPackage

  @moduletag timeout: 300_000
  @example Path.expand("examples/elixir/plugin")
  @opts [max_package_bytes: 50_000_000, require_signature: false, trusted_keys: []]

  test "the example is packaged with its library and none of Flezha's closure", %{tmp: tmp} do
    archive = PluginPackage.example!()
    assert Path.basename(archive) == "flezha_example_plugin-0.1.0.fpz"

    {:ok, entries} = :erl_tar.table(String.to_charlist(archive), [:compressed])

    apps =
      for entry <- entries,
          ["lib", app | _] <- [Path.split(to_string(entry))],
          uniq: true,
          do: app

    assert Enum.sort(apps) == ["flezha_example_plugin-0.1.0", "stamp_lib-0.1.0"]

    {:ok, verified} = Package.verify(archive, Path.join(tmp, "x"), @opts)
    assert verified.manifest.built_with == Manifest.runtime()
    assert verified.manifest.providers == %{"example_stamp" => FlezhaExamplePlugin.Stamp}

    # The project's configuration of its library travels in the library's `.app`;
    # the plugin application's own `.app` is the one Mix built.
    assert app_env(Path.join(tmp, "x"), :stamp_lib) == [prefix: ">>"]

    assert app_env(Path.join(tmp, "x"), :flezha_example_plugin) == [
             flezha_providers: %{"example_stamp" => FlezhaExamplePlugin.Stamp}
           ]
  end

  test "a library setting an .app cannot hold fails the task, naming it", %{tmp: tmp} do
    dir = copy_project(tmp, "fun", & &1)
    configure(dir, "config :stamp_lib, render: fn text -> text end")

    {status, out, archive} = PluginPackage.package(dir)
    assert status != 0
    assert out =~ ":stamp_lib"
    assert out =~ ":render"
    assert archive == nil
  end

  test "a library setting beyond ASCII is written so the .app reads back", %{tmp: tmp} do
    path = Path.join(tmp, "stamp_lib.app")
    File.write!(path, ~s|{application,stamp_lib,[{vsn,"0.1.0"},{env,[]}]}.\n|)

    put = &Keyword.put(&1, :env, greeting: "café", mark: :"→")
    assert :ok = Mix.Tasks.Flezha.Plugin.Package.update_app(path, :stamp_lib, put)

    assert {:ok, [{:application, :stamp_lib, props}]} = :file.consult(String.to_charlist(path))
    assert props[:env] == [greeting: "café", mark: :"→"]
  end

  test "--sign writes a signature the matching public key verifies", %{tmp: tmp} do
    {:ECPrivateKey, _, _, curve, public, _} =
      key = :public_key.generate_key({:namedCurve, :ed25519})

    pem = Path.join(tmp, "key.pem")
    File.write!(pem, :public_key.pem_encode([:public_key.pem_entry_encode(:PrivateKeyInfo, key)]))

    public_pem =
      :public_key.pem_encode([
        :public_key.pem_entry_encode(:SubjectPublicKeyInfo, {{:ECPoint, public}, curve})
      ])

    assert {0, _out, archive} = PluginPackage.package(@example, ["--sign", pem])

    opts = Keyword.merge(@opts, require_signature: true, trusted_keys: [public_pem])
    assert {:ok, %{signed: true}} = Package.verify(archive, Path.join(tmp, "x"), opts)
    assert byte_size(File.read!(Path.join([tmp, "x", "signature.sig"]))) == 64
  end

  # Mix compiles a project's dependencies before it can run a task one of them
  # defines, so the pinned copy builds in a build root of its own, seeded from
  # the repository's so only what the pin touches compiles.
  test "a library pinned away from Flezha's lock is refused, naming both versions",
       %{tmp: tmp} do
    fake = Path.join(tmp, "fake_jason")
    File.cp_r!("deps/jason", fake)
    File.rm_rf!(Path.join(fake, "_build"))
    mix = Path.join(fake, "mix.exs")
    File.write!(mix, String.replace(File.read!(mix), ~r/@version "[^"]+"/, ~s|@version "9.9.9"|))

    build_root = Path.join(tmp, "build")
    File.mkdir_p!(build_root)
    File.cp_r!("_build/prod", Path.join(build_root, "prod"))

    dir =
      copy_project(tmp, "pin", fn mix ->
        String.replace(
          mix,
          "{:stamp_lib,",
          "{:jason, path: #{inspect(fake)}, override: true},\n      {:stamp_lib,"
        )
      end)

    {status, out, archive} = PluginPackage.package(dir, [], build_root)
    assert status != 0
    assert out =~ "jason: this project resolves 9.9.9, Flezha's mix.lock has 1."
    assert out =~ ~s|pin it with {:jason, "1.|
    assert archive == nil
  end

  test "a plugin application with a callback is refused", %{tmp: tmp} do
    dir =
      copy_project(
        tmp,
        "callback",
        &String.replace(&1, "[env: [", "[mod: {FlezhaExamplePlugin.Stamp, []}, env: [")
      )

    {status, out, archive} = PluginPackage.package(dir)
    assert status != 0
    assert out =~ "flz_copy_callback"
    assert out =~ "a plugin's processes are its providers' declared workers"
    assert archive == nil
  end

  test "a plugin application naming no providers is refused", %{tmp: tmp} do
    dir =
      copy_project(
        tmp,
        "providers",
        &String.replace(
          &1,
          ~s|[env: [flezha_providers: %{"example_stamp" => FlezhaExamplePlugin.Stamp}]]|,
          "[]"
        )
      )

    {status, out, archive} = PluginPackage.package(dir)
    assert status != 0
    assert out =~ ":flezha_providers"
    assert archive == nil
  end

  test "native code under priv is refused, naming the file", %{tmp: tmp} do
    dir = copy_project(tmp, "native", & &1)
    File.mkdir_p!(Path.join(dir, "priv"))
    File.write!(Path.join([dir, "priv", "x.so"]), "ELF")

    {status, out, archive} = PluginPackage.package(dir)
    assert status != 0
    assert out =~ "priv/x.so"
    assert out =~ "plugin.native_code"
    assert archive == nil
  end

  defp app_env(dir, app) do
    [path] = Path.wildcard(Path.join([dir, "lib", "#{app}-*", "ebin", "#{app}.app"]))
    {:ok, [{:application, ^app, props}]} = :file.consult(String.to_charlist(path))
    Enum.sort(props[:env])
  end

  defp configure(dir, line) do
    config = Path.join([dir, "config", "config.exs"])
    File.write!(config, File.read!(config) <> "\n" <> line <> "\n")
  end

  # The example project as a project of its own: absolute paths to this checkout's
  # deps and flezha, the example's library by path, a copy of the lockfile so the
  # repository's is never written, and an application name of its own so its
  # build does not replace the example's.
  defp copy_project(tmp, name, edit) do
    dir = Path.join(tmp, name)
    File.mkdir_p!(dir)

    for entry <- ["lib", "config"],
        do: File.cp_r!(Path.join(@example, entry), Path.join(dir, entry))

    File.cp!("mix.lock", Path.join(dir, "mix.lock"))
    on_exit(fn -> File.rm_rf!(Path.expand("_build/prod/lib/flz_copy_#{name}")) end)

    mix =
      @example
      |> Path.join("mix.exs")
      |> File.read!()
      |> String.replace(~s|Path.expand("../../../deps", __DIR__)|, inspect(Path.expand("deps")))
      |> String.replace(
        ~s|Path.expand("../../../mix.lock", __DIR__)|,
        ~s|Path.expand("mix.lock", __DIR__)|
      )
      |> String.replace(~s|Path.expand("../../..", __DIR__)|, inspect(File.cwd!()))
      |> String.replace(
        ~s|path: "stamp_lib"|,
        "path: #{inspect(Path.join(@example, "stamp_lib"))}"
      )
      |> String.replace("app: :flezha_example_plugin", "app: :flz_copy_#{name}")
      |> edit.()

    File.write!(Path.join(dir, "mix.exs"), mix)
    dir
  end
end

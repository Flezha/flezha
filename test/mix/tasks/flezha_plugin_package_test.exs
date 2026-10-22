defmodule Mix.Tasks.Flezha.Plugin.PackageTest do
  @moduledoc """
  "A plugin package SHALL be built by `mix flezha.plugin.package`": the example
  packaged by the task's staging, signed, and the task's refusals of a library
  setting, a provider entry and a pinned library. The refusals the staged
  package's own checks make are `Flezha.Plugin.Installer.PackageTest`'s.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir], async: true

  alias Flezha.Plugin.Installer.Manifest
  alias Flezha.Plugin.Installer.Package
  alias Flezha.TestHelpers.PluginPackage
  alias Mix.Tasks.Flezha.Plugin.Package, as: PackageTask

  @example "test/apps/example_plugin"
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
    path = Path.join(tmp, "stamp_lib.app")
    File.write!(path, ~s|{application,stamp_lib,[{vsn,"0.1.0"},{env,[]}]}.\n|)

    assert_raise Mix.Error, ~r/the setting :render of :stamp_lib cannot be written/, fn ->
      PackageTask.write_env!(:stamp_lib, [render: fn text -> text end], path)
    end
  end

  test "a library setting beyond ASCII is written so the .app reads back", %{tmp: tmp} do
    path = Path.join(tmp, "stamp_lib.app")
    File.write!(path, ~s|{application,stamp_lib,[{vsn,"0.1.0"},{env,[]}]}.\n|)

    put = &Keyword.put(&1, :env, greeting: "café", mark: :"→")
    assert :ok = PackageTask.update_app(path, :stamp_lib, put)

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

    archive = PluginPackage.package!(@example, Path.join(tmp, "out"), pem)

    opts = Keyword.merge(@opts, require_signature: true, trusted_keys: [public_pem])
    assert {:ok, %{signed: true}} = Package.verify(archive, Path.join(tmp, "x"), opts)
    assert byte_size(File.read!(Path.join([tmp, "x", "signature.sig"]))) == 64
  end

  test "a library pinned away from Flezha's lock is refused, naming both versions" do
    lock = Mix.Dep.Lock.read(Path.expand("mix.lock"))
    pinned = put_elem(lock.jason, 2, "9.9.9")

    assert :ok =
             PackageTask.check_locks!(lock, [
               %Mix.Dep{app: :jason, scm: Mix.SCM.Path},
               %Mix.Dep{app: :flz_unlocked, opts: [lock: pinned]}
             ])

    assert_raise Mix.Error,
                 ~r/jason: this project resolves 9\.9\.9, Flezha's mix\.lock has 1\..+pin it with \{:jason, "1\./,
                 fn ->
                   PackageTask.check_locks!(lock, [%Mix.Dep{app: :jason, opts: [lock: pinned]}])
                 end
  end

  test "a plugin application naming no providers, or not a provider module, is refused" do
    assert_raise Mix.Error, ~r/:flz_plugin names no providers.+:flezha_providers/, fn ->
      PackageTask.providers!(:flz_plugin, nil)
    end

    assert_raise Mix.Error, ~r/"x" => String of :flezha_providers is not a provider module/, fn ->
      PackageTask.providers!(:flz_plugin, %{"x" => String})
    end
  end

  defp app_env(dir, app) do
    [path] = Path.wildcard(Path.join([dir, "lib", "#{app}-*", "ebin", "#{app}.app"]))
    {:ok, [{:application, ^app, props}]} = :file.consult(String.to_charlist(path))
    Enum.sort(props[:env])
  end
end

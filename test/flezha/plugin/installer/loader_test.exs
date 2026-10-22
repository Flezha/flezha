defmodule Flezha.Plugin.Installer.LoaderTest do
  @moduledoc """
  The boot loader in this VM, against the example package and one-edit
  variants of it: what a loaded package holds, one case per refusal the loader
  produces, two packages sharing a library, and one package's refusal leaving
  the others loaded.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  @moduletag :capture_log

  import ExUnit.CaptureLog

  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Providers
  alias Flezha.TestHelpers.PluginPackage
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry

  setup %{tmp: tmp} do
    root = Path.join(tmp, "plugins")
    on_exit(fn -> PluginPackage.unload!(root) end)
    {:ok, root: root, example: PluginPackage.example!()}
  end

  defp edited(%{example: example, tmp: tmp}, name, edits),
    do: PluginPackage.rewrite(example, Path.join([tmp, "edited", name]), edits)

  defp refused_code([%{status: :refused, error: error}]), do: error.code

  defp configure(settings),
    do:
      Application.put_env(
        :flezha,
        :plugins,
        Keyword.merge(Application.get_env(:flezha, :plugins, []), settings)
      )

  test "a loaded package: modules, library started, plugin app loaded only, row and worker family",
       %{root: root, example: example} do
    assert [%{id: "flezha_example_plugin", status: :loaded, signed: false, error: nil} = outcome] =
             PluginPackage.load!(root, [example])

    assert outcome.providers == %{"example_stamp" => FlezhaExamplePlugin.Stamp}
    assert Loader.outcomes() == [outcome]

    assert {:module, FlezhaExamplePlugin.Stamp} = :code.ensure_loaded(FlezhaExamplePlugin.Stamp)
    started = Enum.map(Application.started_applications(), &elem(&1, 0))
    assert :stamp_lib in started
    refute :flezha_example_plugin in started
    assert List.keymember?(Application.loaded_applications(), :flezha_example_plugin, 0)

    assert Providers.fetch("example_stamp") == {:ok, FlezhaExamplePlugin.Stamp}
    assert FlezhaExamplePlugin.Stamp.WorkerSup in Providers.worker_families()

    assert to_string(:code.priv_dir(:stamp_lib)) =~
             Path.join([root, ".loaded", "flezha_example_plugin"])

    # The package carries the plugin project's configuration of its library.
    assert Application.get_env(:stamp_lib, :prefix) == ">>"
    stamp = apply(StampLib, :stamp, [])
    assert to_string(stamp) == ">>[stamped]"
    assert to_string(:a) == "a" and to_string(1) == "1"
  end

  test "every archive is verified again at boot: one changed on disk is refused", ctx do
    archive =
      edited(ctx, "tampered.fpz", [
        :keep_checksums,
        {:put, "lib/stamp_lib-0.1.0/ebin/Elixir.StampLib.beam", "tampered"}
      ])

    assert refused_code(PluginPackage.load!(ctx.root, [archive])) == "plugin.package_invalid"
    assert :code.is_loaded(StampLib) == false
  end

  test "a package built on another runtime is refused", ctx do
    archive = edited(ctx, "otp.fpz", [{:manifest, &put_in(&1.built_with.otp, "0")}])
    assert refused_code(PluginPackage.load!(ctx.root, [archive])) == "plugin.runtime_mismatch"
  end

  test "a package built on an older Elixir minor loads, logging one warning naming both versions",
       ctx do
    running = System.version()
    {:ok, %Version{major: major, minor: minor}} = Version.parse(running)
    built = "#{major}.#{minor - 1}.4"
    archive = edited(ctx, "elixir.fpz", [{:manifest, &put_in(&1.built_with.elixir, built)}])
    on_exit(NodeTestHelper.override_log_level(:warning))

    log =
      capture_log([level: :warning], fn ->
        assert [%{status: :loaded}] = PluginPackage.load!(ctx.root, [archive])
      end)

    warning =
      ~r/plugin package flezha_example_plugin was built on Elixir (\S+) and loads on Elixir (\S+);/

    assert [[_, ^built, ^running]] = Regex.scan(warning, log)
  end

  test "a bundled library at another version than the release's is refused, naming the release",
       ctx do
    spec = {:application, :jason, [vsn: ~c"0.0.1", modules: []]}

    archive =
      edited(ctx, "jason.fpz", [
        {:put, "lib/jason-0.0.1/ebin/jason.app", :io_lib.format(~c"~p.~n", [spec])},
        {:manifest, &%{&1 | apps: &1.apps ++ [{:jason, "0.0.1"}]}}
      ])

    assert [%{status: :refused, error: error}] = PluginPackage.load!(ctx.root, [archive])
    assert error.code == "plugin.library_conflict"
    assert error.details["application"] == "jason"
    assert error.details["holder"] == "the release"
    assert :code.is_loaded(StampLib) == false
  end

  test "a module the release holds is refused and stays the release's", ctx do
    held = Flezha.Plugin.Provider.HttpPush
    before = :code.which(held)

    archive =
      edited(ctx, "http_push.fpz", [{:beam, :stamp_lib, held, PluginPackage.beam(held)}])

    assert [%{error: error}] = PluginPackage.load!(ctx.root, [archive])
    assert error.code == "plugin.module_conflict"
    assert error.details["holder"] == "the release"
    assert :code.which(held) == before
  end

  test "a provider id that is already a row is refused, and the row stays first-party", ctx do
    archive =
      edited(ctx, "stdout.fpz", [
        {:manifest, &%{&1 | providers: %{"relay_stdout" => FlezhaExamplePlugin.Stamp}}}
      ])

    assert refused_code(PluginPackage.load!(ctx.root, [archive])) == "plugin.provider_conflict"
    assert Providers.fetch("relay_stdout") == {:ok, Flezha.Plugin.Provider.StdOut}
    refute List.keymember?(Application.started_applications(), :stamp_lib, 0)
  end

  test "two archives declaring one id are both refused, naming both files", ctx do
    a = Path.join([ctx.tmp, "dupes", "a.fpz"])
    b = Path.join([ctx.tmp, "dupes", "b.fpz"])
    File.mkdir_p!(Path.dirname(a))
    File.cp!(ctx.example, a)
    File.cp!(ctx.example, b)

    assert [%{error: e1}, %{error: e2}] = PluginPackage.load!(ctx.root, [a, b])
    assert e1.code == "plugin.duplicate_id" and e2.code == "plugin.duplicate_id"
    assert e1.details["files"] == ["a.fpz", "b.fpz"]
    assert Providers.fetch("example_stamp") == :error
  end

  test "loading is all or nothing: a set atomic_load refuses loads no module", ctx do
    dup = PluginPackage.beam(:flz_dup)

    archive =
      edited(ctx, "dup.fpz", [
        {:beam, :stamp_lib, :flz_dup, dup},
        {:beam, :flezha_example_plugin, :flz_dup, dup}
      ])

    assert refused_code(PluginPackage.load!(ctx.root, [archive])) == "plugin.load_failed"
    assert :code.is_loaded(StampLib) == false
    assert :code.is_loaded(:flz_dup) == false
    refute Enum.any?(:code.get_path(), &List.starts_with?(&1, String.to_charlist(ctx.root)))
    refute List.keymember?(Application.loaded_applications(), :stamp_lib, 0)
  end

  test "a library whose start fails refuses its package and leaves no row", ctx do
    archive =
      edited(ctx, "start.fpz", [
        {:beam, :stamp_lib, :flz_bad_start,
         PluginPackage.beam(:flz_bad_start, start_error: :boom)},
        {:app, :stamp_lib, &Keyword.put(&1, :mod, {:flz_bad_start, []})}
      ])

    assert [%{error: error}] = PluginPackage.load!(ctx.root, [archive])
    assert error.code == "plugin.load_failed"
    assert error.details["what"] == "application stamp_lib"
    assert Providers.fetch("example_stamp") == :error
    refute List.keymember?(Application.started_applications(), :stamp_lib, 0)
  end

  test "the signature policy is applied at boot", ctx do
    {:ECPrivateKey, _, _, curve, public, _} = :public_key.generate_key({:namedCurve, :ed25519})

    trusted =
      :public_key.pem_encode([
        :public_key.pem_entry_encode(:SubjectPublicKeyInfo, {{:ECPoint, public}, curve})
      ])

    forged = edited(ctx, "forged.fpz", [{:put, "signature.sig", :binary.copy(<<0>>, 64)}])

    configure(trusted_keys: [trusted], require_signature: false)
    assert refused_code(PluginPackage.load!(ctx.root, [forged])) == "plugin.signature_invalid"
    PluginPackage.unload!(ctx.root)
    File.rm_rf!(ctx.root)

    configure(trusted_keys: [trusted], require_signature: true)

    assert refused_code(PluginPackage.load!(ctx.root, [ctx.example])) ==
             "plugin.signature_required"

    File.rm_rf!(ctx.root)

    configure(trusted_keys: [], require_signature: true)

    assert refused_code(PluginPackage.load!(ctx.root, [ctx.example])) ==
             "plugin.trust_store_empty"
  end

  test "two packages bundling one library at one version both load, the library once", ctx do
    second = PluginPackage.sibling(Path.join(ctx.tmp, "flz_second.fpz"), "flz_second")

    assert [%{id: "flezha_example_plugin", status: :loaded}, %{id: "flz_second", status: :loaded}] =
             PluginPackage.load!(ctx.root, [second, ctx.example])

    assert to_string(:code.which(StampLib)) =~ "/.loaded/flezha_example_plugin/"
    assert Providers.fetch("flz_second_stamp") == {:ok, FlzSecond.Provider}
  end

  test "one package's refusal leaves the others loaded, and the boot announces both", ctx do
    second =
      PluginPackage.sibling(Path.join(ctx.tmp, "flz_second.fpz"), "flz_second", [
        {:manifest, &put_in(&1.built_with.otp, "0")}
      ])

    File.mkdir_p!(Path.join([ctx.root, "installed", "old"]))
    File.write!(Path.join(ctx.root, "installed_plugins.dets"), "leftover")

    assert [%{status: :loaded}, %{id: "flz_second", status: :refused}] =
             PluginPackage.load!(ctx.root, [ctx.example, second])

    ref = make_ref()

    Telemetry.capture([[:flezha, :plugin_package, :loaded], [:flezha, :plugin_package, :refused]],
      as: fn event, _m, meta -> {ref, event, meta} end
    )

    :ok = Loader.announce()

    assert_received {^ref, [:flezha, :plugin_package, :loaded],
                     %{id: "flezha_example_plugin", signed: false}}

    assert_received {^ref, [:flezha, :plugin_package, :refused],
                     %{id: "flz_second", code: "plugin.runtime_mismatch"}}
  end

  test "a refusal wraps an uncatalogued reason of a loaded package's provider", ctx do
    PluginPackage.load!(ctx.root, [ctx.example])

    assert {:provider_refused, "flezha_example_plugin", "example_stamp", ":stamp_path_empty"} =
             Loader.refusal(:provider_refused, FlezhaExamplePlugin.Stamp, :stamp_path_empty)

    catalogued = {:unknown_provider, "x"}
    assert Loader.refusal(:provider_failed, FlezhaExamplePlugin.Stamp, catalogued) == catalogued

    # A bare atom that shares a catalogued tag is not the provider's own code.
    assert {:provider_refused, "flezha_example_plugin", "example_stamp", ":closed"} =
             Loader.refusal(:provider_refused, FlezhaExamplePlugin.Stamp, :closed)

    partitioned = {:partitioned_host, %{provider: "example_stamp", relay: "pg#p0"}}

    assert Loader.refusal(:provider_refused, FlezhaExamplePlugin.Stamp, partitioned) ==
             partitioned

    # A worker that stopped at start with a detailed reason keeps that code; a
    # bare term that shares a catalogued tag is wrapped.
    detailed = {:http_listen_failed, %{relay: "op", port: 1}}

    assert Loader.refusal(
             :provider_failed,
             FlezhaExamplePlugin.Stamp,
             {:worker_start_failed, "k", detailed}
           ) == detailed

    assert {:provider_failed, "flezha_example_plugin", "example_stamp", _} =
             Loader.refusal(
               :provider_failed,
               FlezhaExamplePlugin.Stamp,
               {:worker_start_failed, "k", :closed}
             )

    assert {:provider_failed, "flezha_example_plugin", "example_stamp", _} =
             Loader.refusal(
               :provider_failed,
               FlezhaExamplePlugin.Stamp,
               {:worker_start_failed, "k", :odd}
             )

    assert Loader.refusal(:provider_refused, Flezha.Plugin.Provider.StdOut, :odd) == :odd
  end
end

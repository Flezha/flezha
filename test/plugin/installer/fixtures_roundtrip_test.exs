defmodule Flezha.Plugin.Installer.FixturesRoundtripTest do
  @moduledoc """
  Closes the loop between `test/fixtures/plugins/build_fixtures.exs` and
  the real installer: every canonical fixture is run through
  `Flezha.Plugin.Installer.install/2` (or `Package.extract/2` / manifest
  parsing, depending on which pipeline stage the fixture is meant to
  exercise) and the expected error shape is asserted.

  Without this test the fixture script could drift from the installer's
  validation rules — CI would stay green even if every fixture silently
  stopped exercising its intended path.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Installer
  alias Flezha.Plugin.Installer.Loader
  alias Flezha.Plugin.Installer.Package
  alias Flezha.Plugin.Installer.Store
  alias Flezha.TestHelpers.Plugins, as: TH

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_fixroundtrip_#{System.unique_integer([:positive])}")
    fixtures_dir = Path.join(tmp, "fixtures")
    plugin_root = Path.join(tmp, "plugins")
    File.mkdir_p!(fixtures_dir)
    File.mkdir_p!(plugin_root)

    prev_data_dir = Application.get_env(:flezha, :recovery_data_dir)
    prev_pi = Application.get_env(:flezha, :plugin_installer)

    Application.put_env(:flezha, :recovery_data_dir, tmp)

    Application.put_env(:flezha, :plugin_installer,
      plugin_root: plugin_root,
      trusted_keys: [],
      require_signature: false,
      allow_unsigned: true,
      max_package_bytes: 50_000_000,
      max_source_files: 200
    )

    stop_if_running(Installer)
    stop_if_running(Loader)
    stop_if_running(Store)

    {:ok, _} = Store.start_link([])
    {:ok, _} = Loader.start_link([])
    {:ok, _} = Installer.start_link([])

    TH.build_all_fixtures!(fixtures_dir)

    on_exit(fn ->
      stop_if_running(Installer)
      stop_if_running(Loader)
      stop_if_running(Store)

      if prev_data_dir do
        Application.put_env(:flezha, :recovery_data_dir, prev_data_dir)
      else
        Application.delete_env(:flezha, :recovery_data_dir)
      end

      if prev_pi do
        Application.put_env(:flezha, :plugin_installer, prev_pi)
      else
        Application.delete_env(:flezha, :plugin_installer)
      end

      File.rm_rf!(tmp)
    end)

    {:ok, tmp: tmp, fixtures_dir: fixtures_dir}
  end

  test "valid_one.fpz installs cleanly", %{fixtures_dir: f} do
    assert {:ok, installed} = Installer.install(Path.join(f, "valid_one.fpz"))
    assert installed.id == "valid_one"
    assert installed.version == "1.0.0"
    assert installed.status == :disabled
  end

  test "common.fpz + with_dep.fpz resolve the dependency", %{fixtures_dir: f} do
    assert {:ok, _} = Installer.install(Path.join(f, "common.fpz"))
    assert {:ok, installed} = Installer.install(Path.join(f, "with_dep.fpz"))
    assert installed.id == "with_dep"
  end

  test "with_dep.fpz rejects missing common.fpz", %{fixtures_dir: f} do
    assert {:error, {:dependency_missing, "common", ">= 1.0.0"}} =
             Installer.install(Path.join(f, "with_dep.fpz"))
  end

  test "cycle_a + cycle_b fixtures both fail with missing-dep when installed in isolation", %{fixtures_dir: f} do
    # A true dependency cycle cannot occur through install-only because the
    # first install always hits :dependency_missing before the second can
    # land (proper cycle-detection is exercised against the resolver
    # directly in `dependency_resolver_test.exs`). Drive the install-flow
    # behaviour here: neither fixture can land without the other first.
    assert {:error, {:dependency_missing, "cycle_a", ">= 0.0.0"}} =
             Installer.install(Path.join(f, "cycle_b.fpz"))

    assert {:error, {:dependency_missing, "cycle_b", ">= 0.0.0"}} =
             Installer.install(Path.join(f, "cycle_a.fpz"))
  end

  test "tampered.fpz is rejected at checksum step", %{fixtures_dir: f} do
    assert {:error, {:checksum_mismatch, _}} =
             Installer.install(Path.join(f, "tampered.fpz"))
  end

  test "missing_manifest.fpz fails layout validation", %{fixtures_dir: f} do
    assert {:error, {:invalid_layout, :manifest_missing}} =
             Installer.install(Path.join(f, "missing_manifest.fpz"))
  end

  test "compile_error.fpz fails at compile step", %{fixtures_dir: f} do
    assert {:error, {:compile_failed, _file, _reason}} =
             Installer.install(Path.join(f, "compile_error.fpz"))
  end

  test "module_mismatch.fpz fails with :module_mismatch", %{fixtures_dir: f} do
    assert {:error, {:manifest_invalid, {:module_mismatch, _info}}} =
             Installer.install(Path.join(f, "module_mismatch.fpz"))
  end

  test "module_collision.fpz rejects clash with stdlib before compiling", %{fixtures_dir: f} do
    # The collision check runs BEFORE Compiler.compile/2 so the installer
    # never calls `Code.compile_file/1` on a source that would shadow a
    # stdlib or release module — otherwise the post-compile purge could
    # unload the original module from the VM.
    assert {:error, {:module_collision, Enum}} =
             Installer.install(Path.join(f, "module_collision.fpz"))
  end

  test "oversize.fpz is rejected when max_package_bytes is tight", %{fixtures_dir: f} do
    # Re-configure to a small ceiling and restart the Installer so it
    # picks the new config up.
    Application.put_env(:flezha, :plugin_installer,
      Keyword.merge(Application.get_env(:flezha, :plugin_installer, []),
        max_package_bytes: 1_000_000
      )
    )

    assert {:error, {:package_too_large, _size, 1_000_000}} =
             Installer.install(Path.join(f, "oversize.fpz"))
  end

  test "signed.fpz verifies with paired public key", %{fixtures_dir: f} do
    pub = File.read!(Path.join(f, "signed.pub"))

    Application.put_env(:flezha, :plugin_installer,
      Keyword.merge(Application.get_env(:flezha, :plugin_installer, []),
        trusted_keys: [pub],
        require_signature: true,
        allow_unsigned: false
      )
    )

    assert {:ok, installed} = Installer.install(Path.join(f, "signed.fpz"))
    assert installed.id == "signed"
  end

  test "unsigned_required.fpz rejected when require_signature is on", %{fixtures_dir: f} do
    Application.put_env(:flezha, :plugin_installer,
      Keyword.merge(Application.get_env(:flezha, :plugin_installer, []),
        require_signature: true,
        allow_unsigned: false
      )
    )

    assert {:error, :signature_required} =
             Installer.install(Path.join(f, "unsigned_required.fpz"))
  end

  test "Package.extract still works against every fixture file type", %{fixtures_dir: f} do
    # Sanity that every canonical fixture is at least a valid gzip tar
    # (except the obviously-malformed ones). We walk every `.fpz` and
    # confirm we can list its table without crashing.
    Path.wildcard(Path.join(f, "*.fpz"))
    |> Enum.each(fn fpz ->
      assert {:ok, _entries} =
               :erl_tar.table(String.to_charlist(fpz), [:compressed, :verbose]),
             "fixture #{Path.basename(fpz)} is not a valid .fpz tarball"
    end)

    # `Package.extract/3` is called everywhere — spot-check valid_one
    # extracts into a clean staging dir.
    staging = Path.join(f, "staging_check")

    assert :ok =
             Package.extract(
               Path.join(f, "valid_one.fpz"),
               staging,
               max_package_bytes: 50_000_000,
               max_source_files: 200
             )

    assert File.exists?(Path.join(staging, "manifest.exs"))
  end

  defp stop_if_running(mod) do
    try do
      case Process.whereis(mod) do
        nil -> :ok
        pid -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
      end
    catch
      :exit, _ -> :ok
    end
  end
end

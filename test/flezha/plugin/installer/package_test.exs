defmodule Flezha.Plugin.Installer.PackageTest do
  @moduledoc """
  "Plugin package format": the archive's entries, its `lib/` layout and the
  native code it may not carry, each refusal one edit of the example package.
  """

  use FlezhaTest.Case, fixtures: [:tmp_dir], async: true

  alias Flezha.Plugin.Installer.Package
  alias Flezha.TestHelpers.PluginPackage

  @opts [max_package_bytes: 50_000_000, require_signature: false, trusted_keys: []]

  setup %{tmp: tmp} do
    {:ok, example: PluginPackage.example!(), dir: Path.join(tmp, "x")}
  end

  defp verify(archive, dir, opts \\ @opts), do: Package.verify(archive, dir, opts)

  defp rewrite(%{example: example, tmp: tmp}, edits),
    do: PluginPackage.rewrite(example, Path.join(tmp, "edited.fpz"), edits)

  test "a well-formed package passes the format checks", %{example: example, dir: dir} do
    assert {:ok, verified} = verify(example, dir)
    assert verified.manifest.id == "flezha_example_plugin"
    assert Enum.map(verified.apps, & &1.name) == [:flezha_example_plugin, :stamp_lib]
    assert verified.signed == false
    assert verified.digest == Package.digest(example)
  end

  test "a source file is refused, naming it", %{dir: dir} = ctx do
    archive = rewrite(ctx, [{:put, "src/plugin.ex", "defmodule X do end"}])

    assert {:error, {:package_invalid, :source_file, %{entry: "src/plugin.ex"}}} =
             verify(archive, dir)

    archive = rewrite(ctx, [{:put, "lib/stamp_lib-0.1.0/priv/x.ex", ""}])
    assert {:error, {:package_invalid, :source_file, _}} = verify(archive, Path.join(dir, "2"))
  end

  test "a missing manifest or checksum file is refused", %{dir: dir} = ctx do
    archive = rewrite(ctx, [{:delete, "manifest.exs"}, :keep_checksums])
    assert {:error, {:package_invalid, :manifest_missing, _}} = verify(archive, dir)

    archive = rewrite(ctx, [{:delete, "CHECKSUMS.sha256"}, :keep_checksums])

    assert {:error, {:package_invalid, :checksums_missing, _}} =
             verify(archive, Path.join(dir, "2"))
  end

  test "an unknown top-level entry is refused, naming it", %{dir: dir} = ctx do
    archive = rewrite(ctx, [{:put, "README", "hi"}])

    assert {:error, {:package_invalid, :unexpected_entry, %{entry: "README"}}} =
             verify(archive, dir)
  end

  test "a symbolic link is refused", %{tmp: tmp, dir: dir} do
    src = Path.join(tmp, "link_src")
    File.mkdir_p!(src)
    File.ln_s!("/etc/passwd", Path.join(src, "manifest.exs"))
    archive = Path.join(tmp, "link.fpz")
    :ok = Package.create(src, archive)

    assert {:error, {:package_invalid, :unsafe_entry, %{type: :symlink}}} = verify(archive, dir)
  end

  test "native code is refused: a shared library under priv or a module with on_load",
       %{dir: dir} = ctx do
    archive = rewrite(ctx, [{:put, "lib/stamp_lib-0.1.0/priv/nif.so", "ELF"}])

    assert {:error, {:native_code, "lib/stamp_lib-0.1.0/priv/nif.so"}} = verify(archive, dir)

    archive =
      rewrite(ctx, [
        {:beam, :stamp_lib, :flz_native, PluginPackage.beam(:flz_native, on_load: true)}
      ])

    assert {:error, {:native_code, "flz_native"}} = verify(archive, Path.join(dir, "2"))
  end

  test "beams and the modules list must agree", %{dir: dir} = ctx do
    archive =
      rewrite(ctx, [
        {:put, "lib/stamp_lib-0.1.0/ebin/flz_stray.beam", PluginPackage.beam(:flz_stray)}
      ])

    assert {:error, {:package_invalid, :beam_not_listed, %{module: "flz_stray"}}} =
             verify(archive, dir)

    archive =
      rewrite(ctx, [
        {:app, :stamp_lib, &Keyword.update!(&1, :modules, fn m -> [:flz_gone | m] end)}
      ])

    assert {:error, {:package_invalid, :module_without_beam, %{module: "flz_gone"}}} =
             verify(archive, Path.join(dir, "2"))
  end

  test "an app directory whose .app names another version is refused", %{dir: dir} = ctx do
    archive = rewrite(ctx, [{:app, :stamp_lib, &Keyword.put(&1, :vsn, ~c"9.9.9")}])
    assert {:error, {:package_invalid, :app_mismatch, _}} = verify(archive, dir)
  end

  test "an application that depends on flezha may declare no callback", %{dir: dir} = ctx do
    archive =
      rewrite(ctx, [{:app, :flezha_example_plugin, &Keyword.put(&1, :mod, {:flz_cb, []})}])

    assert {:error, {:app_callback, :flezha_example_plugin}} = verify(archive, dir)
  end

  test "an oversized package is refused before anything is extracted", %{
    example: example,
    dir: dir
  } do
    assert {:error, {:package_invalid, :too_large, %{bytes: bytes, limit: 1_000}}} =
             verify(example, dir, Keyword.put(@opts, :max_package_bytes, 1_000))

    assert bytes > 1_000
    refute File.exists?(dir)
  end

  test "an archive that is not a gzip tar is refused", %{tmp: tmp, dir: dir} do
    archive = Path.join(tmp, "junk.fpz")
    File.write!(archive, "not a tar")
    assert {:error, {:package_invalid, :unreadable, _}} = verify(archive, dir)
  end
end

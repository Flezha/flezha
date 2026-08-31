defmodule Flezha.Plugin.Installer.PackageTest do
  @moduledoc """
  Spec-driven tests for `Flezha.Plugin.Installer.Package`. Each scenario in
  the spec's `Requirement: Plugin package format` maps to a test here.
  Fixtures are built in-test via `:erl_tar` so every test owns its archive.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Installer.Package

  @opts [max_package_bytes: 50_000_000, max_source_files: 200]

  setup do
    tmp = tmp!("flz_pkg_test_")
    on_exit(fn -> File.rm_rf!(tmp) end)
    {:ok, tmp: tmp}
  end

  test "well-formed package extracts and validates", %{tmp: tmp} do
    fpz = Path.join(tmp, "valid.fpz")
    staging = Path.join(tmp, "staging")

    write_fpz!(fpz, [
      {"manifest.exs", "%{id: \"x\"}"},
      {"src/x.ex", "defmodule X do end"},
      {"CHECKSUMS.sha256", "deadbeef  src/x.ex\n"}
    ])

    assert :ok = Package.extract(fpz, staging, @opts)
    assert :ok = Package.validate_layout(staging)
    assert File.exists?(Path.join(staging, "manifest.exs"))
    assert File.exists?(Path.join(staging, "src/x.ex"))
  end

  test "missing manifest rejected", %{tmp: tmp} do
    fpz = Path.join(tmp, "no_manifest.fpz")
    staging = Path.join(tmp, "staging")

    write_fpz!(fpz, [
      {"src/x.ex", "defmodule X do end"},
      {"CHECKSUMS.sha256", "deadbeef  src/x.ex\n"}
    ])

    :ok = Package.extract(fpz, staging, @opts)
    assert {:error, {:invalid_layout, :manifest_missing}} = Package.validate_layout(staging)
  end

  test "missing checksums rejected", %{tmp: tmp} do
    fpz = Path.join(tmp, "no_checksums.fpz")
    staging = Path.join(tmp, "staging")

    write_fpz!(fpz, [
      {"manifest.exs", "%{id: \"x\"}"},
      {"src/x.ex", "defmodule X do end"}
    ])

    :ok = Package.extract(fpz, staging, @opts)
    assert {:error, {:invalid_layout, :checksums_missing}} = Package.validate_layout(staging)
  end

  test "missing src directory rejected", %{tmp: tmp} do
    fpz = Path.join(tmp, "no_src.fpz")
    staging = Path.join(tmp, "staging")

    write_fpz!(fpz, [
      {"manifest.exs", "%{id: \"x\"}"},
      {"CHECKSUMS.sha256", ""}
    ])

    :ok = Package.extract(fpz, staging, @opts)
    assert {:error, {:invalid_layout, :src_missing}} = Package.validate_layout(staging)
  end

  test "rogue .beam file rejected at extract time", %{tmp: tmp} do
    fpz = Path.join(tmp, "beam.fpz")
    staging = Path.join(tmp, "staging")

    write_fpz!(fpz, [
      {"manifest.exs", "%{}"},
      {"src/x.ex", "defmodule X do end"},
      {"src/evil.beam", "precompiled"},
      {"CHECKSUMS.sha256", ""}
    ])

    assert {:error, {:invalid_layout, :beam_forbidden}} = Package.extract(fpz, staging, @opts)
  end

  test "path traversal rejected", %{tmp: tmp} do
    fpz = Path.join(tmp, "traversal.fpz")
    staging = Path.join(tmp, "staging")

    write_fpz!(fpz, [
      {"manifest.exs", "%{}"},
      {"../escape.ex", "oops"},
      {"CHECKSUMS.sha256", ""}
    ])

    assert {:error, {:unsafe_entry, _, :path_traversal}} = Package.extract(fpz, staging, @opts)
  end

  test "oversize package rejected", %{tmp: tmp} do
    fpz = Path.join(tmp, "big.fpz")
    staging = Path.join(tmp, "staging")

    big = :binary.copy("a", 2_000_000)

    write_fpz!(fpz, [
      {"manifest.exs", "%{}"},
      {"src/big.ex", big},
      {"CHECKSUMS.sha256", ""}
    ])

    tiny_opts = [max_package_bytes: 1_000_000, max_source_files: 200]

    assert {:error, {:package_too_large, _size, 1_000_000}} =
             Package.extract(fpz, staging, tiny_opts)
  end

  test "too many source files rejected", %{tmp: tmp} do
    fpz = Path.join(tmp, "many.fpz")
    staging = Path.join(tmp, "staging")

    files =
      for i <- 1..10 do
        {"src/f#{i}.ex", "defmodule F#{i} do end"}
      end

    write_fpz!(fpz, [{"manifest.exs", "%{}"} | files] ++ [{"CHECKSUMS.sha256", ""}])

    tiny_opts = [max_package_bytes: 50_000_000, max_source_files: 5]

    assert {:error, {:too_many_source_files, 10, 5}} =
             Package.extract(fpz, staging, tiny_opts)
  end

  test "unexpected top-level entry rejected by validate_layout", %{tmp: tmp} do
    fpz = Path.join(tmp, "extra.fpz")
    staging = Path.join(tmp, "staging")

    write_fpz!(fpz, [
      {"manifest.exs", "%{}"},
      {"src/x.ex", "defmodule X do end"},
      {"CHECKSUMS.sha256", ""},
      {"README.md", "hi"}
    ])

    :ok = Package.extract(fpz, staging, @opts)

    assert {:error, {:invalid_layout, {:unexpected_entry, "README.md"}}} =
             Package.validate_layout(staging)
  end

  test "non-source file under src rejected", %{tmp: tmp} do
    fpz = Path.join(tmp, "nonsource.fpz")
    staging = Path.join(tmp, "staging")

    write_fpz!(fpz, [
      {"manifest.exs", "%{}"},
      {"src/x.ex", "defmodule X do end"},
      {"src/README", "hi"},
      {"CHECKSUMS.sha256", ""}
    ])

    :ok = Package.extract(fpz, staging, @opts)

    assert {:error, {:invalid_layout, {:unexpected_source, _}}} =
             Package.validate_layout(staging)
  end

  ## Helpers

  defp write_fpz!(fpz_path, entries) do
    :ok = File.mkdir_p!(Path.dirname(fpz_path))

    tar_entries =
      Enum.map(entries, fn {name, content} ->
        {String.to_charlist(name), content}
      end)

    :ok =
      :erl_tar.create(
        String.to_charlist(fpz_path),
        tar_entries,
        [:compressed]
      )
  end

  defp tmp!(prefix) do
    path = Path.join(System.tmp_dir!(), "#{prefix}#{System.unique_integer([:positive])}")
    File.mkdir_p!(path)
    path
  end
end

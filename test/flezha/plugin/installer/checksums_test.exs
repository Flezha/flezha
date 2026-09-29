defmodule Flezha.Plugin.Installer.ChecksumsTest do
  @moduledoc "\"SHA-256 checksum verification\" over the example package's `lib/`."

  use FlezhaTest.Case, fixtures: [:tmp_dir], async: true

  alias Flezha.Plugin.Installer.Checksums
  alias Flezha.Plugin.Installer.Package
  alias Flezha.TestHelpers.PluginPackage

  @opts [max_package_bytes: 50_000_000, require_signature: false, trusted_keys: []]
  @beam "lib/stamp_lib-0.1.0/ebin/Elixir.StampLib.beam"

  setup %{tmp: tmp} do
    {:ok, example: PluginPackage.example!(), dir: Path.join(tmp, "x")}
  end

  defp verify_edited(%{example: example, tmp: tmp, dir: dir}, edits) do
    example
    |> PluginPackage.rewrite(Path.join(tmp, "edited.fpz"), [:keep_checksums | edits])
    |> Package.verify(dir, @opts)
  end

  test "every file matching its line proceeds", %{example: example, dir: dir} do
    assert {:ok, _} = Package.verify(example, dir, @opts)
    assert :ok = Checksums.check(dir)
  end

  test "a beam changed after the checksums were written is refused, naming it", ctx do
    assert {:error, {:package_invalid, :checksum_mismatch, %{path: @beam}}} =
             verify_edited(ctx, [{:put, @beam, "tampered"}])
  end

  test "a line naming a file the archive lacks is refused, naming it", ctx do
    assert {:error, {:package_invalid, :checksum_file_missing, %{path: @beam}}} =
             verify_edited(ctx, [{:delete, @beam}])
  end

  test "a file under lib/ with no line is refused, naming it", ctx do
    extra = "lib/stamp_lib-0.1.0/priv/extra.txt"

    assert {:error, {:package_invalid, :checksum_line_missing, %{path: ^extra}}} =
             verify_edited(ctx, [{:put, extra, "x"}])
  end

  test "a line naming a path outside the package is refused" do
    assert {:error, {:package_invalid, :unsafe_entry, _}} =
             Checksums.verify([%{digest: String.duplicate("0", 64), path: "../x"}], "/tmp")
  end

  test "a malformed line is refused with its number" do
    assert {:error, {:package_invalid, :checksums_malformed, %{line: 2}}} =
             Checksums.parse("# comment\nnot-a-digest  manifest.exs\n")
  end
end

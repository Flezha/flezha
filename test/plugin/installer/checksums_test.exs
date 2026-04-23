defmodule Flezha.Plugin.Installer.ChecksumsTest do
  @moduledoc """
  Spec-driven tests for `Flezha.Plugin.Installer.Checksums`.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Installer.Checksums

  setup do
    tmp =
      Path.join(
        System.tmp_dir!(),
        "flz_checksums_test_#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(Path.join(tmp, "src"))
    on_exit(fn -> File.rm_rf!(tmp) end)
    {:ok, tmp: tmp}
  end

  test "parse tolerates blank lines and comments" do
    body = """

    # a comment
    #{String.duplicate("a", 64)}  manifest.exs

    #{String.duplicate("b", 64)}  src/x.ex
    """

    assert {:ok, entries} = Checksums.parse(body)
    assert length(entries) == 2
    assert Enum.at(entries, 0).path == "manifest.exs"
    assert Enum.at(entries, 1).path == "src/x.ex"
  end

  test "parse rejects malformed line" do
    body = "no_hex_here  manifest.exs\n"
    assert {:error, {:malformed_line, 1, _}} = Checksums.parse(body)
  end

  test "verify passes when every digest matches", %{tmp: tmp} do
    File.write!(Path.join(tmp, "manifest.exs"), "m")
    File.write!(Path.join(tmp, "src/x.ex"), "x")

    entries = [
      %{digest: sha256("m"), path: "manifest.exs"},
      %{digest: sha256("x"), path: "src/x.ex"}
    ]

    assert :ok = Checksums.verify(entries, tmp)
  end

  test "tampered source detected", %{tmp: tmp} do
    File.write!(Path.join(tmp, "manifest.exs"), "m")
    File.write!(Path.join(tmp, "src/x.ex"), "tampered")

    entries = [
      %{digest: sha256("m"), path: "manifest.exs"},
      %{digest: sha256("original"), path: "src/x.ex"}
    ]

    assert {:error, {:checksum_mismatch, "src/x.ex"}} = Checksums.verify(entries, tmp)
  end

  test "missing referenced file reported", %{tmp: tmp} do
    File.write!(Path.join(tmp, "manifest.exs"), "m")

    entries = [
      %{digest: sha256("m"), path: "manifest.exs"},
      %{digest: sha256("x"), path: "src/ghost.ex"}
    ]

    assert {:error, {:checksum_file_missing, "src/ghost.ex"}} =
             Checksums.verify(entries, tmp)
  end

  test "extra source file not covered by checksums rejected", %{tmp: tmp} do
    File.write!(Path.join(tmp, "manifest.exs"), "m")
    File.write!(Path.join(tmp, "src/x.ex"), "x")
    File.write!(Path.join(tmp, "src/extra.ex"), "e")

    entries = [
      %{digest: sha256("m"), path: "manifest.exs"},
      %{digest: sha256("x"), path: "src/x.ex"}
    ]

    assert {:error, {:checksum_line_missing, "src/extra.ex"}} =
             Checksums.verify(entries, tmp)
  end

  defp sha256(binary), do: :crypto.hash(:sha256, binary) |> Base.encode16(case: :lower)
end

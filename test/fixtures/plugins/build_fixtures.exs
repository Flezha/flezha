# Builds canonical `.fpz` fixtures used across installer tests.
#
# Usage (from a test helper):
#   Flezha.TestHelpers.Plugins.build_all_fixtures!(out_dir)
#
# Direct CLI use (manual debugging):
#   mix run test/fixtures/plugins/build_fixtures.exs /tmp/fpz_out
#
# The script writes a deterministic set of `.fpz` archives into `out_dir`:
#   valid_one.fpz            — minimal valid package
#   with_dep.fpz             — depends on common.fpz
#   common.fpz               — the dep target
#   cycle_a.fpz, cycle_b.fpz — circular dep pair
#   tampered.fpz             — source bytes changed after CHECKSUMS
#   missing_manifest.fpz     — no manifest.exs
#   compile_error.fpz        — src has a syntax error
#   module_mismatch.fpz      — manifest modules don't match compile output
#   unsigned_required.fpz    — valid package with no signature.sig
#   signed.fpz + test_key    — signed package + paired private key
#   module_collision.fpz     — declares a module already in the release (Enum)
#   oversize.fpz             — ~2MB of source
#
# All archives are rebuilt every run; callers should treat `out_dir` as
# disposable.

defmodule BuildFixtures do
  @moduledoc false

  def build_all!(out_dir) do
    File.mkdir_p!(out_dir)

    minimal(out_dir, "valid_one", "FlzFixture.Valid")
    minimal(out_dir, "common", "FlzFixture.Common")
    with_dep(out_dir, "with_dep", "FlzFixture.WithDep")
    cycle(out_dir)
    tampered(out_dir)
    missing_manifest(out_dir)
    compile_error(out_dir)
    module_mismatch(out_dir)
    unsigned_required(out_dir)
    signed(out_dir)
    module_collision(out_dir)
    oversize(out_dir)

    :ok
  end

  ## Scenarios

  defp minimal(out, id, mod) do
    manifest = manifest(id, "1.0.0", [mod])
    src = trivial_module(mod)
    fpz = Path.join(out, "#{id}.fpz")
    write(fpz, manifest, [{"src/#{mod_filename(mod)}", src}])
  end

  defp with_dep(out, id, mod) do
    manifest = manifest(id, "1.0.0", [mod], [{"common", ">= 1.0.0"}])
    src = trivial_module(mod)
    fpz = Path.join(out, "#{id}.fpz")
    write(fpz, manifest, [{"src/#{mod_filename(mod)}", src}])
  end

  defp cycle(out) do
    ma = manifest("cycle_a", "1.0.0", ["FlzFixture.CycleA"], [{"cycle_b", ">= 0.0.0"}])
    mb = manifest("cycle_b", "1.0.0", ["FlzFixture.CycleB"], [{"cycle_a", ">= 0.0.0"}])

    write(Path.join(out, "cycle_a.fpz"), ma, [
      {"src/flz_fixture_cycle_a.ex", trivial_module("FlzFixture.CycleA")}
    ])

    write(Path.join(out, "cycle_b.fpz"), mb, [
      {"src/flz_fixture_cycle_b.ex", trivial_module("FlzFixture.CycleB")}
    ])
  end

  defp tampered(out) do
    mod = "FlzFixture.Tampered"
    id = "tampered"
    manifest = manifest(id, "1.0.0", [mod])
    src_correct = trivial_module(mod)
    src_filename = "src/#{mod_filename(mod)}"
    checksums = checksums_for(manifest, [{src_filename, src_correct}])

    # Write the archive with TAMPERED src bytes (different hash).
    fpz = Path.join(out, "#{id}.fpz")

    entries = [
      {"manifest.exs", manifest},
      {src_filename, src_correct <> "\n# tampered"},
      {"CHECKSUMS.sha256", checksums}
    ]

    create_tarball(fpz, entries)
  end

  defp missing_manifest(out) do
    fpz = Path.join(out, "missing_manifest.fpz")

    entries = [
      {"src/x.ex", trivial_module("FlzFixture.MissingManifest")},
      {"CHECKSUMS.sha256", ""}
    ]

    create_tarball(fpz, entries)
  end

  defp compile_error(out) do
    mod = "FlzFixture.CompileError"
    id = "compile_error"
    manifest = manifest(id, "1.0.0", [mod])

    src = "defmodule FlzFixture.CompileError do\n  def broken do\n    no\n"

    fpz = Path.join(out, "#{id}.fpz")

    entries = [
      {"manifest.exs", manifest},
      {"src/#{mod_filename(mod)}", src},
      {"CHECKSUMS.sha256", checksums_for(manifest, [{"src/#{mod_filename(mod)}", src}])}
    ]

    create_tarball(fpz, entries)
  end

  defp module_mismatch(out) do
    mod = "FlzFixture.ModuleMismatch"
    id = "module_mismatch"

    # Manifest declares TWO modules; only ONE exists in src.
    manifest =
      manifest(id, "1.0.0", [mod, "FlzFixture.ModuleMismatch.Missing"])

    src = trivial_module(mod)

    entries = [
      {"manifest.exs", manifest},
      {"src/#{mod_filename(mod)}", src},
      {"CHECKSUMS.sha256", checksums_for(manifest, [{"src/#{mod_filename(mod)}", src}])}
    ]

    create_tarball(Path.join(out, "#{id}.fpz"), entries)
  end

  defp unsigned_required(out) do
    mod = "FlzFixture.UnsignedRequired"
    id = "unsigned_required"
    minimal(out, id, mod)
    # alias: same as valid_one.fpz shape but renamed for clarity in the test.
    File.cp!(Path.join(out, "#{id}.fpz"), Path.join(out, "unsigned_required.fpz"))
  end

  defp signed(out) do
    mod = "FlzFixture.Signed"
    id = "signed"
    manifest = manifest(id, "1.0.0", [mod])
    src = trivial_module(mod)
    src_filename = "src/#{mod_filename(mod)}"
    checksums = checksums_for(manifest, [{src_filename, src}])

    {pub, priv} = :crypto.generate_key(:eddsa, :ed25519)
    sig = :crypto.sign(:eddsa, :none, checksums, [priv, :ed25519])

    entries = [
      {"manifest.exs", manifest},
      {src_filename, src},
      {"CHECKSUMS.sha256", checksums},
      {"signature.sig", sig}
    ]

    create_tarball(Path.join(out, "#{id}.fpz"), entries)

    # Write the paired public key in raw form so tests can load it.
    File.write!(Path.join(out, "signed.pub"), pub)
  end

  defp module_collision(out) do
    id = "module_collision"
    # Manifest declares Enum — a stdlib module already loaded in the VM.
    manifest = manifest(id, "1.0.0", ["Enum"])
    src = "defmodule Enum do\n  def hi, do: :ok\nend"

    entries = [
      {"manifest.exs", manifest},
      {"src/enum.ex", src},
      {"CHECKSUMS.sha256", checksums_for(manifest, [{"src/enum.ex", src}])}
    ]

    create_tarball(Path.join(out, "#{id}.fpz"), entries)
  end

  defp oversize(out) do
    mod = "FlzFixture.Oversize"
    id = "oversize"
    big_blob = String.duplicate("# pad\n", 500_000)
    src = trivial_module(mod) <> "\n" <> big_blob
    manifest = manifest(id, "1.0.0", [mod])

    entries = [
      {"manifest.exs", manifest},
      {"src/#{mod_filename(mod)}", src},
      {"CHECKSUMS.sha256", checksums_for(manifest, [{"src/#{mod_filename(mod)}", src}])}
    ]

    create_tarball(Path.join(out, "#{id}.fpz"), entries)
  end

  ## Helpers

  defp manifest(id, version, mods, deps \\ []) do
    mod_list = mods |> Enum.join(", ")

    """
    %{
      id: "#{id}",
      version: "#{version}",
      description: "#{id} fixture",
      author: "fixture",
      modules: [#{mod_list}],
      provides: [
        %{
          id: "main",
          provider: #{hd(mods)},
          subscriptions: [{:on_input, []}],
          default_opts: %{},
          scope: :processor
        }
      ],
      dependencies: #{inspect(deps)}
    }
    """
  end

  defp trivial_module(mod), do: "defmodule #{mod} do\n  def hi, do: :ok\nend\n"

  defp mod_filename(mod), do: Macro.underscore(mod) <> ".ex"

  defp checksums_for(manifest, src_entries) do
    base = [{"manifest.exs", manifest}] ++ src_entries

    base
    |> Enum.map(fn {path, body} ->
      "#{sha256(body)}  #{path}"
    end)
    |> Enum.join("\n")
    |> Kernel.<>("\n")
  end

  defp sha256(bin), do: :crypto.hash(:sha256, bin) |> Base.encode16(case: :lower)

  defp write(fpz_path, manifest, src_entries) do
    checksums = checksums_for(manifest, src_entries)

    entries =
      [{"manifest.exs", manifest}] ++
        src_entries ++
        [{"CHECKSUMS.sha256", checksums}]

    create_tarball(fpz_path, entries)
  end

  defp create_tarball(fpz_path, entries) do
    File.mkdir_p!(Path.dirname(fpz_path))

    :ok =
      :erl_tar.create(
        String.to_charlist(fpz_path),
        Enum.map(entries, fn {name, body} -> {String.to_charlist(name), body} end),
        [:compressed]
      )
  end
end

case System.argv() do
  [out] ->
    BuildFixtures.build_all!(out)
    IO.puts("fixtures built under #{out}")

  _ ->
    # No-op when loaded as a script for helper usage.
    :ok
end

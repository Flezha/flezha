defmodule Flezha.Boot.StatePathMigratorTest do
  use ExUnit.Case, async: false

  alias Flezha.Boot.StatePathMigrator
  alias Flezha.State.Registry

  setup do
    base =
      Path.join([
        System.tmp_dir!(),
        "flezha_state_migrator_test_" <>
          Integer.to_string(System.unique_integer([:positive]))
      ])

    File.mkdir_p!(base)
    Application.put_env(:flezha, :state_root, base)

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base}
  end

  test "migrates legacy runner-keyed layout to flat layout", %{base: base} do
    legacy = Path.join([base, "ce-aaaa1111", "MyOperative", "procedure-0"])
    File.mkdir_p!(legacy)
    File.write!(Path.join(legacy, "p1.wal"), "wal-bytes")
    File.write!(Path.join(legacy, "p1.snap.5"), "snap-bytes")

    result = StatePathMigrator.migrate()

    assert result.renamed == 2
    assert result.collisions == 0

    flat = Path.join([base, "MyOperative", "procedure-0"])
    assert File.read!(Path.join(flat, "p1.wal")) == "wal-bytes"
    assert File.read!(Path.join(flat, "p1.snap.5")) == "snap-bytes"
    refute File.dir?(Path.join(base, "ce-aaaa1111"))
  end

  test "is idempotent on second run after the layout is already flat", %{base: base} do
    flat_dir = Path.join([base, "MyOperative", "procedure-0"])
    File.mkdir_p!(flat_dir)
    File.write!(Path.join(flat_dir, "p1.wal"), "wal-bytes")

    result = StatePathMigrator.migrate()

    assert result.renamed == 0
    assert result.collisions == 0
    assert File.read!(Path.join(flat_dir, "p1.wal")) == "wal-bytes"
  end

  test "preserves the loser file on collision", %{base: base} do
    legacy_a = Path.join([base, "ce-aaaa1111", "MyOperative", "procedure-0"])
    legacy_b = Path.join([base, "ce-bbbb2222", "MyOperative", "procedure-0"])
    File.mkdir_p!(legacy_a)
    File.mkdir_p!(legacy_b)

    File.write!(Path.join(legacy_a, "p1.wal"), "older")
    File.touch!(Path.join(legacy_a, "p1.wal"), {{2020, 1, 1}, {0, 0, 0}})
    File.write!(Path.join(legacy_b, "p1.wal"), "newer")
    File.touch!(Path.join(legacy_b, "p1.wal"), {{2025, 1, 1}, {0, 0, 0}})

    result = StatePathMigrator.migrate()

    assert result.collisions == 1
    flat_path = Path.join([base, "MyOperative", "procedure-0", "p1.wal"])
    assert File.read!(flat_path) == "newer"
    assert Enum.count(result.preserved_files) == 1
    [preserved] = result.preserved_files
    assert File.read!(preserved) == "older"
  end

  test "no-op when state root does not exist" do
    Application.put_env(:flezha, :state_root, "/nonexistent/path/should/not/exist/flz")
    result = StatePathMigrator.migrate()
    assert result.renamed == 0
  end

  test "leaves existing flat directories untouched", %{base: base} do
    legacy = Path.join([base, "ce-aaaa1111", "OperativeA", "procedure-0"])
    flat = Path.join([base, "OperativeB", "procedure-0"])
    File.mkdir_p!(legacy)
    File.mkdir_p!(flat)
    File.write!(Path.join(legacy, "x.wal"), "x")
    File.write!(Path.join(flat, "y.wal"), "y")

    StatePathMigrator.migrate()

    assert File.read!(Path.join(flat, "y.wal")) == "y"
    assert File.read!(Path.join([base, "OperativeA", "procedure-0", "x.wal"])) == "x"
  end

  test "Registry.scan_dir reads the migrated layout", %{base: base} do
    legacy = Path.join([base, "ce-aaaa1111", "MyOperative", "procedure-0"])
    File.mkdir_p!(legacy)
    File.write!(Path.join(legacy, "p1.wal"), "wal-bytes")

    StatePathMigrator.migrate()

    %{wal_files: wals} = Registry.scan_dir({"MyOperative", "procedure-0", ""})
    assert [{"p1", _path}] = wals
  end
end

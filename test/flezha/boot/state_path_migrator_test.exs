defmodule Flezha.Boot.StatePathMigratorTest do
  # async: false: writes Application env; a shared on-disk root.
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

  test "migrates legacy runner-keyed layout under the _system namespace", %{base: base} do
    legacy = Path.join([base, "ce-aaaa1111", "MyOperative", "procedure-0"])
    File.mkdir_p!(legacy)
    File.write!(Path.join(legacy, "p1.wal"), "wal-bytes")
    File.write!(Path.join(legacy, "p1.snap.5"), "snap-bytes")

    result = StatePathMigrator.migrate()

    # Two file renames (pass 1) + one class-dir lift under _system (pass 2).
    assert result.renamed == 3
    assert result.collisions == 0

    migrated = Path.join([base, "_system", "MyOperative", "procedure-0"])
    assert File.read!(Path.join(migrated, "p1.wal")) == "wal-bytes"
    assert File.read!(Path.join(migrated, "p1.snap.5")) == "snap-bytes"
    # Built by hand on purpose: `<root>/<class>` is the PRE-migration layout,
    # so its absence is the thing under test and `class_dir/1` cannot name it.
    refute File.dir?(Path.join(base, "ce-aaaa1111"))
    refute File.dir?(Path.join(base, "MyOperative"))
  end

  test "moves a flat class dir under _system, then the marker makes re-runs no-ops", %{
    base: base
  } do
    flat_dir = Path.join([base, "MyOperative", "procedure-0"])
    File.mkdir_p!(flat_dir)
    File.write!(Path.join(flat_dir, "p1.wal"), "wal-bytes")

    result = StatePathMigrator.migrate()
    assert result.renamed == 1

    migrated = Path.join([base, "_system", "MyOperative", "procedure-0", "p1.wal"])
    assert File.read!(migrated) == "wal-bytes"
    assert File.exists?(Path.join(base, ".layout"))

    # Idempotent: the second run must not mistake `_system` (or any namespace
    # dir) for a legacy layout — structurally they are indistinguishable, so
    # the marker is the discriminator.
    second = StatePathMigrator.migrate()
    assert second.renamed == 0
    assert File.read!(migrated) == "wal-bytes"
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
    migrated = Path.join([base, "_system", "MyOperative", "procedure-0", "p1.wal"])
    assert File.read!(migrated) == "newer"

    # The preserved sibling travelled with its class dir into `_system`, and
    # the reported path follows the move.
    assert [preserved] = result.preserved_files
    assert preserved =~ "/_system/"
    assert File.read!(preserved) == "older"
  end

  test "no-op when the state root cannot be created" do
    Application.put_env(:flezha, :state_root, "/nonexistent/path/should/not/exist/flz")
    result = StatePathMigrator.migrate()
    assert result.renamed == 0
  end

  test "a fresh root is created already stamped with the namespaced layout" do
    fresh =
      Path.join([
        System.tmp_dir!(),
        "flezha_fresh_root_#{System.unique_integer([:positive])}"
      ])

    Application.put_env(:flezha, :state_root, fresh)
    on_exit(fn -> File.rm_rf!(fresh) end)

    result = StatePathMigrator.migrate()

    assert result.renamed == 0
    assert File.exists?(Path.join(fresh, ".layout"))
  end

  test "an interrupted namespace pass finishes on the next run", %{base: base} do
    # Simulate a crash after one class moved but before the marker landed.
    File.mkdir_p!(Path.join([base, "_system", "AlreadyMoved", "procedure-0"]))
    File.mkdir_p!(Path.join([base, "NotYetMoved", "procedure-0"]))
    File.write!(Path.join([base, "NotYetMoved", "procedure-0", "p1.wal"]), "wal")

    result = StatePathMigrator.migrate()

    assert result.renamed == 1
    assert File.dir?(Path.join([base, "_system", "AlreadyMoved"]))

    assert File.read!(Path.join([base, "_system", "NotYetMoved", "procedure-0", "p1.wal"])) ==
             "wal"
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

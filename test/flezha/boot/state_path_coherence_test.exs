defmodule Flezha.Boot.StatePathCoherenceTest do
  @moduledoc """
  State-path coherence (self-healing-control-plane, Phase 9 / D8;
  finish-platform-authn-authz §5 moved the layout under a namespace level).

  The boot migrator and the reader must agree on one on-disk layout so a
  controller restart never orphans WAL/snapshot files: paths the writer
  resolves before a restart must be exactly the paths the reader resolves
  after `migrate/0`, and `FLEZHA_STATE_DIR` must be the single source for
  both the resolver and the migrator.
  """
  # async: false: writes Application env; a shared on-disk root; OS environment variables.
  use ExUnit.Case, async: false

  alias Flezha.Boot.StatePathMigrator
  alias Flezha.State.Paths
  alias Flezha.State.TestHelpers

  setup do
    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      System.delete_env("FLEZHA_STATE_DIR")
    end)

    :ok
  end

  test "migrate/0 leaves resolver-written WAL in place and the reader finds it" do
    root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf!(root) end)
    pid = TestHelpers.sample_entity_id()

    # Written through the resolver — which places it under the `_system`
    # namespace level for an unclaimed class.
    wal = Paths.wal_path(pid)
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, TestHelpers.build_wal_record(1, "change"))

    # Simulate the boot-time migration that runs before the State supervisor.
    _ = StatePathMigrator.migrate()

    assert Paths.state_root() == root
    assert File.exists?(wal), "migrate/0 orphaned the resolver-written WAL"
    assert pid in Paths.all_entities()
  end

  test "FLEZHA_STATE_DIR is the single source for both resolver and migrator" do
    # With :state_root unset, the resolver falls back to FLEZHA_STATE_DIR.
    Application.delete_env(:flezha, :state_root)

    dir =
      Path.join([
        System.tmp_dir!(),
        "flezha_state_dir_#{System.unique_integer([:positive])}"
      ])

    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    System.put_env("FLEZHA_STATE_DIR", dir)

    assert Paths.state_root() == dir

    pid = TestHelpers.sample_entity_id("envdir")
    wal = Paths.wal_path(pid)
    assert String.starts_with?(wal, dir)

    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, TestHelpers.build_wal_record(1, "change"))

    # The migrator resolves the same root, so the WAL is found after "restart".
    _ = StatePathMigrator.migrate()
    assert File.exists?(wal)
    assert pid in Paths.all_entities()
  end
end

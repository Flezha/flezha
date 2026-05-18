defmodule Flezha.Boot.StatePathMigratorNamespaceTest do
  @moduledoc """
  Tests the `migrate_to_namespaced_layout/1` pass added by the
  platform-authn-authz change. Verifies the legacy
  `app_data/state/{relay_class}/...` tree moves under
  `app_data/state/_system/{relay_class}/...` and that a re-run is a
  no-op (idempotency).
  """

  use ExUnit.Case, async: false

  alias Flezha.Boot.StatePathMigrator

  setup do
    tmp = Path.join(System.tmp_dir!(), "flz_migrator_ns_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)

    on_exit(fn -> File.rm_rf!(tmp) end)

    {:ok, root: tmp}
  end

  test "moves legacy {relay_class}/{shard}/... under _system/", %{root: root} do
    seed_legacy_tree(root, ["orders", "payments"])

    :ok = StatePathMigrator.migrate_to_namespaced_layout(root)

    refute File.dir?(Path.join(root, "orders"))
    refute File.dir?(Path.join(root, "payments"))
    assert File.dir?(Path.join([root, "_system", "orders", "shard-0"]))
    assert File.dir?(Path.join([root, "_system", "payments", "shard-0"]))

    assert File.read!(Path.join([root, "_system", "orders", "shard-0", "k.wal"])) == "orders-wal"
  end

  test "is idempotent — second run is a no-op", %{root: root} do
    seed_legacy_tree(root, ["orders"])

    :ok = StatePathMigrator.migrate_to_namespaced_layout(root)
    first_listing = listing(root)

    :ok = StatePathMigrator.migrate_to_namespaced_layout(root)
    second_listing = listing(root)

    assert first_listing == second_listing
  end

  test "skips when _system/<rc> already exists (operator collision)", %{root: root} do
    seed_legacy_tree(root, ["orders"])
    File.mkdir_p!(Path.join([root, "_system", "orders"]))

    :ok = StatePathMigrator.migrate_to_namespaced_layout(root)

    # Legacy dir was not moved (collision skip), so it's still present.
    assert File.dir?(Path.join(root, "orders"))
  end

  test "leaves an already-migrated tree alone", %{root: root} do
    File.mkdir_p!(Path.join([root, "_system", "orders", "shard-0"]))

    :ok = StatePathMigrator.migrate_to_namespaced_layout(root)
    assert File.dir?(Path.join([root, "_system", "orders", "shard-0"]))
  end

  defp seed_legacy_tree(root, relay_classes) do
    Enum.each(relay_classes, fn rc ->
      shard_dir = Path.join([root, rc, "shard-0"])
      File.mkdir_p!(shard_dir)
      File.write!(Path.join(shard_dir, "k.wal"), "#{rc}-wal")
    end)
  end

  defp listing(root) do
    File.cd!(root, fn -> Path.wildcard("**", match_dot: false) end)
    |> Enum.sort()
  end
end

defmodule Flezha.State.RegistryNamespaceTest do
  @moduledoc """
  finish-platform-authn-authz §5: durable state lives under
  `<state_root>/<namespace>/<class>/`, resolved through the class→namespace
  claim an Operative records when it binds — the entity_id stays the wire's
  3-tuple, and isolation holds at the path seam.
  """
  # async: false: writes Application env; a shared on-disk root.
  use ExUnit.Case, async: false

  alias Flezha.State.Registry

  setup do
    base =
      Path.join([
        System.tmp_dir!(),
        "flz_registry_ns_#{System.unique_integer([:positive])}"
      ])

    File.mkdir_p!(base)
    Application.put_env(:flezha, :state_root, base)
    Registry.ensure_claims_table()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base}
  end

  test "a claimed class resolves every path under its namespace", %{base: base} do
    assert :ok = Registry.claim_namespace("orders-op", "team-orders")

    assert Registry.namespace_of_class("orders-op") == "team-orders"
    assert Registry.class_dir("orders-op") == Path.join([base, "team-orders", "orders-op"])

    assert Registry.wal_path({"orders-op", "proc-0", "k1"}) ==
             Path.join([base, "team-orders", "orders-op", "proc-0", "k1.wal"])

    assert Registry.input_log_path("orders-op") ==
             Path.join([base, "team-orders", "orders-op", "input.log"])
  end

  test "a write through the registry paths restores through the same resolution", %{base: base} do
    assert :ok = Registry.claim_namespace("orders-op", "team-orders")

    entity_id = {"orders-op", "proc-0", "k1"}
    wal = Registry.wal_path(entity_id)
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, "wal-bytes")

    # The file landed under the namespace prefix...
    assert File.read!(Path.join([base, "team-orders", "orders-op", "proc-0", "k1.wal"])) ==
             "wal-bytes"

    # ...and every disk-driven reader resolves the same entity.
    assert %{wal_files: [{"k1", ^wal}]} = Registry.scan_dir({"orders-op", "proc-0", ""})
    assert Registry.entities_of("orders-op") == [entity_id]
    assert "orders-op" in Registry.operative_classes()
  end

  test "an unclaimed class resolves from the on-disk layout, else _system", %{base: base} do
    File.mkdir_p!(Path.join([base, "team-a", "disk-op", "proc-0"]))

    assert Registry.namespace_of_class("disk-op") == "team-a"
    assert Registry.namespace_of_class("never-seen-op") == "_system"
  end

  test "a real namespace adopts a class migrated under _system", %{base: base} do
    legacy = Path.join([base, "_system", "adopted-op", "proc-0"])
    File.mkdir_p!(legacy)
    File.write!(Path.join(legacy, "k1.wal"), "legacy-bytes")

    assert :ok = Registry.claim_namespace("adopted-op", "default")

    assert File.read!(Path.join([base, "default", "adopted-op", "proc-0", "k1.wal"])) ==
             "legacy-bytes"

    # Built by hand on purpose: this module's subject IS the layout, so
    # deriving the path would make the assertion follow the resolution it pins.
    refute File.dir?(Path.join([base, "_system", "adopted-op"]))
    assert Registry.namespace_of_class("adopted-op") == "default"
  end

  test "a class owned by another namespace is refused, not hijacked", %{base: base} do
    File.mkdir_p!(Path.join([base, "team-a", "contested-op", "proc-0"]))

    assert {:error, {:namespace_conflict, "contested-op", "team-a"}} =
             Registry.claim_namespace("contested-op", "team-b")

    # The rightful owner still claims fine.
    assert :ok = Registry.claim_namespace("contested-op", "team-a")
  end

  test "deleting the last entity releases the claim", %{base: base} do
    assert :ok = Registry.claim_namespace("gone-op", "team-a")

    entity_id = {"gone-op", "proc-0", "k1"}
    wal = Registry.wal_path(entity_id)
    File.mkdir_p!(Path.dirname(wal))
    File.write!(wal, "bytes")

    :ok = Registry.delete_files(entity_id)

    # Hand-built for the same reason as the adoption case above.
    refute File.dir?(Path.join([base, "team-a", "gone-op"]))
    # Released: another namespace may now claim the name.
    assert :ok = Registry.claim_namespace("gone-op", "team-b")
  end
end

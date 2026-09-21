defmodule Flezha.State.PathsTest do
  @moduledoc """
  Where durable state lives: how the root is chosen, and how a class resolves to
  `<state_root>/<namespace>/<class>/` through the claim an Operative records
  when it binds. The cell_id stays the wire's 3-tuple; isolation holds at the
  path seam.
  """
  # async: false: writes Application env; a shared on-disk root; OS environment variables.
  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.State.Paths

  describe "state_root/0" do
    setup do
      Application.delete_env(:flezha, :state_root)
      :ok
    end

    # fails when: the controller ignores the configured root and silently
    # writes/restores the per-Cell WAL under the default `app_data/state`, so
    # a deployment that mounts a dedicated state volume warm-restarts from an
    # empty tree. The variable that fills the key at boot is the runtime
    # drill's; this is the read every site makes afterwards.
    test "the configured root is what every site reads" do
      Application.put_env(:flezha, :state_root, "/tmp/app-config-root")
      assert Paths.state_root() == "/tmp/app-config-root"
    end

    test "falls back to the declared default, resolved against the working directory" do
      # The row's default is relative and stays relative; what every site reads
      # is that default expanded, once, here. A site reading the relative string
      # would name a different tree from every working directory than the
      # advisory lock does.
      assert Flezha.Config.default(:state_root) == "app_data/state"
      assert Paths.state_root() == Path.expand("app_data/state")
      assert Path.type(Paths.state_root()) == :absolute
    end

    test "a relative configured root is resolved too" do
      Application.put_env(:flezha, :state_root, "app_data/elsewhere")

      assert Paths.state_root() == Path.expand("app_data/elsewhere")
      assert String.starts_with?(Paths.wal_path({"c", "p", "k"}), Paths.state_root())
    end
  end

  describe "namespace resolution" do
    setup do
      context = %{tmp: base} = FlezhaTest.Case.install([:tmp_dir])
      Application.put_env(:flezha, :state_root, base)
      Paths.ensure_claims_table()

      context
    end

    test "a claimed class resolves every path under its namespace", %{tmp: base} do
      assert :ok = Paths.claim_namespace("orders-op", "team-orders")

      assert Paths.namespace_of_class("orders-op") == "team-orders"
      assert Paths.class_dir("orders-op") == Path.join([base, "team-orders", "orders-op"])

      assert Paths.wal_path({"orders-op", "task-0", "k1"}) ==
               Path.join([base, "team-orders", "orders-op", "task-0", "k1.wal"])

      assert Paths.input_log_path("orders-op") ==
               Path.join([base, "team-orders", "orders-op", "input.log"])
    end

    test "a write through the registry paths restores through the same resolution", %{tmp: base} do
      assert :ok = Paths.claim_namespace("orders-op", "team-orders")

      cell_id = {"orders-op", "task-0", "k1"}
      wal = Paths.wal_path(cell_id)
      File.mkdir_p!(Path.dirname(wal))
      File.write!(wal, "wal-bytes")

      # The file landed under the namespace prefix...
      assert File.read!(Path.join([base, "team-orders", "orders-op", "task-0", "k1.wal"])) ==
               "wal-bytes"

      # ...and every disk-driven reader resolves the same cell.
      assert %{wal_files: [{"k1", ^wal}]} = Paths.scan_dir({"orders-op", "task-0", ""})
      assert Paths.cells_of("orders-op") == [cell_id]
      assert "orders-op" in Paths.operative_classes()
    end

    test "an unclaimed class resolves from the on-disk layout, else _system", %{tmp: base} do
      File.mkdir_p!(Path.join([base, "team-a", "disk-op", "task-0"]))

      assert Paths.namespace_of_class("disk-op") == "team-a"
      assert Paths.namespace_of_class("never-seen-op") == "_system"
    end

    test "a real namespace adopts a class migrated under _system", %{tmp: base} do
      legacy = Path.join([base, "_system", "adopted-op", "task-0"])
      File.mkdir_p!(legacy)
      File.write!(Path.join(legacy, "k1.wal"), "legacy-bytes")

      assert :ok = Paths.claim_namespace("adopted-op", "default")

      assert File.read!(Path.join([base, "default", "adopted-op", "task-0", "k1.wal"])) ==
               "legacy-bytes"

      # Built by hand on purpose: this module's subject IS the layout, so
      # deriving the path would make the assertion follow the resolution it pins.
      refute File.dir?(Path.join([base, "_system", "adopted-op"]))
      assert Paths.namespace_of_class("adopted-op") == "default"
    end

    test "a class owned by another namespace is refused, not hijacked", %{tmp: base} do
      File.mkdir_p!(Path.join([base, "team-a", "contested-op", "task-0"]))

      assert {:error, {:namespace_conflict, "contested-op", "team-a"}} =
               Paths.claim_namespace("contested-op", "team-b")

      # The rightful owner still claims fine.
      assert :ok = Paths.claim_namespace("contested-op", "team-a")
    end

    test "deleting the last cell releases the claim", %{tmp: base} do
      assert :ok = Paths.claim_namespace("gone-op", "team-a")

      cell_id = {"gone-op", "task-0", "k1"}
      wal = Paths.wal_path(cell_id)
      File.mkdir_p!(Path.dirname(wal))
      File.write!(wal, "bytes")

      :ok = Paths.delete_files(cell_id)

      # Hand-built for the same reason as the adoption case above.
      refute File.dir?(Path.join([base, "team-a", "gone-op"]))
      # Released: another namespace may now claim the name.
      assert :ok = Paths.claim_namespace("gone-op", "team-b")
    end
  end
end

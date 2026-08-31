defmodule Flezha.State.PathsTest do
  @moduledoc """
  Where durable state lives: how the root is chosen, and how a class resolves to
  `<state_root>/<namespace>/<class>/` through the claim an Operative records
  when it binds. The entity_id stays the wire's 3-tuple; isolation holds at the
  path seam.
  """
  # async: false: writes Application env; a shared on-disk root; OS environment variables.
  use ExUnit.Case, async: false

  alias Flezha.State.Paths

  describe "state_root/0" do
    setup do
      prev_env = System.get_env("FLEZHA_STATE_DIR")
      prev_app = Application.fetch_env(:flezha, :state_root)
      Application.delete_env(:flezha, :state_root)

      on_exit(fn ->
        case prev_app do
          {:ok, v} -> Application.put_env(:flezha, :state_root, v)
          :error -> Application.delete_env(:flezha, :state_root)
        end

        case prev_env do
          nil -> System.delete_env("FLEZHA_STATE_DIR")
          v -> System.put_env("FLEZHA_STATE_DIR", v)
        end
      end)

      :ok
    end

    # fails when: the controller ignores the operator-configured FLEZHA_STATE_DIR
    # and silently writes/restores the per-Entity WAL under the default
    # app_data/state, so a deployment that mounts a dedicated state volume warm-
    # restarts from an empty tree.
    test "honors FLEZHA_STATE_DIR when no explicit :state_root is set" do
      System.put_env("FLEZHA_STATE_DIR", "/tmp/flezha-state-test-root")
      assert Paths.state_root() == "/tmp/flezha-state-test-root"
    end

    test "explicit :state_root application config takes precedence over FLEZHA_STATE_DIR" do
      System.put_env("FLEZHA_STATE_DIR", "/tmp/env-root")
      Application.put_env(:flezha, :state_root, "/tmp/app-config-root")
      assert Paths.state_root() == "/tmp/app-config-root"
    end

    test "falls back to app_data/state when neither is configured" do
      System.delete_env("FLEZHA_STATE_DIR")
      assert Paths.state_root() == "app_data/state"
    end
  end

  describe "namespace resolution" do
    setup do
      base =
        Path.join([
          System.tmp_dir!(),
          "flz_registry_ns_#{System.unique_integer([:positive])}"
        ])

      File.mkdir_p!(base)
      Application.put_env(:flezha, :state_root, base)
      Paths.ensure_claims_table()

      on_exit(fn ->
        Application.delete_env(:flezha, :state_root)
        File.rm_rf!(base)
      end)

      %{base: base}
    end

    test "a claimed class resolves every path under its namespace", %{base: base} do
      assert :ok = Paths.claim_namespace("orders-op", "team-orders")

      assert Paths.namespace_of_class("orders-op") == "team-orders"
      assert Paths.class_dir("orders-op") == Path.join([base, "team-orders", "orders-op"])

      assert Paths.wal_path({"orders-op", "proc-0", "k1"}) ==
               Path.join([base, "team-orders", "orders-op", "proc-0", "k1.wal"])

      assert Paths.input_log_path("orders-op") ==
               Path.join([base, "team-orders", "orders-op", "input.log"])
    end

    test "a write through the registry paths restores through the same resolution", %{base: base} do
      assert :ok = Paths.claim_namespace("orders-op", "team-orders")

      entity_id = {"orders-op", "proc-0", "k1"}
      wal = Paths.wal_path(entity_id)
      File.mkdir_p!(Path.dirname(wal))
      File.write!(wal, "wal-bytes")

      # The file landed under the namespace prefix...
      assert File.read!(Path.join([base, "team-orders", "orders-op", "proc-0", "k1.wal"])) ==
               "wal-bytes"

      # ...and every disk-driven reader resolves the same entity.
      assert %{wal_files: [{"k1", ^wal}]} = Paths.scan_dir({"orders-op", "proc-0", ""})
      assert Paths.entities_of("orders-op") == [entity_id]
      assert "orders-op" in Paths.operative_classes()
    end

    test "an unclaimed class resolves from the on-disk layout, else _system", %{base: base} do
      File.mkdir_p!(Path.join([base, "team-a", "disk-op", "proc-0"]))

      assert Paths.namespace_of_class("disk-op") == "team-a"
      assert Paths.namespace_of_class("never-seen-op") == "_system"
    end

    test "a real namespace adopts a class migrated under _system", %{base: base} do
      legacy = Path.join([base, "_system", "adopted-op", "proc-0"])
      File.mkdir_p!(legacy)
      File.write!(Path.join(legacy, "k1.wal"), "legacy-bytes")

      assert :ok = Paths.claim_namespace("adopted-op", "default")

      assert File.read!(Path.join([base, "default", "adopted-op", "proc-0", "k1.wal"])) ==
               "legacy-bytes"

      # Built by hand on purpose: this module's subject IS the layout, so
      # deriving the path would make the assertion follow the resolution it pins.
      refute File.dir?(Path.join([base, "_system", "adopted-op"]))
      assert Paths.namespace_of_class("adopted-op") == "default"
    end

    test "a class owned by another namespace is refused, not hijacked", %{base: base} do
      File.mkdir_p!(Path.join([base, "team-a", "contested-op", "proc-0"]))

      assert {:error, {:namespace_conflict, "contested-op", "team-a"}} =
               Paths.claim_namespace("contested-op", "team-b")

      # The rightful owner still claims fine.
      assert :ok = Paths.claim_namespace("contested-op", "team-a")
    end

    test "deleting the last entity releases the claim", %{base: base} do
      assert :ok = Paths.claim_namespace("gone-op", "team-a")

      entity_id = {"gone-op", "proc-0", "k1"}
      wal = Paths.wal_path(entity_id)
      File.mkdir_p!(Path.dirname(wal))
      File.write!(wal, "bytes")

      :ok = Paths.delete_files(entity_id)

      # Hand-built for the same reason as the adoption case above.
      refute File.dir?(Path.join([base, "team-a", "gone-op"]))
      # Released: another namespace may now claim the name.
      assert :ok = Paths.claim_namespace("gone-op", "team-b")
    end
  end
end

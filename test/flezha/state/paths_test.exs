defmodule Flezha.State.PathsTest do
  @moduledoc """
  Where durable state lives: how the root is chosen, and how a class resolves to
  `<state_root>/<namespace>/<class>/` through the claim a Relay records
  when it binds. The cell_id stays the wire's 3-tuple; isolation holds at the
  path seam.
  """
  # async: false: writes Application env; a shared on-disk root; OS environment variables.
  use FlezhaTest.Case, fixtures: [:env], async: false

  alias Flezha.State.Paths
  alias Flezha.State.TestHelpers

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
      assert "orders-op" in Paths.relay_classes()
    end

    test "the task-journal is the one reserved per-instance file, and no Cell", %{tmp: base} do
      assert Paths.reserved_cell_keys() == [Paths.journal_cell_key()]

      journal = Paths.journal_path("wf-op")
      assert journal == Path.join([base, "_system", "wf-op", "wf-op", "task.journal"])
      File.mkdir_p!(Path.dirname(journal))
      File.write!(journal, "journal-bytes")

      assert %{wal_files: []} = Paths.scan_dir(Paths.journal_id("wf-op"))
      assert Paths.cells_of("wf-op") == []
    end

    # KB-489: a hydration writes the dedup cache of every Cell it restores, the
    # journal's reserved identity included; that file is still not a Cell.
    test "a reserved identity's dedup side-file is no Cell" do
      journal = Paths.journal_path("wf-hydrated")
      File.mkdir_p!(Path.dirname(journal))
      File.write!(journal, "journal-bytes")

      assert :ok =
               Flezha.State.Outbox.write(Paths.journal_id("wf-hydrated"), %{
                 last_persisted_seq: 3,
                 outbox_flushed_seq: -1,
                 epoch: 0
               })

      assert Paths.cells_of("wf-hydrated") == []
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

  describe "the state walk" do
    setup do
      %{tmp: base} = FlezhaTest.Case.install([:tmp_dir])
      Paths.ensure_claims_table()

      # Relative to the working directory, so a walk that expands every path
      # again would ask the file server for the cwd each time.
      Application.put_env(:flezha, :state_root, Path.relative_to(base, File.cwd!(), force: true))
      # The one expansion a configured value costs.
      Paths.state_root()

      seed = fn namespace, class, tasklet, name ->
        dir = Path.join([base, namespace, class, tasklet])
        File.mkdir_p!(dir)
        File.write!(Path.join(dir, name), "bytes")
      end

      seed.("team-a", "orders-op", "task-0", "k1.wal")
      seed.("team-a", "orders-op", "task-1", "k2.snap.4")
      seed.("team-b", "fills-op", "task-0", "k3.outbox.dedup")
      seed.("team-b", "fills-op", "task-0", "k3.wal")
      File.write!(Path.join([base, "team-b", "fills-op", "input.log"]), "log")
      File.mkdir_p!(Path.join([base, "team-b", "empty-op"]))

      %{base: base}
    end

    test "the walk keeps the namespace it found each class under", %{base: base} do
      expected = [
        {"fills-op", "task-0", "k3"},
        {"orders-op", "task-0", "k1"},
        {"orders-op", "task-1", "k2"}
      ]

      {cells, calls} = TestHelpers.file_calls(&Paths.all_cells/0)
      assert Enum.sort(cells) == expected

      refute Enum.any?(calls, &match?({:get_cwd, _}, &1)),
             "the walk asked the file server for the cwd: #{inspect(calls)}"

      listed = for {:list_dir, [dir]} <- calls, do: Path.expand(to_string(dir))

      for namespace_dir <- [base, Path.join(base, "team-a"), Path.join(base, "team-b")] do
        assert Enum.count(listed, &(&1 == namespace_dir)) == 1,
               "#{namespace_dir} listed #{Enum.count(listed, &(&1 == namespace_dir))} times"
      end

      assert Enum.sort(Paths.relay_classes()) == ["empty-op", "fills-op", "orders-op"]

      assert Enum.sort(Paths.cells_of("orders-op")) == [
               {"orders-op", "task-0", "k1"},
               {"orders-op", "task-1", "k2"}
             ]

      assert Paths.cells_of("empty-op") == []
    end

    test "the input-log sweep finds its classes in the same walk", %{base: base} do
      {classes, calls} = TestHelpers.file_calls(&Flezha.State.InputLog.Sweep.all_classes/0)

      assert classes == ["fills-op"]
      assert Enum.count(calls, &match?({:list_dir, [dir]} when dir == base, &1)) == 1
      refute Enum.any?(calls, &match?({:get_cwd, _}, &1))
    end
  end
end

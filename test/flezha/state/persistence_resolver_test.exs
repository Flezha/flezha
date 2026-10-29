defmodule Flezha.State.PersistenceResolverTest do
  @moduledoc """
  The per-class resolvers: compaction thresholds, the remote switch and the
  finished-relay retention window.
  """
  # async: false: writes Application env.
  use FlezhaTest.Case, fixtures: [:env, :tmp_app_data], async: false

  alias Flezha.Relay.ClassRegistry
  alias Flezha.State.PersistenceResolver

  describe "compact_after_bytes/2" do
    test "defaults to 8 MiB" do
      Application.delete_env(:flezha, :default_compact_after_bytes)
      assert PersistenceResolver.compact_after_bytes("task") == 8 * 1024 * 1024
    end

    test "global override wins over the default" do
      Application.put_env(:flezha, :default_compact_after_bytes, 123)
      assert PersistenceResolver.compact_after_bytes("task") == 123
    end

    test "per-Tasklet metadata wins over the global override" do
      Application.put_env(:flezha, :default_compact_after_bytes, 123)

      assert PersistenceResolver.compact_after_bytes("task", %{"compact_after_bytes" => 456}) ==
               456

      assert PersistenceResolver.compact_after_bytes("task", %{compact_after_bytes: 789}) == 789
    end

    # The Watchdog calls `compact_after_bytes(relay_class)` with NO metadata
    # argument (it holds only the class), so the SDK-declared value must be
    # consulted through `ClassRegistry`. The metadata-passing tests above mask
    # that path; these exercise it the way production does.
    test "the SDK-declared value (persisted in ClassRegistry) governs the Watchdog's one-arg call" do
      Application.put_env(:flezha, :default_compact_after_bytes, 8 * 1024 * 1024)
      ClassRegistry.put_compact_after_bytes("sdk_bytes_class", 4 * 1024 * 1024)
      on_exit(fn -> ClassRegistry.delete("sdk_bytes_class") end)

      assert PersistenceResolver.compact_after_bytes("sdk_bytes_class") == 4 * 1024 * 1024
    end

    test "the SDK-declared value wins over the global row" do
      ClassRegistry.put_compact_after_bytes("ovr_bytes_class", 4 * 1024 * 1024)
      Application.put_env(:flezha, :default_compact_after_bytes, 1024)

      on_exit(fn -> ClassRegistry.delete("ovr_bytes_class") end)

      assert PersistenceResolver.compact_after_bytes("ovr_bytes_class") == 4 * 1024 * 1024
    end

    test "an undeclared class falls back to the global default" do
      Application.put_env(:flezha, :default_compact_after_bytes, 777)
      assert PersistenceResolver.compact_after_bytes("undeclared_bytes_class") == 777
    end
  end

  describe "remote?/1" do
    test "a class that declares nothing never waits on a remote store" do
      refute PersistenceResolver.remote?("undeclared_sync_class")
    end

    test "the class's own durable setting is the one answer" do
      ClassRegistry.put_remote("sdk_sync_class", true)
      on_exit(fn -> ClassRegistry.delete("sdk_sync_class") end)

      assert PersistenceResolver.remote?("sdk_sync_class")
    end
  end

  describe "compact_after_age_ms/0" do
    test "defaults to 300_000" do
      Application.delete_env(:flezha, :compact_after_age_ms)
      assert PersistenceResolver.compact_after_age_ms() == 300_000
    end

    test "0 disables the age trigger" do
      Application.put_env(:flezha, :compact_after_age_ms, 0)
      assert PersistenceResolver.compact_after_age_ms() == 0
    end
  end

  describe "completed_retention_hours/1 and kept_until/2" do
    setup do
      ClassRegistry.ensure_table()
      class = "kept-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :default_relay_completed_retention_hours, 72)

      on_exit(fn ->
        ClassRegistry.delete(class)
        Application.delete_env(:flezha, :keep_finished_by_default)
      end)

      %{class: class}
    end

    defp declare(class, keep_finished, persistent? \\ true) do
      :ok = ClassRegistry.put_keep_finished(class, keep_finished)
      :ok = ClassRegistry.put_persistent(class, persistent?)
    end

    test "a class that declares nothing keeps nothing", %{class: class} do
      declare(class, nil)
      assert PersistenceResolver.completed_retention_hours(class) == 0
    end

    test "declared hours win over every default", %{class: class} do
      declare(class, 24)
      Application.put_env(:flezha, :keep_finished_by_default, true)
      assert PersistenceResolver.completed_retention_hours(class) == 24
    end

    test "`true` keeps for the controller's window", %{class: class} do
      declare(class, :default)
      Application.put_env(:flezha, :default_relay_completed_retention_hours, 5)
      assert PersistenceResolver.completed_retention_hours(class) == 5
    end

    test "the controller-wide default keeps an undeclared class for the window",
         %{class: class} do
      declare(class, nil)
      Application.put_env(:flezha, :keep_finished_by_default, true)
      assert PersistenceResolver.completed_retention_hours(class) == 72
    end

    test "a memory-only class keeps nothing whatever the default", %{class: class} do
      declare(class, nil, false)
      Application.put_env(:flezha, :keep_finished_by_default, true)
      assert PersistenceResolver.completed_retention_hours(class) == 0
    end

    test "a directory that already holds a record expires after the declared hours or the window",
         %{class: class} do
      declare(class, nil)
      assert PersistenceResolver.kept_until(class, 100) == 100 + 72 * 3600

      declare(class, 6)
      assert PersistenceResolver.kept_until(class, 100) == 100 + 6 * 3600
    end
  end

  describe "compact_after_age_ms/2 — a declared correction window raises the floor" do
    alias Flezha.Relay.ClassRegistry

    setup do
      ClassRegistry.ensure_table()
      :ok
    end

    test "a floor shorter than the window is raised to it" do
      class = "corrector-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :compact_after_age_ms, 60_000)
      :ok = ClassRegistry.put_correction_window(class, 300_000)

      assert PersistenceResolver.configured_compact_after_age_ms(class) == 60_000

      assert PersistenceResolver.compact_after_age_ms(class) == 300_000,
             "compaction must never collapse the WAL history the rewind needs a baseline inside"
    end

    test "a floor longer than the window is left alone" do
      class = "generous-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :compact_after_age_ms, 600_000)
      :ok = ClassRegistry.put_correction_window(class, 60_000)

      assert PersistenceResolver.compact_after_age_ms(class) == 600_000
    end

    test "a floor of 0 stays 0 — the polarity trap" do
      class = "stream-hist-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :compact_after_age_ms, 0)
      :ok = ClassRegistry.put_correction_window(class, 60_000)

      # 0 DISABLES age compaction, i.e. WAL history is retained without an age
      # bound — which already satisfies any window. A naive max(0, window) would
      # ENABLE compaction at the window and make retention strictly worse.
      assert PersistenceResolver.compact_after_age_ms(class) == 0
    end

    test "a class declaring no window is untouched" do
      class = "plain-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :compact_after_age_ms, 60_000)
      :ok = ClassRegistry.put_correction_window(class, nil)

      assert PersistenceResolver.compact_after_age_ms(class) == 60_000
    end
  end
end

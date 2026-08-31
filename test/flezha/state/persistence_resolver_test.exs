defmodule Flezha.State.PersistenceResolverTest do
  @moduledoc """
  §5b.1: the kind+class `persistent_state?` resolution (workflow durable-by-default,
  opt-out per class) and the finished-job retention window resolver.
  """
  # async: false: writes Application env.
  use ExUnit.Case, async: false

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.PersistenceResolver

  setup do
    # runtime.exs seeds these two in every env (age is 0 in :test so the
    # global Watchdog never surprise-compacts); restore them after the
    # default-probing tests below delete/override them.
    prev_bytes = Application.get_env(:flezha, :default_compact_after_bytes)
    prev_age = Application.get_env(:flezha, :compact_after_age_ms)

    on_exit(fn ->
      Application.delete_env(:flezha, :operative_persistence)
      Application.delete_env(:flezha, :workflow_completed_retention_hours)
      Application.delete_env(:flezha, :default_workflow_completed_retention_hours)
      Application.put_env(:flezha, :default_compact_after_bytes, prev_bytes)
      Application.put_env(:flezha, :compact_after_age_ms, prev_age)
    end)

    :ok
  end

  describe "persistent_state?/3" do
    test ":stream is always durable" do
      assert PersistenceResolver.persistent_state?(:stream, "any_class")
    end

    test ":workflow is durable by default" do
      assert PersistenceResolver.persistent_state?(:workflow, "bnd_default")
    end

    test ":workflow honours a per-class operator opt-out; other classes stay durable" do
      Application.put_env(:flezha, :operative_persistence, %{"bnd_optout" => false})
      refute PersistenceResolver.persistent_state?(:workflow, "bnd_optout")
      assert PersistenceResolver.persistent_state?(:workflow, "bnd_other")
    end

    test ":workflow honours an instance-metadata opt-out" do
      refute PersistenceResolver.persistent_state?(:workflow, "bnd_meta", %{
               "operative_persistence_enabled" => false
             })
    end

    test "an unknown kind is not durable" do
      refute PersistenceResolver.persistent_state?(:made_up, "x")
    end
  end

  describe "compact_after_bytes/2" do
    test "defaults to 8 MiB" do
      Application.delete_env(:flezha, :default_compact_after_bytes)
      assert PersistenceResolver.compact_after_bytes("proc") == 8 * 1024 * 1024
    end

    test "global override wins over the default" do
      Application.put_env(:flezha, :default_compact_after_bytes, 123)
      assert PersistenceResolver.compact_after_bytes("proc") == 123
    end

    test "per-Procedure metadata wins over the global override" do
      Application.put_env(:flezha, :default_compact_after_bytes, 123)

      assert PersistenceResolver.compact_after_bytes("proc", %{"compact_after_bytes" => 456}) ==
               456

      assert PersistenceResolver.compact_after_bytes("proc", %{compact_after_bytes: 789}) == 789
    end

    # The Watchdog calls `compact_after_bytes(operative_class)` with NO metadata
    # argument (it holds only the class), so the SDK-declared value must be
    # consulted through `ClassRegistry`. The metadata-passing tests above mask
    # that path; these exercise it the way production does.
    test "the SDK-declared value (persisted in ClassRegistry) governs the Watchdog's one-arg call" do
      Application.put_env(:flezha, :default_compact_after_bytes, 8 * 1024 * 1024)
      ClassRegistry.put_compact_after_bytes("sdk_bytes_class", 4 * 1024 * 1024)
      on_exit(fn -> ClassRegistry.delete("sdk_bytes_class") end)

      assert PersistenceResolver.compact_after_bytes("sdk_bytes_class") == 4 * 1024 * 1024
    end

    test "an operator override wins over the SDK-declared value" do
      ClassRegistry.put_compact_after_bytes("ovr_bytes_class", 4 * 1024 * 1024)
      Application.put_env(:flezha, :compact_after_bytes, %{"ovr_bytes_class" => 1024})

      on_exit(fn ->
        ClassRegistry.delete("ovr_bytes_class")
        Application.delete_env(:flezha, :compact_after_bytes)
      end)

      assert PersistenceResolver.compact_after_bytes("ovr_bytes_class") == 1024
    end

    test "an undeclared class falls back to the global default" do
      Application.put_env(:flezha, :default_compact_after_bytes, 777)
      assert PersistenceResolver.compact_after_bytes("undeclared_bytes_class") == 777
    end
  end

  describe "sync_durability?/2" do
    test "a class that declares nothing never waits on a remote store" do
      refute PersistenceResolver.sync_durability?("undeclared_sync_class")
    end

    test "the SDK declaration governs" do
      ClassRegistry.put_sync_durability("sdk_sync_class", true)
      on_exit(fn -> ClassRegistry.delete("sdk_sync_class") end)

      assert PersistenceResolver.sync_durability?("sdk_sync_class")
    end

    test "an operator override wins over the SDK declaration, in both directions" do
      ClassRegistry.put_sync_durability("ovr_sync_class", true)
      Application.put_env(:flezha, :sync_durability, %{"ovr_sync_class" => false})

      on_exit(fn ->
        ClassRegistry.delete("ovr_sync_class")
        Application.delete_env(:flezha, :sync_durability)
      end)

      refute PersistenceResolver.sync_durability?("ovr_sync_class")

      Application.put_env(:flezha, :sync_durability, %{"ovr_sync_class" => true})
      ClassRegistry.put_sync_durability("ovr_sync_class", false)
      assert PersistenceResolver.sync_durability?("ovr_sync_class")
    end

    test "instance metadata resolves ahead of the declaration" do
      assert PersistenceResolver.sync_durability?("meta_sync_class", %{"sync_durability" => true})
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

    test "an invalid value falls back to the default" do
      Application.put_env(:flezha, :compact_after_age_ms, -5)
      assert PersistenceResolver.compact_after_age_ms() == 300_000
    end
  end

  describe "completed_retention_hours/2" do
    test "defaults to 72" do
      assert PersistenceResolver.completed_retention_hours("c") == 72
    end

    test "honours the global default override" do
      Application.put_env(:flezha, :default_workflow_completed_retention_hours, 5)
      assert PersistenceResolver.completed_retention_hours("c") == 5
    end

    test "honours a per-class operator override; other classes fall back to default" do
      Application.put_env(:flezha, :workflow_completed_retention_hours, %{"c" => 1})
      assert PersistenceResolver.completed_retention_hours("c") == 1
      assert PersistenceResolver.completed_retention_hours("d") == 72
    end

    test "honours an instance-metadata override" do
      assert PersistenceResolver.completed_retention_hours("c", %{
               "workflow_completed_retention_hours" => 3
             }) == 3
    end
  end

  describe "compact_after_age_ms/2 — a declared correction window raises the floor" do
    alias Flezha.Operative.ClassRegistry

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

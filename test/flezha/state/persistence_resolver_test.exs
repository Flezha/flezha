defmodule Flezha.State.PersistenceResolverTest do
  @moduledoc """
  §5b.1: the kind+class `persistent_state?` resolution (bounded durable-by-default,
  opt-out per class) and the finished-job retention window resolver.
  """
  use ExUnit.Case, async: false

  alias Flezha.State.PersistenceResolver

  setup do
    # runtime.exs seeds these two in every env (age is 0 in :test so the
    # global Watchdog never surprise-compacts); restore them after the
    # default-probing tests below delete/override them.
    prev_bytes = Application.get_env(:flezha, :default_compact_after_bytes)
    prev_age = Application.get_env(:flezha, :compact_after_age_ms)

    on_exit(fn ->
      Application.delete_env(:flezha, :operative_persistence)
      Application.delete_env(:flezha, :bounded_completed_retention_hours)
      Application.delete_env(:flezha, :default_bounded_completed_retention_hours)
      Application.put_env(:flezha, :default_compact_after_bytes, prev_bytes)
      Application.put_env(:flezha, :compact_after_age_ms, prev_age)
    end)

    :ok
  end

  describe "persistent_state?/3" do
    test ":unbounded is always durable" do
      assert PersistenceResolver.persistent_state?(:unbounded, "any_class")
    end

    test ":bounded is durable by default" do
      assert PersistenceResolver.persistent_state?(:bounded, "bnd_default")
    end

    test ":bounded honours a per-class operator opt-out; other classes stay durable" do
      Application.put_env(:flezha, :operative_persistence, %{"bnd_optout" => false})
      refute PersistenceResolver.persistent_state?(:bounded, "bnd_optout")
      assert PersistenceResolver.persistent_state?(:bounded, "bnd_other")
    end

    test ":bounded honours an instance-metadata opt-out" do
      refute PersistenceResolver.persistent_state?(:bounded, "bnd_meta", %{
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
      assert PersistenceResolver.compact_after_bytes("op") == 8 * 1024 * 1024
    end

    test "global override wins over the default" do
      Application.put_env(:flezha, :default_compact_after_bytes, 123)
      assert PersistenceResolver.compact_after_bytes("op") == 123
    end

    test "per-Operation metadata wins over the global override" do
      Application.put_env(:flezha, :default_compact_after_bytes, 123)
      assert PersistenceResolver.compact_after_bytes("op", %{"compact_after_bytes" => 456}) == 456
      assert PersistenceResolver.compact_after_bytes("op", %{compact_after_bytes: 789}) == 789
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

  describe "bounded_completed_retention_hours/2" do
    test "defaults to 72" do
      assert PersistenceResolver.bounded_completed_retention_hours("c") == 72
    end

    test "honours the global default override" do
      Application.put_env(:flezha, :default_bounded_completed_retention_hours, 5)
      assert PersistenceResolver.bounded_completed_retention_hours("c") == 5
    end

    test "honours a per-class operator override; other classes fall back to default" do
      Application.put_env(:flezha, :bounded_completed_retention_hours, %{"c" => 1})
      assert PersistenceResolver.bounded_completed_retention_hours("c") == 1
      assert PersistenceResolver.bounded_completed_retention_hours("d") == 72
    end

    test "honours an instance-metadata override" do
      assert PersistenceResolver.bounded_completed_retention_hours("c", %{
               "bounded_completed_retention_hours" => 3
             }) == 3
    end
  end
end

defmodule Flezha.State.PersistenceResolverTest do
  @moduledoc """
  §5b.1: the kind+class `persistent_state?` resolution (bounded durable-by-default,
  opt-out per class) and the finished-job retention window resolver.
  """
  use ExUnit.Case, async: false

  alias Flezha.State.PersistenceResolver

  setup do
    on_exit(fn ->
      Application.delete_env(:flezha, :operative_persistence)
      Application.delete_env(:flezha, :bounded_completed_retention_hours)
      Application.delete_env(:flezha, :default_bounded_completed_retention_hours)
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

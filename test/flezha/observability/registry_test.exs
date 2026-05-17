defmodule Flezha.Observability.RegistryTest do
  use ExUnit.Case, async: false

  alias Flezha.Observability.Registry

  setup do
    # Use unique source/sink names per test so parallel teardown
    # cannot collide. Tests run async: false because they share the
    # global Registry, but unique names keep teardown clean.
    name = :"obs_test_src_#{:erlang.unique_integer([:positive])}"
    on_exit(fn -> Registry.unregister_source(name) end)
    {:ok, name: name}
  end

  describe "register_source/1" do
    test "registers and lists the source", %{name: name} do
      assert :ok = Registry.register_source(%{name: name, kinds: [:metric], flavour: :declarative})
      sources = Registry.list_sources() |> Enum.map(& &1.name)
      assert name in sources
    end

    test "rejects invalid kind", %{name: name} do
      assert {:error, {:invalid_kinds, _}} =
               Registry.register_source(%{
                 name: name,
                 kinds: [:not_a_kind],
                 flavour: :declarative
               })
    end

    test "lookup_source returns the entry", %{name: name} do
      :ok =
        Registry.register_source(%{
          name: name,
          kinds: [:metric, :event],
          flavour: :declarative,
          cardinality_budget: 5
        })

      {:ok, entry} = Registry.lookup_source(name)
      assert entry.cardinality_budget == 5
      assert entry.status == :active
    end
  end

  describe "enable_source/1 + disable_source/1" do
    test "transitions status :active ↔ :disabled", %{name: name} do
      :ok = Registry.register_source(%{name: name, kinds: [:metric], flavour: :declarative})
      :ok = Registry.disable_source(name)
      {:ok, %{status: :disabled}} = Registry.lookup_source(name)
      :ok = Registry.enable_source(name)
      {:ok, %{status: :active}} = Registry.lookup_source(name)
    end

    test "returns error for unknown source" do
      assert {:error, :not_found} =
               Registry.disable_source(:"obs_test_nonexistent_#{:erlang.unique_integer()}")
    end
  end

  describe "check_cardinality/2" do
    test "accepts shapes under budget", %{name: name} do
      :ok =
        Registry.register_source(%{
          name: name,
          kinds: [:metric],
          flavour: :declarative,
          cardinality_budget: 3
        })

      assert :ok = Registry.check_cardinality(name, %{a: 1})
      assert :ok = Registry.check_cardinality(name, %{b: 1})
      assert :ok = Registry.check_cardinality(name, %{c: 1})
    end

    test "drops shapes at and past budget", %{name: name} do
      :ok =
        Registry.register_source(%{
          name: name,
          kinds: [:metric],
          flavour: :declarative,
          cardinality_budget: 2
        })

      assert :ok = Registry.check_cardinality(name, %{a: 1})
      assert :ok = Registry.check_cardinality(name, %{b: 1})

      assert {:dropped, :cardinality_budget_exceeded} =
               Registry.check_cardinality(name, %{c: 1})

      assert {:dropped, :cardinality_budget_exceeded} =
               Registry.check_cardinality(name, %{d: 1})
    end

    test "increments :budget_exceeded counter", %{name: name} do
      :ok =
        Registry.register_source(%{
          name: name,
          kinds: [:metric],
          flavour: :declarative,
          cardinality_budget: 1
        })

      Registry.check_cardinality(name, %{a: 1})
      Registry.check_cardinality(name, %{b: 1})
      Registry.check_cardinality(name, %{c: 1})

      assert Registry.source_counter(name, :budget_exceeded) >= 2
    end

    test "repeat shape is :ok (not counted as new)", %{name: name} do
      :ok =
        Registry.register_source(%{
          name: name,
          kinds: [:metric],
          flavour: :declarative,
          cardinality_budget: 1
        })

      assert :ok = Registry.check_cardinality(name, %{a: 1})
      assert :ok = Registry.check_cardinality(name, %{a: 2})
      assert :ok = Registry.check_cardinality(name, %{a: 3})
    end

    test "unregistered source returns :ok (no enforcement)" do
      assert :ok =
               Registry.check_cardinality(
                 :"obs_test_unregistered_#{:erlang.unique_integer()}",
                 %{a: 1}
               )
    end
  end

  describe "list_sources/0" do
    test "reads from ETS directly without going through the GenServer", %{name: name} do
      :ok = Registry.register_source(%{name: name, kinds: [:metric], flavour: :declarative})

      # If list_sources/0 used GenServer.call it would block on a busy
      # registry; here we just verify the call succeeds and returns
      # the registered row.
      sources = Registry.list_sources()
      assert Enum.any?(sources, &(&1.name == name))
    end
  end
end

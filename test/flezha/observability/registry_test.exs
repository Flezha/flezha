defmodule Flezha.Observability.RegistryTest do
  # async: false: the observability registry.
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
      assert :ok =
               Registry.register_source(%{name: name, kinds: [:metric], flavour: :declarative})

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

    test "rejects a name outside the Prometheus exposition grammar" do
      # A metric name is `<source>_<name>`, and a name outside
      # `[a-zA-Z_][a-zA-Z0-9_]*` fails the WHOLE scrape rather than one series.
      for bad <- [:"weird source.name", :"3leading_digit", :__reserved] do
        assert {:error, {:invalid_source_name, ^bad}} =
                 Registry.register_source(%{name: bad, kinds: [:metric], flavour: :declarative}),
               "#{inspect(bad)} was accepted"

        refute bad in Enum.map(Registry.list_sources(), & &1.name)
      end
    end

    test "keeps a source's description, area and signals", %{name: name} do
      signals = [%{name: :received, kind: :metric, description: "An order arrived."}]

      assert :ok =
               Registry.register_source(%{
                 name: name,
                 kinds: [:metric],
                 flavour: :declarative,
                 description: "Orders received.",
                 area: :relays,
                 signals: signals
               })

      entry = Enum.find(Registry.list_sources(), &(&1.name == name))
      assert %{description: "Orders received.", area: :relays, signals: ^signals} = entry
    end

    test "a source declaring none of them still registers", %{name: name} do
      :ok = Registry.register_source(%{name: name, kinds: [:metric], flavour: :declarative})

      entry = Enum.find(Registry.list_sources(), &(&1.name == name))
      assert %{description: nil, area: nil, signals: []} = entry
    end

    test "refuses a malformed description, area or signal list", %{name: name} do
      base = %{name: name, kinds: [:metric], flavour: :declarative}

      for {key, value, error} <- [
            {:description, "  ", :invalid_description},
            {:description, :text, :invalid_description},
            {:area, :elsewhere, :invalid_area},
            {:signals, [%{name: :x, kind: :metric}], :invalid_signals},
            {:signals, [%{name: :x, kind: :nope, description: "x"}], :invalid_signals},
            {:signals, :none, :invalid_signals}
          ] do
        assert {:error, {^error, ^value}} = Registry.register_source(Map.put(base, key, value)),
               "#{key} #{inspect(value)} was accepted"

        refute name in Enum.map(Registry.list_sources(), & &1.name)
      end
    end

    test "accepts a legal name unchanged", %{name: name} do
      assert :ok =
               Registry.register_source(%{name: name, kinds: [:metric], flavour: :declarative})
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

      assert {:dropped, :cardinality_budget_exceeded, :first} =
               Registry.check_cardinality(name, %{c: 1})

      assert {:dropped, :cardinality_budget_exceeded, :repeat} =
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

  describe "the sources core registers" do
    test "each has a description, an area and a meaning for every signal" do
      for entry <- core_registrations() do
        assert is_binary(entry.description) and String.trim(entry.description) != "",
               "#{entry.name} has no description"

        assert entry.area in Registry.areas(), "#{entry.name} has no area"
        assert entry.signals != [], "#{entry.name} declares no signal"

        for signal <- entry.signals do
          assert signal.kind in entry.kinds,
                 "#{entry.name}.#{signal.name} is a #{signal.kind}, which the source does not declare"

          assert is_binary(signal.description) and String.trim(signal.description) != "",
                 "#{entry.name}.#{signal.name} has no meaning"
        end
      end
    end

    test "every source the booted registry holds is described", %{name: own} do
      # The test boot starts the bridge and the registry; the pollers, audit log
      # and authz are not started here and are judged by their registrations above.
      # Other modules' tests register sources named with a unique number and
      # do not all remove them; no source core registers is named so.
      booted =
        Enum.reject(Registry.list_sources(), fn %{name: name} ->
          name == own or Atom.to_string(name) =~ ~r/_\d+$/
        end)

      assert length(booted) >= 20

      undescribed = for entry <- booted, not is_binary(entry.description), do: entry.name
      assert undescribed == [], "registered without a description: #{inspect(undescribed)}"
    end

    test "none of them is one that nothing can make emit" do
      gone = [:flezha_reconcile, :flezha_leadership, :flezha_bounded_mailbox]

      names =
        Enum.map(core_registrations(), & &1.name) ++ Enum.map(Registry.list_sources(), & &1.name)

      assert Enum.filter(names, &(&1 in gone)) == []
    end
  end

  defp core_registrations() do
    Flezha.Observability.TelemetryBridge.registrations(%{}) ++
      Enum.map(Flezha.Observability.OtpBackend.Supervisor.source_modules(), & &1.registration()) ++
      Registry.own_registrations() ++
      [
        Flezha.Observability.Export.Supervisor.registration(),
        Flezha.Observability.AuditLog.Supervisor.registration(),
        Flezha.AuthZ.SourceRegistration.registration()
      ]
  end
end

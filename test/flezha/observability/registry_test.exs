defmodule Flezha.Observability.RegistryTest do
  # async: false: the observability registry.
  use ExUnit.Case, async: false

  alias Flezha.Observability.{Registry, SignalWords, Source, TelemetryBridge, TestSinks}

  @observability Flezha.Observability.Supervisor

  defmodule BlankDescription do
    @moduledoc false
    use Flezha.Observability.Source,
      name: :registry_test_blank,
      emit: false,
      area: :controller,
      title: "Blank",
      description: " ",
      signals: [noted: {:event, "Something was noted."}]
  end

  doctest SignalWords

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
      assert %{title: nil, description: nil, area: nil, signals: []} = entry
    end

    test "keeps a title and a signal's label, outcome and sentence", %{name: name} do
      signals =
        Source.signals(
          received:
            {:metric,
             label: "Received",
             outcome: :normal,
             meaning: "An order arrived.",
             sentence: "{shop} received {count:order}"}
        )

      :ok =
        Registry.register_source(%{
          name: name,
          kinds: [:metric],
          flavour: :declarative,
          title: "Orders",
          signals: signals
        })

      assert %{title: "Orders", signals: [signal]} =
               Enum.find(Registry.list_sources(), &(&1.name == name))

      assert signal == %{
               name: :received,
               kind: :metric,
               description: "An order arrived.",
               label: "Received",
               outcome: :normal,
               sentence: "{shop} received {count:order}"
             }
    end

    test "refuses a malformed description, area or signal list", %{name: name} do
      base = %{name: name, kinds: [:metric], flavour: :declarative}

      for {key, value, error} <- [
            {:description, "  ", :invalid_description},
            {:description, :text, :invalid_description},
            {:area, :elsewhere, :invalid_area},
            {:signals, [%{name: :x, kind: :metric}], :invalid_signals},
            {:signals, [%{name: :x, kind: :nope, description: "x"}], :invalid_signals},
            {:signals, :none, :invalid_signals},
            {:title, " ", :invalid_title},
            {:title, :orders, :invalid_title},
            {:signals, Source.signals(x: {:metric, meaning: "x", label: " "}), :invalid_signals},
            {:signals, Source.signals(x: {:metric, meaning: "x", outcome: :warning}),
             :invalid_signals},
            {:signals, Source.signals(x: {:metric, meaning: "x", sentence: "{shop received"}),
             :invalid_signals}
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

  describe "Source.signals/1" do
    test "the short form takes the humanized name, a normal outcome and no sentence" do
      assert [signal] = Source.signals(order_refused: {:event, "An order was refused."})

      assert signal == %{
               name: :order_refused,
               kind: :event,
               description: "An order was refused.",
               label: "Order refused",
               outcome: :normal
             }
    end

    test "the long form keeps what it declares and defaults the rest" do
      assert [%{label: "Refused", outcome: :failure, description: "No."} = signal] =
               Source.signals(
                 refused: {:event, meaning: "No.", label: "Refused", outcome: :failure}
               )

      refute Map.has_key?(signal, :sentence)

      assert [%{label: "Flushed", outcome: :normal, sentence: "{n}"}] =
               Source.signals(flushed: {:metric, meaning: "Saved.", sentence: "{n}"})
    end
  end

  describe "SignalWords.placeholders/1" do
    test "reads keys and nouns in order" do
      assert {:ok, [{"plugin_id", nil}, {"count", "record"}, {"path", nil}]} =
               SignalWords.placeholders("{plugin_id} saved {count:record} to {path}")

      assert {:ok, []} = SignalWords.placeholders("Nothing to fill.")
      assert {:ok, [{"n", "dead letter"}]} = SignalWords.placeholders("{n:dead letter}")
    end

    test "refuses any other brace" do
      for bad <- ["{shop", "shop}", "{}", "{Shop}", "{a-b}", "{a:Noun}", "{a:}", "{{a}}", :text] do
        assert SignalWords.placeholders(bad) == :error, "#{inspect(bad)} parsed"
      end
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
    test "each has a title, a description, an area, and words for every signal" do
      assert undescribed(core_registrations()) == []
    end

    test "a declaration with a blank description is named by the check" do
      blank = BlankDescription.__source__()

      assert undescribed(core_registrations() ++ [blank]) == [
               "#{blank.name} has no description"
             ]
    end

    test "every source outside the bridge is listed with the kinds its signals have" do
      kinds =
        Map.new(core_registrations() -- TelemetryBridge.registrations(), &{&1.name, &1.kinds})

      assert kinds == %{
               platform_authz: [:event],
               audit_log: [:event],
               relay_history: [:event],
               observability_export: [:event],
               observability_registry: [:event],
               observability_service: [:event],
               beam_vm: [:metric],
               host: [:metric, :event],
               processhub: [:metric],
               flezha_transport: [:metric]
             }
    end

    test "every sentence template they declare parses" do
      unparsed =
        for entry <- core_registrations(),
            %{sentence: sentence} = signal <- entry.signals,
            SignalWords.placeholders(sentence) == :error,
            do: "#{entry.name}.#{signal.name}: #{inspect(sentence)}"

      assert unparsed == [], "templates that do not parse: #{Enum.join(unparsed, "; ")}"

      assert Enum.count(core_registrations(), fn entry ->
               Enum.any?(entry.signals, &Map.has_key?(&1, :sentence))
             end) >= 10
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

  describe "a registry restart" do
    setup do
      on_exit(&TestSinks.restart_after_registry/0)
    end

    test "lists every declared source again" do
      :ok = Supervisor.terminate_child(@observability, Registry)
      {:ok, _pid} = Supervisor.restart_child(@observability, Registry)

      listed = MapSet.new(Registry.list_sources(), & &1.name)

      for entry <- Source.declared(),
          do: assert(entry.name in listed, "#{entry.name} is gone after the restart")
    end
  end

  defp text?(text), do: is_binary(text) and String.trim(text) != ""

  # What a reader would miss about each registration, as one line per gap.
  defp undescribed(entries) do
    for entry <- entries, gap <- gaps(entry), do: "#{entry.name} #{gap}"
  end

  defp gaps(entry) do
    source = [
      {text?(entry[:title]), "has no title"},
      {text?(entry[:description]), "has no description"},
      {entry[:area] in Source.areas(), "has no area"},
      {entry.signals != [], "declares no signal"}
    ]

    signals =
      for signal <- entry.signals,
          check <- [
            {signal.kind in entry.kinds, "#{signal.name} is a kind the source does not declare"},
            {text?(signal.description), "#{signal.name} has no meaning"},
            {text?(signal[:label]), "#{signal.name} has no label"},
            {signal[:outcome] in SignalWords.outcomes(), "#{signal.name} has no outcome"}
          ],
          do: check

    for {false, gap} <- source ++ signals, do: gap
  end

  defp core_registrations() do
    TelemetryBridge.registrations() ++
      Enum.map(Flezha.Observability.OtpBackend.Supervisor.source_modules(), & &1.__source__()) ++
      Source.declared()
  end
end

defmodule Flezha.Observability.SourceTest do
  # async: false: the observability registry. The fixtures live here, not in
  # test/helpers, so `Source.declared/0` never reads them.
  use ExUnit.Case, async: false

  alias Flezha.Observability.{Registry, Signal}
  alias Flezha.Observability.TestSinks.Counting

  defmodule TwoKinds do
    @moduledoc false
    use Flezha.Observability.Source,
      name: :source_test_two_kinds,
      cardinality_budget: 10,
      area: :controller,
      title: "Two kinds",
      description: "A source that counts and notes.",
      signals: [
        counted: {:metric, "Something was counted."},
        noted: {:event, label: "Noted", meaning: "Something was noted."}
      ]
  end

  defmodule NoBudget do
    @moduledoc false
    use Flezha.Observability.Source,
      name: :source_test_no_budget,
      area: :controller,
      title: "No budget",
      description: "A source that names no budget.",
      signals: [noted: {:event, "Something was noted."}]
  end

  setup do
    on_exit(fn ->
      Registry.unregister_source(:source_test_two_kinds)
      Registry.unregister_source(:source_test_no_budget)
    end)
  end

  test "a declaration registers with the kinds its signals have" do
    assert %{name: :source_test_two_kinds, kinds: [:metric, :event], flavour: :declarative} =
             TwoKinds.__source__()

    assert :ok = Registry.register_source(TwoKinds.__source__())

    assert {:ok, %{kinds: [:metric, :event], cardinality_budget: 10}} =
             Registry.lookup_source(:source_test_two_kinds)
  end

  test "emit/3 puts a declared signal on the bus with its declared kind" do
    :ok = Registry.register_source(TwoKinds.__source__())
    sink = :"source_test_sink_#{System.unique_integer([:positive])}"

    {:ok, _pid} =
      Registry.register_sink(%{
        name: sink,
        module: Counting,
        config: %{
          counter_ref: :counters.new(1, []),
          parent: self(),
          sources: [:source_test_two_kinds]
        }
      })

    on_exit(fn -> Registry.unregister_sink(sink) end)

    TwoKinds.emit(:noted, %{count: 1}, %{why: "test"})

    assert_receive {:counted,
                    %Signal{
                      kind: :event,
                      source: :source_test_two_kinds,
                      name: :noted,
                      measurements: %{count: 1},
                      metadata: %{why: "test"}
                    }}
  end

  test "emitting a name the source does not declare raises" do
    # Built at run time: the type checker refuses the literal already.
    undeclared = String.to_atom("undeclared")
    assert_raise FunctionClauseError, fn -> TwoKinds.emit(undeclared, %{}) end
  end

  test "a declaration naming no budget takes the registry's default" do
    refute Map.has_key?(NoBudget.__source__(), :cardinality_budget)
    assert :ok = Registry.register_source(NoBudget.__source__())

    assert {:ok, %{cardinality_budget: budget}} = Registry.lookup_source(:source_test_no_budget)
    assert budget == Flezha.Config.get(:observability_cardinality_budget_per_source)
  end
end

# frozen_string_literal: true

require_relative "test_helper"

# Registration-time refusal replaces the Rust SDK's compile-time one, so the
# message has to carry what a compiler would have pointed at: the class, the
# handler, and the specific defect.
class AuthoringTest < Minitest::Test
  def refusal(&block)
    assert_raises(Flezha::DeclarationError, &block)
  end

  def pass_through(name)
    Class.new(Flezha::Procedure) do
      procedure_name name
      def process(input, _ctx) = input
    end
  end

  def operative(factory, &channels)
    klass = Class.new(Flezha::StreamOperative)
    klass.factory_id factory
    klass.channels(&channels)
    klass
  end

  def test_a_procedure_without_a_process_method_is_refused_by_name
    klass = Class.new(Flezha::Procedure) { procedure_name "no_body" }

    assert_includes refusal { klass.definition }.message, "no_body"
  end

  def test_a_fold_with_no_state_to_fold_into_is_refused
    klass = Class.new(Flezha::Procedure) do
      procedure_name "fold_only"
      def apply_change(state, change) = state
      def process(input, _ctx) = input
    end

    assert_includes refusal { klass.definition }.message, "initial_state"
  end

  def test_the_two_entity_modes_are_exclusive
    klass = Class.new(Flezha::Procedure) do
      procedure_name "both_modes"
      entity_keys "a"
      dynamic_entities
      def entities_for(_input) = ["a"]
      def process(input, _ctx) = input
    end

    assert_includes refusal { klass.definition }.message, "exclusive"
  end

  def test_a_state_that_cannot_be_encoded_is_refused_at_registration
    klass = Class.new(Flezha::Procedure) do
      procedure_name "unencodable"
      def initial_state(_key) = { handler: -> {} }
      def process(input, _ctx) = input
    end

    assert_raises(Flezha::UnrepresentableValueError) { klass.definition }
  end

  def test_an_entry_channel_with_no_consumer_is_refused
    klass = operative("no_consumer") { |c| c.input("input") }

    assert_includes refusal { Flezha::Graph.validate(klass.spec) }.message, "names no consumer"
  end

  def test_a_cycle_is_refused_naming_the_procedure
    first = pass_through("cycle_a")
    second = pass_through("cycle_b")
    klass = operative("cycles") do |c|
      edge = c.input("input").into(first)
      back = edge.named("forward").into(second)
      back.named("backward").into(first)
    end

    assert_includes refusal { Flezha::Graph.validate(klass.spec) }.message, "is on a cycle"
  end

  # Every node the fluent DSL creates is created BY an `into`, so it is fed by
  # construction and this refusal cannot fire through that surface. It is the
  # invariant the Rust builder also checks, and the node is built through the
  # builder directly here because that is the only way to express the shape it
  # rejects — a check with no way to reach it is a check nobody has run.
  def test_an_unreachable_procedure_is_refused
    reached = pass_through("reached")
    orphan = pass_through("orphan")
    klass = operative("unreachable") do |c|
      c.input("input").into(reached).named("out").sink("s")
      Flezha::Edge.new(c, [:procedure, c.node_for(orphan)], {}).sink("dead_sink")
    end

    assert_includes refusal { Flezha::Graph.validate(klass.spec) }.message, "not reachable"
  end

  def test_an_edge_into_a_dynamic_procedure_must_name_its_key
    dynamic = Class.new(Flezha::Procedure) do
      procedure_name "needs_key"
      dynamic_entities
      def process(input, _ctx) = input
    end
    klass = operative("unkeyed") { |c| c.input("input").into(dynamic).named("o").sink("s") }

    assert_includes refusal { Flezha::Graph.validate(klass.spec) }.message, "without a key_by"
  end

  def test_edge_keying_and_a_static_key_set_are_exclusive
    static = Class.new(Flezha::Procedure) do
      procedure_name "static_keys"
      entity_keys "a", "b"
      def entities_for(_input) = ["a"]
      def process(input, _ctx) = input
    end
    klass = operative("both_keyings") do |c|
      c.input("input").key_by { |v| v.to_s }.into(static).named("o").sink("s")
    end

    assert_includes refusal { Flezha::Graph.validate(klass.spec) }.message, "not both"
  end

  def test_the_effectively_once_tier_needs_persistence
    klass = Class.new(Flezha::StreamOperative)
    klass.factory_id "tier_without_state"
    klass.persistent false
    klass.channels { |c| c.effectively_once.input("input").into(pass_through("p")).named("o").sink("s") }

    assert_includes refusal { Flezha::Graph.validate(klass.spec) }.message, "durable state"
  end

  def test_a_workflow_cannot_declare_both_a_run_body_and_channels
    klass = Class.new(Flezha::WorkflowOperative)
    klass.factory_id "both_shapes"
    klass.run { |args, _ctx| args }
    klass.channels { |c| c.input("input").into(pass_through("w")).named("o").sink("s") }

    assert_includes refusal { klass.spec }.message, "one or the other"
  end

  def test_two_unnamed_mentions_of_one_procedure_are_one_node
    shared = pass_through("shared")
    upstream = pass_through("upstream")
    klass = operative("diamond") do |c|
      edge = c.input("input").into(upstream)
      edge.named("left").into(shared)
      edge.named("right").into(shared)
    end
    Flezha::Graph.validate(klass.spec)

    assert_equal 2, klass.spec.procedures.size
  end

  def test_a_second_procedure_claiming_one_node_identity_is_refused
    first = pass_through("same_name")
    second = pass_through("same_name")
    klass = operative("collide") do |c|
      edge = c.input("input").into(first)
      edge.named("hop").into(second)
    end

    assert_includes refusal { klass.spec }.message, "two different procedures"
  end

  def test_an_undeclared_class_never_reaches_the_registry
    # Registration is explicit: defining a class does not register it.
    defined_but_unregistered = pass_through("never_registered")
    registry = Flezha::Registry.new

    assert_nil registry.get("never_registered")
    refute_nil defined_but_unregistered.definition
  end

  # `named` is honoured on the inter-proc path and was dropped on the plugin
  # path, so `named("input").sink("relay_out")` produced a channel called
  # `relay_out` — and the channel name is what a forwarded frame carries and
  # what selects the receiving operative's entry channel.
  def test_a_named_edge_terminating_on_a_sink_names_the_channel_too
    klass = operative("named_sink") do |c|
      c.input("input").into(pass_through("relay")).named("in").sink("relay_out")
    end

    assert_equal %w[input in], klass.spec.channels.map(&:name)
  end

  # Compatibility, not conformance: `sdk-graph-authoring` says an underived name
  # comes from the PRODUCER NODE, and these veneers use the sink's. That second
  # divergence is open and is not what this change moves — this test pins that
  # it moves nothing but the named case.
  def test_an_unnamed_edge_terminating_on_a_sink_keeps_the_name_it_has_today
    klass = operative("unnamed_sink") do |c|
      c.input("input").into(pass_through("plain")).sink("relay_out")
    end

    assert_equal %w[input relay_out], klass.spec.channels.map(&:name)
  end
end

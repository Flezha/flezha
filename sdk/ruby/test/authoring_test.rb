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

  def test_a_checkpoint_option_without_the_tier_is_refused
    {
      "checkpoint_ack" => ->(c) { c.checkpoint_ack },
      "checkpoint_every" => ->(c) { c.checkpoint_every(4) },
      "outputs_at_checkpoint" => ->(c) { c.outputs_at_checkpoint(250) }
    }.each do |option, declare|
      klass = operative("ckpt_#{option}") do |c|
        declare.call(c)
        c.input("in").into(pass_through("p_#{option}")).keyed.sink("out")
      end

      assert_includes refusal { Flezha::Graph.validate(klass.spec) }.message,
                      "checkpoint acknowledgement without effectively_once",
                      "#{option} must require the tier"
    end
  end

  def test_the_checkpoint_options_compose_in_any_order_and_none_clears_another
    klass = operative("compose") do |c|
      c.effectively_once
      c.outputs_at_checkpoint(250)
      c.checkpoint_ack
      c.checkpoint_every(4)
      c.input("in").into(pass_through("p")).keyed.sink("out")
    end
    Flezha::Graph.validate(klass.spec)
    decl = Flezha::Graph.to_host_class_decl(klass.spec)

    assert_equal "checkpoint", decl["ack_granularity"]
    assert_equal 4, decl["checkpoint_every"]
    assert_equal "at_checkpoint", decl["checkpoint_outputs"]
    assert_equal 250, decl["checkpoint_max_delay_ms"]
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

  # `sdk-graph-authoring`: an underived channel name
  # comes from the PRODUCER NODE, never from the sink it terminates on — the
  # sink's name is the plugin's, and several channels may name one sink.
  def test_an_unnamed_edge_terminating_on_a_sink_takes_the_producing_nodes_name
    klass = operative("unnamed_sink") do |c|
      c.input("input").into(pass_through("plain")).sink("relay_out")
    end

    assert_equal %w[input plain], klass.spec.channels.map(&:name)
  end
  # -- one output channel per producing procedure -----------------------------
  #
  # The model Rust and Java carry: a procedure node owns ONE output channel,
  # created on first wiring and reused after. A held edge wired twice appends a
  # second consumer to it — it does not declare a second channel claiming the
  # same producer, which the core resolves by first match
  # (`host/register.rs` `procedure_wiring`) and so silently truncates.

  def channels_of(klass)
    Flezha::Graph.to_host_class_decl(klass.spec)["channels"]
                 .map { |c| [c["name"], c["kind"], c["producer"], c["consumers"]] }
  end

  def test_a_held_edge_wired_twice_is_one_channel_with_two_consumers
    b = pass_through("b")
    c_proc = pass_through("c")
    klass = operative("fan_out") do |c|
      edge = c.input("in").into(pass_through("producer"))
      edge.into(b)
      edge.into(c_proc)
    end
    Flezha::Graph.validate(klass.spec)

    assert_equal [["in", "entry", nil, [0]], ["producer", "inter_proc", 0, [1, 2]]],
                 channels_of(klass)
  end

  def test_branch_and_two_into_statements_build_the_identical_declaration
    producer = pass_through("producer")
    b = pass_through("b")
    c_proc = pass_through("c")
    separate = operative("separate") do |c|
      edge = c.input("in").into(producer)
      edge.into(b)
      edge.into(c_proc)
    end
    branched = operative("branched") do |c|
      c.input("in").into(producer).branch(->(arm) { arm.into(b) }, ->(arm) { arm.into(c_proc) })
    end

    assert_equal channels_of(separate), channels_of(branched)
  end

  def test_sink_returns_the_edge_so_a_consumer_wires_behind_it
    klass = operative("hybrid") do |c|
      c.input("in").into(pass_through("producer")).named("hybrid_indicator")
       .sink("hybrid_indicators").into(pass_through("consumer"))
    end
    Flezha::Graph.validate(klass.spec)

    assert_equal [["in", "entry", nil, [0]], ["hybrid_indicator", "inter_proc", 0, [1]]],
                 channels_of(klass)
  end

  def test_a_second_sink_on_one_channel_is_refused
    message = refusal do
      operative("twice") { |c| c.input("in").into(pass_through("a")).sink("out").sink("again") }.spec
    end.message

    assert_includes message, "declares a plugin sink more than once"
  end

  def test_keyed_is_refused_without_a_plugin_sink_and_accepted_with_one
    no_sink = operative("keyed_no_sink") do |c|
      c.input("in").into(pass_through("producer")).named("mid").keyed.into(pass_through("consumer"))
    end
    message = refusal { Flezha::Graph.validate(no_sink.spec) }.message

    assert_includes message, "is declared keyed but has no plugin wire"

    hybrid = operative("keyed_hybrid") do |c|
      c.input("in").into(pass_through("producer")).named("mid").keyed
       .sink("mid_sink").into(pass_through("consumer"))
    end
    Flezha::Graph.validate(hybrid.spec)

    assert_equal true, hybrid.spec.channels[1].keyed
  end

  def test_a_transform_before_one_into_reaches_that_consumer_only
    b = pass_through("b")
    c_proc = pass_through("c")
    klass = operative("per_edge_transform") do |c|
      edge = c.input("in").into(pass_through("producer"))
      edge.map { |value| "#{value}!" }.into(b)
      edge.into(c_proc)
    end
    Flezha::Graph.validate(klass.spec)

    assert_equal [[1, 1], [2, 0]],
                 klass.spec.channels[1].consumers.map { |e| [e.procedure, e.transforms.size] }
  end

  # The reference declares exactly this — `tier_commit_block_workflow` opens
  # with `c.effectively_once()` on a workflow class in Rust and Java — so the
  # flag is read where the workflow spec is assembled rather than written false.
  def test_a_workflow_class_carries_its_effectively_once_declaration
    klass = Class.new(Flezha::WorkflowOperative)
    klass.factory_id "tier_workflow"
    klass.channels do |c|
      c.effectively_once
      c.input("in").into(pass_through("producer")).named("out").keyed.sink("out")
    end
    Flezha::Graph.validate(klass.spec)

    assert_equal true, klass.spec.effectively_once
    assert_equal true, Flezha::Graph.to_host_class_decl(klass.spec)["effectively_once"]
  end
end

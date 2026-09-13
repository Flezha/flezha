# frozen_string_literal: true

require_relative "test_helper"

# What this SDK's authoring surface answers, and what it hands the core.
#
# The topology itself is judged once, in `flz_core::host::register::check`, and
# the reference refusals are pinned by `test/fixtures/reference_topologies.txt`
# — not here. What is left here is the ergonomics: which shapes the fluent
# builder BUILDS, the two defects a record cannot carry (a Ruby class that does
# not define the method its declaration promises, a plugin sink on a channel no
# procedure produces on), and that the core's verdict reaches an author as this
# SDK's own refusal.
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

  # The submission needs a live core; without the compiled veneer the case says
  # so rather than passing vacuously.
  def validate(spec)
    skip "needs the compiled veneer (scripts/build-ruby-sdk.sh)" unless Flezha::TestSupport.native?

    Flezha::TestSupport.validate(spec)
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

  # A clock is a promise about a method, and a record carries the clock but not
  # the class: only this SDK can see that the method is missing.
  def test_an_event_time_clock_without_its_extractor_is_refused_here
    klass = Class.new(Flezha::Procedure) do
      procedure_name "no_extractor"
      clock :event_time
      def process(input, _ctx) = input
    end
    graph = operative("no_extractor_op") { |c| c.input("in").into(klass).sink }

    # No core at all, which is the proof it was refused before the record
    # could reach one.
    assert_includes refusal { Flezha::Graph.validate(graph.spec, nil) }.message,
                    "declares an event_time clock but no event_time method"
  end

  # The other refusal the builder keeps: a channel with no producer has no
  # value to serialize, and the core reads a sink as the terminal of the
  # channel it sits on — it cannot see that this one is fed by the controller.
  def test_a_plugin_sink_on_an_entry_channel_is_refused_here
    klass = operative("entry_sink") { |c| c.input("in").sink }

    assert_includes refusal { klass.spec }.message, "a plugin sink is fed by a procedure"
  end

  # `validate` IS the submission: the record crosses the ABI and the core's
  # verdict comes back as this SDK's own refusal, in the core's vocabulary.
  def test_the_cores_verdict_reaches_the_author_as_a_declaration_refusal
    target = Class.new(Flezha::Procedure) do
      procedure_name "keyed"
      dynamic_entities
      def process(input, _ctx) = input
    end
    klass = operative("dup_key_by") do |c|
      c.input("in").key_by { "a" }.key_by { "b" }.into(target)
    end

    error = refusal { validate(klass.spec) }

    assert error.message.start_with?("dup_key_by: topology rejected [DuplicateKeyBy]:\n  - "), error.message
    assert_includes error.message, "declares .key_by(...) twice before one `.into(...)`"
    # And the names as DATA: the core hands the verdict out beside the rendered
    # message, so nothing here reads them out of the prose.
    assert_equal ["DuplicateKeyBy"], error.refusals
  end

  # The three checkpoint options each write their own raw flag now — the
  # granularity is the core's to imply — so none of them may clear another.
  def test_the_checkpoint_options_compose_in_any_order_and_none_clears_another
    klass = operative("compose") do |c|
      c.effectively_once
      c.outputs_at_checkpoint(250)
      c.checkpoint_ack
      c.checkpoint_every(4)
      c.input("in").into(pass_through("p")).keyed.sink
    end
    decl = Flezha::Graph.to_host_class_decl(klass.spec)

    assert_equal true, decl["checkpoint_ack"]
    assert_equal 4, decl["checkpoint_every"]
    assert_equal "at_checkpoint", decl["checkpoint_outputs"]
    assert_equal 250, decl["checkpoint_max_delay_ms"]
    refute_includes decl.keys, "ack_granularity", "the granularity is derived, never declared"
  end

  def test_a_workflow_cannot_declare_both_a_run_body_and_channels
    klass = Class.new(Flezha::WorkflowOperative)
    klass.factory_id "both_shapes"
    klass.run { |args, _ctx| args }
    klass.channels { |c| c.input("input").into(pass_through("w")).sink("o") }

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
    validate(klass.spec)

    assert_equal 2, klass.spec.procedures.size
    # And one edge: `named` renames the one channel `upstream` produces on, so
    # the second bare mention re-declares the first rather than wiring `shared`
    # a second time off it.
    assert_equal [1], klass.spec.channels[1].consumers.map(&:procedure)
  end

  def test_a_second_procedure_claiming_one_node_identity_is_refused
    first = pass_through("same_name")
    second = pass_through("same_name")
    klass = operative("collide") do |c|
      edge = c.input("input").into(first)
      edge.named("hop").into(second)
    end

    error = refusal { klass.spec }

    # The one refusal this builder raises under a core name reads the way the core's do.
    assert_equal ["NodeIdentityCollision"], error.refusals
    assert error.message.start_with?(
      "collide: same_name: topology rejected [NodeIdentityCollision]:\n  - two different procedures"
    ), error.message
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
      c.input("input").into(pass_through("relay")).sink("in")
    end

    assert_equal %w[input in], klass.spec.channels.map(&:name)
  end

  # `sdk-graph-authoring`: the no-argument `sink` names nothing, so the channel
  # keeps the PRODUCER NODE's name — the derivation a channel with no sink at
  # all would take.
  def test_an_unnamed_edge_terminating_on_a_sink_takes_the_producing_nodes_name
    klass = operative("unnamed_sink") do |c|
      c.input("input").into(pass_through("plain")).sink
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
  #
  # Read off the DERIVED declaration: the consumers and the plugin kind are
  # what the core made of the chains this builder recorded.

  def channels_of(klass)
    validate(klass.spec)["channels"].map do |channel|
      [channel["name"], channel["kind"], channel["producer"],
       channel["consumers"].map { |edge| edge["procedure"] }, channel["sink"]]
    end
  end

  def test_a_held_edge_wired_twice_is_one_channel_with_two_consumers
    b = pass_through("b")
    c_proc = pass_through("c")
    klass = operative("fan_out") do |c|
      edge = c.input("in").into(pass_through("producer"))
      edge.into(b)
      edge.into(c_proc)
    end

    assert_equal [["in", "entry", nil, [0], nil], ["producer", "inter_proc", 0, [1, 2], nil]],
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
      c.input("in").into(pass_through("producer"))
       .sink("hybrid_indicator").into(pass_through("consumer"))
    end

    assert_equal [["in", "entry", nil, [0], nil],
                  ["hybrid_indicator", "inter_proc", 0, [1], "hybrid_indicator"]],
                 channels_of(klass)
  end

  def test_the_sink_names_the_channel_and_the_no_argument_form_does_not
    named = operative("sink_named") do |c|
      c.input("in").into(pass_through("producer")).sink("items_output")
    end
    derived = operative("sink_derived") do |c|
      c.input("in").into(pass_through("producer")).sink
    end

    assert_equal %w[in items_output], channels_of(named).map(&:first)
    assert_equal %w[in producer], channels_of(derived).map(&:first)
  end

  def test_a_channel_named_twice_is_refused_naming_both
    klass = operative("named_twice") do |c|
      c.input("in").into(pass_through("producer")).named("a").sink("b")
    end

    error = assert_raises(Flezha::DeclarationError) { validate(klass.spec) }
    assert_match(/ChannelNamedTwice/, error.message)
    assert_match(/both name it/, error.message)
  end

  def test_a_channel_option_with_nothing_wired_after_it_still_reaches_the_channel
    renamed = operative("trailing_named") { |c| c.input("in").into(pass_through("producer")).named("renamed") }
    keyed = operative("trailing_keyed") { |c| c.input("in").into(pass_through("producer")).keyed }

    assert_equal %w[in renamed], channels_of(renamed).map(&:first)
    error = assert_raises(Flezha::DeclarationError) { validate(keyed.spec) }
    assert_match(/KeyedChannelWithoutPluginWire/, error.message)
  end

  def test_two_channels_naming_one_sink_is_refused
    klass = operative("one_sink_two_channels") do |c|
      c.input("in").into(pass_through("a")).sink("shared")
       .into(pass_through("b")).sink("shared")
    end

    error = assert_raises(Flezha::DeclarationError) { validate(klass.spec) }
    assert_match(/DuplicateSinkName/, error.message)
  end

  def test_a_transform_before_one_into_reaches_that_consumer_only
    b = pass_through("b")
    c_proc = pass_through("c")
    klass = operative("per_edge_transform") do |c|
      edge = c.input("in").into(pass_through("producer"))
      edge.map { |value| "#{value}!" }.into(b)
      edge.into(c_proc)
    end
    validate(klass.spec)

    assert_equal [[1, 1], [2, 0]],
                 klass.spec.channels[1].consumers.map { |e| [e.procedure, e.transforms.size] }
  end

  # A re-declaration carrying neither transform nor key is the fan-in /
  # repeated-`into` case both references make a true no-op: one consumer edge,
  # not two, and not a refusal.
  def test_a_bare_re_declared_edge_is_a_no_op_not_a_second_edge
    target = pass_through("target")
    klass = operative("edge_redeclared_bare") do |c|
      edge = c.input("in")
      edge.into(target)
      edge.into(target)
    end
    derived = validate(klass.spec)

    assert_equal [[0, 0]],
                 klass.spec.channels[0].consumers.map { |e| [e.procedure, e.transforms.size] }
    assert_equal [0], derived["channels"][0]["consumers"].map { |e| e["procedure"] }
  end

  # The reference declares exactly this — `tier_commit_block_workflow` opens
  # with `c.effectively_once()` on a workflow class in Rust and Java — so the
  # flag is read where the workflow spec is assembled rather than written false.
  # It stays off the wire: the core refuses `effectively_once` on a workflow
  # class, and Java's `HostClassDeclaration.forWorkflow` never writes it either.
  def test_a_workflow_class_carries_its_effectively_once_declaration
    klass = Class.new(Flezha::WorkflowOperative)
    klass.factory_id "tier_workflow"
    klass.channels do |c|
      c.effectively_once
      c.input("in").into(pass_through("producer")).keyed.sink("out")
    end
    validate(klass.spec)

    assert_equal true, klass.spec.effectively_once
    refute_includes Flezha::Graph.to_host_class_decl(klass.spec).keys, "effectively_once"
  end
end

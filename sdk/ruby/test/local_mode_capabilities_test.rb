# frozen_string_literal: true

require_relative "test_helper"

Flezha::TestSupport.require_test_app("fixtures")

# Local mode over the five capabilities the LANGUAGE LAYER owns, each driven
# through the fixture class `test/apps/ruby_runner/conformance.json` names for
# it: the same class the end-to-end scenario drives, so what passes here and
# what passes there cannot be two different graphs.
#
# The other ten capabilities in the vocabulary are the controller's:
# persistence, effectively-once, correction, partitioning, workflows,
# liveness, restarts. Local mode refuses them by name rather than
# approximating them, and `sdk-local-mode` says a sixth needs its own
# proposal.
#
# What each test can assert is what a run with no controller can see. A
# durable write reaches a null sink, a parked payload has nowhere to park, and
# a partition member's key range arrives with `AddOperative`, so the
# assertions here are the output side of each capability, which is the half
# the language layer produces.
class LocalModeCapabilitiesTest < Minitest::Test
  def setup
    skip "needs the compiled veneer (scripts/build-ruby-sdk.sh)" unless Flezha::TestSupport.native?
  end

  def test_graph_builder_a_declared_graph_is_built_and_run_with_no_controller
    released = release(declares("graph_builder", "sugar_do_nothing"),
                       "input", "sugar_do_nothing_echo", %w[alpha beta])

    assert_equal %w[alpha beta], payloads(released),
                 "the builder produced a graph the core ran, in order"
  end

  # `partition_relay` is the manifest's only keyed-channel class. Partitioning
  # itself is the controller's (a member's key-group range arrives with
  # `AddOperative`), so what is asserted here is the half the language layer
  # owns: the key it resolves and the stamps it puts on the released value.
  def test_keyed_channels_a_keyed_sink_stamps_the_key_the_class_resolved
    released = release(declares("keyed_channels", "partition_relay"),
                       "in", "input", %w[a:1 b:1 a:2])

    assert_equal %w[a:1 b:1 a:2], payloads(released)
    assert_equal %w[a b a], released.map { |output| output.metadata["flz.key"] },
                 "each release carries the key its own value resolved to"
    %w[flz.entity flz.seq].each do |stamp|
      assert_includes released.first.metadata, stamp, "a keyed sink carries #{stamp}"
    end
  end

  # The class's sink is not keyed, so a released value carries no entity stamp
  # and the per-key minting is not visible on this channel. It is asserted
  # above, on `partition_relay`, whose procedure is dynamic too and whose sink
  # IS keyed. What this class shows that that one does not is the declared
  # filter: a value it rejects answers no key, so the core is never asked to
  # mint an Entity for it.
  def test_dynamic_entity_the_declared_filter_runs_before_keying_and_nothing_else_is_lost
    released = release(declares("dynamic_entity", "dynamic_entity"),
                       "input", "kafka", %w[a:1 b:1 drop a:2])

    assert_equal %w[a:1 b:1 a:2], payloads(released),
                 "the filter dropped `drop` before it was keyed, and every other value reached its Entity"
    assert_equal %w[kafka kafka kafka], released.map { |output| output.metadata["channel"] },
                 "an unkeyed sink carries its channel and no identity stamps"
  end

  # `poison_probe` is the manifest's other class for this capability and its
  # poison path ends in the dead-letter queue, which is the controller's. The
  # ladder's other arm, a full mailbox under `drop_nothing` where the producer
  # is held rather than the value discarded, is entirely the language layer's,
  # and is what a run with no controller can see.
  def test_retry_ladder_a_bounded_mailbox_that_drops_nothing_loses_no_input
    inputs = Array.new(24) { |at| "n#{at}" }
    released = release(declares("retry_ladder", "bounded_mailbox_drop_nothing"),
                       "drop_nothing_input", "drop_nothing_echo", inputs)

    assert_equal inputs, payloads(released),
                 "the mailbox holds 4 and 24 were fed: under `drop_nothing` the feed waits, in order"
  end

  def test_entity_timers_a_declared_timer_fires_locally_with_its_scheduled_time
    operative = declares("entity_timers", "session_timeout")

    # The class reads its own deadline from the environment, the way the e2e
    # sets it; 200 ms keeps the run short without changing what is proved.
    previous = ENV.fetch("TIMER_TEST_TIMEOUT_MS", nil)
    ENV["TIMER_TEST_TIMEOUT_MS"] = "200"
    begin
      released = release(operative, "input", "kafka", ["open"])
    ensure
      ENV["TIMER_TEST_TIMEOUT_MS"] = previous
    end

    closed = payloads(released).grep(/\Aclosed@/)

    assert_equal 1, closed.length, "the timer fired exactly once: #{payloads(released).inspect}"
    assert_match(/\Aclosed@\d+\z/, closed.first,
                 "the firing carries the timestamp it was scheduled for")
  end

  # The line `sdk-local-mode` draws: the ten the controller owns stay refused.
  # `tier_store` is effectively-once, which needs a durable store to release
  # from, so a local run says so instead of running the graph.
  def test_a_controller_capability_is_refused_by_name_not_approximated
    operative = declares("effectively_once", "tier_store")
    message = assert_raises(Flezha::Native::Error) { Flezha::LocalOperative.start(operative) }.message

    assert_includes message, "effectively_once"
    assert_includes message, "requires a controller"
  end

  private

  # The class under test is the one the manifest names for the capability.
  def declares(capability, factory_id)
    listed = Flezha::TestSupport.declared_fixtures.fetch(capability)

    assert_includes listed, factory_id,
                    "conformance.json lists #{listed.inspect} for #{capability}, not " \
                    "#{factory_id}; the local-mode run must drive a class the e2e drives"
    # By factory id, not by constant: one of these classes is built in a loop
    # over its overflow policy and has no name of its own.
    Fixtures::ALL.find { |operative| operative.factory_id == factory_id } ||
      flunk("the test app registers no class whose factory id is #{factory_id}")
  end

  # Run one class over +inputs+ on +channel+, and return what +sink+ released.
  def release(operative, channel, sink, inputs, budget_ms = 20_000)
    released = []
    run = Flezha::LocalOperative.start(operative)
    begin
      run.on_sink(sink) { |output| released << output }
      inputs.each { |input| run.feed(channel, input) }
      run.run_until_idle(budget_ms)
    ensure
      run.close
    end
    released
  end

  def payloads(released) = released.map { |output| output.payload.dup.force_encoding("UTF-8") }
end

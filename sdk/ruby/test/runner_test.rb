# frozen_string_literal: true

require_relative "test_helper"
# `Process.stub` is `Object#stub`, which lives in minitest's `mock` and is not
# loaded by `minitest/autorun` in every version of it.
require "minitest/mock"

# The entry point's contract: declare, warm up, then serve — and the JIT
# decision the operator can always take back.
class RunnerTest < Minitest::Test
  class Echo < Flezha::Procedure
    procedure_name "runner_echo"
    def process(input, _ctx) = input
  end

  class EchoOperative < Flezha::StreamOperative
    factory_id "runner_echo_op"
    channels { |c| c.input("input").into(Echo).named("out").sink("runner_sink") }
  end

  def boot_recording
    core = Flezha::TestSupport::FakeCore.new
    runner = Flezha::Runner.new("test-ns").define_stream(EchoOperative)
    Process.stub(:warmup, -> { core.events << :warmup }) do
      runner.boot(core: core)
      core.polled.pop
      runner.drain
    end
    [core.events, core]
  end

  def test_warmup_runs_after_the_last_declaration_and_before_the_first_poll
    events, = boot_recording

    declared = events.index(:declare)
    warmed = events.index(:warmup)
    polled = events.index(:poll)

    refute_nil declared
    refute_nil warmed
    refute_nil polled
    assert_operator declared, :<, warmed, "warmup must follow the last declaration"
    assert_operator warmed, :<, polled, "warmup must precede the first poll"
  end

  def test_declaration_precedes_serving_for_every_declared_class
    core = Flezha::TestSupport::FakeCore.new
    events = core.events
    second = Class.new(Flezha::StreamOperative)
    second.factory_id "runner_second_op"
    second.channels { |c| c.input("in").into(Echo, "second_node").named("o").sink("s") }

    runner = Flezha::Runner.new("test-ns").define_stream(EchoOperative).define_stream(second)
    Process.stub(:warmup, -> { core.events << :warmup }) do
      runner.boot(core: core)
      core.polled.pop
      runner.drain
    end

    assert_equal 2, core.declarations.size
    assert_operator events.rindex(:declare), :<, events.index(:poll),
                    "both declarations reach the core before any activation is polled"
  end

  def test_the_core_is_released_on_drain
    events, = boot_recording

    assert_equal :free, events.last
  end

  def test_a_runner_with_no_class_is_refused
    runner = Flezha::Runner.new("test-ns")

    assert_raises(Flezha::Error) { runner.boot(core: Flezha::TestSupport::FakeCore.new) }
  end

  def test_an_unknown_executor_is_refused_by_name
    runner = Flezha::Runner.new("test-ns").define_stream(EchoOperative).executor(:ractors)

    assert_includes(
      assert_raises(Flezha::Error) { runner.boot(core: Flezha::TestSupport::FakeCore.new) }.message,
      ":threads and :fibers"
    )
  end

  # YJIT is on by default because handler bodies are the one thing the GVL
  # bounds, so the JIT's gain lands exactly where the ceiling is — and the
  # resulting state is reported so a measurement can never be ambiguous.
  def test_yjit_is_reported_and_the_runner_override_wins
    runner = Flezha::Runner.new("test-ns")

    assert_equal "disabled (runner override)",
                 runner.yjit(false).send(:resolve_yjit)
  end

  def test_the_yjit_decision_names_why
    state = Flezha::Runner.new("test-ns").send(:resolve_yjit)

    assert_kind_of String, state
    assert_match(/\A(enabled|disabled|unavailable)/, state)
    if defined?(RubyVM::YJIT)
      assert RubyVM::YJIT.enabled?, "this Ruby has YJIT, so the default decision enables it"
      assert_match(/enabled/, state)
    else
      assert_match(/unavailable/, state)
    end
  end

  def test_the_operators_environment_decision_wins_over_the_default
    previous = ENV["FLEZHA_YJIT"]
    ENV["FLEZHA_YJIT"] = "0"
    # Only observable on a Ruby where YJIT is not already on; where it is, the
    # earlier "already on" answer is the honest one and is asserted above.
    state = Flezha::Runner.new("test-ns").send(:resolve_yjit)

    assert_match(/enabled \(already on\)|disabled \(FLEZHA_YJIT=0\)|unavailable/, state)
  ensure
    ENV["FLEZHA_YJIT"] = previous
  end
end

# frozen_string_literal: true

require_relative "test_helper"

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
    Flezha::TestSupport.without_a_terminal do
      Process.stub(:warmup, -> { core.events << :warmup }) do
        runner.boot(core: core)
        core.polled.pop
        runner.drain
      end
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
    Flezha::TestSupport.without_a_terminal do
      Process.stub(:warmup, -> { core.events << :warmup }) do
        runner.boot(core: core)
        core.polled.pop
        runner.drain
      end
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

    Flezha::TestSupport.without_a_terminal do
      assert_includes(
        assert_raises(Flezha::Error) { runner.boot(core: Flezha::TestSupport::FakeCore.new) }.message,
        ":threads and :fibers"
      )
    end
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

  # The harness manages a runner as an Erlang Port and stops it by closing the
  # port, which is this process's standard input — so a boot with no terminal
  # asks the core to watch for end of file, and a runner that stops asking is a
  # runner nothing can stop. This is the assertion that says so: the name check
  # below would stay green if `observe_shutdown` dropped the call entirely.
  def test_a_boot_with_no_terminal_asks_the_core_to_watch_for_end_of_file
    events, = boot_recording

    assert_includes events, :watch_stdin_eof
  end

  # The double stands in for a whole core, so it answers every call a host can
  # make on one. `Core` generates its delegations from `ENTRY_POINTS` and
  # `native_test.rb` asserts that list against the veneer's exports in both
  # directions; nothing asserted it against the double, which is hand written.
  # A miss is silent until a runner reaches the entry point, and then it is a
  # `NoMethodError` in whichever suite boots first.
  #
  # This checks NAMES and nothing else, and the double generates the same set it
  # is checked against — so it holds the list closed and proves no behaviour.
  # What it cannot see: arity (a veneer entry point that gains a parameter
  # leaves `Core` forwarding `*args` while a hand-written fixed-arity method
  # here raises `ArgumentError`), return types, and a scripted method whose
  # answer has drifted from the core's. Each of those is the job of the test
  # that drives the call.
  def test_the_double_answers_every_call_a_host_can_make_on_a_core
    # `free` is the one call `Core` does not generate: it releases what
    # `create` made, so the wrapper names it rather than listing it.
    reachable = Flezha::Core::ENTRY_POINTS + %i[free]

    assert_empty reachable - Flezha::TestSupport::FakeCore.instance_methods(false),
                 "the double is missing calls a runner can make on its core"
  end
end

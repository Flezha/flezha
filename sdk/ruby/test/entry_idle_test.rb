# frozen_string_literal: true

require_relative "test_helper"
require "minitest/mock"

# The observer's host half — registration, the tick, contained delivery and
# the accessor's mapping — against a scripted core. The latch itself is the
# core's and is proved there; what this proves is the wiring over it.
class EntryIdleTest < Minitest::Test
  GAP_NANOS = 1_500_000_000

  # A core whose polls answer a script — the ABI's statuses in order, then none.
  class ScriptedCore
    attr_reader :freed

    def initialize(answers, stamped: 0)
      @answers = answers
      @stamped = stamped
      @freed = []
      @next = 0
    end

    def monotonic_nanos = 10**12
    def entry_last_delivery_nanos(channel) = channel == "fed" ? @stamped : 0
    def idle_watch_new(channel, _threshold_nanos) = channel.empty? ? 0 : (@next += 1)

    def idle_watch_poll(_id)
      status = @answers.shift || 0
      [status, status.positive? ? GAP_NANOS : 0]
    end

    def idle_watch_free(id) = @freed << id
  end

  class Recorder
    attr_reader :seen

    def initialize = @seen = []
    def idle(gap_ms) = @seen << [:idle, gap_ms]
    def resumed(gap_ms) = @seen << [:resumed, gap_ms]
  end

  class RecordingLogger
    attr_reader :lines

    def initialize = @lines = []
    def debug(_message) = nil
    def info(_message) = nil
    def error(_message) = nil
    def warn(message) = @lines << message
  end

  class Echo < Flezha::Procedure
    procedure_name "entry_idle_echo"
    def process(input, _ctx) = input
  end

  class EchoOperative < Flezha::StreamOperative
    factory_id "entry_idle_echo_op"
    channels { |c| c.input("input").into(Echo).named("out").sink("entry_idle_sink") }
  end

  def watcher_over(core) = Flezha::EntryIdleWatcher.new(core, ticking: false)

  def test_one_idle_and_one_resumed_reach_the_observer_in_the_order_the_core_answers
    watcher = watcher_over(ScriptedCore.new([0, 1, 0, 2]))
    recorder = Recorder.new
    watcher.register("trades_in", 1_000, recorder)
    4.times { watcher.tick }

    assert_equal [[:idle, 1500.0], [:resumed, 1500.0]], recorder.seen
    watcher.stop
  end

  def test_a_block_is_an_observer_whose_resume_does_nothing
    watcher = watcher_over(ScriptedCore.new([1, 2, 1]))
    gaps = []
    watcher.register("trades_in", 1_000) { |gap| gaps << gap }
    3.times { watcher.tick }

    assert_equal [1500.0, 1500.0], gaps
    watcher.stop
  end

  def test_a_raising_observer_is_logged_and_the_next_episode_still_reaches_it
    previous = Flezha.log
    logger = RecordingLogger.new
    Flezha.log = logger
    watcher = watcher_over(ScriptedCore.new([1, 2, 1]))
    calls = 0
    observer = Object.new
    observer.define_singleton_method(:idle) do |_gap|
      calls += 1
      raise "observer under test"
    end
    watcher.register("book_in", 1_000, observer)
    3.times { watcher.tick }

    assert_equal 2, calls, "the episode after the raise still reaches the observer"
    assert_equal 2, logger.lines.size
    assert_match(
      /entry-idle observer raised on the idle episode of channel 'book_in': observer under test/,
      logger.lines.first
    )
    watcher.stop
  ensure
    Flezha.log = previous
  end

  def test_stop_frees_every_watch_the_core_handed_out_and_is_idempotent
    core = ScriptedCore.new([])
    watcher = watcher_over(core)
    watcher.register("a", 1_000) { nil }
    watcher.register("b", 2_000) { nil }
    watcher.stop

    assert_equal [1, 2], core.freed
    watcher.stop
    assert_equal [1, 2], core.freed
  end

  def test_the_first_registration_starts_the_tick_thread_and_stop_ends_it
    watcher = Flezha::EntryIdleWatcher.new(ScriptedCore.new([]))
    watcher.register("a", 1_000) { nil }
    thread = Thread.list.find { |t| t.name == "flezha-entry-idle" }

    refute_nil thread, "the first registration starts the tick thread"
    watcher.stop
    refute_predicate thread, :alive?, "stop ends the tick thread"
  end

  def test_a_refused_name_a_non_positive_threshold_and_a_missing_observer_are_refused
    watcher = watcher_over(ScriptedCore.new([]))

    assert_raises(Flezha::Error) { watcher.register("", 1_000) { nil } }
    assert_raises(ArgumentError) { watcher.register("x", 0) { nil } }
    assert_raises(ArgumentError) { watcher.register("x", 1_000) }
  end

  def test_the_tick_is_a_quarter_of_the_threshold_within_its_bounds
    assert_in_delta 1.0, Flezha::EntryIdleWatcher.tick_period_s(60_000)
    assert_in_delta 0.5, Flezha::EntryIdleWatcher.tick_period_s(2_000)
    assert_in_delta 0.25, Flezha::EntryIdleWatcher.tick_period_s(10)
  end

  def test_the_accessor_is_absent_before_the_first_frame_and_an_instant_from_the_gap_after
    core = ScriptedCore.new([], stamped: (10**12) - (2 * (10**9)))

    assert_nil Flezha::EntryIdleWatcher.last_entry_delivery(core, "never")
    before = Time.now
    at = Flezha::EntryIdleWatcher.last_entry_delivery(core, "fed")
    after = Time.now

    assert_kind_of Time, at
    assert_operator at, :>=, before - 2.0
    assert_operator at, :<=, after - 2.0
  end

  # The runner wires the watcher: a registration after boot reaches the core
  # the runner booted with, and `drain` frees it.
  def test_the_runner_frees_its_watches_on_drain
    core = Flezha::TestSupport::FakeCore.new
    runner = Flezha::Runner.new("test-ns").define_stream(EchoOperative)
    Process.stub(:warmup, -> {}) do
      runner.boot(core: core)

      assert_same runner, runner.on_entry_idle("input", 1_000) { nil }
      assert_nil runner.last_entry_delivery("input")
      core.polled.pop
      runner.drain
    end

    assert_includes core.events, :idle_watch_free
  end
end

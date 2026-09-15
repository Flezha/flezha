# frozen_string_literal: true

require_relative "test_helper"

# The author's harness, and the property it exists for: declared state is
# mutated only through declared changes, and this SDK has no runtime guard
# enforcing it — so `replay_equals!` is what makes the rule safe.
class TestingTest < Minitest::Test
  class Counter < Flezha::Procedure
    procedure_name "harness_counter"
    def initial_state(_entity_key) = { total: 0, seen: [] }

    def apply_change(state, change)
      state[:total] += change[:by]
      state[:seen] << change[:by]
    end

    def process(input, ctx)
      ctx.change({ by: Integer(input) })
      ctx.state[:total]
    end

    def on_timer(_firing, ctx) = ctx.change({ by: 100 })
    def compensate(outputs, ctx) = ctx.change({ by: -outputs.sum })
  end

  # Mutates the state without recording a change: live in this process, gone
  # after a restart, and reported nowhere until then.
  class Unrecorded < Counter
    procedure_name "harness_unrecorded"
    def process(input, ctx)
      ctx.state[:total] += Integer(input)
      ctx.state[:total]
    end
  end

  # Records the change but folds it impurely — the fold reads something
  # neither the state nor the change carries, so a replay, where the handler
  # never ran, produces a different answer.
  class ImpureFold < Counter
    procedure_name "harness_impure"
    def process(input, ctx)
      @handled = (@handled || 0) + 1
      super
    end

    def apply_change(state, change)
      state[:total] += change[:by] * (@handled || 0)
      state[:seen] << change[:by]
    end
  end

  def test_the_harness_runs_the_procedure_and_records_what_it_produced
    harness = Flezha::Testing::Harness.new(Counter)

    first = harness.feed("3")
    assert_equal [3], first.outputs
    assert_equal [{ by: 3 }], first.changes

    harness.feed("4")
    assert_equal({ total: 7, seen: [3, 4] }, harness.state)
  end

  def test_replay_equals_agrees_for_a_recorded_pure_fold
    harness = Flezha::Testing::Harness.new(Counter)
    %w[1 2 3].each { |input| harness.feed(input) }

    assert harness.replay_equals!
  end

  def test_replay_equals_catches_a_mutation_that_was_never_recorded
    harness = Flezha::Testing::Harness.new(Unrecorded)
    harness.feed("5")

    error = assert_raises(Flezha::Testing::ReplayMismatch) { harness.replay_equals! }
    assert_includes error.message, "harness_unrecorded"
    assert_includes error.message, "without recording a change"
  end

  def test_replay_equals_catches_an_impure_fold
    harness = Flezha::Testing::Harness.new(ImpureFold)
    harness.feed("1")
    harness.feed("1")

    assert_raises(Flezha::Testing::ReplayMismatch) { harness.replay_equals! }
  end

  def test_a_timer_firing_reaches_on_timer
    harness = Flezha::Testing::Harness.new(Counter)
    harness.fire_timer(:processing_time, 900)

    assert_equal 100, harness.state[:total]
    assert harness.replay_equals!
  end

  def test_a_registered_timer_is_anchored_on_now
    inner = Class.new(Flezha::Procedure) do
      procedure_name "timer_registrar"
      def process(_input, ctx) = ctx.timer_after(50)
    end
    harness = Flezha::Testing::Harness.new(inner)

    outcome = harness.feed("x", now: 1_000)

    assert_equal 1_050, outcome.returned, "the timer anchors on now, never an ambient read"
    assert_equal [{ kind: "processing_time", scheduled_at: 1_050, register: true }], outcome.timers
  end

  def test_compensation_runs_over_recorded_forward_outputs
    harness = Flezha::Testing::Harness.new(Counter)
    harness.feed("10")
    harness.compensate([4, 6])

    assert_equal 0, harness.state[:total]
  end

  # The harness restores through the same function the dispatcher answers a
  # restore fold with, so it proves what a warm restart does.
  def test_restore_rebuilds_the_state_from_a_change_log
    harness = Flezha::Testing::Harness.new(Counter)
    harness.restore(changes: [{ by: 2 }, { by: 5 }].map { |c| Flezha::Codec.encode(c) })

    assert_equal({ total: 7, seen: [2, 5] }, harness.state)
  end

  def test_a_spawn_on_a_replayed_invocation_is_skipped
    spawner = Class.new(Flezha::Procedure) do
      procedure_name "spawner"
      def process(input, ctx) = ctx.spawn_workflow("i-1", "wf", input)
    end
    harness = Flezha::Testing::Harness.new(spawner)

    assert_equal "spawned", harness.feed("x").returned
    assert_equal "skipped_on_replay", harness.feed("x", replay: true).returned,
                 "a spawn is an external effect; a replay must not issue it twice"
  end

  def test_stable_id_is_replay_stable_within_an_invocation
    identifier = Class.new(Flezha::Procedure) do
      procedure_name "identifier"
      def process(_input, ctx) = [ctx.stable_id, ctx.stable_id]
    end

    first = Flezha::Testing::Harness.new(identifier).feed("x").returned
    second = Flezha::Testing::Harness.new(identifier).feed("x").returned

    assert_equal first, second, "the n-th call of a replayed invocation equals the n-th original"
    refute_equal first[0], first[1], "repeated calls in one invocation differ"
    assert_equal 32, first[0].length
  end

  class Emitter < Flezha::Procedure
    procedure_name "harness_emitter"

    def process(input, ctx)
      ctx.emit({ said: input })
      "echo: #{input}"
    end
  end

  # The harness reads its sink back as MessagePack, so a string output is
  # reported as the string rather than as the bytes a plugin would receive.
  def test_the_harness_reports_a_string_and_a_map_exactly_as_emitted
    assert_equal [{ said: "hi" }, "echo: hi"], Flezha::Testing::Harness.new(Emitter).feed("hi").outputs
  end
end

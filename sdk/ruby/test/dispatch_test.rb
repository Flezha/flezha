# frozen_string_literal: true

require_relative "test_helper"

# Every activation kind the boundary defines is answered with its own outcome.
#
# A kind that fell through to the unknown-kind refusal would retire the entity
# it addressed for the life of the process, and the first sign of it would be a
# stalled operative — so the sweep is over the whole vocabulary rather than the
# kinds one fixture happens to exercise.
class DispatchTest < Minitest::Test
  class Counter < Flezha::Procedure
    procedure_name "counter"
    def initial_state(_key) = { total: 0, seen: [] }
    def apply_change(state, change) = state[:total] += change[:by]

    def process(input, ctx)
      ctx.change({ by: Integer(input) })
      "total=#{ctx.state[:total]}"
    end

    def on_timer(firing, ctx) = ctx.change({ by: firing[:scheduled_at].zero? ? 0 : 1 })
    def compensate(outputs, ctx) = ctx.change({ by: -outputs.size })
  end

  class CounterOperative < Flezha::StreamOperative
    factory_id "counter_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .into(Counter).named("out").sink("counter_sink", encode: ->(v) { v.to_s.b })
    end
  end

  def setup
    @core = Flezha::TestSupport::FakeCore.new
    @deps = Flezha::TestSupport.deps(CounterOperative, @core)
    @registry = @deps.registry
  end

  def activation(kind, **extra)
    Flezha::TestSupport.activation(factory: "counter_op", kind: kind, **extra)
  end

  def answer(kind, **extra) = Flezha::Dispatch.answer(activation(kind, **extra), @deps)
  def input_item(payload, **extra) = Flezha::TestSupport.input_item(payload, **extra)

  def test_a_work_batch_answers_one_effect_per_item
    result = answer(:work, items: [input_item("2"), input_item("3")])

    assert_equal "work", result[:outcome]
    assert_equal 2, result[:effects].size
    assert_equal %w[ok ok], result[:effects].map { |e| e[:verdict] }
    assert_equal 1, result[:effects][0][:outputs].size
    # The sink declares a text encoder, so the frame is the bytes it made.
    assert_equal "total=5", result[:effects][1][:outputs][0][:payload]
  end

  def test_a_raised_handler_stops_the_batch_and_reports_a_retryable_verdict
    result = answer(:work, items: [input_item("1"), input_item("not a number"), input_item("1")])

    assert_equal 2, result[:effects].size, "items after the failure are re-handed by the core"
    assert_equal "retryable", result[:effects][1][:verdict]
    assert_includes result[:effects][1][:message], "ArgumentError"
  end

  def test_a_timer_item_reaches_on_timer_with_its_scheduled_time_as_now
    result = answer(:work, items: [{ kind: :timer, timer_kind: :processing_time,
                                     scheduled_at: 4_242, now: 4_242 }])

    assert_equal "ok", result[:effects][0][:verdict]
    assert_equal 4_242, result[:effects][0][:event_time]
  end

  def test_a_restore_fold_rebuilds_the_state_from_its_change_log
    changes = [{ by: 5 }, { by: 6 }].map { |c| { bytes: Flezha::Codec.encode(c) } }

    assert_equal "restored", answer(:restore_fold, changes: changes)[:outcome]

    result = answer(:work, items: [input_item("1")])
    assert_equal "total=12", result[:effects][0][:outputs][0][:payload]
  end

  def test_a_snapshot_answers_the_encoded_state
    answer(:work, items: [input_item("7")])
    result = answer(:snapshot_request)

    assert_equal "snapshot", result[:outcome]
    assert_equal 7, Flezha::Codec.decode(result[:state])[:total]
  end

  def test_a_live_state_read_and_a_migration_serialize_answer_json
    answer(:work, items: [input_item("3")])

    %i[live_state_read migration_serialize].each do |kind|
      result = answer(kind)
      assert_equal({ live_state_read: "state_json", migration_serialize: "migration_state" }[kind],
                   result[:outcome])
      assert_equal 3, Flezha::Codec.from_state_json(result[:json])[:total]
    end
  end

  def test_apply_state_materializes_a_migrated_state
    json = Flezha::Codec.to_state_json({ total: 99, seen: [] })
    assert_equal "state_applied", answer(:apply_state, state_json: json.b)[:outcome]

    result = answer(:work, items: [input_item("1")])
    assert_equal "total=100", result[:effects][0][:outputs][0][:payload]
  end

  def test_a_key_request_answers_one_prepare_answer_per_payload
    result = answer(:key_request,
                    key_request: { channel: 0, payloads: ["1".b, "2".b],
                                   want_keys: true, want_event_time: false })

    assert_equal "prepared", result[:outcome]
    assert_equal 2, result[:prepared].size
    assert_equal [[], []], result[:prepared].map { |a| a[:keys] },
                 "a single-entity procedure names no key"
  end

  def test_a_compensate_run_folds_the_recorded_outputs
    result = answer(:compensate_run, outputs: [Flezha::Codec.encode("a"), Flezha::Codec.encode("b")])

    assert_equal "work", result[:outcome]
    assert_equal "ok", result[:effects][0][:verdict]
  end

  def test_an_unknown_factory_is_refused_non_retryably
    result = Flezha::Dispatch.answer(activation(:work, factory: "nobody", items: []), @deps)

    assert_equal "failed", result[:outcome]
    assert result[:non_retryable]
  end

  def test_every_activation_kind_is_answered
    # The sweep: no kind may reach the unknown-kind arm.
    kinds = {
      work: { items: [] },
      restore_fold: { changes: [] },
      snapshot_request: {},
      live_state_read: {},
      migration_serialize: {},
      apply_state: { state_json: Flezha::Codec.to_state_json({ total: 0 }).b },
      key_request: { key_request: { channel: 0, payloads: [], want_keys: false,
                                    want_event_time: false } },
      compensate_run: { outputs: [] }
    }

    kinds.each do |kind, extra|
      result = answer(kind, **extra)
      refute_equal "failed", result[:outcome], "#{kind} fell through to a refusal"
    end
  end

  def test_a_workflow_run_reaches_the_run_body
    workflow = Class.new(Flezha::WorkflowOperative)
    workflow.factory_id "wf"
    workflow.run { |args, ctx| ctx.complete("ran:#{args}") }
    @registry.add(workflow.spec)

    result = Flezha::Dispatch.answer(
      activation(:workflow_run, factory: "wf", args: Flezha::Codec.encode("seed")), @deps
    )

    assert_equal "workflow_done", result[:outcome]
    assert result[:workflow_completed]
    assert_equal "ran:seed", Flezha::Codec.decode(result[:workflow_result])
  end

  # The handout contract, against the collaborator that decides it. A real core
  # created without `start` never handed these tokens out, so it is the core
  # itself that says so — a fake that accepts whatever it is handed cannot.
  #
  # An unanswered activation retires its entity for the life of the process, so
  # the guard is the contract rather than politeness.
  def test_an_activation_the_core_never_handed_out_is_abandoned_not_dropped
    skip "needs the compiled veneer" unless Flezha::TestSupport.native?

    core = Flezha::Core.create
    said = reported { Flezha::Dispatch.dispatch(activation(:work, items: []), core, @deps) }
    core.free

    assert_match(/unknown activation token 1/, said)
    assert_match(/abandoning activation 1 failed/, said)
  end

  def test_a_dispatch_that_completes_is_not_abandoned
    skip "needs the compiled veneer" unless Flezha::TestSupport.native?

    # Only `complete` is scripted, and only to reach the path a real handout
    # would reach: the abandon is the core's own, so a guard that abandoned an
    # answered activation would earn its refusal and report it.
    core = Flezha::Core.create
    peer = Flezha::TestSupport::FakeCore.new
    peer.define_singleton_method(:abandon) { |token| core.abandon(token) }
    said = reported { Flezha::Dispatch.dispatch(activation(:work, items: []), peer, @deps) }
    core.free

    assert_equal 1, peer.completed.size
    assert_equal "", said
  end

  # What the dispatcher said while it ran. Its guard never raises, so the log
  # line is the only evidence there is.
  class RecordingLog
    def initialize = @lines = []
    def to_s = @lines.join("\n")

    %i[debug info warn error].each { |level| define_method(level) { |message| @lines << message } }
  end

  def reported
    previous = Flezha.log
    Flezha.log = RecordingLog.new
    yield
    Flezha.log.to_s
  ensure
    Flezha.log = previous
  end
end

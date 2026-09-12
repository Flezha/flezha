# frozen_string_literal: true

# Twins of the rust_runner e2e fixtures (test/apps/rust_runner/src/main.rs)
# under the same factory ids, procedure names, channel names and output bytes.
#
# Entry payloads and plugin frames are UTF-8 text on the wire, the same as the
# reference: the SDK's default codec is MessagePack, so every fixture that
# exchanges bytes with another SDK or with a plugin declares its own decoder
# and encoder. An inter-procedure hop between two Ruby procedures carries
# MessagePack, which no one else reads.

require "flezha"

# The persisted-state family is registered from its own file.
require_relative "fixtures_state"

module Fixtures
  TEXT = ->(bytes) { bytes.dup.force_encoding(Encoding::UTF_8) }
  BYTES = ->(value) { value.to_s.b }

  # The key convention every keyed fixture shares: text before the first ':'.
  PREFIX = ->(input) { input.to_s.split(":").first.to_s }

  # -- do_nothing / bench_echo ---------------------------------------------

  class DoNothing < Flezha::Procedure
    procedure_name "do_nothing_procedure"
    def process(input, _ctx) = input
  end

  class DoNothingOperative < Flezha::StreamOperative
    factory_id "do_nothing"
    channels do |c|
      c.input("input", decode: TEXT).into(DoNothing)
       .sink("echo", encode: BYTES)
    end
  end

  # Echo with a declared DropNothing on its entry mailbox. `do_nothing` is the
  # merge-blocking bench factory and must not change; this keeps the round-trip
  # bench's back-pressure posture independent of it.
  class BenchEchoOperative < Flezha::StreamOperative
    factory_id "bench_echo"
    channels do |c|
      c.input("input", decode: TEXT).on_full(:drop_nothing)
       .into(DoNothing)
       .sink("echo", encode: BYTES)
    end
  end

  # -- compare_filter -------------------------------------------------------

  # State seeded by the controller's init values and then only read: there is
  # nothing to journal, so the procedure declares no change fold.
  class CompareFilter < Flezha::Procedure
    procedure_name "compare_filter_procedure"
    def initial_state(_entity_key) = { comparable: "default", inverted: false }
    def process(input, ctx) = ctx.state[:comparable] == input ? input : nil
  end

  class CompareFilterOperative < Flezha::StreamOperative
    factory_id "compare_filter"
    channels do |c|
      c.input("input", decode: TEXT).into(CompareFilter)
       .sink("sink", encode: BYTES)
    end
  end

  # -- static_entity / sugar_static_entity / mixed_operative ---------------

  STATIC_KEYS = %w[a b c d].freeze

  # The static fan-out every static fixture shares.
  module StaticFanOut
    def entities_for(input)
      first = input.to_s[0]
      [STATIC_KEYS.include?(first) ? first : "d"]
    end
  end

  class StaticEntry < Flezha::Procedure
    include StaticFanOut
    procedure_name "static_entry_procedure"
    entity_keys(*STATIC_KEYS)
    def initial_state(_entity_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]

    def process(input, ctx)
      ctx.change({ push: input })
      input
    end
  end

  class MacroStaticEntry < StaticEntry
    procedure_name "macro_static_entry_procedure"
  end

  class PassThrough < Flezha::Procedure
    def process(input, _ctx) = input
  end

  class StaticDownstream < PassThrough
    procedure_name "static_downstream_procedure"
  end

  class MacroStaticDownstream < PassThrough
    procedure_name "macro_static_downstream_procedure"
  end

  class StaticEntityOperative < Flezha::StreamOperative
    factory_id "static_entity"
    channels do |c|
      c.input("input", decode: TEXT).into(StaticEntry).named("hop")
       .into(StaticDownstream)
       .sink("kafka", encode: BYTES)
    end
  end

  # The reference pipeline sugar leaves the hop unnamed and lets the sink name
  # the producing channel.
  class SugarStaticEntityOperative < Flezha::StreamOperative
    factory_id "sugar_static_entity"
    channels do |c|
      c.input("input", decode: TEXT).into(MacroStaticEntry)
       .into(MacroStaticDownstream)
       .sink("test_kafka", encode: BYTES)
    end
  end

  class MixedMacroEntry < PassThrough
    procedure_name "mixed_macro_entry_procedure"
  end

  class MixedHandDownstream < PassThrough
    procedure_name "mixed_hand_downstream"
  end

  class MixedOperative < Flezha::StreamOperative
    factory_id "mixed_operative"
    channels do |c|
      c.input("input", decode: TEXT).into(MixedMacroEntry).named("hop")
       .into(MixedHandDownstream)
       .sink("kafka", encode: BYTES)
    end
  end

  # -- sugar_do_nothing / sugar_store_input --------------------------------

  class SugarDoNothing < PassThrough
    procedure_name "sugar_do_nothing"
  end

  class SugarDoNothingOperative < Flezha::StreamOperative
    factory_id "sugar_do_nothing"
    channels do |c|
      c.input("input", decode: TEXT).into(SugarDoNothing)
       .sink("sugar_do_nothing_echo", encode: BYTES)
    end
  end

  class SugarStoreInput < Flezha::Procedure
    procedure_name "sugar_store_input"
    def initial_state(_entity_key) = { text_inputs: [] }
    def apply_change(state, change) = state[:text_inputs] << change[:push]

    def process(input, ctx)
      ctx.change({ push: input })
      input
    end
  end

  class SugarStoreInputOperative < Flezha::StreamOperative
    factory_id "sugar_store_input"
    channels do |c|
      c.input("input", decode: TEXT).into(SugarStoreInput)
       .sink("sugar_store_sink", encode: BYTES)
    end
  end

  # -- hybrid_channel ------------------------------------------------------
  #
  # One emit reaching two destinations: an inter-procedure consumer and a
  # plugin wire with its own serializer. The serializer refuses a sentinel
  # value, so an operator can observe end to end that a refusal is isolated to
  # the emit rather than failing the item.

  HYBRID_SENTINEL = 0xdeadbeef

  class HybridProducer < Flezha::Procedure
    procedure_name "hybrid_producer_procedure"
    def process(input, ctx)
      parsed = Integer(input.to_s, exception: false) || 0
      value = parsed.between?(0, 0xffffffff) ? parsed : 0
      begin
        ctx.emit(value)
      rescue StandardError => e
        warn("hybrid_producer_procedure: serializer refused an emit: #{e.message}")
      end
      nil
    end
  end

  class HybridConsumer < Flezha::Procedure
    procedure_name "hybrid_consumer_procedure"
    def process(input, _ctx)
      puts("hybrid_consumer_procedure: received #{input}")
      nil
    end
  end

  HYBRID_ENCODE = lambda do |value|
    if value == HYBRID_SENTINEL
      raise "hybrid_indicators: serializer rejected sentinel 0xDEADBEEF"
    end

    [value].pack("N")
  end

  class HybridChannelOperative < Flezha::StreamOperative
    factory_id "hybrid_channel"
    channels do |c|
      c.input("input", decode: TEXT).into(HybridProducer).branch(
        # The consumer edge first, so a refused plugin serialization leaves the
        # in-process fan-out already recorded — the reference's isolation.
        ->(edge) { edge.named("hybrid_indicator").into(HybridConsumer) },
        ->(edge) { edge.sink(encode: HYBRID_ENCODE) }
      )
    end
  end

  # -- entity_stress -------------------------------------------------------

  STRESS_KEYS = %w[p0 p1 p2 p3 p4 p5 p6 p7].freeze

  class StressA < Flezha::Procedure
    procedure_name "stress_a"
    entity_keys(*STRESS_KEYS)
    def entities_for(_input) = STRESS_KEYS
    def process(input, _ctx) = input
  end

  class StressToP0 < Flezha::Procedure
    entity_keys(*STRESS_KEYS)
    def entities_for(_input) = ["p0"]
    def process(input, _ctx) = input
  end

  class StressB < StressToP0
    procedure_name "stress_b"
  end

  class StressC < StressToP0
    procedure_name "stress_c"
  end

  class StressD < StressToP0
    procedure_name "stress_d"
  end

  class EntityStressOperative < Flezha::StreamOperative
    factory_id "entity_stress"
    channels do |c|
      c.input("input", decode: TEXT).into(StressA).named("a_to_b")
       .into(StressB).named("b_to_c")
       .into(StressC).named("c_to_d")
       .into(StressD)
       .sink("echo", encode: BYTES)
    end
  end

  # -- poison_probe / wedge_probe ------------------------------------------

  class PoisonProbe < Flezha::Procedure
    procedure_name "poison_probe_procedure"
    ANTIDOTE = { taken: false }

    def process(input, _ctx)
      ANTIDOTE[:taken] = true if input == "antidote"
      raise "poison payload" if input == "poison" && !ANTIDOTE[:taken]

      "ok:#{input}"
    end
  end

  class PoisonProbeOperative < Flezha::StreamOperative
    factory_id "poison_probe"
    channels do |c|
      c.input("input", decode: TEXT).retry_backoff_ms(25)
       .into(PoisonProbe)
       .sink("out", encode: BYTES)
    end
  end

  # The `pool_wedge` fixture: the two ways a runner stops doing work, on one
  # operative.
  #
  #   "wedge" — blocks the dispatch inside user code forever, so the SDK's
  #     dispatch counter stops advancing while the heartbeat keeps reporting
  #     the runner alive: connected, heartbeating, processing nothing.
  #   "abort" — ends its activation as violently as this host allows, WITHOUT
  #     emitting, once per process. Ruby has no way for user code to kill a
  #     worker outright — the dispatcher rescues per item and the raise rides
  #     the retry ladder — so what the scenario asserts is that the Entity is
  #     served again and the aborting item does not produce twice.
  class WedgeProbe < Flezha::Procedure
    procedure_name "wedge_probe_procedure"
    ABORTED = { done: false }

    def process(input, _ctx)
      loop { sleep(3600) } if input == "wedge"

      if input == "abort" && !ABORTED[:done]
        ABORTED[:done] = true
        raise "wedge_probe: killing the worker holding this activation"
      end

      "ok:#{input}"
    end
  end

  class WedgeProbeOperative < Flezha::StreamOperative
    factory_id "wedge_probe"
    channels do |c|
      c.input("input", decode: TEXT).into(WedgeProbe)
       .sink("out", encode: BYTES)
    end
  end

  # -- candle ---------------------------------------------------------------
  #
  # Late-input correction is about fold ORDER, and the only way to prove that
  # is to look at the order. Send 100, 300, then the late 200. Without
  # correction the fold order is [100, 300, 200] and the candle is permanently
  # wrong. With it, the engine rewinds behind 200, replays the retained input
  # log sorted by event time, and the fold order becomes [100, 200, 300].

  class Candle < Flezha::Procedure
    procedure_name "candle_procedure"
    clock :event_time
    # Event times in fold order: the assertion surface.
    def initial_state(_entity_key) = { folded: [] }
    def apply_change(state, change) = state[:folded] << change[:fold]
    def event_time(input) = AS_INT.call(input.to_s.split(":", 2).first)

    def process(input, ctx)
      # `ctx.now` IS the extracted event time under an event-time clock.
      event_time = ctx.now
      ctx.change({ fold: event_time })
      # One row per candle bucket, so a corrected replay overwrites by key
      # rather than appending a second row.
      bucket = event_time / 1_000
      ctx.emit("#{bucket}=#{input}", key: "candle/#{bucket}")
      nil
    end
  end

  class CandleOperative < Flezha::StreamOperative
    factory_id "candle"
    channels do |c|
      c.effectively_once
      c.correction_window(600_000)
      c.input("input", decode: TEXT).into(Candle).keyed
       .sink("sink", encode: BYTES)
    end
  end

  # -- session_timeout ------------------------------------------------------
  #
  # The worked session-timeout shape: each input pushes the idle deadline out,
  # and the firing closes the session. Single-entity so the e2e's crash-resume
  # half exercises a durable Entity. The timeout is read from
  # `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).

  class SessionTimeout < Flezha::Procedure
    procedure_name "session_timeout_procedure"
    def initial_state(_entity_key) = { touched: [], closed_at: [] }

    def apply_change(state, change)
      if change.key?(:touched)
        state[:touched] << change[:touched]
      else
        state[:closed_at] << change[:closed]
      end
    end

    def process(input, ctx)
      ctx.change({ touched: input })
      # A processing-time timer, because the answer to "nothing has arrived"
      # cannot be an event-time one: nothing would advance the clock.
      timeout = ENV.fetch("TIMER_TEST_TIMEOUT_MS", "").to_i
      ctx.timer_after(timeout.positive? ? timeout : 1000)
      nil
    end

    def on_timer(firing, ctx)
      fired_at = firing[:scheduled_at]
      ctx.change({ closed: fired_at })
      ctx.emit("closed@#{fired_at}")
    end
  end

  class SessionTimeoutOperative < Flezha::StreamOperative
    factory_id "session_timeout"
    channels do |c|
      c.input("input", decode: TEXT).into(SessionTimeout)
       .sink("kafka", encode: BYTES)
    end
  end

  # -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} --------------

  class BoundedMailbox < Flezha::Procedure
    mailbox_capacity 4
    def initial_state(_entity_key) = { processed: 0 }
    def apply_change(state, _change) = state[:processed] += 1

    def process(input, ctx)
      ctx.change({ processed: true })
      input
    end
  end

  # One operative per overflow policy, over the same bounded procedure. The
  # capacity and the fold come from the base class: a subclass inherits every
  # declaration its parent made.
  BOUNDED_MAILBOX = %w[drop_nothing drop_newest drop_oldest].map do |policy|
    procedure = Class.new(BoundedMailbox) { procedure_name "#{policy}_test_procedure" }

    Class.new(Flezha::StreamOperative) do
      factory_id "bounded_mailbox_#{policy}"
      channels do |c|
        c.input("#{policy}_input", decode: TEXT).on_full(policy.to_sym)
         .into(procedure)
         .sink("#{policy}_echo", encode: BYTES)
      end
    end
  end.freeze

  # -- partition_relay -----------------------------------------------------
  #
  # The partitioned-operatives upstream: keyed on the wiring so it can be
  # partitioned at all, and emitting keyed so a partitioned forward target can
  # resolve the owning member. The plugin channel is named `input` because a
  # forwarded frame keeps its producing channel's name, and that is what
  # selects the receiving operative's entry channel — so the entry here is `in`.

  class PartitionRelay < Flezha::Procedure
    procedure_name "partition_relay_procedure"
    dynamic_entities
    def initial_state(_entity_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]

    def process(input, ctx)
      ctx.change({ push: input })
      ctx.emit(input, key: PREFIX.call(input))
      nil
    end
  end

  class PartitionRelayOperative < Flezha::StreamOperative
    factory_id "partition_relay"
    channels do |c|
      c.input("in", decode: TEXT).key_by { |value| PREFIX.call(value) }
       .into(PartitionRelay).keyed
       .sink("input", encode: BYTES)
    end
  end

  # -- dynamic_entity ------------------------------------------------------

  class DynamicEntity < Flezha::Procedure
    procedure_name "dynamic_entity_procedure"
    dynamic_entities idle_ttl_ms: (ENV["DYN_TEST_TTL_MS"].to_i if ENV["DYN_TEST_TTL_MS"].to_i.positive?)
    def initial_state(_entity_key) = { seen: [] }
    def apply_change(state, change) = state[:seen] << change[:push]

    def process(input, ctx)
      ctx.change({ push: input })
      input
    end
  end

  class DynamicEntityOperative < Flezha::StreamOperative
    factory_id "dynamic_entity"
    channels do |c|
      c.input("input", decode: TEXT).on_full(:drop_nothing)
       # Filtered before keying: a dropped value answers no key, so the core
       # never mints an entity for it.
       .filter { |input| input != "drop" }
       .key_by { |value| PREFIX.call(value) }
       .into(DynamicEntity)
       .sink("kafka", encode: BYTES)
    end
  end

  # Every operative this app registers, in declaration order.
  ALL = [
    DoNothingOperative, BenchEchoOperative, CompareFilterOperative,
    StaticEntityOperative, SugarStaticEntityOperative, MixedOperative,
    SugarDoNothingOperative, SugarStoreInputOperative, HybridChannelOperative,
    EntityStressOperative, *STATE, CandleOperative, PoisonProbeOperative,
    WedgeProbeOperative, SessionTimeoutOperative, *BOUNDED_MAILBOX,
    PartitionRelayOperative, DynamicEntityOperative
  ].freeze
end

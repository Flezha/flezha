# frozen_string_literal: true

# Fixtures whose subject is persisted Entity state: the keyed upsert the bench
# shares, and the counter and resume twins — the same partition as
# test/apps/python_runner/fixtures_state.py and the reference's state.rs.

require "flezha"

module Fixtures
  # The reference parses `i64` and reads an unparsable input as 0.
  AS_INT = ->(value) { Integer(value.to_s, exception: false) || 0 }

  # -- bench_keyed_nopersist / bench_keyed / the tier stores -----------------
  #
  # The reference's `TierStoreState` and its upsert fold: `tier_store`,
  # `checkpoint_store` and `bench_checkpoint` are the same keyed upsert under
  # three acknowledgement postures; only `process` differs.
  class KeyedUpsert < Flezha::Procedure
    def initial_state(_entity_key) = { values: {} }
    def apply_change(state, change) = state[:values][change[:key]] = change[:value]

    # Record and emit one `key:value` input under its natural key.
    def upsert(input, ctx)
      key, value = input.to_s.split(":", 2)
      ctx.change({ key: key, value: value })
      ctx.emit("#{key}=#{value}", key: key)
      nil
    end
  end

  class TierStore < KeyedUpsert
    procedure_name "tier_store_procedure"

    def process(input, ctx)
      # Dead-letter e2e: a "poison" value fails every attempt, exhausting the
      # retry budget so the tier-on atomic park advances the entity past it.
      raise "poison value" if input.to_s.split(":", 2)[1] == "poison"

      upsert(input, ctx)
    end
  end

  # `tier_store`'s twin, whose interval closes on a `commit:` input.
  class CheckpointStore < KeyedUpsert
    procedure_name "checkpoint_store_procedure"

    def process(input, ctx)
      if input.to_s.split(":", 2)[0] == "commit"
        ctx.checkpoint
        return nil
      end

      upsert(input, ctx)
    end
  end

  # `checkpoint_store` driven by a DECLARED cadence rather than an input.
  class BenchCheckpoint < KeyedUpsert
    procedure_name "bench_checkpoint_procedure"
    def process(input, ctx) = upsert(input, ctx)
  end

  # The same keyed upsert under both durability postures, so the round-trip
  # bench can isolate the cost of per-Entity persistence.
  class KeyedUpsertOperative < Flezha::StreamOperative
    channels do |c|
      c.input("input", decode: TEXT).on_full(:drop_nothing)
       .into(TierStore).keyed
       .sink("sink", encode: BYTES)
    end
  end

  # Per-entity persistence off: this is the heap-only posture the bench
  # isolates against the durable one.
  class BenchKeyedNoPersistOperative < KeyedUpsertOperative
    factory_id "bench_keyed_nopersist"
    persistent false
  end

  class BenchKeyedOperative < KeyedUpsertOperative
    factory_id "bench_keyed"
  end

  # The effectively-once reference: a keyed upsert under the tier. A 300 ms
  # backoff base keeps the min-watermark freeze observable in the DLQ e2e.
  # Declared DropNothing: a durable-input-log operative that shed at its
  # mailbox would hold the min-watermark on the shed offset forever while the
  # log behind it kept growing.
  class TierStoreOperative < Flezha::StreamOperative
    factory_id "tier_store"
    channels do |c|
      c.effectively_once
      c.input("input", decode: TEXT).on_full(:drop_nothing).retry_backoff_ms(300)
       .into(TierStore).keyed
       .sink("sink", encode: BYTES)
    end
  end

  class CheckpointStoreOperative < Flezha::StreamOperative
    factory_id "checkpoint_store"
    channels do |c|
      c.effectively_once
      c.checkpoint_ack
      c.input("input", decode: TEXT).into(CheckpointStore).keyed
       .sink("sink", encode: BYTES)
    end
  end

  # The same procedure declared `outputs_at_checkpoint`: the interval's outputs
  # are held with its changes and ship inside its checkpoint record.
  # `checkpoint_every` is deliberately absent so the test drives the close
  # itself — with `commit:` for the batch case, and with the delay alone for
  # the quiet-Entity case.
  class CheckpointBatchedOperative < Flezha::StreamOperative
    factory_id "checkpoint_batched_store"
    channels do |c|
      c.effectively_once
      c.checkpoint_ack
      c.outputs_at_checkpoint(1_500)
      c.input("input", decode: TEXT).into(CheckpointStore).keyed
       .sink("sink", encode: BYTES)
    end
  end

  # A positive integer from the environment, or the default.
  ENV_INT = ->(key, default) { (value = ENV.fetch(key, "").to_i).positive? ? value : default }

  # `checkpoint_store` driven by a DECLARED cadence instead of a `commit:`
  # input: acknowledging every `FLEZHA_BENCH_CHECKPOINT_EVERY` inputs (default
  # 500) per Entity. `FLEZHA_BENCH_CHECKPOINT_OUTPUTS=at_checkpoint` (with
  # `FLEZHA_BENCH_CHECKPOINT_DELAY_MS`) additionally holds the interval's
  # outputs until it closes.
  class BenchCheckpointOperative < Flezha::StreamOperative
    factory_id "bench_checkpoint"
    channels do |c|
      c.effectively_once
      c.checkpoint_every(ENV_INT.call("FLEZHA_BENCH_CHECKPOINT_EVERY", 500))
      if ENV["FLEZHA_BENCH_CHECKPOINT_OUTPUTS"] == "at_checkpoint"
        c.outputs_at_checkpoint(ENV_INT.call("FLEZHA_BENCH_CHECKPOINT_DELAY_MS", 50))
      end
      c.input("input", decode: TEXT).on_full(:drop_nothing)
       .into(BenchCheckpoint).keyed
       .sink("sink", encode: BYTES)
    end
  end

  # -- store_input -----------------------------------------------------------

  # The durable-state reference: every input is appended through a change.
  class StoreInput < Flezha::Procedure
    procedure_name "store_input_procedure"
    def initial_state(_entity_key) = { binary_inputs: [], text_inputs: [] }
    def apply_change(state, change) = state[:text_inputs] << change[:push_text]

    def process(input, ctx)
      ctx.change({ push_text: input })
      input
    end
  end

  class StoreInputOperative < Flezha::StreamOperative
    factory_id "store_input"
    channels do |c|
      c.input("input", decode: TEXT).into(StoreInput)
       .sink("sink", encode: BYTES)
    end
  end

  # -- stateful_counter / persistence_off_counter ----------------------------

  class CounterEntry < Flezha::Procedure
    procedure_name "counter_entry_procedure"
    def process(input, _ctx) = AS_INT.call(input)
  end

  class Counter < Flezha::Procedure
    procedure_name "counter_procedure"
    def initial_state(_entity_key) = { value: 0 }
    def apply_change(state, change) = state[:value] += change[:delta]

    def process(input, ctx)
      ctx.change({ delta: input })
      nil
    end
  end

  class StatefulCounterOperative < Flezha::StreamOperative
    factory_id "stateful_counter"
    channels do |c|
      c.input("input", decode: TEXT).into(CounterEntry).named("ticks").into(Counter)
    end
  end

  # The same topology with per-Entity persistence off, under the reference's
  # node names: the toggle is a class property, not a different computation.
  class PersistenceOffCounterOperative < Flezha::StreamOperative
    factory_id "persistence_off_counter"
    persistent false
    channels do |c|
      c.input("input", decode: TEXT)
       .into(CounterEntry, "persistence_off_entry_procedure").named("ticks")
       .into(Counter, "persistence_off_counter_procedure")
    end
  end

  # -- coalescing_counter ----------------------------------------------------

  # Wire format: `<interval>:<ts>:<delta>`; a missing or unparsable part reads 0.
  class TickEntry < Flezha::Procedure
    procedure_name "tick_entry_procedure"

    def process(input, _ctx)
      interval, ts, delta = input.to_s.split(":", 3)
      { interval: AS_INT.call(interval), ts: AS_INT.call(ts), delta: AS_INT.call(delta) }
    end
  end

  # The reference coalesces this change on `(interval, ts)` through a `Change`
  # trait method. The SDK carries no coalesce-key seam, so the change ships
  # uncoalesced — a throughput property, not a semantic one: the fold is
  # idempotent per `(interval, ts)` either way, so replay produces the same
  # state.
  class Tick < Flezha::Procedure
    procedure_name "tick_procedure"
    def initial_state(_entity_key) = { last: 0 }
    def apply_change(state, change) = state[:last] = change[:delta]

    def process(input, ctx)
      ctx.change(input.dup)
      nil
    end
  end

  class CoalescingCounterOperative < Flezha::StreamOperative
    factory_id "coalescing_counter"
    channels do |c|
      c.input("input", decode: TEXT).into(TickEntry).named("ticks").into(Tick)
    end
  end

  # -- resume_probe ----------------------------------------------------------

  # Emits the running total, so a resume is observable on the wire. Default +
  # change replay rebuilds the total on restore; no snapshot fast path needed.
  class ResumeProbe < Flezha::Procedure
    procedure_name "resume_probe_procedure"
    def initial_state(_entity_key) = { total: 0 }
    def apply_change(state, change) = state[:total] += change[:add]

    def process(input, ctx)
      delta = AS_INT.call(input)
      total = ctx.state[:total] + delta
      ctx.change({ add: delta })
      total.to_s
    end
  end

  class ResumeProbeOperative < Flezha::StreamOperative
    factory_id "resume_probe"
    channels do |c|
      c.input("input", decode: TEXT).into(ResumeProbe)
       .sink("total", encode: BYTES)
    end
  end

  # -- schema_evolution_v1 / schema_evolution_v2 -----------------------------
  #
  # The same class either side of a deploy that edits its own state type. Both
  # halves declare the SAME procedure name, so an operative started under v1
  # and restarted under v2 restores from the very WAL v1 wrote — which is what
  # a deploy does, with the two binaries collapsed into one fixture file.
  #
  # v1: `{total}`. v2: `{total, currency}` at schema version 2, reading down to
  # 1 through a declared migration. Output is `total:currency` so the e2e can
  # read the migrated field off the wire.

  class EvolvingCartV1 < Flezha::Procedure
    procedure_name "evolving_cart"
    def initial_state(_entity_key) = { total: 0 }
    def apply_change(state, change) = state[:total] += change[:add]

    def process(input, ctx)
      delta = AS_INT.call(input)
      total = ctx.state[:total] + delta
      ctx.change({ add: delta })
      "#{total}:"
    end
  end

  class EvolvingCartV2 < Flezha::Procedure
    procedure_name "evolving_cart"
    schema_version 2
    min_readable_schema_version 1
    migrate_state do |from_version, bytes|
      raise "no migration from #{from_version}" unless from_version == 1

      old = Flezha::Codec.decode(bytes) || {}
      { total: old.fetch(:total, 0), currency: "EUR" }
    end
    migrate_change do |from_version, bytes|
      raise "no migration from #{from_version}" unless from_version == 1

      old = Flezha::Codec.decode(bytes) || {}
      { add: old.fetch(:add, 0), note: "migrated" }
    end

    def initial_state(_entity_key) = { total: 0, currency: "" }
    def apply_change(state, change) = state[:total] += change[:add]

    def process(input, ctx)
      delta = AS_INT.call(input)
      total = ctx.state[:total] + delta
      currency = ctx.state[:currency].to_s.empty? ? "EUR" : ctx.state[:currency]
      ctx.change({ add: delta, note: "live" })
      "#{total}:#{currency}"
    end
  end

  class SchemaEvolutionV1Operative < Flezha::StreamOperative
    factory_id "schema_evolution_v1"
    channels do |c|
      c.input("input", decode: TEXT).into(EvolvingCartV1)
       .sink("total", encode: BYTES)
    end
  end

  class SchemaEvolutionV2Operative < Flezha::StreamOperative
    factory_id "schema_evolution_v2"
    channels do |c|
      c.input("input", decode: TEXT).into(EvolvingCartV2)
       .sink("total", encode: BYTES)
    end
  end

  # The persisted-state family, in declaration order.
  STATE = [
    BenchKeyedNoPersistOperative, StoreInputOperative, StatefulCounterOperative,
    PersistenceOffCounterOperative, CoalescingCounterOperative, ResumeProbeOperative,
    BenchKeyedOperative, SchemaEvolutionV1Operative, SchemaEvolutionV2Operative,
    TierStoreOperative, CheckpointStoreOperative, CheckpointBatchedOperative,
    BenchCheckpointOperative
  ].freeze
end

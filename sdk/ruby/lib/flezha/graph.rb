# frozen_string_literal: true

require "json"

require_relative "builder"
require_relative "errors"

module Flezha
  # The `HostClassDecl` the core is handed, and the verdict it answers.
  #
  # The topology is judged in ONE place, `flz_core::host::register::check`:
  # this module records what the author declared, submits it, and renders the
  # core's verdict as this SDK's own refusal. What is refused here instead is
  # what a record cannot carry — whether a Ruby class defines the method a
  # declaration promises, and whether it supplies the migration a schema floor
  # needs. Those the core cannot see.
  module Graph
    # The Ruby SDK's byte in the core's `sdk_lang` allocation table.
    SDK_LANG_RUBY = 5

    # This SDK's own released version, stamped into every durable record it
    # writes; the core packs it. `Flezha::VERSION` is the single literal
    # `lib/flezha/version.rb` holds and the gemspec already reads, so the gem
    # cannot ship a version its records disagree with.
    SDK_VERSION = Flezha::VERSION

    # MessagePack, shared with every SDK whose codec is MessagePack.
    FORMAT_ID_MSGPACK = 5

    module_function

    def refuse(spec, handler, defect)
      raise DeclarationError.new(spec.factory_id, handler, defect)
    end

    # Judge one declaration and answer what the core derived from it: the
    # channel names, kinds, consumers, sink names and acknowledgement
    # granularity it read off the record.
    #
    # The defects this SDK can see that the record cannot carry are refused
    # first, so an author reads "no event_time method" rather than a topology
    # verdict about a clock the class does declare.
    def validate(spec, core)
      check_procedures(spec)
      derived = submit(spec, core)
      bind_edges(spec, derived)
      derived
    end

    # The record, over the ABI, and the verdict back.
    #
    # `Flezha::Native::Error` is what the veneer raises; it exists because
    # `core` came from the extension that defines it.
    def submit(spec, core)
      JSON.parse(core.check_class(JSON.generate(to_host_class_decl(spec))))
    rescue Native::Error => e
      raise DeclarationError.new(spec.factory_id, nil, e.message, refusals_of(e))
    end

    # The core's own names for what it refused, read off the verdict it hands
    # back as data beside the rendered message. Empty when the failure carried
    # no verdict — a context refusal rather than a judgement on the record.
    def refusals_of(error)
      verdict = error.verdict
      return [] unless verdict

      JSON.parse(verdict).fetch("issues", []).map { |issue| issue["name"] }
    end

    # The consumers the CORE derived, each carrying the closures of the chain
    # that produced it: the builder held them aside per chain, and the derived
    # edge names the chain. Which edge survives a re-declaration is therefore
    # the core's answer, not a rule this SDK repeats.
    def bind_edges(spec, derived)
      derived["channels"].each do |channel|
        target = spec.channels[channel["index"]]
        target.consumers = channel["consumers"].map { |edge| target.chain_edges[edge["chain"]] }
      end
    end

    def check_procedures(spec)
      spec.procedures.each do |node|
        check_clocks(spec, node)
        check_schema(spec, node)
      end
    end

    def check_clocks(spec, node)
      definition = node.definition
      clock = definition.clock

      if clock == :event_time && !definition.event_time?
        refuse(spec, node.name,
               "declares an event_time clock but no event_time method; the core evaluates " \
               "the declared clock per input and has nothing to evaluate")
      end
      return unless clock != :event_time && definition.event_time?

      refuse(spec, node.name,
             "declares an event_time method with clock `#{clock}`; the extractor is only " \
             "consulted by an event_time clock, so one of the two is not what was meant")
    end

    def check_schema(spec, node)
      definition = node.definition
      floor = definition.min_readable_schema_version
      return unless floor

      running = definition.schema_version || 1
      if floor > running
        refuse(spec, node.name,
               "declares min_readable_schema_version #{floor} above its schema_version " \
               "#{running}")
      end
      return unless floor < running && !definition.migrate_state && !definition.migrate_change

      refuse(spec, node.name,
             "declares it reads back to schema version #{floor} but supplies no " \
             "migration; a floor without one fails at the first old record instead of " \
             "at startup")
    end

    # Drop the keys whose value is nil, so the JSON omits them.
    def compact(hash) = hash.reject { |_key, value| value.nil? }

    # The channel as the record carries it: the kind the author declared and
    # the sequences declared off its handle. The consumers, the plugin kind and
    # the sink name are the core's to DERIVE from those chains.
    def channel_json(channel)
      compact(
        "name" => channel.name,
        "kind" => channel.kind.to_s,
        "producer" => channel.producer,
        "chains" => channel.chains,
        "retry_max" => channel.retry_max,
        "retry_backoff_ms" => channel.retry_backoff_ms,
        "overflow" => channel.overflow.to_s,
        "keyed" => channel.keyed
      )
    end

    def procedure_json(node)
      definition = node.definition
      compact(
        "name" => node.name,
        "entity_mode" => definition.entity_mode.to_s,
        "entity_keys" => definition.entity_keys || [],
        "idle_ttl_ms" => definition.idle_ttl_ms,
        "max_entities" => definition.max_entities,
        "entities_for_overridden" => definition.entities_for?,
        "compensate_overridden" => definition.compensate?,
        "clock" => definition.clock.to_s,
        "mailbox_capacity" => definition.mailbox_capacity || 1024,
        # A procedure with no state journals nothing, so a logical clock over
        # it never advances. Ruby has no per-procedure Change type; the
        # presence of an initial state IS the same statement.
        "change_is_nochange" => !definition.initial_state?,
        "schema_version" => definition.schema_version || 1,
        "min_readable_schema_version" => definition.min_readable_schema_version
      )
    end

    # The tier, checkpoint and correction keys, which only a stream class puts
    # on the wire. The core refuses `effectively_once` and
    # `correction_window_ms` on a workflow class outright, and the checkpoint
    # family rests on a checkpoint acknowledgement that refusal leaves
    # unreachable there.
    #
    # The spec keeps the flag either way: `Ctx#stamp_identity` reads it to
    # decide whether a keyed plugin frame carries its own identity, so gating
    # the declaration must not gate the in-process tier.
    def stream_tier_json(spec)
      return {} unless spec.kind == :stream

      {
        "effectively_once" => spec.effectively_once,
        "checkpoint_ack" => spec.checkpoint_ack,
        "checkpoint_every" => spec.checkpoint_every,
        "checkpoint_outputs" => spec.checkpoint_outputs.to_s,
        "checkpoint_max_delay_ms" => spec.checkpoint_max_delay_ms,
        "correction_window_ms" => spec.correction_window_ms,
        "beyond_window_policy" => spec.beyond_window_policy.to_s
      }
    end

    # The declaration the core deserializes. Every key here is pinned by
    # `test/fixtures/host_class_decl_keys.txt`, which this gem's own test
    # reads: a key the core learns that Ruby cannot spell fails that test.
    def to_host_class_decl(spec)
      compact(
        "factory_id" => spec.factory_id,
        "persistent" => spec.persistent,
        "sdk_lang" => SDK_LANG_RUBY,
        "sdk_version" => SDK_VERSION,
        "change_format_id" => FORMAT_ID_MSGPACK,
        **stream_tier_json(spec),
        "compact_after_bytes" => spec.compact_after_bytes,
        "sync_durability" => spec.sync_durability,
        "channels" => spec.channels.map { |channel| channel_json(channel) },
        "procedures" => spec.procedures.map { |node| procedure_json(node) },
        "workflow" => spec.workflow && compact(
          "loop_mode" => spec.workflow[:loop_mode].to_s,
          "rollback_mode" => spec.workflow[:rollback_mode].to_s,
          "retry_attempts" => spec.workflow[:retry_attempts],
          "retry_backoff_ms" => spec.workflow[:retry_backoff_ms],
          "barrier_retry_attempts" => spec.workflow[:barrier_retry_attempts],
          "max_duration_ms" => spec.workflow[:max_duration_ms],
          "wake_after_ms" => spec.workflow[:wake_after_ms]
        )
      )
    end
  end
end

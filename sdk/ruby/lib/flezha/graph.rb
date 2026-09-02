# frozen_string_literal: true

require "json"

require_relative "builder"
require_relative "errors"

module Flezha
  # Registration-time refusals, and the `HostClassDecl` the core is handed.
  #
  # Ruby has no compile step, so everything the Rust builder refuses at compile
  # time is refused here instead, naming the class, the handler and the
  # specific defect — the same set, for the same reasons, as the Rust builder's
  # `TopologyIssue`, so an author moving between SDKs meets one vocabulary.
  #
  # The core validates again on its side. That is not redundancy: a
  # hand-written declaration reaching the ABI directly must not slip past, and
  # an author who declared through this surface deserves the error before any
  # data moves.
  module Graph
    MAX_MAILBOX_CAPACITY = 1_048_576

    # The Ruby SDK's byte in the core's `sdk_lang` allocation table.
    SDK_LANG_RUBY = 5

    # MessagePack, shared with every SDK whose codec is MessagePack.
    FORMAT_ID_MSGPACK = 5

    module_function

    def refuse(spec, handler, defect)
      raise DeclarationError.new(spec.factory_id, handler, defect)
    end

    # Refuse every defect a type system would have caught, naming the handler
    # and the defect.
    def validate(spec)
      check_workflow(spec)
      check_tier(spec)
      check_window(spec)
      check_channels(spec)
      check_procedures(spec)
      check_keying(spec)
      check_topology(spec)
    end

    def check_channels(spec)
      names = {}

      spec.channels.each do |channel|
        refuse(spec, channel.name, "this channel name is declared more than once") if names[channel.name]
        names[channel.name] = true
        check_channel_wiring(spec, channel)
        check_overflow(spec, channel)

        if channel.kind == :entry && channel.consumers.empty?
          refuse(spec, channel.name, "is an entry channel that names no consumer")
        end
        if declared_kind(channel) == :inter_proc && channel.consumers.empty?
          refuse(spec, channel.name, "is an inter-procedure channel that names no consumer")
        end
      end
    end

    # The kind the wire carries.
    #
    # Terminality is derived from the wiring, not from which method the author
    # called: a sink with nothing consuming behind it IS the plugin-bound
    # terminal, and a sink declared alongside consumers is the hybrid — one
    # channel the core journals as inter-procedure and whose values also reach
    # the plugin.
    def declared_kind(channel)
      channel.sink && channel.consumers.empty? ? :plugin : channel.kind
    end

    def check_channel_wiring(spec, channel)
      if channel.sink.nil? && channel.keyed
        refuse(spec, channel.name,
               "is declared keyed but has no plugin wire; output identity stamps only " \
               "exist on plugin-bound frames, so add a sink(...) or drop the keyed")
      end

      return unless spec.effectively_once && channel.sink && !channel.keyed

      refuse(spec, channel.name,
             "has a plugin wire but is not declared keyed; the effectively-once tier " \
             "deduplicates by output identity, so every plugin-bound channel of a class " \
             "that declares effectively_once must opt in with keyed")
    end

    def check_overflow(spec, channel)
      if channel.overflow != :drop_nothing
        if spec.effectively_once
          refuse(spec, channel.name,
                 "declares on_full(#{channel.overflow}) on a class that declares " \
                 "effectively_once; a shed input's offset is still advanced past by the " \
                 "tier's watermark, so the loss is permanent and reported nowhere — " \
                 "declare one or the other, not both")
        end
        if spec.correction_window_ms
          refuse(spec, channel.name,
                 "declares on_full(#{channel.overflow}) on a class that declares a " \
                 "correction window; a correction re-folds a replayed slice and its answer " \
                 "may not depend on which items a full mailbox happened to keep")
        end
      end

      return unless channel.retry_max == 0

      refuse(spec, channel.name,
             "declares retry_max(0); at least one attempt is required — raise a " \
             "non-retryable error to park without retries")
    end

    def check_keying(spec)
      dynamic = spec.procedures.select { |node| node.definition.entity_mode == :dynamic }
                    .map(&:index).to_h { |index| [index, true] }

      spec.channels.each do |channel|
        channel.consumers.each do |edge|
          node = spec.procedures[edge.procedure]
          if edge.key_by && node.definition.entity_mode == :static
            refuse(spec, channel.name,
                   "declares key_by into procedure `#{node.name}`, which declares a static " \
                   "key set; static keying routes by entities_for and may name several " \
                   "entities, edge keying names exactly one — declare one or the other, " \
                   "not both")
          end
          if !edge.key_by && dynamic[edge.procedure]
            refuse(spec, channel.name,
                   "feeds dynamic procedure `#{node.name}` without a key_by; every edge into " \
                   "a dynamic procedure must name the entity its inputs reach")
          end
        end
      end
    end

    def check_procedures(spec)
      spec.procedures.each do |node|
        check_clocks(spec, node)
        check_entities(spec, node)
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
      if clock != :event_time && definition.event_time?
        refuse(spec, node.name,
               "declares an event_time method with clock `#{clock}`; the extractor is only " \
               "consulted by an event_time clock, so one of the two is not what was meant")
      end
      return unless clock == :logical && !definition.initial_state?

      refuse(spec, node.name,
             "declares a logical clock but no state; logical time IS the durable change " \
             "sequence, which never advances for a procedure that records no changes")
    end

    def check_entities(spec, node)
      definition = node.definition

      if definition.entity_mode == :static && !definition.entities_for?
        refuse(spec, node.name,
               "declares a static key set without an entities_for; static keying needs the " \
               "method that says which declared entities an input belongs to")
      end
      capacity = definition.mailbox_capacity
      if capacity && (capacity < 1 || capacity > MAX_MAILBOX_CAPACITY)
        refuse(spec, node.name,
               "declares mailbox_capacity #{capacity}; capacity must be in " \
               "1..#{MAX_MAILBOX_CAPACITY}")
      end
      return unless definition.max_entities == 0

      refuse(spec, node.name,
             "declares max_entities 0; a dynamic procedure that may hold no entity can " \
             "process nothing")
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

    def check_tier(spec)
      if spec.effectively_once && !spec.persistent
        refuse(spec, nil, "declares effectively_once without persistence: the tier IS durable state")
      end
      if spec.ack_granularity == :checkpoint && !spec.effectively_once
        refuse(spec, nil,
               "declares checkpoint acknowledgement without effectively_once; the checkpoint " \
               "is where the tier's consumed offset advances, and a class without the tier " \
               "has no durable offset to hold back")
      end
      if spec.checkpoint_every == 0
        refuse(spec, nil,
               "declares checkpoint_every(0); an interval that closes every zero inputs is " \
               "not an interval — omit it to close only on an explicit ctx.checkpoint")
      end
      if spec.checkpoint_outputs == :at_checkpoint && spec.checkpoint_max_delay_ms.nil?
        refuse(spec, nil,
               "declares outputs_at_checkpoint with no maximum delay; an interval that never " \
               "closes would hold its outputs forever on a quiet entity")
      end
    end

    def check_window(spec)
      return if spec.correction_window_ms.nil?

      if spec.correction_window_ms == 0
        refuse(spec, nil, "declares a correction window of zero; a zero window can correct nothing")
      end
      unless spec.effectively_once
        refuse(spec, nil,
               "declares a correction window without effectively_once; correction replays the " \
               "input log against a rewound baseline, and only the tier journals inputs " \
               "before forwarding them")
      end
      unless spec.procedures.any? { |node| node.definition.clock == :event_time }
        refuse(spec, nil,
               "declares a correction window but no procedure declares an event_time clock; " \
               "correction orders a replayed slice by event time")
      end
    end

    # Every procedure reachable from an entry channel, and no cycle among them.
    def check_topology(spec)
      entries = spec.channels.select { |channel| channel.kind == :entry }
      if spec.kind == :stream && entries.empty?
        refuse(spec, nil, "declares no entry channel; a stream class is fed by the controller")
      end
      if spec.procedures.empty?
        refuse(spec, nil, "declares no procedure") if spec.kind == :stream
        return
      end

      successors = Hash.new { |hash, key| hash[key] = [] }
      spec.channels.each do |channel|
        next unless channel.kind == :inter_proc && channel.producer

        successors[channel.producer].concat(channel.consumers.map(&:procedure))
      end

      reachable = {}
      frontier = entries.flat_map { |channel| channel.consumers.map(&:procedure) }
      until frontier.empty?
        node = frontier.pop
        next if reachable[node]

        reachable[node] = true
        frontier.concat(successors[node])
      end

      # A cycle is found before unreachability is reported: a node on a cycle
      # with no path from an entry is both, and the cycle is the more useful
      # answer.
      colour = {}
      visit = lambda do |index|
        case colour[index]
        when :black then return
        when :grey
          refuse(spec, spec.procedures[index].name,
                 "is on a cycle; an operative topology is a DAG — a node's output may never " \
                 "reach its own input")
        end
        colour[index] = :grey
        successors[index].each { |next_index| visit.call(next_index) }
        colour[index] = :black
      end
      spec.procedures.each { |node| visit.call(node.index) }

      spec.procedures.each do |node|
        refuse(spec, node.name, "is not reachable from any entry channel") unless reachable[node.index]
      end
    end

    def check_workflow(spec)
      unless spec.kind == :workflow
        if spec.sync_durability
          refuse(spec, nil,
                 "declares sync_durability on a stream class; a stream class acknowledges at " \
                 "input rates, and a remote round trip per acknowledgement is not a guarantee " \
                 "it can keep")
        end
        return
      end

      if spec.correction_window_ms
        refuse(spec, nil, "is a workflow class and cannot declare a correction window")
      end
      if spec.run && !spec.channels.empty?
        refuse(spec, nil, "declares a run body and channels; a run-sugar workflow is one " \
                          "imperative body")
      end
      if spec.sync_durability && !spec.persistent
        refuse(spec, nil, "declares sync_durability without persistence")
      end
    end

    # Drop the keys whose value is nil, so the JSON omits them.
    def compact(hash) = hash.reject { |_key, value| value.nil? }

    def channel_json(channel)
      compact(
        "name" => channel.name,
        "kind" => declared_kind(channel).to_s,
        "producer" => channel.producer,
        "consumers" => channel.consumers.map(&:procedure),
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
        "schema_version" => definition.schema_version || 1,
        "min_readable_schema_version" => definition.min_readable_schema_version
      )
    end

    # The declaration the core deserializes. Every key here is pinned by
    # `test/fixtures/host_class_decl_keys.txt`, which this gem's own test
    # reads: a key the core learns that Ruby cannot spell fails that test.
    def to_host_class_decl(spec)
      compact(
        "factory_id" => spec.factory_id,
        "persistent" => spec.persistent,
        "sdk_lang" => SDK_LANG_RUBY,
        "change_format_id" => FORMAT_ID_MSGPACK,
        "effectively_once" => spec.effectively_once,
        "ack_granularity" => spec.ack_granularity.to_s,
        "checkpoint_every" => spec.checkpoint_every,
        "checkpoint_outputs" => spec.checkpoint_outputs.to_s,
        "checkpoint_max_delay_ms" => spec.checkpoint_max_delay_ms,
        "correction_window_ms" => spec.correction_window_ms,
        "beyond_window_policy" => spec.beyond_window_policy.to_s,
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

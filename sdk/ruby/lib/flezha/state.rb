# frozen_string_literal: true

require_relative "codec"

module Flezha
  # Declared state: how it is built and how it is restored.
  #
  # Declared state is mutated only through declared changes, because the change
  # log IS how the state is rebuilt: a field assigned directly is live in this
  # process and gone after a restart, and nothing would report the difference
  # until the restart. Ruby has no cheap deep read-only view — the Node SDK's
  # guard is a `Proxy`, which has no counterpart here that does not cost an
  # allocation per field read — so the rule is documented and `ctx.change` is
  # the path, rather than paid for on every access.
  module State
    module_function

    # Decode one durable body, choosing between the three things it could be:
    # a payload from an older schema version, one the procedure declared its
    # own codec for, or the MessagePack default.
    #
    # One function rather than the same three-way choice written at each of the
    # places a body is decoded — the two halves of a restore fold, on the
    # dispatch path and in the test harness.
    def decode_body(definition, which, body, procedure)
      from_version = body[:from_version]
      bytes = body[:bytes] || "".b
      if from_version
        migration = which == :state ? definition.migrate_state : definition.migrate_change
        unless migration
          raise Error,
                "#{procedure} was handed a #{which} record written at schema version " \
                "#{from_version} and declares no migrate_#{which}; declare one, or raise " \
                "min_readable_schema_version so the record is refused rather than misread"
        end

        return migration.call(from_version, bytes)
      end

      codec = which == :state ? definition.state_codec : definition.change_codec
      codec ? codec.decode(bytes) : Codec.decode(bytes)
    end

    # Rebuild an entity's state: the declared starting value, the snapshot
    # baseline when there is one, then every change folded on top in order.
    #
    # This IS the restore, and both callers are the real thing — the dispatcher
    # answering a restore fold, and the test harness constructing from a
    # payload — which is why the harness proves what a warm restart does rather
    # than a second implementation that agrees with it.
    def fold_restore(definition, payload, entity_key, procedure)
      state = definition.build_initial_state(entity_key)
      snapshot = payload[:snapshot]
      state = decode_body(definition, :state, snapshot, procedure) if snapshot

      Array(payload[:changes]).each do |body|
        unless definition.apply_change?
          raise Error,
                "#{procedure} was handed a change log but declares no apply_change; a " \
                "procedure that records no changes should have no durable records to fold"
        end

        definition.fold_change(state, decode_body(definition, :change, body, procedure))
      end
      state
    end
  end
end

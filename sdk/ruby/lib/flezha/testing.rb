# frozen_string_literal: true

require_relative "builder"
require_relative "codec"
require_relative "dispatch"
require_relative "state"

module Flezha
  # Driving one procedure without a controller, for the author's own tests.
  #
  # The harness is not a second implementation of the dispatcher: it drives
  # `Dispatch.invoke` — the same function every activation kind that reaches a
  # handler goes through — over a one-procedure `RegisteredClass`, and
  # `replay_equals!` folds through `State.fold_restore`, the function a real
  # restore calls. So what it proves is what the runner does, rather than
  # something that agrees with it.
  module Testing
    # The recorded change log and the live state disagree.
    class ReplayMismatch < Error
      def initialize(procedure, live, replayed)
        super(
          "#{procedure}: replaying the recorded change log against a fresh initial state " \
          "produced #{replayed.inspect}, but the live state is #{live.inspect}. Either a " \
          "handler mutated the state without recording a change, or apply_change is not a " \
          "pure fold — both are lost on restart and reported nowhere until then."
        )
      end
    end

    # What one invocation produced.
    Outcome = Struct.new(:outputs, :changes, :timers, :returned, keyword_init: true)

    class Harness
      attr_reader :log

      def initialize(procedure_class, entity_key: nil, operative: "harness")
        @definition = procedure_class.definition
        @entity_key = entity_key
        @operative = operative
        @node = ProcNode.new(0, @definition.name, procedure_class, @definition)
        # One plugin channel so `ctx.emit` has somewhere to go. A procedure
        # under test emits to be observed, not to be routed.
        @channel = ChannelSpec.new(
          index: 0, name: "out", kind: :plugin, producer: 0, consumers: [],
          overflow: :drop_nothing, keyed: false, transforms: []
        )
        @spec = ClassSpec.new(
          factory_id: "<harness>", kind: :stream, persistent: true, effectively_once: false,
          ack_granularity: :per_input, checkpoint_outputs: :as_produced,
          beyond_window_policy: :process_as_arrival, sync_durability: false,
          procedures: [@node], channels: [@channel]
        )
        # The dispatcher's own entity store, so the state this harness reports
        # is the state a runner would hold.
        @registered = RegisteredClass.new(@spec)
        @deps = DispatchDeps.new(registry: nil, bridge: self, workflow_ctx: nil)
        @log = []
        @change_seq = 0
      end

      # The entity's live state, as the dispatcher left it.
      def state = @registered.state_of(@node, @entity_key)

      # Run the procedure against one input.
      def feed(input, now: 0, replay: false) = run(now, replay) { |ctx| invoke(input, ctx) }

      # Fire a per-entity timer. `now` IS the scheduled time, as it is live.
      def fire_timer(kind, scheduled_at)
        run(scheduled_at, false) do |ctx|
          @definition.instance.on_timer({ kind: kind, scheduled_at: scheduled_at }, ctx)
          nil
        end
      end

      # Run the declared compensation over recorded forward outputs.
      def compensate(outputs) = run(0, false) { |ctx| @definition.instance.compensate(outputs, ctx) }

      # Rebuild the state the way a restore does, from a snapshot and a change
      # log — the same call the dispatcher answers `restore_fold` with.
      def restore(snapshot: nil, changes: [])
        @registered.set_state(
          @node, @entity_key,
          State.fold_restore(
            @definition,
            { snapshot: snapshot && { bytes: snapshot },
              changes: changes.map { |bytes| { bytes: bytes } } },
            @entity_key, @definition.name
          )
        )
      end

      # Fold every recorded change into a fresh initial state and require the
      # answer to equal the live one.
      #
      # In this SDK that catches both failures at once: a handler that mutated
      # the state without recording a change, and an `apply_change` that is not
      # a pure fold. The SDKs whose state is guarded can only catch the second.
      def replay_equals!
        replayed = State.fold_restore(
          @definition, { changes: @log.map { |bytes| { bytes: bytes } } },
          @entity_key, @definition.name
        )
        raise ReplayMismatch.new(@definition.name, state, replayed) unless replayed == state

        true
      end

      # The spawn bridge a `Ctx` asks. A harness has no controller, so a spawn
      # answers what one would have and records nothing.
      def spawn_workflow(*) = "spawned"

      private

      def run(now, replay)
        activation = { operative: @operative, entity_key: @entity_key, change_seq: @change_seq }
        ctx, returned = Dispatch.invoke(activation, @registered, @node, @deps, now, replay) do |c|
          yield(c)
        end
        # A returned value is emitted, exactly as `run_work` does it.
        ctx.emit(returned) unless returned.nil?
        @log.concat(ctx.changes)
        @change_seq += ctx.changes.size
        Outcome.new(
          outputs: ctx.outputs.map { |o| Codec.decode(o[:payload]) },
          changes: ctx.changes.map { |bytes| decode_change(bytes) },
          timers: ctx.timers, returned: returned
        )
      end

      def invoke(input, ctx) = @definition.instance.process(input, ctx)

      def decode_change(bytes)
        codec = @definition.change_codec
        codec ? codec.decode(bytes) : Codec.decode(bytes)
      end
    end

    # Drive `procedure_class` over `inputs` and assert the replay agrees.
    def self.replay_equals?(procedure_class, inputs, entity_key: nil)
      harness = Harness.new(procedure_class, entity_key: entity_key)
      inputs.each { |input| harness.feed(input) }
      harness.replay_equals!
    end
  end
end

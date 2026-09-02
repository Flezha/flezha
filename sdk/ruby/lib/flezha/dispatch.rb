# frozen_string_literal: true

require_relative "codec"
require_relative "ctx"
require_relative "graph"
require_relative "logger"
require_relative "state"

module Flezha
  # A declared class, with the lookups dispatch needs on the hot path, and the
  # live state of every entity it owns.
  class RegisteredClass
    attr_reader :spec, :outgoing

    def initialize(spec)
      @spec = spec
      @outgoing = spec.procedures.map do |node|
        spec.channels.select { |channel| channel.producer == node.index }
      end
      @entities = {}
      # Entities of one class are touched from every worker thread, and the
      # core's per-entity exclusivity does not extend to this Hash: two
      # different entities of the same class run concurrently by design.
      @lock = Mutex.new
    end

    # The entity's live state, created from the declaration on first touch.
    def state_of(node, entity_key)
      key = [node.index, entity_key]
      @lock.synchronize do
        @entities.fetch(key) { @entities[key] = node.definition.build_initial_state(entity_key) }
      end
    end

    def set_state(node, entity_key, state)
      @lock.synchronize { @entities[[node.index, entity_key]] = state }
    end
  end

  # The declared classes this runner serves.
  class Registry
    def initialize
      @classes = {}
    end

    def add(spec)
      if @classes.key?(spec.factory_id)
        raise Error, "duplicate factory id `#{spec.factory_id}`: a runner declares each factory once"
      end

      @classes[spec.factory_id] = RegisteredClass.new(spec)
    end

    def get(factory_id) = @classes[factory_id]
    def all = @classes.values
    def empty? = @classes.empty?
  end

  # Turning one activation into one answer, exactly once.
  #
  # Per-entity exclusivity IS the handout: the core holds an entity's next
  # activation until this one is answered, so an activation that returns
  # unanswered retires that entity for the life of the process. Every path out
  # of `dispatch` therefore ends in a completion or an abandon, through an
  # `ensure` rather than a rule applied at each return site — a raised handler,
  # an encoding failure and a killed thread all reach the same guard.
  module Dispatch
    module_function

    def failed(token, message, non_retryable: false)
      { token: token, outcome: "failed", error: message, non_retryable: non_retryable }
    end

    # What a failure says on the wire. `class: message`, in family with what
    # the other SDKs send — this string is stored in a dead-letter entry and
    # read by an operator, and a multi-line backtrace there is noise the retry
    # ladder multiplies. The backtrace is logged host-side instead, where it is
    # a diagnostic rather than a record.
    def error_message(error) = "#{error.class}: #{error.message}"

    # A handler declares an error non-retryable by answering `non_retryable?`.
    def non_retryable?(error)
      error.respond_to?(:non_retryable?) && error.non_retryable?
    end

    # A payload as the handler sees it: the channel's declared decoder (or the
    # MessagePack default), then its transforms in declaration order. `DROPPED`
    # when a filter rejected it.
    #
    # One function for the two places a payload crosses in — an input item, and
    # a payload at the prepare crossing — so a filter cannot mean one thing on
    # one path and another on the other.
    def value_of(channel, edge, payload)
      decoded = channel&.decode ? channel.decode.call(payload) : Codec.decode(payload)
      Flezha.apply_transforms(edge&.transforms || [], decoded)
    end

    # The channel's edge that feeds `node`, or nil when it feeds none.
    def edge_into(channel, node)
      channel&.consumers&.find { |edge| edge.procedure == node }
    end

    # Produce the answer for one activation. Raising is not an option here:
    # every failure becomes a result the core can act on, because the
    # alternative is an entity that never receives work again.
    def answer(activation, deps)
      registered = deps.registry.get(activation[:factory])
      unless registered
        return failed(activation[:token],
                      "no class registered for factory `#{activation[:factory]}`",
                      non_retryable: true)
      end

      # The workflow body is the one kind addressed to a class rather than to a
      # procedure, so it is answered before the procedure is resolved — which
      # is what lets every arm below hold a procedure rather than assert one.
      return run_workflow(activation, registered, deps) if activation[:kind] == :workflow_run

      node = registered.spec.procedures[activation[:procedure]]
      unless node
        return failed(activation[:token],
                      "class `#{activation[:factory]}` has no procedure at index " \
                      "#{activation[:procedure]}", non_retryable: true)
      end

      case activation[:kind]
      when :work then run_work(activation, registered, node, deps)
      when :restore_fold then restore_fold(activation, registered, node)
      when :snapshot_request then snapshot(activation, registered, node)
      when :live_state_read
        { token: activation[:token], outcome: "state_json",
          json: Codec.to_state_json(registered.state_of(node, activation[:entity_key])).b }
      when :migration_serialize
        { token: activation[:token], outcome: "migration_state",
          json: Codec.to_state_json(registered.state_of(node, activation[:entity_key])).b }
      when :apply_state
        registered.set_state(node, activation[:entity_key],
                             Codec.from_state_json(activation[:state_json]))
        { token: activation[:token], outcome: "state_applied" }
      when :key_request then prepare(activation, registered)
      when :compensate_run then compensate(activation, registered, node, deps)
      else
        failed(activation[:token], "unknown activation kind `#{activation[:kind]}`",
               non_retryable: true)
      end
    rescue StandardError => e
      failed(activation[:token], error_message(e), non_retryable: non_retryable?(e))
    end

    # Run user code against one entity, and keep the state it produced.
    #
    # Every activation kind that reaches a handler goes through here — an
    # input, a timer firing, a compensation — so the context they receive and
    # the state they leave behind cannot differ between them. The state is
    # written back whether or not the body raised: a change recorded before the
    # raise is already journaled, and dropping it here would make the live
    # entity disagree with its own log.
    def invoke(activation, registered, node, deps, now, replay)
      ctx = Ctx.new(
        spec: registered.spec, node: node, outgoing: registered.outgoing[node.index],
        operative: activation[:operative], entity_key: activation[:entity_key],
        change_seq: activation[:change_seq], now: now, replay: replay,
        state: registered.state_of(node, activation[:entity_key]), bridge: deps.bridge
      )
      begin
        [ctx, yield(ctx)]
      ensure
        registered.set_state(node, activation[:entity_key], ctx.state)
      end
    end

    def run_work(activation, registered, node, deps)
      effects = []

      Array(activation[:items]).each do |item|
        now = item[:now] || 0
        begin
          ctx, returned = invoke(activation, registered, node, deps, now, item[:replay] || false) do |c|
            run_item(registered, node, item, now, c)
          end

          # A returned value is emitted; `nil` emits nothing, which is the
          # warmup gate. A handler that emits through the context returns
          # nothing, so the two spellings compose rather than conflict.
          ctx.emit(returned) unless returned.nil?
          effects << ctx.effect(item[:consumed_offset])
        rescue StandardError => e
          # The backtrace is a host diagnostic, not a record: the verdict below
          # travels to the dead-letter entry and the controller's log.
          Flezha.log.debug("#{node.name} failed an item: #{e.class}: #{e.message}\n" \
                           "#{Array(e.backtrace).first(8).join("\n")}")
          # Items after this one are re-handed by the core, so the batch stops
          # here rather than reporting effects the entity never saw.
          effects << {
            verdict: non_retryable?(e) ? "non_retryable" : "retryable",
            message: error_message(e), event_time: now,
            consumed_offset: item[:consumed_offset]
          }
          break
        end
      end

      { token: activation[:token], outcome: "work", effects: effects }
    end

    # One item against the handler its kind names, inside an invocation.
    def run_item(registered, node, item, now, ctx)
      definition = node.definition
      if item[:kind] == :timer
        unless definition.on_timer?
          raise Error,
                "#{node.name} registered a timer but declares no on_timer; registration " \
                "is a runtime call, so this cannot be refused at declaration time"
        end

        definition.instance.on_timer(
          { kind: item[:timer_kind], scheduled_at: item[:scheduled_at] || now }, ctx
        )
        return nil
      end

      channel = registered.spec.channels[item[:channel] || 0]
      # This node's own edge off the channel: a transform chain declared before
      # one `into` belongs to that consumer and to no other.
      input = value_of(channel, edge_into(channel, node.index), item[:payload] || "".b)
      # A filtered value is consumed and reaches no handler. It still produces
      # an effect, so its offset advances and nothing is retried.
      return nil if input.equal?(DROPPED)

      definition.instance.process(input, ctx)
    end

    def restore_fold(activation, registered, node)
      registered.set_state(
        node, activation[:entity_key],
        State.fold_restore(
          node.definition,
          { snapshot: activation[:snapshot], changes: activation[:changes] },
          activation[:entity_key], node.name
        )
      )
      { token: activation[:token], outcome: "restored" }
    end

    def snapshot(activation, registered, node)
      codec = node.definition.state_codec
      state = registered.state_of(node, activation[:entity_key])
      { token: activation[:token], outcome: "snapshot",
        state: (codec ? codec.encode(state) : Codec.encode(state)).b }
    end

    def prepare(activation, registered)
      request = activation[:key_request]
      return failed(activation[:token], "a key request carried no payloads", non_retryable: true) unless request

      channel = registered.spec.channels[request[:channel]]
      # The crossing answers one key set for the whole payload, so it reads the
      # first consumer edge — the one the core routes the answer to.
      edge = channel&.consumers&.first
      node = edge && registered.spec.procedures[edge.procedure]
      definition = node&.definition

      answers = request[:payloads].map do |payload|
        begin
          value = value_of(channel, edge, payload)
          # A filtered payload answers no key. For a dynamic consumer that is
          # what tells the core not to route it at all — the drop happens
          # before an entity is minted rather than inside one.
          next { keys: [] } if value.equal?(DROPPED)

          keys =
            if !request[:want_keys] then []
            elsif edge&.key_by then [edge.key_by.call(value).to_s]
            elsif definition&.entities_for? then definition.entities_for(value).map(&:to_s)
            else []
            end
          event_time =
            request[:want_event_time] && definition&.event_time? ? definition.event_time_of(value) : nil
          { keys: keys, event_time: event_time }
        rescue StandardError => e
          # A per-payload refusal enters the input channel's retry ladder; it
          # is not a failure of the crossing, whose other payloads still have
          # answers.
          { error: error_message(e), non_retryable: non_retryable?(e) }
        end
      end

      { token: activation[:token], outcome: "prepared", prepared: answers }
    end

    def run_workflow(activation, registered, deps)
      run = registered.spec.run
      unless run
        return failed(activation[:token],
                      "class `#{activation[:factory]}` was handed a workflow body but " \
                      "declares none", non_retryable: true)
      end

      ctx = deps.workflow_ctx.call(activation[:operative])
      args = activation[:args] ? Codec.decode(activation[:args]) : nil
      returned = run.call(args, ctx)
      completed = ctx.completed? || !returned.nil?
      {
        token: activation[:token], outcome: "workflow_done",
        workflow_completed: completed,
        workflow_result: Codec.encode(ctx.completed? ? ctx.result : returned).b
      }
    end

    def compensate(activation, registered, node, deps)
      definition = node.definition
      unless definition.compensate?
        return failed(activation[:token], "#{node.name} declares no compensate hook",
                      non_retryable: true)
      end

      outputs = Array(activation[:outputs]).map { |bytes| Codec.decode(bytes) }
      ctx, = invoke(activation, registered, node, deps, 0, false) do |c|
        definition.instance.compensate(outputs, c)
      end
      { token: activation[:token], outcome: "work", effects: [ctx.effect(nil)] }
    end

    # Dispatch one activation and answer it exactly once.
    #
    # The guard is the contract, not politeness. An unanswered activation
    # retires its entity for the life of the process, so a raise from the
    # dispatcher, from encoding the result, or from `complete` itself abandons
    # rather than logging and returning.
    def dispatch(activation, native, deps)
      answered = false
      begin
        native.complete(answer(activation, deps))
        answered = true
      rescue StandardError => e
        Flezha.log.error("activation processing failed: #{error_message(e)}")
      ensure
        unless answered
          begin
            native.abandon(activation[:token])
          rescue StandardError => e
            # The core refuses an unknown token: `complete` had already landed
            # and only the bookkeeping after it raised. Nothing is wedged.
            Flezha.log.warn("abandoning activation #{activation[:token]} failed: " \
                            "#{error_message(e)}")
          end
        end
      end
    end
  end

  # What a dispatch loop needs to answer an activation, over one extension.
  DispatchDeps = Struct.new(:registry, :bridge, :workflow_ctx, keyword_init: true)
end

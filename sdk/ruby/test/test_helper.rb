# frozen_string_literal: true

require "minitest/autorun"

$LOAD_PATH.unshift(File.expand_path("../lib", __dir__))
require "flezha"

module Flezha
  module TestSupport
    # The repository root, for the fixtures the gem's tests share with the
    # other SDKs' tests.
    def self.repo_root = File.expand_path("../../..", __dir__)

    # Whether the compiled veneer is present. The pure-Ruby tests run without
    # it; the ones that drive the core say why they are skipped rather than
    # passing vacuously.
    def self.native?
      @native = begin
        Core.load_extension
        true
      rescue StandardError
        false
      end
      @native
    end

    # The dispatch dependencies over one declared class and one peer. Every
    # suite that drives the dispatcher needs exactly this, so it is built here
    # rather than once per fixture.
    def self.deps(operative_class, core)
      registry = Flezha::Registry.new
      registry.add(operative_class.spec)
      Flezha::DispatchDeps.new(
        registry: registry, bridge: core,
        workflow_ctx: ->(id) { Flezha::WorkflowCtx.new(id, core) }
      )
    end

    # One activation, with the fields every kind carries; a caller merges what
    # its own kind adds.
    def self.activation(factory:, **extra)
      { token: 1, operative: "op-1", procedure: 0, entity_key: nil, change_seq: 0,
        factory: factory, kind: :work }.merge(extra)
    end

    # One input item on the first channel.
    def self.input_item(payload, **extra)
      { kind: :input, channel: 0, payload: payload.b, metadata: [], replay: false,
        now: 100 }.merge(extra)
    end

    # The peer at the poll boundary: a source of activations with no
    # controller, recording the order the host drives it in.
    #
    # It scripts what the core would answer, never what the dispatcher decides
    # — a test that wants the dispatcher's answer asserts `Dispatch.answer`,
    # and a test that wants the handout contract drives a real context. Both
    # blocking calls answer on a signal, so no test waits on a clock.
    class FakeCore
      attr_reader :events, :declarations, :completed, :abandoned, :polled

      # `activations` are handed out in order; `starvations` is how many times
      # `wait_starvation` reports one before it starts timing out; `hold` runs
      # inside `complete`, which is where a test holds a worker without
      # reaching through the dispatcher.
      def initialize(activations: [], starvations: 0, hold: nil)
        @pending = Thread::Queue.new
        activations.each { |activation| @pending.push(activation) }
        @starvations = starvations
        @hold = hold
        @events = []
        @declarations = []
        @completed = []
        @abandoned = []
        # The first poll is what "serving" means, and it happens on a worker
        # thread — so a test that drains without waiting for it would be
        # timing the join, not the order.
        @polled = Thread::Queue.new
        @lock = Mutex.new
        @starved = ConditionVariable.new
        @stopping = false
        @handed_out = false
      end

      # Whether anything has been handed out yet, so a test can wait for the
      # worker to be inside the handler rather than racing it.
      def polled_once? = @lock.synchronize { @handed_out }

      def declare_class(json)
        @lock.synchronize { @declarations << json }
        record(:declare)
      end

      def start(_namespace) = record(:start)
      def free = record(:free)

      def poll(timeout_ms)
        record(:poll)
        @polled.push(:polled)
        return :shutting_down if stopping?

        activation = @pending.pop(timeout: timeout_ms / 1000.0)
        return :shutting_down if activation.nil? && stopping?
        return nil if activation.nil?

        @lock.synchronize { @handed_out = true }
        activation
      end

      def wait_starvation(timeout_ms)
        @lock.synchronize do
          next 2 if @stopping
          next take_starvation if @starvations.positive?

          @starved.wait(@lock, timeout_ms / 1000.0)
          next 2 if @stopping
          next take_starvation if @starvations.positive?

          0
        end
      end

      def complete(result)
        @hold&.call
        @lock.synchronize { @completed << result }
      end

      def abandon(token) = @lock.synchronize { @abandoned << token }
      def spawn_workflow(*) = "spawned"
      def workflow_emit(*) = nil
      def workflow_recv(*) = nil
      def workflow_cancelled(*) = false
      def monotonic_nanos = 1_234
      def entry_last_delivery_nanos(_channel) = 0
      def idle_watch_new(_channel, _threshold_nanos) = @lock.synchronize { @idle_watches = (@idle_watches || 0) + 1 }
      def idle_watch_poll(_id) = [0, 0]
      def idle_watch_free(_id) = record(:idle_watch_free)

      # Wake every blocked call: a shutdown a test has to wait out is a sleep
      # wearing a signal's name.
      def shutdown
        @lock.synchronize do
          @stopping = true
          @starved.broadcast
        end
        @pending.close
      end

      private

      def stopping? = @lock.synchronize { @stopping }
      def record(event) = @lock.synchronize { @events << event }

      # Called with the lock held.
      def take_starvation
        @starvations -= 1
        1
      end
    end
  end
end

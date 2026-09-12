# frozen_string_literal: true

require "minitest/autorun"
# `Object#stub` lives in minitest's `mock`. `minitest/autorun` does not load it
# in every 5.x, and minitest 6 ships no `mock.rb` at all — mock is extracted to
# its own gem — so an unpinned resolution fails here, once, by name, instead of
# as a missing method on whatever a suite happened to stub first.
require "minitest/mock"

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

    # A block driven as if standard input were not a terminal.
    #
    # `Runner#observe_shutdown` asks `$stdin.tty?`, so a suite run by hand and
    # the same suite under CI, a pipe or a log redirect take different paths
    # through `boot`. Pinning the answer means both drive the one the harness
    # uses, where the core is asked to watch for end of file.
    def self.without_a_terminal(&) = $stdin.stub(:tty?, false, &)

    # One declaration's verdict, in a context of its own.
    #
    # `validate` submits through the ABI now, so a suite that judges a
    # topology needs a live core. `flz_core_check_class` registers nothing and
    # a context is a plain allocation, so a test pays one per call rather than
    # sharing a process-wide handle with the suites that make and free theirs.
    def self.validate(spec)
      core = Flezha::Core.create
      Flezha::Graph.validate(spec, core)
    ensure
      core&.free
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

      # A fake derives nothing: the verdict is the core's, and a test that
      # wants one drives a real context. This answers the empty derivation so
      # a host's declare-then-serve order stays observable here.
      def check_class(_json) = '{"channels":[],"ack_granularity":"per_input"}'

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

      # Wake every blocked call: a shutdown a test has to wait out is a sleep
      # wearing a signal's name.
      def shutdown
        @lock.synchronize do
          @stopping = true
          @starved.broadcast
        end
        @pending.close
      end

      # Every remaining call a host can make on a core, answered by recording
      # it and nothing else. `Core` generates its delegations from the same
      # list, so an entry point the veneer gains reaches this double as well,
      # and a host that calls one meets a recorded event rather than a
      # `NoMethodError`.
      #
      # A test that needs a real answer scripts the method ABOVE this loop,
      # which wins. Above, not below: `instance_methods(false)` does not see a
      # private method, so an override written after `private` is generated over
      # AND made private, and the host's call raises `NoMethodError` again.
      #
      # `nil`, not `record`'s return: that is the `@events` array itself, so a
      # caller branching on the answer would take the truthy path and a caller
      # appending to it would corrupt the log every test here asserts on.
      (Flezha::Core::ENTRY_POINTS - instance_methods(false)).each do |entry|
        define_method(entry) do |*|
          record(entry)
          nil
        end
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

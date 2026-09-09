# frozen_string_literal: true

require "json"

require_relative "ctx"
require_relative "dispatch"
require_relative "entry_idle"
require_relative "executor"
require_relative "graph"
require_relative "logger"
require_relative "native"
require_relative "operative"

module Flezha
  # The runner: what an author starts, and everything it owns while running.
  #
  # Classes are registered explicitly, by naming them. There is no autoload, no
  # load-path walk and no package scan: what runs is what the author said runs,
  # and a class that is defined but not registered never reaches the core.
  #
  # ## Startup order
  #
  #   declare classes -> warm up -> (fork, if the operator forks) -> start
  #
  # `Process.warmup` compacts the heap and promotes the declared object graph
  # to the old generation, which is what makes a subsequent fork
  # copy-on-write-friendly. It runs after the last declaration and before the
  # first `poll`. The core must NOT be started before a fork: it owns native
  # threads that `fork(2)` does not carry, and `Core#guard!` raises naming that
  # rule rather than letting a child serve nothing.
  class Runner
    # What a healthy runner has to drain in after standard input closes.
    STDIN_EOF_GRACE_MS = 5_000

    attr_reader :namespace

    def initialize(namespace)
      @namespace = namespace
      @classes = []
      @registry = Registry.new
      @core = nil
      @executor = nil
      @executor_kind = (ENV["FLEZHA_RUBY_EXECUTOR"] || "threads").to_sym
      @executor_options = {}
      @yjit = nil
      @stopped = Thread::Queue.new
      @stopping = false
      @teardown = []
      @idle = nil
    end

    # Register a stream operative class.
    def define_stream(klass)
      @classes << klass
      self
    end

    # Register a workflow operative class.
    #
    # The same registration as a stream, and deliberately the same code: what a
    # class declares is read from the class, so the two names are for the
    # reader rather than for the runner.
    def define_workflow(klass) = define_stream(klass)

    # Which executor dispatches: `:threads` (the default) or `:fibers`.
    def executor(kind, **options)
      @executor_kind = kind.to_sym
      @executor_options = options
      self
    end

    def logger(logger)
      Flezha.log = logger
      self
    end

    # When the runner last delivered a frame to entry channel `channel`, or
    # nil before its first frame (and for a name no declared operative has —
    # both mean "nothing has arrived here"). Legal before `boot`; reading
    # never blocks delivery.
    #
    # The instant is the *signal*; whether the silence since then is anomalous
    # is the application's call. Register an observer with `on_entry_idle`
    # instead of polling this in a loop.
    def last_entry_delivery(channel)
      EntryIdleWatcher.last_entry_delivery(@core || Core.context_free, channel)
    end

    # Administer this namespace from ordinary application code: start, stop and
    # feed operatives, enable and disable plugins, read what is running.
    #
    # The controller judges every call as the identity this connection proved,
    # so no credential is held here and nothing expires. An operator grants a
    # namespace's applications the authority once, with
    # `flz role bind runner@<namespace> <namespace> app`; until then every call
    # is refused.
    def admin = Admin.new(@core || Core.context_free)

    # Observe silence on entry channel `channel`: `observer` (or the block) is
    # notified once per idle episode, when the gap since the last delivered
    # frame first exceeds `threshold_ms`, and once again when a frame resumes
    # it. Silence before the channel's very first frame is never an episode.
    #
    # Legal before and after `boot`. Evaluation runs on one thread per runner
    # — never on the delivery path — so an observer that raises costs the data
    # plane nothing. Several observers may watch one channel.
    def on_entry_idle(channel, threshold_ms, observer = nil, &block)
      @idle ||= EntryIdleWatcher.new(@core || Core.context_free).tap do |watcher|
        @teardown << -> { watcher.stop }
      end
      @idle.register(channel, threshold_ms, observer, &block)
      self
    end

    # Override the YJIT decision. `nil` (the default) means "decide", `false`
    # means "leave it off".
    def yjit(enabled)
      @yjit = enabled
      self
    end

    # Declare, warm up, connect and start serving — everything up to and
    # including the first poll, and nothing that blocks.
    #
    # Separate from `run` so a host with its own supervision tree can own the
    # waiting, and so the startup ORDER is a thing a test can observe rather
    # than a comment.
    def boot(core: nil)
      jit = resolve_yjit

      # Before anything else: a native library built against a different
      # activation schema is refused here, with no controller connection
      # attempted.
      @core = core || Core.create

      declare_all
      # After the last declaration and before the first poll.
      Process.warmup

      @core.start(@namespace)
      observe_shutdown

      @executor = build_executor
      @executor.start

      Flezha.log.info(
        "runner up on namespace #{@namespace}: #{@executor.name} executor, " \
        "#{@executor.size} workers, yjit #{jit}"
      )
      self
    end

    # Declare, warm up, connect and serve until shutdown.
    def run(core: nil)
      boot(core: core)
      # Blocks here: the queue is pushed by `stop`, from a signal handler, the
      # stdin watcher, or the core's own shutdown.
      @stopped.pop
      drain
      self
    end

    # Stop fetching and drain what is in flight.
    def stop
      return if @stopping

      @stopping = true
      @stopped.push(:stop)
      nil
    end

    # Stop fetching, drain what is in flight, and release the core. `run` calls
    # it when the wait ends; a host that called `boot` calls it itself.
    def drain
      Flezha.log.info("draining")
      @core&.shutdown
      undrained = @executor ? @executor.stop : 0
      @teardown.each(&:call)
      @teardown.clear
      # The context is released only when nothing is still running against it.
      # A worker the bounded drain gave up on is inside user code and may yet
      # call the core; freeing the context under it would be a use-after-free
      # in native code. The process is on its way out either way, so the
      # kernel reclaims what this skips.
      @core&.free if undrained.zero?
      @core = nil
    end

    private

    def declare_all
      # The verdict before the registration: the core judges the record and
      # this SDK renders it, so an author meets a declaration refusal rather
      # than a native failure at the first connection.
      @classes.each { |klass| Graph.declare(klass.spec, @core, @registry) }
      raise Error, "a runner declares at least one operative class" if @registry.empty?
    end

    def build_executor
      deps = DispatchDeps.new(
        registry: @registry,
        bridge: @core,
        workflow_ctx: ->(instance_id) { WorkflowCtx.new(instance_id, @core) }
      )
      case @executor_kind
      when :threads then ThreadExecutor.new(@core, deps, **@executor_options)
      when :fibers then FiberExecutor.new(@core, deps, **@executor_options)
      else
        raise Error, "unknown executor `#{@executor_kind}`; the seam has :threads and :fibers"
      end
    end

    # Shutdown is observed three ways, because the harness, an operator and the
    # controller each have their own: end of file on standard input, a signal,
    # and the core's own stop — which every worker sees as `:shutting_down`
    # from its next poll.
    #
    # The first two are the core's, on a thread that holds no GVL and no Ruby
    # object: it installs the SIGINT and SIGTERM handler when the runner starts,
    # and the watch below is the same exit by another name. The harness manages
    # a runner as an Erlang Port and stops it by closing the port, which is this
    # process's standard input.
    def observe_shutdown
      @core.watch_stdin_eof(STDIN_EOF_GRACE_MS) unless $stdin.tty?
    end

    # YJIT is on by default because handler bodies are the one thing the GVL
    # bounds, so the JIT's gain lands exactly where the ceiling is. The
    # operator's decision wins, and the resulting state is reported in the
    # startup line so a measurement can never be ambiguous about it.
    def resolve_yjit
      return "disabled (runner override)" if @yjit == false
      return "unavailable (this Ruby was built without it)" unless defined?(RubyVM::YJIT)
      return "enabled (already on)" if RubyVM::YJIT.enabled?
      return "disabled (RUBY_YJIT_ENABLE set)" if ENV.key?("RUBY_YJIT_ENABLE")
      return "disabled (FLEZHA_YJIT=0)" if ENV["FLEZHA_YJIT"] == "0"

      RubyVM::YJIT.enable
      RubyVM::YJIT.enabled? ? "enabled" : "unavailable (enable refused)"
    end
  end

  # Start here: `Flezha.create_runner("default").define_stream(MyOperative).run`.
  def self.create_runner(namespace) = Runner.new(namespace)
end

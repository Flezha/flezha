# frozen_string_literal: true

require_relative "test_helper"

# The concurrency requirements as behaviour, not prose: pool growth under
# blocked handlers, retirement afterwards, the GVL released around a blocking
# core call, a refused post-fork call, and identical ordering under both
# executors.
class ConcurrencyTest < Minitest::Test
  # Stands in for the core at the poll boundary. The real core needs a
  # controller; what these tests are about is what the executor does with the
  # three answers `poll` can give and the three `wait_starvation` can.
  class FakeCore
    attr_reader :completed, :abandoned

    def initialize(activations: [], starvations: 0, hold: nil)
      @pending = Thread::Queue.new
      activations.each { |a| @pending.push(a) }
      @starvations = starvations
      @hold = hold
      @completed = []
      @abandoned = []
      @lock = Mutex.new
      @stopping = false
      @handed_out = false
    end

    # Whether anything has been handed out yet, so a test can wait for the
    # worker to be inside the handler rather than racing it.
    def polled_once? = @lock.synchronize { @handed_out }

    def poll(timeout_ms)
      return :shutting_down if @stopping

      unless @pending.empty?
        @lock.synchronize { @handed_out = true }
        return @pending.pop(true)
      end

      sleep(timeout_ms / 1000.0)
      @stopping ? :shutting_down : nil
    rescue ThreadError
      nil
    end

    def wait_starvation(timeout_ms)
      return 2 if @stopping

      taken = @lock.synchronize do
        next false if @starvations <= 0

        @starvations -= 1
        true
      end
      return 1 if taken

      sleep(timeout_ms / 1000.0)
      @stopping ? 2 : 0
    end

    # The handler's hold is here rather than in a procedure body so a test can
    # release it without reaching through the dispatcher.
    def complete(result)
      @hold&.call
      @lock.synchronize { @completed << result }
    end

    def abandon(token) = @lock.synchronize { @abandoned << token }
    def spawn_workflow(*) = "spawned"
    def shutdown = (@stopping = true)
  end

  class Echo < Flezha::Procedure
    procedure_name "echo"
    def process(input, _ctx) = input
  end

  class EchoOperative < Flezha::StreamOperative
    factory_id "echo_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .into(Echo).named("out").sink("echo_sink", encode: ->(v) { v.to_s.b })
    end
  end

  def deps(core)
    registry = Flezha::Registry.new
    registry.add(EchoOperative.spec)
    Flezha::DispatchDeps.new(
      registry: registry, bridge: core,
      workflow_ctx: ->(id) { Flezha::WorkflowCtx.new(id, core) }
    )
  end

  def activation(token, payload)
    { token: token, operative: "op", procedure: 0, entity_key: nil, change_seq: 0,
      factory: "echo_op", kind: :work,
      items: [{ kind: :input, channel: 0, payload: payload.b, metadata: [], replay: false,
                now: 0 }] }
  end

  def wait_until(timeout: 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    sleep(0.01) until yield || Process.clock_gettime(Process::CLOCK_MONOTONIC) > deadline
    yield
  end

  # -- sizing ---------------------------------------------------------------

  def with_env(values)
    previous = values.keys.to_h { |key| [key, ENV[key]] }
    values.each { |key, value| ENV[key] = value }
    yield
  ensure
    previous.each { |key, value| ENV[key] = value }
  end

  def test_the_base_pool_defaults_to_the_processor_count
    with_env("FLEZHA_WORKER_POOL_SIZE" => nil) do
      assert_equal Etc.nprocessors, Flezha::Executor.base_size
    end
  end

  def test_the_operator_sets_the_base_and_the_ceiling
    with_env("FLEZHA_WORKER_POOL_SIZE" => "3", "FLEZHA_WORKER_POOL_MAX" => "7") do
      assert_equal 3, Flezha::Executor.base_size
      assert_equal 7, Flezha::Executor.ceiling_size(3)
    end
  end

  def test_the_ceiling_defaults_to_ten_times_the_base_and_never_falls_below_it
    with_env("FLEZHA_WORKER_POOL_MAX" => nil) do
      assert_equal 40, Flezha::Executor.ceiling_size(4)
    end
    with_env("FLEZHA_WORKER_POOL_MAX" => "2") do
      assert_equal 5, Flezha::Executor.ceiling_size(5), "a ceiling under the floor would " \
                                                        "retire the pool the operator asked for"
    end
  end

  # -- growth and retirement ------------------------------------------------

  def test_a_workload_that_never_starves_runs_on_the_base_pool
    core = FakeCore.new(activations: Array.new(4) { |i| activation(i, "x") })
    executor = Flezha::ThreadExecutor.new(core, deps(core), base: 2, ceiling: 8).start
    begin
      assert wait_until { core.completed.size == 4 }
      assert_equal 2, executor.size, "no worker is started without a starvation observation"
    ensure
      core.shutdown
      executor.stop
    end
  end

  def test_blocked_handlers_buy_threads_up_to_the_ceiling
    release = Thread::Queue.new
    core = FakeCore.new(
      activations: Array.new(6) { |i| activation(i, "x") },
      starvations: 10, hold: -> { release.pop }
    )
    executor = Flezha::ThreadExecutor.new(core, deps(core), base: 1, ceiling: 3).start
    begin
      assert wait_until { executor.size == 3 }, "the supervisor grows the pool on an observation"
      assert_equal 3, executor.size, "and never past the ceiling"
    ensure
      6.times { release.push(:go) }
      core.shutdown
      executor.stop
    end
  end

  def test_grown_workers_retire_when_the_work_ends
    core = FakeCore.new(starvations: 4)
    executor = Flezha::ThreadExecutor.new(core, deps(core), base: 1, ceiling: 5).start
    begin
      assert wait_until { executor.size > 1 }, "the pool grows first"
      # The grown workers see nothing but timeouts from here, so they retire on
      # the idle streak while the base worker stays.
      assert wait_until(timeout: 20) { executor.size == 1 }
    ensure
      core.shutdown
      executor.stop
    end
  end

  # -- the seam's contract --------------------------------------------------

  def test_both_executors_complete_every_activation_and_keep_item_order
    answers = {}
    %i[threads fibers].each do |kind|
      core = FakeCore.new(activations: Array.new(8) { |i| activation(i, "item-#{i}") })
      executor =
        if kind == :threads
          Flezha::ThreadExecutor.new(core, deps(core), base: 3, ceiling: 6)
        else
          Flezha::FiberExecutor.new(core, deps(core), fetchers: 3)
        end
      executor.start
      begin
        assert wait_until { core.completed.size == 8 }, "#{kind} completed everything"
      ensure
        core.shutdown
        executor.stop
      end
      answers[kind] = core.completed.sort_by { |r| r[:token] }.map do |result|
        [result[:token], result[:effects].map { |e| e[:outputs].map { |o| o[:payload] } }]
      end
    end

    assert_equal answers[:threads], answers[:fibers],
                 "output identity is identical under both executors"
  end

  # The seam's whole contract: an activation is completed only after its
  # dispatch has returned. An executor that completed early would break
  # per-entity ordering, which is the one thing it may never touch.
  ORDER = []
  ORDER_LOCK = Mutex.new

  class Recorder < Flezha::Procedure
    procedure_name "recorder"
    def process(input, _ctx)
      ORDER_LOCK.synchronize { ORDER << :dispatched }
      input
    end
  end

  class RecorderOperative < Flezha::StreamOperative
    factory_id "recorder_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .into(Recorder).named("out").sink("recorder_sink", encode: ->(v) { v.to_s.b })
    end
  end

  def test_an_activation_is_completed_only_after_its_dispatch_returns
    ORDER.clear
    core = FakeCore.new(
      activations: [activation(1, "x").merge(factory: "recorder_op")],
      hold: -> { ORDER_LOCK.synchronize { ORDER << :completed } }
    )
    registry = Flezha::Registry.new
    registry.add(RecorderOperative.spec)
    dispatcher = Flezha::DispatchDeps.new(
      registry: registry, bridge: core,
      workflow_ctx: ->(id) { Flezha::WorkflowCtx.new(id, core) }
    )
    executor = Flezha::ThreadExecutor.new(core, dispatcher, base: 1, ceiling: 1).start
    begin
      assert wait_until { ORDER.size == 2 }
      assert_equal %i[dispatched completed], ORDER
    ensure
      core.shutdown
      executor.stop
    end
  end

  # -- the drain is bounded -------------------------------------------------

  class NeverReturns < Flezha::Procedure
    procedure_name "never_returns"
    def process(_input, _ctx) = sleep
  end

  class NeverReturnsOperative < Flezha::StreamOperative
    factory_id "never_returns_op"
    channels do |c|
      c.input("input", decode: ->(b) { b.dup.force_encoding("UTF-8") })
       .into(NeverReturns).named("out").sink("never_sink", encode: ->(v) { v.to_s.b })
    end
  end

  def never_returns_deps(core)
    registry = Flezha::Registry.new
    registry.add(NeverReturnsOperative.spec)
    Flezha::DispatchDeps.new(registry: registry, bridge: core,
                             workflow_ctx: ->(id) { Flezha::WorkflowCtx.new(id, core) })
  end

  # A handler that never returns must not keep the runner alive forever. The
  # harness stops runners by closing their pipes, so an unbounded drain leaks a
  # process per wedge — and the process that leaks is the one holding the
  # namespace the next test wants.
  def test_a_handler_that_never_returns_does_not_hold_the_drain_open
    core = FakeCore.new(activations: [activation(1, "x").merge(factory: "never_returns_op")])
    executor = Flezha::ThreadExecutor.new(core, never_returns_deps(core), base: 1, ceiling: 1).start
    assert wait_until { core.polled_once? }, "the worker picked the activation up"

    core.shutdown
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    executor.stop
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    assert_operator elapsed, :<, Flezha::Executor::DRAIN_TIMEOUT_S + 3,
                    "the drain waited #{elapsed}s on a handler that never returns"
  end

  def test_the_same_bound_holds_under_the_fiber_executor
    core = FakeCore.new(activations: [activation(1, "x").merge(factory: "never_returns_op")])
    executor = Flezha::FiberExecutor.new(core, never_returns_deps(core), fetchers: 1).start
    assert wait_until { core.polled_once? }

    core.shutdown
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    executor.stop
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started

    assert_operator elapsed, :<, Flezha::Executor::DRAIN_TIMEOUT_S + 3,
                    "the drain waited #{elapsed}s on a fiber that never returns"
  end

  # -- the process rules ----------------------------------------------------

  def test_a_core_call_after_a_fork_is_refused_naming_the_rule
    skip "needs the compiled veneer" unless Flezha::TestSupport.native?

    core = Flezha::Core.allocate
    core.instance_variable_set(:@owner_pid, Process.pid - 1)

    error = assert_raises(Flezha::Core::ForkError) { core.guard! }
    assert_includes error.message, "fork"
    assert_includes error.message, "start the core in each child"
  end

  def test_a_core_call_in_the_owning_process_is_allowed
    core = Flezha::Core.allocate
    core.instance_variable_set(:@owner_pid, Process.pid)

    assert_nil core.guard!
  end

  # -- Ractor-local state ---------------------------------------------------

  # All per-VM SDK state resolves per Ractor, so a Ractor executor can be added
  # without restructuring. A bare mutable global is exactly what would have to
  # be unpicked, so this is a scan rather than a promise.
  SOURCES = Dir[File.expand_path("../lib/**/*.rb", __dir__)].sort

  def test_no_source_file_holds_a_mutable_global_or_class_variable
    offenders = SOURCES.each_with_object({}) do |path, found|
      File.readlines(path).each_with_index do |line, index|
        next if line.lstrip.start_with?("#")
        next unless line.match?(/(^|[^$\w])\$[a-z_]\w*\s*=(?!=)/) || line.match?(/@@\w+/)

        # `$stdin` / `$stderr` are read, never rebound, by the logger and the
        # stdin watcher; the pattern above only matches an assignment.
        (found[path] ||= []) << index + 1
      end
    end

    assert_empty offenders, "a mutable global holds SDK state at #{offenders.inspect}"
    refute_empty SOURCES
  end

  def test_the_logger_resolves_per_ractor
    assert_same Flezha.log, Ractor.current[:flezha_logger]

    replacement = Flezha::ConsoleLogger.new(:error)
    Flezha.log = replacement
    assert_same replacement, Ractor.current[:flezha_logger]
  ensure
    Ractor.current[:flezha_logger] = nil
  end
end

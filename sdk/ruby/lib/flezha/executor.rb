# frozen_string_literal: true

require "etc"

require_relative "dispatch"
require_relative "logger"

module Flezha
  # The concurrency host: what runs `poll -> dispatch -> complete`, and on what.
  #
  # Two members in v1, and the seam between them may never observe or affect
  # per-entity ordering. Exclusivity is the core's handout guarantee — one
  # outstanding activation per Entity — and an activation is completed only
  # after its dispatch has returned, so ordering holds for any executor that
  # does not complete early. That invariant is this seam's entire contract.
  module Executor
    # How long a worker parks in one `poll`.
    #
    # `flz_core_shutdown` wakes *every* poller, so the ABI has no per-thread
    # wake and none is in scope to add. This value is therefore the documented
    # upper bound on how quickly `Thread#kill`, `Interrupt` and runner close
    # are observed: a stated bound, not an accident. It is Java's value, for
    # the same reason.
    POLL_TIMEOUT_MS = 250

    # How long the supervisor parks in one starvation wait. Shorter than the
    # poll timeout would only spin; longer would delay the first grown worker.
    STARVATION_WAIT_MS = 250

    # How long a drain has before the runner stops waiting for it.
    #
    # A handler that never returns holds its worker forever — the `wedge`
    # fixture is exactly that, and so is any real handler that blocks on
    # something that never answers. A drain that joined it unconditionally
    # would never finish, and a runner stopped by closing its pipes would leak
    # a process per wedge rather than exiting. So what is in flight gets this
    # long, and then the runner goes away regardless: the alternative is not
    # "no work is lost", it is "the runner never leaves".
    DRAIN_TIMEOUT_S = 5.0

    # Consecutive idle polls after which an above-base worker retires. At the
    # poll timeout above this is two seconds of no work, which is long enough
    # that a burst does not churn the pool and short enough that a spike does
    # not leave threads parked for the life of the process.
    IDLE_RETIREMENT_STREAK = 8

    module_function

    # One fetch loop, and the one place the three answers `poll` can give are
    # interpreted.
    #
    # Both executors fetch, and the deferred Ractor one will too — so what a
    # timeout, an activation and a shutdown each mean lives here rather than in
    # one correct copy per executor. The block is yielded each activation, and
    # `nil` on a timeout, so a caller that retires on an idle streak can count
    # them; answering `:stop` ends the loop, as does `stopping` answering true
    # or the core saying it is shutting down.
    def fetch(core, stopping)
      until stopping.call
        activation = core.poll(POLL_TIMEOUT_MS)
        return :shutting_down if activation == :shutting_down
        return :stopped if yield(activation) == :stop
      end
      :stopped
    end

    # The base pool size: the machine's processor count unless the operator
    # says otherwise. The same knob the Rust host reads, deliberately — this
    # host is the Rust host's model, and a second vocabulary for the same
    # decision is what makes a fleet hard to reason about.
    def base_size
      Env.positive("FLEZHA_WORKER_POOL_SIZE", Etc.nprocessors)
    end

    # When a drain must be finished by.
    def deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + DRAIN_TIMEOUT_S

    # How long is left of it, never negative — `Thread#join(0)` polls rather
    # than blocking, which is what a passed deadline should do.
    def remaining(deadline)
      [deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC), 0].max
    end

    # Say what the drain did not finish. Silence here is the failure mode the
    # bound exists to avoid reintroducing: a runner that leaves without
    # reporting what it abandoned looks identical to one that drained.
    def report_undrained(count)
      return if count.zero?

      Flezha.log.warn(
        "#{count} activation(s) were still running after the #{DRAIN_TIMEOUT_S}s drain; the " \
        "runner is exiting anyway. The core re-hands unanswered work on the next bind — find " \
        "the handler that does not return."
      )
    end

    # The ceiling: ten times the base unless the operator says otherwise, and
    # never below the base — a ceiling under the floor would retire the pool
    # the runner was told to keep.
    def ceiling_size(base)
      [Env.positive("FLEZHA_WORKER_POOL_MAX", base * 10), base].max
    end
  end

  # The default executor: a base pool of Ruby threads each looping
  # `poll -> dispatch -> complete` on itself, plus one supervisor parked in
  # `wait_starvation` that grows the pool on an observation.
  #
  # This is the Rust host's model rather than Java's hand-off, and the reason
  # is the reason behind both shipped hosts: Java hands off because a parked
  # virtual thread costs a stack and nothing else — which is also exactly why
  # Java never starves and never calls `wait_starvation`. An MRI thread is a
  # real OS thread with a real stack, so hand-off would buy nothing and cost a
  # thread per activation up to the core's in-flight cap.
  #
  # Concurrency is bounded by the core's `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`,
  # never by the pool size. The pool is a servicing detail.
  class ThreadExecutor
    attr_reader :base, :ceiling

    def initialize(core, deps, base: nil, ceiling: nil)
      @core = core
      @deps = deps
      @base = base || Executor.base_size
      @ceiling = ceiling || Executor.ceiling_size(@base)
      @workers = []
      @supervisor = nil
      @lock = Mutex.new
      @stopping = false
    end

    def name = "threads"

    # How many workers are alive now. Reported in the startup line and read by
    # the pool-growth tests.
    def size = @lock.synchronize { @workers.count(&:alive?) }

    def start
      @lock.synchronize { @base.times { spawn_worker(above_base: false) } }
      @supervisor = Thread.new { supervise }
      @supervisor.name = "flezha-starvation"
      self
    end

    # Stop fetching and drain. The core says when: a worker leaves on the poll
    # that reports the core shutting down, which the core reports only once
    # the shutdown sequence has shipped every Entity's state through these
    # same workers; a worker inside a handler finishes it, completes it, and
    # only then observes it. Bounded by `DRAIN_TIMEOUT_S` — a handler that
    # never returns must not keep the runner alive forever — and the flag is
    # raised after the join so a worker spawned late still leaves.
    def stop
      deadline = Executor.deadline
      @lock.synchronize { @workers.dup }.each { |worker| worker.join(Executor.remaining(deadline)) }
      @stopping = true
      @supervisor&.join(Executor.remaining(deadline))
      undrained = @lock.synchronize { @workers.count(&:alive?) }
      Executor.report_undrained(undrained)
      @lock.synchronize { @workers.clear }
      undrained
    end

    private

    # One worker. `above_base` workers retire after an idle streak; base
    # workers never do, so the pool the operator asked for stays up.
    def spawn_worker(above_base:)
      worker = Thread.new { run_worker(above_base) }
      worker.name = "flezha-worker-#{@workers.size}"
      @workers << worker
      worker
    end

    def run_worker(above_base)
      idle = 0
      Executor.fetch(@core, -> { @stopping }) do |activation|
        if activation.nil?
          idle += 1
          next :stop if above_base && idle >= Executor::IDLE_RETIREMENT_STREAK
        else
          idle = 0
          Dispatch.dispatch(activation, @core, @deps)
        end
      end
    rescue StandardError => e
      # A worker that dies silently is a pool that shrinks with no report; the
      # supervisor grows it back only if the core starves, which it will not
      # while the remaining workers keep up.
      Flezha.log.error("a worker thread ended on #{e.class}: #{e.message}")
    end

    # The supervisor: parked in the core's starvation wait, which answers when
    # work is queued and no thread is inside `poll`. One worker per
    # observation, up to the ceiling.
    def supervise
      at_ceiling_logged = false
      until @stopping
        case @core.wait_starvation(Executor::STARVATION_WAIT_MS)
        when 1
          grown = @lock.synchronize do
            next false if @workers.count(&:alive?) >= @ceiling

            spawn_worker(above_base: true)
            true
          end
          if grown
            at_ceiling_logged = false
          elsif !at_ceiling_logged
            Flezha.log.warn(
              "the core is starving and the worker pool is at its ceiling of #{@ceiling}; " \
              "raise FLEZHA_WORKER_POOL_MAX, or find what is holding the handlers"
            )
            at_ceiling_logged = true
          end
        when 2
          break
        end
      end
    rescue StandardError => e
      Flezha.log.error("the starvation supervisor ended on #{e.class}: #{e.message}")
    end
  end

  # The opt-in executor for I/O-heavy handlers: real-thread fetchers hand
  # activations to fibers through a `Thread::Queue`, which is scheduler-aware,
  # so a handler's I/O yields instead of blocking a thread.
  #
  # The fetcher stays a real thread under this executor too. `poll` blocks in
  # native code, and `docs/sdk/poll_core.md` is explicit that fetching on a
  # runtime's shared I/O pool starves everything else that pool serves.
  #
  # Completion happens on the fiber that dispatched, not on the fetcher: the
  # ABI accepts a completion from any thread, and the ordering guarantee is the
  # core's handout, not this executor's.
  class FiberExecutor
    # The queue depth between the fetchers and the fibers. Bounded so a fetcher
    # cannot run arbitrarily far ahead of the handlers; the core's in-flight
    # cap bounds the total either way.
    QUEUE_DEPTH = 64

    def initialize(core, deps, fetchers: nil, scheduler: nil)
      @core = core
      @deps = deps
      @fetchers = fetchers || Executor.base_size
      @scheduler = scheduler
      @queue = Thread::SizedQueue.new(QUEUE_DEPTH)
      @threads = []
      @stopping = false
    end

    def name = "fibers"
    def size = @threads.count(&:alive?)

    def start
      @dispatcher = Thread.new { run_dispatcher }
      @dispatcher.name = "flezha-fibers"
      @fetchers.times do |index|
        thread = Thread.new { run_fetcher }
        thread.name = "flezha-fetcher-#{index}"
        @threads << thread
      end
      self
    end

    # The fetchers leave on the core's word, as the thread executor's workers
    # do; the flag is raised after their join.
    def stop
      deadline = Executor.deadline
      @threads.each { |thread| thread.join(Executor.remaining(deadline)) }
      @stopping = true
      # Closing the queue is what ends the dispatcher: a scheduler-aware pop
      # answers nil on a closed queue rather than parking forever.
      @queue.close
      # Bounded for the reason the thread executor's join is: closing a
      # scheduler runs its outstanding fibers to completion, and a fiber that
      # never returns would hold the drain open forever.
      @dispatcher&.join(Executor.remaining(deadline))
      undrained = [@dispatcher, *@threads].count { |thread| thread&.alive? }
      Executor.report_undrained(undrained)
      undrained
    end

    private

    def run_fetcher
      Executor.fetch(@core, -> { @stopping }) do |activation|
        @queue.push(activation) if activation
      end
    rescue ClosedQueueError
      nil
    rescue StandardError => e
      Flezha.log.error("a fetcher thread ended on #{e.class}: #{e.message}")
    end

    # The scheduler is never closed by hand: Ruby closes a thread's scheduler
    # when the thread ends, and closing it runs every outstanding fiber to
    # completion first. That is what makes a drain under this executor mean the
    # same thing it means under the thread one — and closing it here as well
    # would be a second close, which the scheduler refuses.
    def run_dispatcher
      Fiber.set_scheduler(build_scheduler)
      while (activation = @queue.pop)
        Fiber.schedule { Dispatch.dispatch(activation, @core, @deps) }
      end
    rescue ClosedQueueError
      nil
    rescue StandardError => e
      Flezha.log.error("the fiber dispatcher ended on #{e.class}: #{e.message}")
    end

    def build_scheduler
      return @scheduler.call if @scheduler

      begin
        require "async"
      rescue LoadError
        raise Error,
              "the fiber executor needs a Fiber::Scheduler. Add the `async` gem, or pass " \
              "`scheduler:` a callable answering one."
      end
      Async::Scheduler.new
    end
  end
end

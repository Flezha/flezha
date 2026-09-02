# frozen_string_literal: true

require_relative "errors"
require_relative "logger"

module Flezha
  # Idle observation for entry channels: the policy half of the input-liveness
  # signal the core records.
  #
  # The core knows when a frame last arrived and owns the episode latch over
  # it (`flz_core::entry_input::IdleWatch`, reached by watch id); only the
  # application knows whether the silence since then is anomalous — a quiet
  # overnight market is not a dead feed. So the SDK ships no default threshold
  # and no default action: `Runner#on_entry_idle` takes both from the caller
  # and reports episodes.
  #
  # The tick is one Ruby thread per runner, woken by a queue pop with a
  # timeout — never `sleep` — so `Runner#drain` ends it by pushing. It runs
  # while the GVL is free: a pure-Ruby body holding the lock past the tick
  # delays the report by its length and never changes the count, because the
  # latch compares stamps, not clock readings.
  class EntryIdleWatcher
    # Tick floor and ceiling, the same in every SDK. The floor bounds how often
    # a fleet of observers wakes; the ceiling bounds how late an episode is
    # reported.
    TICK_MIN_S = 0.25
    TICK_MAX_S = 1.0

    # The two poll answers that are episode boundaries; 0 is none and -1 an id
    # no watch has.
    IDLE = 1
    RESUMED = 2

    Watch = Struct.new(:channel, :id, :observer)

    # A bare block or callable is an observer whose `resumed` does nothing.
    class CallableObserver
      def initialize(callable)
        @callable = callable
      end

      def idle(gap_ms) = @callable.call(gap_ms)
    end

    # The tick a `threshold_ms` asks for: a quarter of it, so an episode is
    # reported within 25% of the threshold, bounded at both ends.
    def self.tick_period_s(threshold_ms)
      (threshold_ms / 4000.0).clamp(TICK_MIN_S, TICK_MAX_S)
    end

    # When `channel` last received a frame, or nil before its first frame (and
    # for a name no declared operative has — both mean "nothing has arrived
    # here"). The core's stamp is monotonic; the wall-clock instant is derived
    # from the gap, so a clock step between then and now cannot skew it.
    def self.last_entry_delivery(core, channel)
      last = core.entry_last_delivery_nanos(channel.to_s)
      return nil if last.zero?

      gap = [core.monotonic_nanos - last, 0].max
      Time.now - (gap / 1_000_000_000.0)
    end

    # `ticking: false` leaves the tick to the caller — a test's seam, the way
    # the core's `stamp_at` is one.
    def initialize(core, ticking: true)
      @core = core
      @ticking = ticking
      @watches = []
      @lock = Mutex.new
      @wake = Thread::Queue.new
      @thread = nil
      @period_s = nil
    end

    # Register `observer` (or the block) against `channel`. Legal before and
    # after `boot`: the core interns the observation by name, so a
    # registration made before the class is declared watches the same channel
    # it later gets. Several observers may watch one channel.
    def register(channel, threshold_ms, observer = nil, &block)
      observer = admit(observer || block, threshold_ms)
      id = @core.idle_watch_new(channel.to_s, (threshold_ms * 1_000_000).round)
      raise Error, "on_entry_idle: the core refused the channel name `#{channel}`" if id.zero?

      wanted = self.class.tick_period_s(threshold_ms)
      @lock.synchronize do
        @watches << Watch.new(channel.to_s, id, observer)
        @period_s = wanted if @period_s.nil? || wanted < @period_s
        @thread ||= spawn_ticker if @ticking
      end
      # A shorter period re-arms the wait at once rather than after the old one.
      @wake << :period
    end

    # Evaluate every watch once and deliver the episodes the core found.
    def tick
      @lock.synchronize { @watches.dup }.each do |watch|
        status, gap_nanos = @core.idle_watch_poll(watch.id)
        case status
        when IDLE then deliver(watch, :idle, gap_nanos)
        when RESUMED then deliver(watch, :resumed, gap_nanos)
        end
      end
    end

    # Stop the tick and drop every registration, in the core too. Idempotent.
    def stop
      thread = @lock.synchronize { @thread.tap { @thread = nil } }
      if thread
        @wake << :stop
        thread.join unless thread == Thread.current
      end
      @lock.synchronize do
        @watches.slice!(0..).each { |watch| @core.idle_watch_free(watch.id) }
        @period_s = nil
      end
    end

    private

    def admit(observer, threshold_ms)
      raise ArgumentError, "on_entry_idle: an observer or a block is required" unless observer
      unless threshold_ms.is_a?(Numeric) && threshold_ms.positive?
        raise ArgumentError,
              "on_entry_idle: the threshold must be a positive number of milliseconds, " \
              "got #{threshold_ms.inspect}"
      end

      observer.respond_to?(:idle) ? observer : CallableObserver.new(observer)
    end

    def spawn_ticker
      Thread.new { run_ticks }.tap { |thread| thread.name = "flezha-entry-idle" }
    end

    def run_ticks
      loop do
        period = @lock.synchronize { @period_s } || TICK_MAX_S
        break if @wake.pop(timeout: period) == :stop

        tick
      end
    end

    # Invoke the observer, contained: application code runs on the SDK's own
    # thread, so a raise must become a log line rather than a dead ticker.
    def deliver(watch, episode, gap_nanos)
      gap_ms = gap_nanos / 1_000_000.0
      if episode == :idle
        watch.observer.idle(gap_ms)
      elsif watch.observer.respond_to?(:resumed)
        watch.observer.resumed(gap_ms)
      end
    rescue StandardError => e
      Flezha.log.warn(
        "entry-idle observer raised on the #{episode} episode of channel '#{watch.channel}': " \
        "#{e.message}"
      )
    end
  end
end

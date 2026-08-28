# frozen_string_literal: true

module Flezha
  # The SDK's diagnostics, through a logger the host supplies.
  #
  # The default writes to standard error at `warn` and above, which is what the
  # other SDKs' defaults amount to: a runner that says nothing about a wedge is
  # the failure mode the diagnostics exist to close, and one that narrates
  # every activation is noise. No observability dependency — a host that wants
  # spans or structured fields supplies its own logger.
  class ConsoleLogger
    ORDER = { debug: 0, info: 1, warn: 2, error: 3 }.freeze

    def initialize(level = :warn)
      @level = ORDER.fetch(level.to_sym, 2)
    end

    ORDER.each_key do |level|
      define_method(level) { |message| write(level, message) }
    end

    private

    def write(level, message)
      return if ORDER[level] < @level

      $stderr.puts("[flezha] #{message}")
    rescue StandardError
      # A logger must never take the process down. The case that reaches here
      # is a closed standard stream during shutdown — the harness closes the
      # runner's pipes to stop it — where writing raises EPIPE, and an
      # unhandled stream error would turn a clean drain into a crash.
      nil
    end
  end

  # Per-Ractor rather than a bare global, for the reason every handle in this
  # SDK is: a Ractor executor must be addable without restructuring, and a
  # shared mutable global is exactly what would have to be unpicked.
  def self.log
    Ractor.current[:flezha_logger] ||= ConsoleLogger.new(ENV.fetch("FLEZHA_LOG_LEVEL", "warn"))
  end

  def self.log=(logger)
    Ractor.current[:flezha_logger] = logger
  end
end

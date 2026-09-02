#!/usr/bin/env ruby
# frozen_string_literal: true

# The Ruby SDK's e2e test app.
#
# Registers the reference fixture twins and runs until shutdown. The namespace
# is ARGV[0] (default "default"), matching rust_runner / java_runner /
# node_runner / elixir_runner. The executor is an axis of the run rather than a
# property of this file: FLEZHA_RUBY_EXECUTOR selects the thread pool (the
# default) or the fiber scheduler, and the fixtures are identical under both.

$LOAD_PATH.unshift(File.expand_path("../../../sdk/ruby/lib", __dir__))

require "flezha"
require_relative "fixtures"

namespace = ARGV[0] || "default"

runner = Flezha.create_runner(namespace)
runner.logger(Flezha::ConsoleLogger.new(ENV.fetch("FLEZHA_LOG_LEVEL", "info")))
Fixtures::ALL.each { |operative| runner.define_stream(operative) }
# A runner-level registration, not a declaration: armed beside the fixtures.
Fixtures.arm_entry_idle_probe(runner)

# The harness stops a runner by closing its pipes. A write that lands after
# that raises EPIPE on the stream, and an unhandled stream error would turn a
# clean drain into a crash the run reports as a runner failure.
[$stdout, $stderr].each { |stream| stream.sync = true }

# The configuration this run is under, before it starts: readiness is the
# SDK's own startup line, which reports the executor, the pool and the JIT.
warn(
  "[ruby_runner] namespace=#{namespace} transport=#{ENV.fetch('FLEZHA_TRANSPORT', 'uds')} " \
  "executor=#{ENV.fetch('FLEZHA_RUBY_EXECUTOR', 'threads')}"
)

runner.run

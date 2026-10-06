# frozen_string_literal: true

# The runner: connects to the controller and serves the starter relay until a
# SIGTERM or its standard input closing.

require "flezha"
require_relative "starter_relay"

Flezha.create_runner(ARGV[0] || "default").register_factory(StarterRelay).run

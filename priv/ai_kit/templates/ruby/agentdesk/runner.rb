# frozen_string_literal: true

# Starts the desk's runner: the process that connects to the controller and
# serves agentdesk's relays, in the `agents` namespace.

require "flezha"
require_relative "agent_relay"
require_relative "gateway_relay"

# The sessions print their undos; written at once, they reach the log in the
# order they happened.
$stdout.sync = true

runner = Flezha.create_runner("agents")
               .register_factory(AgentRelay)
               .register_factory(GatewayRelay)
               .start

runner.await_termination

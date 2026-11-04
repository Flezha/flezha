# frozen_string_literal: true

# Starts the desk's runner: the process that connects to the controller and
# serves tradedesk's relays.
#
# The namespace is the first argument and defaults to `trading`. Under an
# enforced controller the runner proves its namespace with that namespace's
# key, read from `FLEZHA_NAMESPACE_SECRET`.
#
# Registering a factory starts nothing: it offers the controller relays of
# that type, which are started from it later. `register_factory` is the same
# call for both kinds, the stream that tracks prices and the steps relay
# that places one order.

require "flezha"
require_relative "trade_watcher_relay"
require_relative "order_executor_relay"

# The desk prints its bookings and undos; written at once, they reach a log in
# the order they happened.
$stdout.sync = true

runner = Flezha.create_runner(ARGV[0] || "trading")
               .register_factory(TradeWatcherRelay)
               .register_factory(OrderExecutorRelay)
               .start

# Blocks until shutdown: a SIGTERM, Ctrl-C, or standard input closing.
runner.await_termination

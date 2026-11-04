"""Starts the desk's runner: the process that connects to the controller and
serves tradedesk's relays.

The namespace is the first argument and defaults to `trading`. Under an
enforced controller the runner proves its namespace with that namespace's key,
read from `FLEZHA_NAMESPACE_SECRET`.

Registering a factory starts nothing: it offers the controller relays of
that type, which are started from it later. `register_factory` is the same call
for both kinds, the stream that tracks prices and the steps relay that
places one order.
"""

import sys

from order_executor import order_executor
from trade_watcher import trade_watcher

import flezha


def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "trading"
    flezha.configure_logging("info")

    runner = (
        flezha.create_runner(namespace)
        .register_factory(trade_watcher)
        .register_factory(order_executor)
    )

    # Blocks until shutdown: a SIGTERM, Ctrl-C, or standard input closing.
    runner.run()


if __name__ == "__main__":
    main()

"""The runner: connects to the controller and serves the starter relay until
shutdown, a SIGTERM or its standard input closing."""

import sys

from starter_relay import starter_relay

import flezha

if __name__ == "__main__":
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging("info")
    flezha.create_runner(namespace).register_factory(starter_relay).run()

"""Starts the desk's runner: the process that connects to the controller and
serves agentdesk's relays, in the `agents` namespace.
"""

from gateway import gateway

import flezha
from agent import agent


def main() -> None:
    runner = flezha.create_runner("agents").register_factory(agent).register_factory(gateway)

    runner.run()


if __name__ == "__main__":
    main()

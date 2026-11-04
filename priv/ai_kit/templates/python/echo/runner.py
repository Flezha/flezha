"""Starts a runner: a process that connects to the controller and serves your
relays.

Declare every relay you want served before `start`. One runner can serve
several, and `register_factory` is the same call for the steps kind.
"""

import sys

from echo_relay import echo_relay

import flezha


def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging("info")

    # the relays this runner serves
    runner = flezha.create_runner(namespace).register_factory(echo_relay)

    # `run()` is `start()` followed by `await_termination()`. Splitting them
    # leaves room for the calls below, and still blocks until shutdown.
    runner.start()  # connects to the controller

    # Everything `flz relay` and `flz plugin` do is also a method of
    # `control()`, and `send` hands a message to a relay. Uncomment to drive
    # this namespace from the application instead of from a terminal:
    #
    # control = runner.control()
    # control.start_relay("echo", id="my_echo")
    # control.enable_plugin("writer", "relay_file_output", opts={"path": "echo-out.txt"})
    # control.bind_plugin("writer", "my_echo", hooks=[["on_output", {}]])
    # runner.send("my_echo", "hello world", "in", msg_id="hello-1")
    #
    # docs/public/topics/core/sending_and_calling.md covers sending. An enforced
    # controller refuses all of it until `flz role bind runner@default app default`.

    # Blocks until shutdown: a SIGTERM, or standard input closing.
    runner.await_termination()


if __name__ == "__main__":
    main()

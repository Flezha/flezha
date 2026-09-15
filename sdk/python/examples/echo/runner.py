"""Starts a runner: a process that connects to the controller and serves your
operatives.

Declare every operative you want served before `start`. One runner can serve
several, and `define_workflow` is the same call for the workflow kind.
"""

import sys

from echo_operative import echo_operative

import flezha


def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging("info")

    runner = flezha.create_runner(namespace).define_stream(echo_operative)

    # `run()` is `start()` followed by `await_termination()`. Splitting them
    # leaves room for the admin calls below, and still blocks until shutdown.
    runner.start()

    # Everything `flz operative` and `flz plugin` do is also a method here.
    # Uncomment to drive this namespace from the application instead of from a
    # terminal:
    #
    # admin = runner.admin()
    # admin.start_operative("my_echo", "echo")
    # admin.enable_plugin(
    #     "writer",
    #     {
    #         "id": "writer",
    #         "provider": "operative_file_sink",
    #         "operative_id": "my_echo",
    #         "opts": {"path": "echo-out.txt"},
    #         "hooks": [["on_output", {}]],
    #     },
    # )
    # admin.send("my_echo", "hello world", "in")
    #
    # docs/sdk/admin.md has the whole surface. An enforced controller refuses all
    # of it until `flz role bind runner@default app default`.

    # Blocks until shutdown: a SIGTERM, or standard input closing.
    runner.await_termination()


if __name__ == "__main__":
    main()

"""The hello world: a line of text in, the same line prefixed out.

Three parts:

* the **operative**, the graph that says where input comes in, which steps it
  passes through and where the output goes;
* the **procedure**, the processing stage the graph routes each message into;
* the **runner**, this process, which offers the operative to a controller and
  then does what it is told.

Run it against a controller:

    sdk/python/.venv/bin/python examples/python/echo/echo.py

Then, from another terminal:

    flz operative start my_echo echo
    flz operative send my_echo --text 'hello world' --await 2000
"""

from __future__ import annotations

import sys

import flezha


@flezha.procedure(name="echo")
def echo(value: str) -> str:
    """Answer the input with a prefix.

    The name is durable: per-entity state is filed under it, so renaming this
    later declares a different procedure rather than renaming its state.
    """
    return f"echo: {value}"


@flezha.stream_operative("echo")
def echo_operative(c: flezha.Channels) -> None:
    """The graph: where input comes in, the stage it passes through, where output goes.

    ``in`` is the address ``flz operative send --channel`` writes to, and
    ``out`` is the name stamped on every value the channel delivers. Both are
    addresses rather than labels, so changing either is a breaking change.
    """
    c.input("in", decode=lambda body: bytes(body).decode()).into(echo).sink(
        "out", encode=lambda value: str(value).encode()
    )


def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging("info")

    runner = flezha.create_runner(namespace)
    runner.define_stream(echo_operative)
    print(f"[echo] offering 'echo' in namespace {namespace}", file=sys.stderr, flush=True)

    # Connects and offers the operative. Declaring an operative starts nothing:
    # the controller decides when one runs. `run()` is `start()` followed by
    # `await_termination()`, and splitting them leaves room for the admin calls.
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

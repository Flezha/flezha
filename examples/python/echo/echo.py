"""The hello world: one message in, one message out.

Three parts, and every Flezha application has these same three:

* the **procedure**, the code that runs;
* the **operative**, the graph that says where its input comes from and where
  its output goes;
* the **runner**, this process, which offers the operative to a controller and
  then does what it is told.

Run it against a controller:

    sdk/python/.venv/bin/python examples/python/echo/echo.py

Then, from another terminal:

    flz operative start my_echo echo
    flz operative send my_echo --text hello --await 2000
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
    """The graph, and it holds no logic at all.

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

    # Connects, offers the operative, and blocks until shutdown. Declaring an
    # operative starts nothing: the controller decides when one runs.
    runner.run()


if __name__ == "__main__":
    main()

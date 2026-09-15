# flezha

The Flezha Python SDK — a poll host over `flz_core`, the runner kernel the
Rust, Java, Node and Ruby SDKs also drive.

The kernel owns transport, ordering, per-entity exclusivity, durability,
retries, timers, credit, routing and the workflow driver. This package brings
the authoring surface, the codec for user types, the threading host and
idiomatic errors — and no kernel code.

```python
import flezha


@flezha.procedure
def echo(value: str) -> str:
    return value


@flezha.stream_operative("echo")
def echo_operative(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(echo).sink("out")


flezha.create_runner("default").define_stream(echo_operative).run()
```

- **Run a graph with no controller.** `LocalOperative.start` takes the same
  operative and runs its declared graph in this process — for trying Flezha out
  and for developing an operative. State is held in memory and nothing is
  durable; every run says so.
- **Python 3.11+**, CPython only. Two wheel tiers from one source: an `abi3`
  compatibility wheel per platform, and per-version free-threaded wheels.
- **The GIL bounds only handler bodies.** Every blocking core call releases it,
  so transport, framing, durability, retries, credit, routing and the
  activation decode all run in parallel in Rust.
- **Typed end to end**, shipping `py.typed`: a wrong edge is a type-checker
  error before it is a registration error.
- **A channel names its payload format.** `codec=` on `input` and `sink` takes
  `"bytes"`, `"text"`, `"json"` or `"msgpack"`. With none, an entry reads JSON and
  a sink sends bytes unchanged, a string as UTF-8 and anything else as JSON, so a
  sink needs no codec for a string, bytes or a JSON value. An Elixir entry
  declared with no decoder hands over the binary instead.
- **Never `pickle`.** Durable state is MessagePack from author-declared
  dataclasses, Pydantic models or attrs classes.

Full documentation:
[`docs/sdk/python_sdk.md`](https://github.com/alfetahe/flezha/blob/main/docs/sdk/python_sdk.md).

Licensed under Apache-2.0.

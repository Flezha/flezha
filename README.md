# Flezha

A durable, distributed runtime for stateless and stateful data processing.

> Flezha is pre-1.0 alpha software: interfaces may still change between releases.

Code that processes data tends to grow the same scaffolding around it: somewhere
to keep state, a way to recover after a crash, retries, a way to spread the work
over more machines. Flezha provides that scaffolding, so your code can stay about
the data.

You write the logic in Java, Rust, Python, Node, Ruby, PHP or Elixir. Flezha runs
it as many small, independent pieces in parallel, on one machine or across
several, keeps each piece's state on disk, and when a process or a machine fails,
starts the work again elsewhere and carries on from where it stopped.

## What you can build

The same building blocks carry many shapes of data processing:

- **Streams and ETL pipelines** that read from files, HTTP, WebSockets, Kafka or
  Postgres, transform with state that survives a crash, and load into Postgres,
  Kafka or files.
- **Workflows and long-running jobs** that commit progress at every step, retry
  a failing step, and undo finished steps when a later one fails.
- **Scheduled and timer-based work:** batch jobs on a cron schedule, windows,
  timeouts and reminders kept per key.
- **A service per user, device or order,** each copy with its own state, from a
  handful to thousands running side by side.
- **AI agents and chat sessions,** one per conversation, that remember what was
  said and survive a crash (see [Working with AI](#working-with-ai)).

## How it works

- **The controller** coordinates everything: it decides where work runs, keeps
  its state on disk, and starts it again elsewhere when something fails.
- **A runner** is your own program, written with one of the seven SDKs. It
  connects to the controller and runs the code you registered.
- **A relay** is one running copy of that code. A relay is a processing unit
  that works like a worker: it keeps its own state and carries data through its
  tasklets. Flezha runs thousands of relays in parallel. Inside a relay, your
  logic is split into small processing stages, called tasklets, wired into a
  graph.
- **Plugins** connect relays to the outside world (files, HTTP, Kafka,
  Postgres, RabbitMQ and more) without changing their code.

[What is Flezha](docs/getting_started/what_is_flezha.md) explains the pieces in
more detail.

## A look at the code

An echo in Python: it takes a line of text and sends it back.

**1. Write the logic.** A tasklet is one processing stage, run once per message:

```python
import flezha

@flezha.tasklet(name="echo")
class Echo:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(f"echo: {value}")
```

**2. Wire it into a relay.** The relay says where messages come in, which
stages they pass through and where results leave:

```python
@flezha.relay("echo")
def echo_relay(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(Echo).output("out")
```

**3. Serve it from a runner,** a process that connects to the controller:

```python
flezha.create_runner("default").register_factory(echo_relay).run()
```

**4. Start it and send it a message** with `flz`, the command-line tool that
comes with the controller:

```sh
$ ./bin/flz relay start echo --id my_echo
$ ./bin/flz relay send my_echo --text 'hello world' --await 2000
Delivered to 'my_echo'.
out	echo: hello world
```

The same echo exists in every SDK, under `sdk/<language>/examples/echo`.

## Every relay has an API

The entry channels a relay declares are its public API, with no extra step. Describe a channel
and say what it takes, here in Java, where a record is read into a JSON Schema:

```java
public record Order(String pair, BigDecimal amount) {}

c.input(Order.class, "place").describe("Places an order").into(Place.class).output("placed");
```

`flz relay info` shows the API, with an example call per channel:

```sh
$ ./bin/flz relay info orders-eu
```

```output
API of relay 'orders-eu' (factory 'orders'):
  place  json → answer json
    Places an order
    schema: {"properties":{"amount":{"type":"number"},"pair":{"type":"string"}},"required":["pair","amount"],"type":"object"}
    MCP tool: orders_place
    flz relay call orders-eu --channel place --arg pair=... --arg amount=...
```

An application calls it over HTTP with the [HTTP API plugin](docs/plugins/http_api.md):

```sh
$ curl -X POST http://127.0.0.1:8765/v1/relays/orders-eu/place \
    -H "Authorization: Bearer $FLEZHA_TOKEN" -d '{"pair":"EUR/USD","amount":1000}'
{"amount":1000,"pair":"EUR/USD","status":"placed"}
```

AI agents can call any relay as a tool (tool calling): every described channel is a tool on
Flezha's MCP endpoint, so an agent connected with `flz mcp` calls `orders_place` with
`{"relay": "orders-eu", "pair": "EUR/USD", "amount": 1000}` and gets the same answer as
structured content. [A relay's API](docs/topics/basics/relay_api.md) covers the seven SDKs.

## Try it

The echo above runs in Docker, controller included. From the root of the
repository:

```sh
$ cd sdk/python/examples/echo_deployment
$ docker compose up -d --wait
$ docker compose exec controller /flezha/bin/flz relay start echo --id my_echo
$ docker compose exec controller /flezha/bin/flz relay send my_echo --text 'hello world' --await 2000
```

To try a relay without a controller or Docker,
[local mode](docs/topics/basics/local_mode.md) runs it inside your own process.
To start a project of your own, `flz template create <language> <dir>` writes a small,
tested one in any of the seven languages ([Template commands](docs/topics/cli/template_commands.md)).
[Up and running](docs/tutorials/echo/first_steps.md) builds the echo by hand, and the
[tutorials](docs/tutorials/tutorials.md) then build two larger example applications step by step.

## Working with AI

Flezha works with AI from both sides: as a tool AI assistants can drive, and as a
runtime for AI services.

- **For AI assistants.** `flz mcp` is an MCP server an assistant adds with one
  command: it gives the assistant the working rules and the current SDK form for its
  language before any controller runs, and then the controller's operations with a
  token of its own ([`docs/topics/working_with_ai_assistants.md`](docs/topics/working_with_ai_assistants.md)).
  A controller on another machine serves the same over HTTP
  ([`docs/plugins/http_api.md`](docs/plugins/http_api.md)). Your relays are tools too:
  each described entry channel is one an agent can call
  ([`docs/topics/basics/relay_api.md`](docs/topics/basics/relay_api.md)).
  Every error carries a stable code and a hint for the next step
  ([`docs/errors.md`](docs/errors.md)), every `flz` command can answer in JSON, the
  documentation ships an `llms.txt` map to its pages in Markdown, and local mode tries a
  change in seconds.
- **For AI services.** Each conversation runs as its own session with its own
  state; answers stream while they are written, a turn can be taken back, and a
  crash does not end the conversation. Your code calls the model SDK or agent
  framework you already use. [The agentdesk tutorial](docs/tutorials/agentdesk/agentdesk.md)
  builds one.

<!-- TODO: AI plugins, for example a model-call plugin (one place for provider keys,
retries, rate limits and token accounting) and a vector-store source and output.
Decide which ones, then specify each in OpenSpec. -->

## Documentation

| Where to go | What you find |
|---|---|
| [Getting started](docs/getting_started/what_is_flezha.md) | What Flezha is, and the echo running in Docker |
| [Up and running](docs/tutorials/echo/first_steps.md) | The echo built by hand: a controller, your own runner and a relay |
| [Tutorials](docs/tutorials/tutorials.md) | Two example applications: a trading desk on a demo exchange, and AI agent sessions |
| [Documentation index](docs/README.md) | Every topic page, the configuration reference, the plugins and the SDKs |
| [Errors](docs/errors.md) | Every error code and what to do about it |

## SDKs

| Language | Repository |
|---|---|
| Java | [flezha_java_sdk](https://github.com/flezha/flezha_java_sdk) |
| Rust | [flezha_rust_sdk](https://github.com/flezha/flezha_rust_sdk) |
| Python | [flezha_python_sdk](https://github.com/flezha/flezha_python_sdk) |
| Node | [flezha_node_sdk](https://github.com/flezha/flezha_node_sdk) |
| Ruby | [flezha_ruby_sdk](https://github.com/flezha/flezha_ruby_sdk) |
| PHP | [flezha_php_sdk](https://github.com/flezha/flezha_php_sdk) |
| Elixir | [flezha_elixir_sdk](https://github.com/flezha/flezha_elixir_sdk) |

Each one is also a git submodule of this repository, under `sdk/<language>`.

## Contributing

Contributions are welcome, and each one needs a signed [CLA](CLA.md).
[Contributing](docs/contributing/contributing.md) has the rules, and the pages
next to it cover [setting up a checkout](docs/contributing/setting_up.md),
[running the tests](docs/contributing/testing.md) and the
[code standards](docs/contributing/code_standards.md). Report security issues
privately, as [SECURITY.md](SECURITY.md) describes.

## License

| Path | License |
|---|---|
| Everything not listed below: the controller (`lib/`), `cli/`, `flz_nif/`, `config/`, `rel/`, `docker/`, `kubernetes/`, `test/`, `docs/`, `scripts/`, `bench/` | [MPL-2.0](LICENSE) |
| The SDK side: `sdk/`, `flz_core/`, `flz_core_ffi/`, `flz_transport/`, `protobufs/` | [Apache-2.0](LICENSES/Apache-2.0.txt) |

In short: build on the SDKs freely; if you modify and distribute the
controller's files, publish those modifications. Your own files never inherit
either license. [`REUSE.toml`](REUSE.toml) holds the per-path map, and CI checks
it.

Flezha is provided "AS IS", without warranty of any kind (MPL-2.0 §6-7,
Apache-2.0 §7-8). Read [Durability](docs/topics/basics/durability.md) before
trusting it with data. The licenses grant no trademark rights: a fork, derived
work or hosted service must not use the name "Flezha" or one confusingly similar.
Third-party components in binary distributions are listed in
[`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

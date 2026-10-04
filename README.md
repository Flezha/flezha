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
  said and survive a crash (see [Flezha and AI](#flezha-and-ai)).

## How it works

- **The controller** coordinates everything: it decides where work runs, keeps
  its state on disk, and starts it again elsewhere when something fails.
- **A runner** is your own program, written with one of the seven SDKs. It
  connects to the controller and runs the code you registered.
- **An operator** is one running copy of that code. Inside it, your logic is
  split into small processing stages, called tasklets, wired into a graph.
- **Plugins** connect operators to the outside world (files, HTTP, Kafka,
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

**2. Wire it into an operator.** The operator says where messages come in, which
stages they pass through and where results leave:

```python
@flezha.operator("echo")
def echo_operator(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(Echo).sink("out")
```

**3. Serve it from a runner,** a process that connects to the controller:

```python
flezha.create_runner("default").register_factory(echo_operator).run()
```

**4. Start it and send it a message** with `flz`, the command-line tool that
comes with the controller:

```sh
$ ./bin/flz operator start echo --id my_echo
$ ./bin/flz operator send my_echo --text 'hello world' --await 2000
Delivered to 'my_echo'.
out	echo: hello world
```

The same echo exists in every SDK, under `sdk/<language>/examples/echo`.

## Try it

The echo above runs in Docker, controller included. From the root of the
repository:

```sh
$ cd sdk/python/examples/echo_deployment
$ docker compose up -d --wait
$ docker compose exec controller /flezha/bin/flz operator start echo --id my_echo
$ docker compose exec controller /flezha/bin/flz operator send my_echo --text 'hello world' --await 2000
```

To try an operator without a controller or Docker,
[local mode](docs/topics/basics/local_mode.md) runs it inside your own process.
The [tutorials](docs/tutorials/tutorials.md) then build three example
applications step by step.

## Flezha and AI

Flezha is built to work with AI from both sides: AI coding tools can drive it and
build on it, and it can run the services AI applications are made of.

### For AI tools working with Flezha

- **Errors a program can act on.** Every failure has one shape on every surface,
  the `flz` CLI, the consumer API and every SDK: a stable `code`, a `class`, a
  one-sentence `message`, the `details` it names and a `hint` with the next command
  to run. The `flz` exit code follows the class, so a tool can tell "fix the
  arguments" from "retry later" without reading the text. Every code is listed in
  [`docs/errors.md`](docs/errors.md).
- **JSON from every command.** Each `flz` verb takes `--json`, and a failure is then
  one line of JSON ([`docs/topics/cli/cli.md`](docs/topics/cli/cli.md)).
- **Documentation for language models.** The generated documentation carries an
  `llms.txt` index and a Markdown copy of every page next to the HTML one.
- **Fast feedback.** Local mode runs your processing graph inside your own process,
  with no controller, so a change can be tried in seconds
  ([`docs/topics/basics/local_mode.md`](docs/topics/basics/local_mode.md)). A graph
  that cannot work is refused when the runner starts, with an error code that names
  why.

<!-- TODO: Flezha MCP server, so an AI assistant can inspect and operate a running
deployment through MCP: list namespaces, operators and runners, read state, send
messages and calls, read and replay dead letters, and get coded errors back. -->
<!-- TODO: an AGENTS.md template for projects built on Flezha, pointing an AI
assistant at the documentation, llms.txt, the error reference and the CLI's JSON
output. -->
<!-- TODO: a Claude Code skill or plugin that knows the flz CLI and each SDK's
current form, so an assistant writes and runs Flezha code without guessing. -->

### For AI services running on Flezha

Flezha has no AI framework of its own. The code it runs is ordinary code in your
language, so it calls whichever model SDK or agent framework you already use. What
Flezha adds is everything around that code:

- **One session per conversation.** Each conversation runs as its own instance with
  its own state, many run at once, and one that nobody is talking to sleeps
  ([`docs/topics/advanced/sessions.md`](docs/topics/advanced/sessions.md)).
- **Answers as they are written.** A reply can be streamed to the reader while the
  model is still producing it
  ([`docs/topics/advanced/streamed_answers.md`](docs/topics/advanced/streamed_answers.md)).
- **Turns taken back.** A conversation can go back to an earlier turn, and a failed
  turn can fail alone without losing the conversation
  ([`docs/topics/advanced/rewinding_a_turn.md`](docs/topics/advanced/rewinding_a_turn.md)).
- **Behind HTTP, or called by another service.** The HTTP server plugin puts a
  session behind an endpoint, and a call waits for its answer
  ([`docs/plugins/http_server.md`](docs/plugins/http_server.md),
  [`docs/topics/advanced/operator_calls.md`](docs/topics/advanced/operator_calls.md)).
- **A crash does not end the conversation.** A session keeps a journal, so when its
  runner dies mid-turn the conversation carries on from where it stopped, and with
  effectively-once delivery the turn's effects are counted once.

[The agentdesk tutorial](docs/tutorials/agentdesk/agentdesk.md) builds all of this,
with a demo model standing in for a real one.

<!-- TODO: AI plugins, for example a model-call plugin (one place for provider keys,
retries, rate limits and token accounting) and a vector-store source and sink.
Decide which ones, then specify each in OpenSpec. -->

## Documentation

| Where to go | What you find |
|---|---|
| [Getting started](docs/getting_started/what_is_flezha.md) | What Flezha is, and the echo running in Docker |
| [Tutorials](docs/tutorials/tutorials.md) | Three example applications: an echo, a trading desk on a demo exchange, and AI agent sessions |
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

# Writing a plugin

A plugin of your own is an ordinary Elixir Mix project: it depends on `flezha` and on the
libraries it needs, names its providers in its application environment, and is built into
a plugin package (`.fpz`) that a controller loads when it starts. A provider is one
module: it declares its options and its worker, checks its options, and implements the
hooks it can be called on.

Writing one is how Flezha connects to a system no shipped plugin covers, without changing
the controller. Every connector beyond the core's own (the HTTP server, Kafka, RabbitMQ,
Postgres change capture) is built exactly this way, in a repository of its own. The
provider contract is `Flezha.Plugin.ProviderBehaviour`, whose module documentation is the
reference for every callback.

## How it works

The controller runs a provider in two phases. `init_provider/1` validates and normalises
the options with no upstream I/O and starts no process, so a slow or unreachable upstream
cannot stall a plugin being prepared, and a refusal reaches the user who typed the option.
Once the relay's SDK side is bound, the controller starts the provider's declared
worker and calls `activate/1`, where a provider acquires its upstream (a TLS connect, a
subscribe handshake). From then on the controller calls the provider's hooks, probes its
`health/1`, shows its `inspect_state/1` under the plugin in `flz relay info`, and on a
disable or a relay's stop stops its workers and calls `terminate_provider/1`.

A plugin's processes are its providers' declared workers, which the controller supervises
under the provider's own family. A plugin application therefore has no application
callback (`mod`).

| Callback | What it does | Default |
| --- | --- | --- |
| `init_provider/1` | validate and normalise the options; no I/O, no process | the options as given |
| `activate/1` | acquire upstream resources once the relay is bound | nothing to acquire |
| `worker_specs/1` | the workers to start, as `{registry_key, worker_opts}` | one worker keyed by `:plugin_id` |
| `health/1` | the verdict of a running instance: `:healthy` or `{:unhealthy, reason}` | the owned worker being alive |
| `inspect_state/1` | detail shown under the plugin in `flz relay info`; `connected: false` says the upstream is not open yet | `nil` |
| `terminate_provider/1` | teardown of anything that is neither a worker nor an option; must be idempotent | `:ok` |
| `reconfigure/2` | apply changed live options in place | required with a live option |
| `on_*/2` | the hooks it can be called on | none |

## How to use it

### The project

The plugin names its providers in its application environment, under the provider ids a
user enables:

```elixir
def application do
  [env: [flezha_providers: %{"example_stamp" => FlezhaExamplePlugin.Stamp}]]
end

defp deps do
  [
    {:flezha, path: Path.expand("../../..", __DIR__)},
    {:stamp_lib, path: "stamp_lib"}
  ]
end
```

[Full file](https://github.com/flezha/flezha/blob/main/examples/elixir/plugin/mix.exs)

Here `flezha` is a path dependency, because the example builds from the checkout it lives
in; a project outside the Flezha repository depends on `flezha` by git or Hex instead.

### The provider declaration

`use Flezha.Plugin.ProviderBehaviour` takes the whole declaration:

| Key | What it declares | Default |
| --- | --- | --- |
| `options:` | one row per option, `{name, keyword}` | none |
| `worker:` | the worker module and its `restart` value | no worker |
| `type:` | `:relay` (acts on the records of the relays bound to it) or `:controller` (bound to none) | `:relay` |
| `shareable:` | one running copy can serve every relay bound to the plugin on a controller | `false` |
| `feeds:` | the provider's own worker injects records into the relay | `false` |
| `name:` | the name in errors, help and telemetry | the module's last segment, underscored |
| `telemetry:` | the verbs it emits beyond the shared ones, each `:metric` or `:event` | none |

An option row takes `type` (`:int`, `:bool`, `:string`, `:names`, `:enum`, `:json`,
`:map`, `:list`, `:pid`, `:any`), `default`, `required`, `min` and `max`, `values` for an
enum, `doc` (the sentence `flz plugin help` and the catalogue's option table show),
`internal` (not settable by a user), `transient` (held in memory, never persisted, such as
a resolved credential) and `live`. From the rows the declaration derives the provider's
struct, its help screen and a strict allow-list: an option the provider does not declare is
refused at enable rather than ignored.

A row with `live: true` can be changed on a running plugin by `flz plugin update`; the
provider then implements `reconfigure/2`, which the controller calls with the old and the
new options, and a module that declares a live option without it does not compile.

The example provider appends every payload the relay sends out to a file, each line
prefixed with a stamp from a small library the plugin bundles:

```elixir
use Flezha.Plugin.ProviderBehaviour,
  options: [path: [type: :string, required: true, doc: "File the stamped lines go to"]],
  worker: [module: Writer, restart: :transient]

@impl true
def init_provider(%{path: ""}), do: {:error, :stamp_path_empty}
def init_provider(opts), do: {:ok, opts}

# One writer per plugin, under the id it was enabled with.
@impl true
def worker_specs(opts), do: [{opts.source_plugin_id, opts}]

@impl true
def on_output(%Ctx.IO{message: payload} = ctx, opts) do
  Writer.append(WorkerSup.worker(__MODULE__, opts.source_plugin_id), payload)
  {:ok, ctx}
end
```

[Full file](https://github.com/flezha/flezha/blob/main/examples/elixir/plugin/lib/flezha_example_plugin/stamp.ex)

This provider writes one file per plugin, so it cannot be shared. A provider whose one
worker can serve every relay bound to its plugin declares `shareable: true`, and each
controller then runs one copy for all of them.

### A source

A provider whose worker hands records to the relay declares `feeds: true` and calls
`Flezha.Plugin.Host.inject/5` with the relay its options name (`opts.overwatcher_pid`).

A source that is also `shareable` hands each record to every relay sharing its plugin
on the controller instead: `Host.targets(opts)` lists them, each with its entry channel,
and the worker is sent `{:targets, targets}` each time one binds or leaves. A worker that
can miss a message (a socket library dropping them while disconnected) reads them again
with `Host.targets_of/1` from `opts.targets_from` when it reconnects. It keeps them as its
state, so a record costs one `:fire_and_forget` inject per relay and nothing else. A
relay whose input is durable runs the source on its own, with itself as the one target,
so that the worker's own relay is still the one that confirms. A shared worker that
injects into `opts.overwatcher_pid` reaches the controller's copy, which drops the record,
logs one warning and reports `plugin_lagging` with the count dropped.

A source that reads from a system with a position, such as an offset, a cursor or an
acknowledgement, fetches a batch and hands it over with `Host.inject_many/4` in `:verdict`
mode: up to `FLEZHA_PLUGIN_INGEST_WINDOW` (default `64`) records wait for their verdicts
at once, and the answer is one verdict per record, in order,
`{:error, {:unreachable, :timeout}}` for any the relay had not answered in time. It
moves the upstream position only past records whose verdict is `:ok`, and waits for each
batch, which keeps its memory bounded. How often it moves the position follows the
relay, which the plugin's options tell it:

| The relay | Its plugins' options | A verdict `:ok` means | The source moves its position |
| --- | --- | --- | --- |
| a flow relay without `effectively_once` that does not sleep | `durable_input: false` | the relay took the record | every few seconds |
| a durable steps relay, or a flow relay that sleeps | `durable_input: true` | the record is in the relay's input log | after every confirmed batch |
| a flow relay with `effectively_once` | `durable_input: true`, `effectively_once: true` | the record is in the relay's input log | after every confirmed batch; an output plugin that honours `effectively_once` writes each output once |

Both are set by the controller when the plugin starts, over anything its stored options
say. A source that cannot tell which member of a partition group a record's key belongs to,
as a consumer or a table reader cannot, refuses a group member from `init_provider/1` with
`ProviderBehaviour.refuse_partitioned(opts, __MODULE__)`, whose code is
`plugin.partitioned_host`.

### An output plugin

An output plugin implements `on_output` (and `on_input`, to act on what the relay ingests) and
hands each payload to its worker, returning at once. A delivery-guaranteed output is held
in the relay's outbox until the output plugin confirms it; an output plugin that gives one up parks it as a
dead letter rather than dropping it, as the HTTP push and Kafka producer plugins do. The
Postgres output plugin and the relay-forward plugin
are the shapes to read: their catalogue pages describe the queue, the confirmation and the
overflow policy each one follows.

### Telemetry

A provider reports what it did with `ProviderBehaviour.emit/4`: `shipped`, `retried`,
`overflowed` and `failed` mean the same for every provider, and a verb of its own is
declared on the `use`, `telemetry: [rebalanced: :event]`, as a `:metric` or an `:event`.
The controller exports both under the provider's name, sourced `flezha_<name>`; a verb it
does not declare reaches only handlers attached to it.

### A controller plugin

A controller plugin declares `use Flezha.Plugin.ProviderBehaviour, type: :controller` and
is bound to no relay. It is told what happened in its namespace through five hooks,
`on_relay_started`, `on_relay_stopped`, `on_relay_failed`, `on_dead_letter` and
`on_plugin_unhealthy`, each a callback taking the event and the plugin's options, and
defines only those: a module defining a hook of the other type does not compile. A hook
runs in a process of its own and should return quickly; a provider that does slow I/O
hands the event to its own worker, as `hook_webhook` does. [`hooks.md`](hooks.md) has
every event's fields and when it fires.

### Listening to a relay

A plugin that streams a relay's output to a client of its own (an HTTP plugin's
server-sent events, for one) reads it through `Flezha.Service.Listener.listen/4`, the
listener `flz relay listen` uses. It runs in the calling process, hands the handler one
event at a time, and detaches on every way out, a raise in the handler included. It does
not authorize: a plugin decides for itself who may read.

```elixir
Flezha.Service.Listener.listen(namespace, relay_id, opts, handler)
```

`handler` receives an event and answers `:continue` or `:done`:

| Event | What it is |
|---|---|
| `{:output, payload, metadata}` | an output on the relay's output chain |
| `{:part, payload, metadata}` | a part: a preview a tasklet sent while it ran, `flz.part` its number within its attempt, `flz.part_attempt` the attempt |
| `{:dead_lettered, %Flezha.Error{}, metadata}` | an input the relay dead-lettered, with the code `input.dead_lettered`, once its entry is durable |
| `{:input, payload, metadata}` | with `direction: :input`, an input the relay ingests |
| `:idle` | with `idle_ms`, no event for that long |

| Option | Default | Meaning |
|---|---|---|
| `direction` | `:output` | `:output` (outputs, parts and notices) or `:input` |
| `window_ms` | `:infinity` | a positive integer: how long to listen |
| `idle_ms` | `:infinity` | a positive integer: call the handler with `:idle` after that long with no event |
| `before` | none | a function run once after the attach, before the first event |
| `watch` | none | a process whose exit ends the listener |

It answers `{:ok, :window_closed | :done | :caller_down | :relay_ended}`, or
`{:error, %Flezha.Error{}}`: the enable's own refusal (`relay.not_found`, for one),
`listener.overrun` when the handler falls behind by more than `listener_pending_bytes` or
`listener_pending_frames` of outputs (held parts are dropped first, oldest first, and a
reader sees the gap as a jump in `flz.part`), or `listener.detached` when someone else
disabled its plugin. The listener goes on across the relay sleeping and waking, being
rebound, its Overwatch restarting, moving to another controller and a group rescale, and
ends with `:relay_ended` when the relay ends.

A server-sent events stream, with a comment every 15 seconds so a proxy keeps the
connection and a client that left is noticed:

```elixir
Flezha.Service.Listener.listen(namespace, session_id, [idle_ms: 15_000], fn
  {:part, payload, _metadata} -> write_event("part", payload)
  {:output, payload, _metadata} -> write_event("output", payload)
  {:dead_lettered, error, _metadata} -> write_event("dead_lettered", Flezha.Error.render(error, :json))
  :idle -> write_comment()
end)
```

`write_event/2` and `write_comment/0` write through the request's connection, which the
plugin keeps in its own process, and answer `:continue`, or `:done` once the client has
gone.

### Build the package

`mix flezha.plugin.package`, which every project depending on `flezha` has, builds the
archive:

```sh
$ MIX_ENV=prod mix flezha.plugin.package [--output DIR] [--sign key.pem]
```

It compiles the project, bundles every application of the plugin's runtime closure that
the release does not ship, records the Flezha, OTP and Elixir versions it was built with,
and writes `<id>-<version>.fpz` to `--output` (`_build/` by default). It refuses, writing
nothing:

- a library the release ships, resolved at another version than Flezha's own `mix.lock`,
  naming both versions and how to pin it;
- native code;
- an application that depends on `flezha` and declares an application callback (`mod`);
- a `:flezha_providers` entry that is not a provider module of the plugin application;
- a setting of a bundled library that an `.app` file cannot hold (a function other than
  `&Mod.fun/arity`, a pid or a reference), naming the library and the key.

The project's `config/*.exs` settings of each library it bundles, as the `MIX_ENV` the task
runs in loads them (so build with `MIX_ENV=prod`), are written into that library's `.app`
in the archive, where the library reads its defaults on the controller. A library the
release ships keeps the controller's settings, and the plugin application's own settings
are not written. Anyone holding the package can read these settings, so a secret belongs
in a provider option that reads it from the environment, never in `config/*.exs`.

A controller loads a package only on the runtime line it was built on
([`installing_plugins.md`](installing_plugins.md#which-packages-a-controller-loads)), so
build it with the controller's toolchain. For the Docker image, that is the
`plugin-builder` target of the controller's Dockerfile:

```sh
$ docker build --target plugin-builder -t flezha/plugin-builder:0.1.0 \
  -f docker/prod/Dockerfile.controller .
$ docker run --rm -v "$PWD/my_plugin:/src" flezha/plugin-builder:0.1.0 \
  sh -c 'mix deps.get && mix flezha.plugin.package'
```

### Sign it

`mix flezha.plugin.package --sign key.pem` writes `signature.sig` with an Ed25519 private
key in PEM form, made with `openssl genpkey -algorithm ed25519 -out key.pem`. The public
key a controller verifies with is `openssl pkey -in key.pem -pubout`, named in
`FLEZHA_PLUGIN_TRUSTED_KEYS`; [`installing_plugins.md`](installing_plugins.md#signed-packages)
says what a controller does with it.

## Example

The example plugin in `examples/elixir/plugin/` has one provider, `example_stamp`. Its
`config/config.exs` sets the bundled library's one setting, `config :stamp_lib,
prefix: ">>"`, and the package carries it in the library's `.app`, so each line starts
`>>[stamped]`. It is packaged and installed as any package is, then enabled on a
relay like any plugin:

```sh
$ flz plugin install ./flezha_example_plugin-0.1.0.fpz
$ flz plugin enable stamp example_stamp --relay_id my_op \
  --Xpath=/tmp/stamped.txt --hook=on_output
```

The connector packages in `plugins/` (`flezha_http_plugin`, `flezha_kafka_plugin`,
`flezha_rabbitmq_plugin`, `flezha_postgres_cdc_plugin`) are complete plugins laid out the
same way, each with its README, its tests and a docs test that keeps its catalogue page's
option table equal to its declaration.

## Limits

- **No native code.** NIFs and port programs are refused, at packaging and at install.
- **No application callback.** Long-lived processes are a provider's declared workers,
  which the controller supervises.
- **Configuration reaches only bundled libraries.** A library reads the settings the
  package was built with; a provider is configured by its `--X` options at enable, and a
  library the release ships cannot be reconfigured.
- **No error codes of its own.** A provider refusal whose reason the catalogue already
  maps keeps that code; any other is wrapped as `plugin.provider_refused` or
  `plugin.provider_failed`, with the package's own text in `details`.
- **A protocol the package defines itself stays unconsolidated.**
- **Telemetry is exported only for declared verbs.** The observability bridge maps every
  provider's `shipped`, `retried`, `overflowed` and `failed` and the verbs it declares with
  `telemetry:`; a verb a provider emits without declaring it reaches handlers attached to
  it and no exporter.

## See also

- [`installing_plugins.md`](installing_plugins.md): putting the package on a controller
- [`plugins.md`](plugins.md): enabling, binding, sharing and `--single`
- [`hooks.md`](hooks.md): every hook a provider can implement
- [`http_server.md`](../../plugins/http_server.md), [`kafka_consumer.md`](../../plugins/kafka_consumer.md): package plugins to compare with
- Tutorial: [`add_your_own_plugin.md`](tutorials/add_your_own_plugin.html)

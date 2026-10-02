# Custom plugins

A plugin that does not ship with the controller comes as a package: an Elixir project,
built into a `.fpz` file, which the controller loads when it starts. On this page
agentdesk's controller gets the HTTP server plugin, which
[`http_gateway.md`](http_gateway.md) uses, and you see what a plugin of your own looks
like.

## Build the package

A package records the Erlang and Elixir versions it was built with. A controller loads
it on the same Erlang release or one of the two after it, and on the same Elixir 1.x or a
later one, with a warning when the Elixir minors differ. So the package is built in the
controller image's builder stage, which has the same toolchain.

> Get the Flezha repository with its submodules, the HTTP plugin's among them, then build
> the builder image from the checkout.
> {: .step}

```sh
$ git clone --recurse-submodules https://github.com/flezha/flezha
$ cd flezha
$ docker build --target plugin-builder -t flezha/plugin-builder:0.1.0 -f docker/prod/Dockerfile.controller .
```

> Package the plugin in the builder, from the plugin's folder.
> {: .step}

```sh
$ docker run --rm -v "$PWD:/src" -w /src/plugins/flezha_http_plugin flezha/plugin-builder:0.1.0 sh -c 'mix deps.get && mix flezha.plugin.package'
```

```output
...
==> flezha_http
Compiling 6 files (.ex)
Generated flezha_http app
Wrote /src/plugins/flezha_http_plugin/_build/flezha_http-0.1.0.fpz
```

The whole checkout is mounted, because the plugin builds against the Flezha checkout it is
a submodule of. The package holds the plugin and the libraries it needs that the
controller does not ship, Bandit and Plug among them.

## Rebuild the controller's image

agentdesk's controller image copies the project's `controller/plugins/` folder into the
controller's plugin root ([`sessions.md`](sessions.md#the-containers)), and the controller
loads every package there when it starts.

> Copy the package into agentdesk's `controller/plugins/` folder, then rebuild the
> controller's image and replace its container, from agentdesk's folder.
> {: .step}

```sh
$ cp <the plugin's folder>/_build/flezha_http-0.1.0.fpz controller/plugins/
$ docker compose up -d --build --wait controller
$ flz plugin list --installed
```

```output
flezha_http 0.1.0  loaded  providers: http_server
```

A package the controller could not load is listed `refused`, with its error code and what
to do; [`installation.md`](../../plugins/installation.md#errors) lists them.

The new controller starts the sessions again once the runner has connected to it, within
about a minute; a call before that answers `operator.not_found`. Wait until it lists them:

```sh
$ flz operator list --namespace agents
```

```output
Fetching operators for namespace 'agents'...
Operators in namespace 'agents':

  OPERATOR                          HEALTH      PARTS   NODE                  RUNNER
  anna                              suspended   -       flezha@127.0.0.1      agents_47a1f5f1e9eb
  bob                               suspended   -       flezha@127.0.0.1      agents_47a1f5f1e9eb
```

> Send Anna a message.
> {: .step}

```sh
$ flz operator call anna --namespace agents --text 'Did you notice anything?'
```

```output
{"text":"Message 5: you said \"Did you notice anything?\". I am a demo model.","tokens":13}
```

The new controller found the sessions on the `app_data` volume. To upgrade the plugin,
put the newer package in its place and rebuild; to remove it, delete it and rebuild.
[`installation.md`](../../plugins/installation.md#deployment-shapes) compares this with the
other ways in.

## A plugin of your own

A plugin is an ordinary Mix project. It depends on `flezha` and on the libraries it
needs, and names its providers in its application environment. The example in
`examples/elixir/plugin/` has one provider, `example_stamp`, which appends every payload
the operator sends out to a file, each line prefixed with a stamp from a small library the
plugin bundles:

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

Its `config/config.exs` sets the library's one setting, `config :stamp_lib, prefix: ">>"`,
and the package carries it in the library's `.app`, so each line starts `>>[stamped]`. The
package task writes the configuration of the `MIX_ENV` it runs in, so build with
`MIX_ENV=prod`; the plugin application's own settings are not written.
Only the libraries a package bundles are configured this way, and anyone holding the
package can read their settings: a secret goes in a provider option that reads it from
the environment.

It has no `mod`: a plugin's processes are its providers' declared workers, which the
controller supervises. The provider declares its options and its worker, checks its
options, and handles each output payload:

```elixir
use Flezha.Plugin.ProviderBehaviour,
  options: [path: [type: :string, required: true, doc: "File the stamped lines go to"]],
  worker: [module: Writer, restart: :transient]

@impl true
def init_provider(%{path: ""}), do: {:error, :stamp_path_empty}
def init_provider(opts), do: {:ok, opts}

@impl true
def worker_specs(opts), do: [{opts.source_plugin_id, opts}]

@impl true
def on_output(%Ctx.IO{message: payload} = ctx, opts) do
  Writer.append(WorkerSup.worker(__MODULE__, opts.source_plugin_id), payload)
  {:ok, ctx}
end
```

This provider writes one file per plugin, so it cannot be shared. A provider whose
one worker can serve every operator bound to its plugin declares
`shareable: true` on the `use`, and each controller then runs one copy for all of them.
An option row with `live: true` can be changed on a running plugin by
`flz plugin update`; the provider then implements `reconfigure/2`, which the
controller calls with the old and the new options, and a module that declares a live
option without it does not compile.

### A source

A provider whose worker hands records to the operator declares `feeds: true` and calls
`Flezha.Plugin.Host.inject/5` with the operator its options name (`opts.overwatcher_pid`).
A source that reads from a system with a position, such as an offset, a cursor or an
acknowledgement, fetches a batch and hands it over with `Host.inject_many/4` in `:verdict`
mode: up to `FLEZHA_PLUGIN_INGEST_WINDOW` records wait for their verdicts at once, and the
answer is one verdict per record, in order, `{:error, {:unreachable, :timeout}}` for any the
operator had not answered in time. It
moves the upstream position only past records whose verdict is `:ok`, and waits for each
batch, which keeps its memory bounded. How often it moves the position follows the
operator, which the plugin's options tell it:

| The operator | Its plugins' options | A verdict `:ok` means | The source moves its position |
| --- | --- | --- | --- |
| a flow operator without `effectively_once` that does not sleep | `durable_input: false` | the operator took the record | every few seconds |
| a durable steps operator, or a flow operator that sleeps | `durable_input: true` | the record is in the operator's input log | after every confirmed batch |
| a flow operator with `effectively_once` | `durable_input: true`, `effectively_once: true` | the record is in the operator's input log | after every confirmed batch; a sink that honours `effectively_once` writes each output once |

Both are set by the controller when the plugin starts, over anything its stored options
say. A source that cannot tell which member of a partition group a record's key belongs to,
as a consumer or a table reader cannot, refuses a group member from `init_provider/1` with
`ProviderBehaviour.refuse_partitioned(opts, __MODULE__)`, whose code is
`plugin.partitioned_host`.

A provider reports what it did with `ProviderBehaviour.emit/4`: `shipped`, `retried`,
`overflowed` and `failed` mean the same for every provider, and a verb of its own is
declared on the `use`, `telemetry: [rebalanced: :event]`, as a `:metric` or an `:event`.
The controller exports both under the provider's name, sourced `flezha_<name>`; a verb it
does not declare reaches only handlers attached to it.

### A controller plugin

`example_stamp` is an operator plugin: it acts on the records passing through the
operators bound to it, which is the default `type: :operator`. A controller plugin
declares `use Flezha.Plugin.ProviderBehaviour, type: :controller` and is bound to no
operator. It is told what happened in its namespace through five hooks, each a callback
taking the event and the plugin's options, and defines only those: a module defining a
hook of the other type does not compile.

| Hook | Its own fields |
| --- | --- |
| `on_operator_started` | `operator`, `factory` |
| `on_operator_stopped` | `operator`, `reason` (`:stopped` or `:completed`) |
| `on_operator_failed` | `operator`, `error` |
| `on_dead_letter` | `operator`, `entry`, `channel`, `error` |
| `on_plugin_unhealthy` | `plugin`, `operator`, `error` |

Every event also carries `hook`, `namespace`, `controller`, `at` and a one-line `text`,
and never a record's data. [`hook_webhook.md`](../../plugins/hook_webhook.md) says when
each hook fires. The plugin is enabled with its hooks, one `--hook` each, as that
first-party controller plugin is:

```sh
flz plugin enable alerts hook_webhook --Xurl=https://hooks.example.test/x \
  --hook on_operator_failed --hook on_dead_letter
```

It runs on every controller that holds its namespace and hears what happens there, or
once in the cluster with `--single`, and it runs until it is disabled. A hook runs in a
process of its own and should return quickly; a provider that does slow I/O hands the
event to its own worker, as `hook_webhook` does. Delivery is best effort and bounded: a
copy that falls behind drops its oldest events (`FLEZHA_PLUGIN_BACKLOG_FRAMES`), an event
waiting on a controller that stops is lost, and so is one that happens in the first
moments after the enable; while a `single` copy moves, an event can arrive twice.

`Flezha.Plugin.ProviderBehaviour` documents the provider contract. It is packaged and
installed as the HTTP plugin was, from its own folder, and then enabled on an operator
like any plugin, as [`sources_and_sinks.md`](../sources_and_sinks.md) showed. What a
package cannot do in this version, such as carry native code or configure a library the
controller ships, is listed in [`installation.md`](../../plugins/installation.md#limits).

## Next

[`http_gateway.md`](http_gateway.md) puts the sessions behind HTTP with the plugin.

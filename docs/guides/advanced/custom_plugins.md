# Custom plugins

Part 3.

A plugin of your own: an Elixir project that adds a provider to the controller,
packaged as a `.fpz` and loaded when the controller starts. This page builds the
example in `examples/elixir/plugin/` and enables it on `my_echo`, with the controller and runner of Part 1 running and
`my_echo` started as [`running_operators.md`](../running_operators.md) shows.

The example's one provider, `example_stamp`, appends every payload the operator
sends out to a file, each line prefixed with a stamp. The stamp comes from a small
library the plugin bundles, `stamp_lib`, which is there to show what a package can
carry: a library with a process of its own, a file under its `priv/`, and an
implementation of a protocol Elixir defines. An HTTP server plugin works the same
way, with Bandit and Plug in place of `stamp_lib`.

## The project

A plugin is an ordinary Mix project. It depends on `flezha` and on the libraries it
needs, and it names its providers in its application environment:

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

The example depends on `flezha` by path because it lives in this repository; a
project of your own names it by git or Hex. It has no `mod`: a plugin's processes
are its providers' declared workers, which the controller supervises. Its
`config/config.exs` compiles Flezha's SDK the way the controller release does:

```elixir
config :flezha_sdk, socket_nif: false
```

## The provider

`FlezhaExamplePlugin.Stamp`, in
`examples/elixir/plugin/lib/flezha_example_plugin/stamp.ex`, is a provider like the
ones the controller ships. It declares its one option and a worker,
refuses an empty path, and hands each output payload to the worker, which writes the
line:

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

`Flezha.Plugin.ProviderBehaviour` documents the provider contract.

## Packaging

> Build the package in the example's directory.
> {: .step}

```sh
$ cd examples/elixir/plugin
$ MIX_ENV=prod mix deps.get
$ MIX_ENV=prod mix flezha.plugin.package
```

The task compiles the project and writes `_build/flezha_example_plugin-0.1.0.fpz`,
holding the plugin application and `stamp_lib`, the one library of its closure the
controller release does not ship. It records the Flezha, OTP and Elixir versions it
was built with, and a controller loads the package only on that same line, so build
it with the controller's toolchain. For the Docker image, that is the builder target
of its Dockerfile, which [`installation.md`](../../plugins/installation.md#building-a-package)
shows.

## Installing

> Install the package into the controller's plugin root, then restart the controller.
> {: .step}

```sh
$ flz plugin install _build/flezha_example_plugin-0.1.0.fpz
```

The command places the archive in the plugin root and prints where; it changes
nothing in the running controller. Restart the controller the way you started it,
then ask what it loaded:

```sh
$ flz plugin list --installed
```

```output
flezha_example_plugin 0.1.0  loaded  providers: example_stamp
```

A package the controller could not load is listed `refused`, with its error code
and what to do; [`installation.md`](../../plugins/installation.md#errors) lists them.

## Enabling it

> Attach the provider to `my_echo`, writing to `/tmp/stamped.txt`.
> {: .step}

```sh
$ flz plugin enable stamp example_stamp \
  --operator_id=my_echo \
  --hook=on_output \
  --Xpath=/tmp/stamped.txt
```

From here it is a plugin like any other: send `my_echo` a message and a line lands
in `/tmp/stamped.txt`, the stamp read from `stamp_lib`'s `priv/stamp.txt` followed by
the payload. `flz plugin disable stamp` detaches it.

To upgrade, install a newer package of the same id and restart. To remove it, run
`flz plugin uninstall flezha_example_plugin` and restart. What a package cannot do
in this version, such as carry native code or its own `config/*.exs`, is listed in
[`installation.md`](../../plugins/installation.md#limits).

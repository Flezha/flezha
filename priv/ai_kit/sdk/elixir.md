# The elixir SDK's echo example

Every file of `sdk/elixir/examples/echo`, the runner Up and running builds. Its dependency file names the SDK package and the version this release pins.

## examples/echo/README.md

````markdown
# Echo: the Elixir hello world

The Elixir hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `mix.exs` names one released version of `flezha_sdk`, the
way your own project would.

It is a runner of its own, a separate BEAM node that connects to a controller
over its socket. Code running inside the controller's own node is the other way
to run the SDK, described in
[`docs/sdk/elixir_sdk.md`](../../../../docs/sdk/elixir_sdk.md).

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `lib/echo/tasklet.ex` | the tasklet | the processing stage: prefixes each message |
| `lib/echo/relay.ex` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `lib/echo/application.ex` | the runner | this node: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).
From this folder:

```sh
mix deps.get
mix run --no-halt
```

Then, from another terminal:

```sh
flz relay start echo --id my_echo
flz relay send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It runs until the node stops: Ctrl-C twice, or a SIGTERM. The namespace is the
first argument and defaults to `default`: `mix run --no-halt -- my_app`.

On start it warns that it cannot persist a runner id at
`/var/lib/flezha-runner/id` unless that path is writable, and uses a fresh
identity for the boot. Set `FLEZHA_RUNNER_ID_PATH` to a writable file to keep one
identity across restarts.

## Building from a checkout

The package is not on Hex yet. Until it is, point the dependency in `mix.exs` at
the package in your checkout:

```elixir
defp deps do
  [{:flezha_sdk, path: "../.."}]
end
```

A copy of this folder outside the checkout names the package's absolute path
instead. With no release to download it from, the first compile builds the
package's socket client with a Rust toolchain, so it takes a few minutes.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is.
````

## examples/echo/lib/echo/application.ex

````elixir
defmodule Echo.Application do
  @moduledoc """
  Starts a runner: a process that connects to the controller and serves your
  relays.

  The runner is a child of this application's supervision tree, so a crash
  restarts it and it reconnects under the same identity. Declare every relay
  you want served in `relays:`, however it runs: how it runs is read from
  the module, never named by the list.
  """
  use Application

  @impl true
  def start(_type, _args) do
    # `mix run --no-halt -- my_app` names the namespace.
    namespace = List.first(System.argv(), "default")

    children = [
      # connects to the controller and serves these relays
      # `:uds` because this node is not the controller's. The default, `:native`,
      # is for code running inside the controller's own node.
      {Flezha.SDK, namespace: namespace, relays: [Echo.Relay], transport: :uds}
    ]

    {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

    # Everything `flz relay` and `flz plugin` do is also a function in
    # `Flezha.SDK.Control`, and `Flezha.SDK.send/5` hands a message to an
    # relay. The runner is connected once its child has started, so
    # uncomment to drive this namespace from the application instead of from a
    # terminal:
    #
    # alias Flezha.SDK.Control
    # runner = Flezha.SDK.RunnerServer
    # {:ok, %{id: "my_echo"}} = Control.start_relay(runner, "echo", id: "my_echo")
    #
    # {:ok, _} =
    #   Control.enable_plugin(runner, "writer", "relay_file_output",
    #     opts: %{"path" => "echo-out.txt"}
    #   )
    #
    # {:ok, _} = Control.bind_plugin(runner, "writer", "my_echo", hooks: [["on_output", %{}]])
    #
    # {:ok, _} = Flezha.SDK.send(runner, "my_echo", "hello world", "in", msg_id: "hello-1")
    #
    # docs/topics/core/sending_and_calling.md covers sending. An
    # enforced controller refuses all of it until
    # `flz role bind runner@default app default`.

    {:ok, supervisor}
  end
end
````

## examples/echo/lib/echo/relay.ex

````elixir
defmodule Echo.Relay do
  @moduledoc """
  The deployable unit. A relay declares the graph a message travels: the
  entry channel it arrives on, the steps it passes through, and the channel its
  result leaves on.

  The graph is checked as a whole when the runner declares it, so one that
  cannot work is refused at startup.
  """

  use Flezha.SDK.Relay, factory_id: "echo"

  @impl true
  def channels(c) do
    c
    |> input("in")
    |> into(Echo.Tasklet)
    |> output("out")
  end
end
````

## examples/echo/lib/echo/tasklet.ex

````elixir
defmodule Echo.Tasklet do
  @moduledoc """
  A processing stage of the relay's graph, run once per message. This one
  prefixes the input and emits the result.

  With no `state` block, nothing is kept between messages and nothing is
  journaled. Where the result goes is not decided here: the relay decides
  that, so the graph can be rewired without touching this module.
  """

  # state is saved under this name, so choose it once
  use Flezha.SDK.Tasklet, name: "echo"

  @impl true
  def run(input, state, ctx) do
    # where it goes is the relay's business
    emit(ctx, "echo: " <> input)
    {:ok, state}
  end
end
````

## examples/echo/mix.exs

````elixir
defmodule Echo.MixProject do
  use Mix.Project

  # Standalone on purpose: one dependency on a released version, so this
  # builds the way your own project would. It is a runner node of its own,
  # connecting to a controller over a socket, rather than code running inside
  # the controller's node.
  def project do
    [
      app: :echo,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [mod: {Echo.Application, []}, extra_applications: [:logger]]
  end

  defp deps do
    # TODO: change later to {:flezha_sdk, "~> 0.1.0"} when flezha_sdk is released
    [{:flezha_sdk, path: "../.."}]
  end
end
````

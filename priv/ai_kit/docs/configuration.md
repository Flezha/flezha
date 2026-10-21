# Configuration overview

Flezha starts with no configuration. Every setting has a default, chosen for one
controller and its runners on one machine, and you change a setting only when
the default does not fit: another port, a durability backend, a cluster, more
retries for one relay.

Where you change it depends on what the setting is about:

- **The node**: ports, directories, the cluster, durability, security. An
  environment variable when you start the controller, or application env when
  Flezha runs inside your own Elixir application.
- **What a relay promises**: flow or steps, where it keeps its work, how it
  retries. The relay's own code.
- **One plugin**: the URL it polls, the topic it writes to. The plugin's options
  when you enable it.

## The four layers

Each answer above is one of four layers, and the layer decides what a change
costs:

| Layer | Where you set it | Scope | Cost to change | Page |
|---|---|---|---|---|
| 1. Environment variables | the process environment / deploy manifest | whole node | restart the node | [Controller environment variables](configuration/environment_variables.md) |
| 2. Application env | `config/config.exs`, `config/runtime.exs`, `config/<env>.exs` | whole node | rebuild or restart (`runtime.exs` is read at boot) | [Application env keys](configuration/application_env.md) |
| 3. Relay declarations | the SDK, in the relay's `channels()` and Tasklet/Steps class | one relay class | redeploy the runner, frozen at registration | [Relay settings](configuration/relay_settings.md) |
| 4. Plugin options | `--X<option>` flags or a `--plugins` spec's `opts`, when the plugin is enabled | one plugin | `flz plugin update` | [Plugin options](configuration/plugin_options.md) |

Layers 1 and 2 are the same settings under two spellings: the variable
`FLEZHA_HEALTHZ_PORT` sets the key `:healthz_port` at boot. Use the variables
with the release or the image, and application env when Flezha runs inside your
own Elixir application, where the variables are not read.

Layer 3 is the one that surprises people. Anything a relay declares, such as
its delivery tier, its acknowledgement granularity or its correction window, is
frozen when the class registers with the controller, and there is no
user-facing override: these change the semantics of the relay's output, not
just its performance, so they belong with the code that relies on them.

In the reference pages, a default is the value used when nothing is set, and
`(derived)` marks one computed from another setting.

## Setting an environment variable

Set it in the controller's environment when you start it. With the release:

```sh
$ FLEZHA_HEALTHZ_ENABLED=true ./bin/flz controller start
```

With the image:

```sh
$ docker run -d --name flezha-controller \
  -e FLEZHA_HEALTHZ_ENABLED=true \
  -v flezha_data:/flezha/storage/app \
  flezha/flezha:0.1.0
```

On [Kubernetes](topics/deploying/kubernetes.md) the same variables go in the
ConfigMap the pod reads. A change takes effect at the next start. Every
variable is in [Controller environment variables](configuration/environment_variables.md).

## Setting application env

When Flezha runs [inside your own Elixir application](topics/deploying/deploying_a_controller.md#inside-an-elixir-application),
the `FLEZHA_*` variables are not read. Set the same settings as keys of the
`:flezha` application:

```elixir
# config/config.exs
config :flezha,
  healthz_enabled: true,
  state_root: "/var/lib/flezha/state"
```

Every key is in [Application env keys](configuration/application_env.md).

## Declaring a relay setting

A relay's settings are part of its code: written once in the class, and the
same wherever it runs. No variable or start option overrides one; a change is
a new version of the class. The settings tree, how each SDK writes it, and the
edge and tasklet options are in [Relay settings](configuration/relay_settings.md).

## Setting a plugin option

Each `--X<option>` flag is one option of the plugin:

```sh
$ flz plugin enable signal-config relay_http_poll --relay_id=detector \
  --Xurl=https://api.internal/signal-config \
  --Xchannel=config_in --Xinterval_ms=30000
$ flz plugin update signal-config --Xinterval_ms=10000
```

`flz plugin update` changes it on the running plugin: in place when the
provider declares the option live, by restarting the plugin otherwise. Every
provider and its options are in [Plugin options](configuration/plugin_options.md).

## Outside the four layers

| Page | What it covers |
|---|---|
| [Runner environment variables](configuration/runner_environment.md) | What a runner process reads from its own environment, in every SDK |
| [Erlang VM arguments](configuration/vm_arguments.md) | The VM flags the release sets, which spelling wins, and the flags it leaves alone |
| [Files and sockets](configuration/files_and_sockets.md) | The directories Flezha writes, what `flz reset` clears, and where the UDS sockets live |

The VM flags are set like environment variables and cost the same restart, but
the VM reads them before the application starts, so no row of the table can
hold one.

## See also

- [`topics/deploying/deploying_a_controller.md`](topics/deploying/deploying_a_controller.md): the variables a deploy cannot omit, and how to run a node
- [`relay_settings.md`](configuration/relay_settings.md): choosing a relay's settings
- [`topics/core/durability.md`](topics/core/durability.md): what the durability knobs mean for correctness
- [`topics/core/authorization.md`](topics/core/authorization.md): the posture behind the authz keys
- [The kernel architecture](https://github.com/flezha/flezha/blob/main/docs/public/architecture/kernel.md): where the WAL, outbox and fsync knobs sit in the data path
- [The benchmarking guide](https://github.com/flezha/flezha/blob/main/docs/internal/operations/benchmarking.md): measuring the effect of a change to any of these

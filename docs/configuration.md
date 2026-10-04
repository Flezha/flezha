# Configuration overview

Every knob Flezha exposes lives in one of four layers, and the layer decides
whether changing it costs a restart, a redeploy, or nothing. This section is for
users setting up a node and for operator authors wondering why a setting is not
on the node at all. Defaults are the literal the code falls back to when nothing
is set; `(derived)` marks a default computed from another knob.

## The four layers

| Layer | Where you set it | Scope | Cost to change | Page |
|---|---|---|---|---|
| 1. Environment variables | the process environment / deploy manifest | whole node | restart the node | [Controller environment variables](configuration/environment_variables.md) |
| 2. Application env | `config/config.exs`, `config/runtime.exs`, `config/<env>.exs` | whole node | rebuild or restart (`runtime.exs` is read at boot) | [Application env keys](configuration/application_env.md) |
| 3. Operator declarations | the SDK, in the operator's `channels()` and Tasklet/Steps class | one operator class | redeploy the runner, frozen at registration | [Operator settings](configuration/operator_settings.md) |
| 4. Plugin options | the plugin's `opts` map when it is installed | one plugin instance | re-install the plugin | [Plugin options](configuration/plugin_options.md) |

Layers 1 and 2 overlap on purpose: most environment variables exist only to
set an application-env key at boot. **Both layers are one table**:
`lib/flezha/config/keys.ex` declares every knob once (its name, the
application-env key, the environment variable, the type, the default, the
runtimes that read it and the surface a user observes it on), and
`config/runtime.exs` is one call over that table rather than a translation of
its own. Prefer the variable in a container, the config file for an embedded or
local node.

Layer 3 is the one that surprises people. Anything an operator declares, such as
its delivery tier, its acknowledgement granularity or its correction window, is
frozen when the class registers with the controller, and there is no
user-facing override: these change the semantics of the operator's output, not
just its performance, so they belong with the code that relies on them. The
effectively-once, checkpoint and output-batching options are all
[operator settings](configuration/operator_settings.md).

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

- [`topics/basics/deploying_a_controller.md`](topics/basics/deploying_a_controller.md): the variables a deploy cannot omit, and how to run a node
- [`topics/basics/configuring_an_operator.md`](topics/basics/configuring_an_operator.md): choosing an operator's settings
- [`topics/basics/durability.md`](topics/basics/durability.md): what the durability knobs mean for correctness
- [`topics/basics/authorization.md`](topics/basics/authorization.md): the posture behind the authz keys
- [`architecture/kernel.md`](https://github.com/flezha/flezha/blob/main/docs/architecture/kernel.md): where the WAL, outbox and fsync knobs sit in the data path
- [`operations/benchmarking.md`](https://github.com/flezha/flezha/blob/main/docs/operations/benchmarking.md): measuring the effect of a change to any of these

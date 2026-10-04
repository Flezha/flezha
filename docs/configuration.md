# Configuration overview

Flezha starts with no configuration. Every setting has a default, chosen for one
controller and its runners on one machine, and you change a setting only when
the default does not fit: another port, a durability backend, a cluster, more
retries for one operator.

Where you change it depends on what the setting is about:

- **The node**: ports, directories, the cluster, durability, security. An
  environment variable when you start the controller, or application env when
  Flezha runs inside your own Elixir application.
- **What an operator promises**: flow or steps, where it keeps its work, how it
  retries. The operator's own code.
- **One plugin**: the URL it polls, the topic it writes to. The plugin's options
  when you enable it.

**Contents**

1. [The four layers](#the-four-layers)
2. [Setting an environment variable](#setting-an-environment-variable)
3. [Setting application env](#setting-application-env)
4. [Declaring an operator setting](#declaring-an-operator-setting)
5. [Setting a plugin option](#setting-a-plugin-option)
6. [Outside the four layers](#outside-the-four-layers)
7. [See also](#see-also)

## The four layers

Each answer above is one of four layers, and the layer decides what a change
costs:

| Layer | Where you set it | Scope | Cost to change | Page |
|---|---|---|---|---|
| 1. Environment variables | the process environment / deploy manifest | whole node | restart the node | [Controller environment variables](configuration/environment_variables.md) |
| 2. Application env | `config/config.exs`, `config/runtime.exs`, `config/<env>.exs` | whole node | rebuild or restart (`runtime.exs` is read at boot) | [Application env keys](configuration/application_env.md) |
| 3. Operator declarations | the SDK, in the operator's `channels()` and Tasklet/Steps class | one operator class | redeploy the runner, frozen at registration | [Operator settings](configuration/operator_settings.md) |
| 4. Plugin options | `--X<option>` flags or a `--plugins` spec's `opts`, when the plugin is enabled | one plugin | `flz plugin update` | [Plugin options](configuration/plugin_options.md) |

Layers 1 and 2 are the same settings under two spellings: the variable
`FLEZHA_HEALTHZ_PORT` sets the key `:healthz_port` at boot. Use the variables
with the release or the image, and application env when Flezha runs inside your
own Elixir application, where the variables are not read.

Layer 3 is the one that surprises people. Anything an operator declares, such as
its delivery tier, its acknowledgement granularity or its correction window, is
frozen when the class registers with the controller, and there is no
user-facing override: these change the semantics of the operator's output, not
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
  -v flezha_data:/flezha/app_data \
  flezha/flezha:0.1.0
```

On [Kubernetes](topics/advanced/kubernetes.md) the same variables go in the
ConfigMap the pod reads. A change takes effect at the next start. Every
variable is in [Controller environment variables](configuration/environment_variables.md).

## Setting application env

When Flezha runs [inside your own Elixir application](topics/basics/deploying_a_controller.md#inside-an-elixir-application),
the `FLEZHA_*` variables are not read. Set the same settings as keys of the
`:flezha` application:

```elixir
# config/config.exs
config :flezha,
  healthz_enabled: true,
  state_root: "/var/lib/flezha/state"
```

Every key is in [Application env keys](configuration/application_env.md).

## Declaring an operator setting

An operator's settings are part of its code: written once in the class, and the
same wherever it runs. This is `order_executor` from the tradedesk example,
which runs in steps and undoes every finished step when one fails for good:

<!-- tabs-open -->

### Java

```java
@Override
public Run run() {
    return new Run.Steps(Each.ONCE, OnFailure.UNDO_ALL);
}
```

### Rust

```rust
fn settings() -> OperatorSettings {
    OperatorSettings {
        run: Run::Steps { each: Each::Once, on_failure: OnFailure::UndoAll },
        ..OperatorSettings::default()
    }
}
```

### Node

```javascript
export default defineOperator({
  factoryId: 'order_executor',
  steps: { each: 'once', onFailure: 'undo_all' },
  ...
});
```

### Python

```python
@flezha.operator("order_executor", run={"steps": {"each": "once", "on_failure": "undo_all"}})
def order_executor(c: flezha.Channels) -> None:
    ...
```

### Ruby

```ruby
class OrderExecutorOperator < Flezha::Operator
  factory_id "order_executor"
  run steps: { each: :once, on_failure: :undo_all }
  ...
end
```

### PHP

```php
#[Operator(factoryId: 'order_executor', run: ['steps' => ['each' => 'per_input', 'on_failure' => 'undo_all']])]
final class OrderExecutorOperator extends \Flezha\Operator
```

### Elixir

```elixir
use Flezha.SDK.Operator,
  factory_id: "order_executor",
  run: {:steps, each: :once, on_failure: :undo_all}
```

<!-- tabs-close -->

No variable or start option overrides one; a change is a new version of the
class. Every setting is in [Operator settings](configuration/operator_settings.md),
and choosing them is taught in [Configuring an operator](topics/basics/configuring_an_operator.md).

## Setting a plugin option

Each `--X<option>` flag is one option of the plugin:

```sh
$ flz plugin enable signal-config operator_http_poll --operator_id=detector \
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

- [`topics/basics/deploying_a_controller.md`](topics/basics/deploying_a_controller.md): the variables a deploy cannot omit, and how to run a node
- [`topics/basics/configuring_an_operator.md`](topics/basics/configuring_an_operator.md): choosing an operator's settings
- [`topics/basics/durability.md`](topics/basics/durability.md): what the durability knobs mean for correctness
- [`topics/basics/authorization.md`](topics/basics/authorization.md): the posture behind the authz keys
- [`architecture/kernel.md`](https://github.com/flezha/flezha/blob/main/docs/architecture/kernel.md): where the WAL, outbox and fsync knobs sit in the data path
- [`operations/benchmarking.md`](https://github.com/flezha/flezha/blob/main/docs/operations/benchmarking.md): measuring the effect of a change to any of these

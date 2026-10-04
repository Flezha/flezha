# The rust SDK's echo example

Every file of `sdk/rust/examples/echo`, the runner the echo tutorial runs. Its dependency file names the SDK package and the version this release pins.

## examples/echo/Cargo.toml

````toml
# Standalone on purpose: no workspace, and one dependency on a released
# version, so this builds the way your own project would rather than the way a
# crate inside the SDK's workspace does.
[package]
name = "echo"
version = "0.1.0"
edition = "2021"
publish = false

[dependencies]
flezha_runner = "0.1.0"
# The admin calls commented out in main.rs take their fields as JSON.
serde_json = "1"

# Its own workspace, so the Flezha repository's workspace around this folder
# does not claim it. Harmless in a copy of its own.
[workspace]
````

## examples/echo/README.md

````markdown
# Echo: the Rust hello world

The Rust hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `Cargo.toml` names one released version of `flezha_runner`,
the way your own project would.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `src/tasklet.rs` | the tasklet | the processing stage: prefixes each message |
| `src/operator.rs` | the operator | the graph: `in` feeds the tasklet, output lands on `out` |
| `src/main.rs` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).

```sh
cargo run --release
```

Then, from another terminal:

```sh
flz operator start echo --id my_echo
flz operator send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It blocks until shutdown, which is a SIGTERM or Ctrl-C. The namespace is the
first argument and defaults to `default`: `cargo run --release -- my_app`.

## Building from a checkout

`flezha_runner` is not on crates.io yet, and the core it builds on has no
release tag yet, so Cargo cannot resolve either. From this folder, point Cargo
at the checkout once:

```sh
mkdir -p .cargo
cat > .cargo/config.toml <<'EOF'
[patch.crates-io]
flezha_runner = { path = "../.." }

[patch."https://github.com/flezha/flezha"]
flz_core = { path = "../../../../flz_core" }
flz_transport = { path = "../../../../flz_transport" }
EOF
```

Then build with `cargo run --release --offline`, so Cargo does not look for the
missing tag. A copy of this folder outside the checkout names the checkout's `sdk/rust`,
`flz_core` and `flz_transport` instead of the relative paths. Git ignores the `.cargo` folder, and once the crate is
published the step goes away and `cargo run --release` is all you need.

The empty `[workspace]` table in `Cargo.toml` keeps the repository's own
workspace from claiming this folder, and does nothing in a copy of its own.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is. The other Rust examples, covering state, keys, timers, steps operators and the
rest, are the SDK crate's own examples in `sdk/rust/examples/`.
````

## examples/echo/src/main.rs

````rust
//! Starts a runner: a process that connects to the controller and serves your
//! operators.
//!
//! Declare every operator you want served before `start()`. One runner can
//! serve several, and the same call takes a steps operator.

mod operator;
mod tasklet;

use flezha_runner::Flezha;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "default".to_string());

    // `run()` is `start()` followed by `await_termination()`. Splitting them
    // leaves room for the admin calls below, and still blocks until shutdown.
    // The runner is bound to a name because a chained temporary would be
    // dropped, and shut down, at the end of the statement.
    let runner = Flezha::create_runner(namespace)?;
    // connects to the controller and serves these operators
    runner.register_factory(operator::EchoOperator)?.start()?;

    // Everything `flz operator` and `flz plugin` do is also a method here.
    // Uncomment to drive this namespace from the application instead of from a
    // terminal:
    //
    // let admin = runner.admin();
    // admin.start_operator(&flezha_runner::OperatorSpec::new("echo").id("my_echo"))?;
    // admin.enable_plugin(
    //     "writer",
    //     &flezha_runner::PluginSpec::new("operator_file_sink")
    //         .opts(serde_json::json!({ "path": "echo-out.txt" })),
    // )?;
    // admin.bind_plugin("writer", "my_echo", serde_json::json!([["on_output", {}]]), "")?;
    // admin.send("my_echo", "hello world", "in")?;
    //
    // docs/sdk/admin.md has the whole surface. An enforced controller refuses
    // all of it until `flz role bind runner@default app default`.

    runner.await_termination()?;
    Ok(())
}
````

## examples/echo/src/operator.rs

````rust
use flezha_runner::{ChannelBuilder, Operator};

use crate::tasklet::Echo;

/// The deployable unit. An operator declares the graph a message travels: the
/// entry channel it arrives on, the steps it passes through, and the channel its
/// result leaves on.
///
/// The operator is what the controller schedules onto a runner, restarts after
/// a failure and moves when a runner goes away. A tasklet is one stage inside
/// that graph.
pub struct EchoOperator;

impl Operator for EchoOperator {
    /// The operator's stable identity, and the name `flz operator start`
    /// takes. The controller's registry and the durable state both key on it,
    /// so changing it declares a new operator rather than renaming this one.
    fn factory_id() -> &'static str {
        "echo" // the name `flz operator start` takes
    }

    /// Called once when the runner declares the operator. The graph is checked
    /// as a whole there, so one that cannot work is refused at startup.
    ///
    /// An entry channel named `in` carrying strings, routed into the echo
    /// tasklet, whose output ends on the plugin layer as the channel `out`.
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in") // messages arrive on the entry channel "in"
            .into::<Echo>() // each one runs through the echo tasklet
            .sink_named("out"); // the result leaves on "out", for a plugin to pick up
    }
}
````

## examples/echo/src/tasklet.rs

````rust
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

/// A processing stage of the operator's graph, run once per message. This one
/// prefixes the input and emits the result.
///
/// The tasklet is your own type. The attribute generates no type: it fills in
/// what the block leaves out. With no `type State` written, nothing is kept
/// between messages and nothing is journaled. `name` is the tasklet's stable
/// identity. State is filed under it, so renaming it later declares a different
/// tasklet rather than renaming this one; renaming the type is safe.
///
/// Where the result goes is not decided here. The operator decides that, so
/// the graph can be rewired without touching this tasklet.
#[derive(Default)]
pub struct Echo;

// state is saved under this name, so choose it once
#[tasklet(name = "echo")]
impl Tasklet for Echo {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("echo: {input}")) // where it goes is the operator's business
    }
}
````

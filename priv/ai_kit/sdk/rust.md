# The rust SDK's echo example

Every file of `sdk/rust/examples/echo`, the runner Up and running builds. Its dependency file names the SDK package and the version this release pins.

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
# The control calls commented out in main.rs take their fields as JSON.
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

Three files, one for each of the [building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md):

| File | Part | What it does |
|---|---|---|
| `src/tasklet.rs` | the tasklet | the processing stage: prefixes each message |
| `src/relay.rs` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `src/main.rs` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([Up and running](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md)).

```sh
cargo run --release
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

It blocks until shutdown, which is a SIGTERM or Ctrl-C. The namespace is the
first argument and defaults to `default`: `cargo run --release -- my_app`.

## Building from a checkout

To run the example against a Flezha checkout instead of the published crates,
point Cargo at the checkout once, from this folder:

```sh
mkdir -p .cargo
cat > .cargo/config.toml <<'EOF'
[patch.crates-io]
flezha_runner = { path = "../.." }
flz_core = { path = "../../../../crates/flz_core" }
flz_transport = { path = "../../../../crates/flz_transport" }
EOF
```

Then build with `cargo run --release` as before. A copy of this folder outside the
checkout names the checkout's `sdk/rust`, `flz_core` and `flz_transport` instead of the
relative paths. Git ignores the `.cargo` folder; delete it to use the published crates
again.

The empty `[workspace]` table in `Cargo.toml` keeps the repository's own
workspace from claiming this folder, and does nothing in a copy of its own.

## Where to go next

[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what each of the three parts is and why the split is the way it
is. The other Rust examples, covering state, keys, timers, steps relays and the
rest, are the SDK crate's own examples in `sdk/rust/examples/`.
````

## examples/echo/src/main.rs

````rust
//! Starts a runner: a process that connects to the controller and serves your
//! relays.
//!
//! Declare every relay you want served before `start()`. One runner can
//! serve several, and the same call takes a steps relay.

mod relay;
mod tasklet;

use flezha_runner::Flezha;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "default".to_string());

    // `run()` is `start()` followed by `await_termination()`. Splitting them
    // leaves room for the calls below, and still blocks until shutdown.
    // The runner is bound to a name because a chained temporary would be
    // dropped, and shut down, at the end of the statement.
    let runner = Flezha::create_runner(namespace)?;
    // connects to the controller and serves these relays
    runner.register_factory(relay::EchoRelay)?.start()?;

    // Everything `flz relay` and `flz plugin` do is also a method of
    // `control()`, and `send` hands a message to a relay. Uncomment to
    // drive this namespace from the application instead of from a terminal:
    //
    // let control = runner.control();
    // control.start_relay(&flezha_runner::RelaySpec::new("echo").id("my_echo"))?;
    // control.enable_plugin(
    //     "writer",
    //     &flezha_runner::PluginSpec::new("relay_file_output")
    //         .opts(serde_json::json!({ "path": "echo-out.txt" })),
    // )?;
    // control.bind_plugin("writer", "my_echo", serde_json::json!([["on_output", {}]]), "")?;
    // let opts = flezha_runner::SendOpts::new().msg_id("hello-1");
    // runner.send("my_echo", "hello world", "in", &opts)?;
    //
    // docs/public/topics/core/sending_and_calling.md covers sending. An
    // enforced controller refuses all of it until
    // `flz role bind runner@default app default`.

    runner.await_termination()?;
    Ok(())
}
````

## examples/echo/src/relay.rs

````rust
use flezha_runner::{ChannelBuilder, Relay};

use crate::tasklet::Echo;

/// The deployable unit. A relay declares the graph a message travels: the
/// entry channel it arrives on, the steps it passes through, and the channel its
/// result leaves on.
///
/// The relay is what the controller schedules onto a runner, restarts after
/// a failure and moves when a runner goes away. A tasklet is one stage inside
/// that graph.
pub struct EchoRelay;

impl Relay for EchoRelay {
    /// The relay's stable identity, and the name `flz relay start`
    /// takes. The controller's registry and the durable state both key on it,
    /// so changing it declares a new relay rather than renaming this one.
    fn factory_id() -> &'static str {
        "echo" // the name `flz relay start` takes
    }

    /// Called once when the runner declares the relay. The graph is checked
    /// as a whole there, so one that cannot work is refused at startup.
    ///
    /// An entry channel named `in` carrying strings, routed into the echo
    /// tasklet, whose output ends on the plugin layer as the channel `out`.
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in") // messages arrive on the entry channel "in"
            .into::<Echo>() // each one runs through the echo tasklet
            .output_named("out"); // the result leaves on "out", for a plugin to pick up
    }
}
````

## examples/echo/src/tasklet.rs

````rust
use flezha_runner::{tasklet, Ctx, Tasklet, TaskletError};

/// A processing stage of the relay's graph, run once per message. This one
/// prefixes the input and emits the result.
///
/// The tasklet is your own type. The attribute generates no type: it fills in
/// what the block leaves out. With no `type State` written, nothing is kept
/// between messages and nothing is journaled. `name` is the tasklet's stable
/// identity. State is filed under it, so renaming it later declares a different
/// tasklet rather than renaming this one; renaming the type is safe.
///
/// Where the result goes is not decided here. The relay decides that, so
/// the graph can be rewired without touching this tasklet.
#[derive(Default)]
pub struct Echo;

// state is saved under this name, so choose it once
#[tasklet(name = "echo")]
impl Tasklet for Echo {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("echo: {input}")) // where it goes is the relay's business
    }
}
````

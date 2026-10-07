# Up and running

This page and the next two build the smallest complete Flezha deployment by
hand: a controller, a runner of your own that serves an `echo` relay, and a
file on each side of it.

One controller runs throughout: it is started here, and stopped on
[Stop and clean up](stop_and_clean_up.md).

## Before you start

Each step has a purple box with a pencil, right above the command to run. The
steps are not optional: to reach the working example at the end, do every one
of them, in order. The box says what to do and names anything that has to be in
place first.

A command with no purple box above it is there to read or to check, and the
steps do not need it. The copy button on a command copies the command
alone, without the `$` prompt. What it prints is shown right below it, and your
ids, tokens and times will differ.

> #### What you need {: .tip}
>
> - **For the controller**, a Linux machine on x86-64 to run the release, or
>   Docker to run its image.
> - **For your runner**, one language, and only what its row lists:
>
> | Language | You need |
> |---|---|
> | Java | Java 21 and Maven |
> | Rust | Rust with Cargo |
> | Node | Node.js 20 or later, with npm |
> | Python | Python 3.11 or later, with pip |
> | Ruby | Ruby 3.3 or later, with Bundler |
> | PHP | PHP 8.4 or later with `ext-ffi`, and Composer |
> | Elixir | Elixir 1.17 or later |
>
> - **git**, to get the example.

> Get the example: clone the repository. The echo of each language is in
> `sdk/<language>/examples/echo`.
> {: .step}

```sh
$ git clone --recurse-submodules https://github.com/flezha/flezha.git
```

## Start the controller

Here you start one controller and leave it running until the end.
The controller places relays on runners, keeps their state durable and answers
`flz`; [Deploying a controller](../topics/deploying/deploying_a_controller.md)
covers it in full, including Kubernetes and running it inside an Elixir
application.

> Start a controller: pick one of the two tabs and follow it from top to
> bottom. Leave it running until [Stop and clean up](stop_and_clean_up.md).
> {: .step}

<!-- tabs-open -->

### Binaries

Download the release and unpack it:

```sh
$ curl -fLO https://github.com/flezha/flezha/releases/download/v0.1.0-alpha.1/flezha-0.1.0-linux-x86_64.tar.gz
$ tar -xzf flezha-0.1.0-linux-x86_64.tar.gz
```

That gives you a `flezha/` directory, with `bin/flz`, the CLI. The release
carries its own Erlang runtime, so nothing else needs installing. It is built
for Linux x86_64; on another platform,
[build it from source](../topics/deploying/building_from_source.md). Every
`./bin/flz` command in these pages and the tutorials runs from that directory,
in whichever terminal you use, so go there and start the controller:

```sh
$ cd flezha
$ FLEZHA_AUTHZ_ENABLED=false ./bin/flz controller start
```

It runs in the foreground of this terminal. Leave it open, and run everything
after this from a second terminal, in the `flezha` directory. Check that it is
ready:

```sh
$ ./bin/flz boot await
```

```output
boot await: ready
```

And that a runner can reach it, which needs no credential:

```sh
$ ./bin/flz runner check
```

```output
controller reachable
  directory  /run/user/1000/flezha (from $XDG_RUNTIME_DIR/flezha)
  checked    /run/user/1000/flezha
             /run/flezha
             /srv/flezha/app_data/sockets
```

### Docker

```sh
$ docker run -d --name flezha-controller \
  -e FLEZHA_AUTHZ_ENABLED=false \
  -v flezha_data:/flezha/app_data \
  -v /tmp/flz_sock:/flezha/app_data/sockets \
  flezha/flezha:0.1.0
```

The first volume holds everything the controller keeps. The second puts its
socket in `/tmp/flz_sock` on the host, where your runner finds it; the image
runs as uid 1000, so your host user needs that uid to reach it. Check that it
is ready:

```sh
$ docker exec flezha-controller /flezha/bin/flz boot await
```

```output
boot await: ready
```

<!-- tabs-close -->

`FLEZHA_AUTHZ_ENABLED=false` turns [authorization](../topics/core/authorization.md)
off, so no command here needs a token. The tradedesk tutorial turns it on.

## Write the echo runner

Here you get the echo runner in your language and start it. A runner is
a program of your own: it connects to the controller, registers the relay
factories it can serve, and runs what the controller places on it. The echo has
the three pieces every runner has: a tasklet, a relay and the runner itself.
[Building blocks](../topics/core/building_blocks.md) explains each one, and
[Connecting a runner](../topics/deploying/connecting_a_runner.md) covers how a
runner finds its controller.

The three files, in your language. The tasklet emits `echo: ` plus what it
got; the relay routes the channel `in` into it and names its output
channel `out`; the runner registers the relay's factory, `echo`, and
serves.

<!-- tabs-open -->

### Java

In your clone: `sdk/java/examples/echo` ([on GitHub](https://github.com/flezha/flezha_java_sdk/tree/main/examples/echo)).

```java
// Echo.java: the tasklet
@Tasklet.Spec(name = "echo")                 // state is saved under this name, so choose it once
public final class Echo implements Tasklet<String, Void, String> {
    @Override
    public void run(String input, Ctx<Void, String> ctx) throws TaskletException {
        ctx.emit("echo: " + input);          // where it goes is the relay's business
    }
}
```

[`Echo.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/echo/src/main/java/com/flezha/examples/echo/Echo.java)

```java
// EchoRelay.java: the relay
public final class EchoRelay implements Relay {
    @Override
    @Nonnull
    public String factoryId() {
        return "echo";                       // the name `flz relay start` takes
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(String.class, "in")          // messages arrive on the entry channel "in"
                .into(Echo.class)            // each one runs through the echo tasklet
                .output("out");              // the result leaves on "out", for a plugin to pick up
    }
}
```

[`EchoRelay.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/echo/src/main/java/com/flezha/examples/echo/EchoRelay.java)

```java
// Main.java: the runner
public final class Main {
    public static void main(String[] args) throws Exception {
        FlezhaRunner runner = Flezha.createRunner(args.length > 0 ? args[0] : "default")
                .registerFactory(EchoRelay.class)     // the relays this runner serves
                .start();                                // connects to the controller

        runner.awaitTermination();
    }

    private Main() {}
}
```

[`Main.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/echo/src/main/java/com/flezha/examples/echo/Main.java)

### Rust

In your clone: `sdk/rust/examples/echo` ([on GitHub](https://github.com/flezha/flezha_rust_sdk/tree/main/examples/echo)).

```rust
// src/tasklet.rs: the tasklet
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
```

[`src/tasklet.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/echo/src/tasklet.rs)

```rust
// src/relay.rs: the relay
pub struct EchoRelay;

impl Relay for EchoRelay {
    fn factory_id() -> &'static str {
        "echo" // the name `flz relay start` takes
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in") // messages arrive on the entry channel "in"
            .into::<Echo>() // each one runs through the echo tasklet
            .output_named("out"); // the result leaves on "out", for a plugin to pick up
    }
}
```

[`src/relay.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/echo/src/relay.rs)

```rust
// src/main.rs: the runner
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "default".to_string());

    let runner = Flezha::create_runner(namespace)?;
    // connects to the controller and serves these relays
    runner.register_factory(relay::EchoRelay)?.start()?;

    runner.await_termination()?;
    Ok(())
}
```

[`src/main.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/echo/src/main.rs)

### Node

In your clone: `sdk/node/examples/echo` ([on GitHub](https://github.com/flezha/flezha_node_sdk/tree/main/examples/echo)).

```javascript
// echo_tasklet.mjs: the tasklet
export const EchoTasklet = defineTasklet({
  name: 'echo', // state is saved under this name, so choose it once
  run: (input, ctx) => ctx.emit(`echo: ${input}`), // where it goes is the relay's business
});
```

[`echo_tasklet.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/echo/echo_tasklet.mjs)

```javascript
// echo_relay.mjs: the relay
export default defineRelay({
  factoryId: 'echo', // the name `flz relay start` takes
  channels: (c) => {
    c.input('in', { codec: 'text' }) // messages arrive on the entry channel "in"
      .into(EchoTasklet) // each one runs through the echo tasklet
      .output('out'); // the result leaves on "out", for a plugin to pick up
  },
});
```

[`echo_relay.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/echo/echo_relay.mjs)

```javascript
// runner.mjs: the runner
const namespace = process.argv[2] ?? 'default';

const runner = createRunner(namespace);
await runner.registerFactory('./echo_relay.mjs'); // the relays this runner serves

await runner.run(); // connects to the controller
```

[`runner.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/echo/runner.mjs)

### Python

In your clone: `sdk/python/examples/echo` ([on GitHub](https://github.com/flezha/flezha_python_sdk/tree/main/examples/echo)).

```python
# echo_tasklet.py: the tasklet
@flezha.tasklet(name="echo")  # state is saved under this name, so choose it once
class Echo:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(f"echo: {value}")  # where it goes is the relay's business
```

[`echo_tasklet.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/echo/echo_tasklet.py)

```python
# echo_relay.py: the relay
@flezha.relay("echo")  # the name `flz relay start` takes
def echo_relay(c: flezha.Channels) -> None:
    (
        c.input("in", codec="text")  # messages arrive on the entry channel "in"
        .into(Echo)  # each one runs through the echo tasklet
        .output("out")  # the result leaves on "out", for a plugin to pick up
    )
```

[`echo_relay.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/echo/echo_relay.py)

```python
# runner.py: the runner
def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging("info")

    # the relays this runner serves
    runner = flezha.create_runner(namespace).register_factory(echo_relay)

    runner.start()  # connects to the controller

    runner.await_termination()

if __name__ == "__main__":
    main()
```

[`runner.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/echo/runner.py)

### Ruby

In your clone: `sdk/ruby/examples/echo` ([on GitHub](https://github.com/flezha/flezha_ruby_sdk/tree/main/examples/echo)).

```ruby
# echo_tasklet.rb: the tasklet
class EchoTasklet < Flezha::Tasklet
  tasklet_name "echo" # state is saved under this name, so choose it once

  def run(input, ctx) = ctx.emit("echo: #{input}") # where it goes is the relay's business
end
```

[`echo_tasklet.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/echo/echo_tasklet.rb)

```ruby
# echo_relay.rb: the relay
class EchoRelay < Flezha::Relay
  factory_id "echo" # the name `flz relay start` takes

  channels do |c|
    c.input("in", codec: :text) # messages arrive on the entry channel "in"
     .into(EchoTasklet)          # each one runs through the echo tasklet
     .output("out")   # the result leaves on "out", for a plugin to pick up
  end
end
```

[`echo_relay.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/echo/echo_relay.rb)

```ruby
# runner.rb: the runner
namespace = ARGV[0] || "default"

# the relays this runner serves
runner = Flezha.create_runner(namespace).register_factory(EchoRelay)

runner.boot # connects to the controller

runner.await_termination
```

[`runner.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/echo/runner.rb)

### PHP

In your clone: `sdk/php/examples/echo` ([on GitHub](https://github.com/flezha/flezha_php_sdk/tree/main/examples/echo)).

```php
// src/EchoTasklet.php: the tasklet
#[Task(name: 'echo')] // state is saved under this name, so choose it once
final class EchoTasklet extends Tasklet
{
    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->emit('echo: ' . $input); // where it goes is the relay's business
    }
}
```

[`src/EchoTasklet.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/echo/src/EchoTasklet.php)

```php
// src/EchoRelay.php: the relay
#[Relay(factoryId: 'echo')] // the name `flz relay start` takes
final class EchoRelay extends \Flezha\Relay
{
    public static function channels(Builder $c): void
    {
        $c->input('in', codec: ChannelCodec::Text) // messages arrive on the entry channel "in"
            ->into(EchoTasklet::class)             // each one runs through the echo tasklet
            ->output('out');                       // the result leaves on "out", for a plugin to pick up
    }
}
```

[`src/EchoRelay.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/echo/src/EchoRelay.php)

```php
// runner.php: the runner
$runner = Flezha::createRunner($argv[1] ?? 'default')
    ->registerFactory(EchoRelay::class) // the relays this runner serves
    ->start();                             // connects to the controller

$runner->awaitTermination();
```

[`runner.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/echo/runner.php)

### Elixir

In your clone: `sdk/elixir/examples/echo` ([on GitHub](https://github.com/flezha/flezha_elixir_sdk/tree/main/examples/echo)).

```elixir
# lib/echo/tasklet.ex: the tasklet
defmodule Echo.Tasklet do
  # state is saved under this name, so choose it once
  use Flezha.SDK.Tasklet, name: "echo"

  @impl true
  def run(input, state, ctx) do
    # where it goes is the relay's business
    emit(ctx, "echo: " <> input)
    {:ok, state}
  end
end
```

[`lib/echo/tasklet.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/echo/lib/echo/tasklet.ex)

```elixir
# lib/echo/relay.ex: the relay
defmodule Echo.Relay do
  # the name `flz relay start` takes
  use Flezha.SDK.Relay, factory_id: "echo"

  @impl true
  def channels(c) do
    c
    # messages arrive on the entry channel "in"
    |> input("in")
    # each one runs through the echo tasklet
    |> into(Echo.Tasklet)
    # the result leaves on "out", for a plugin to pick up
    |> output("out")
  end
end
```

[`lib/echo/relay.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/echo/lib/echo/relay.ex)

```elixir
# lib/echo/application.ex: the runner
defmodule Echo.Application do
  use Application

  @impl true
  def start(_type, _args) do
    namespace = List.first(System.argv(), "default")

    children = [
      # connects to the controller and serves these relays
      {Flezha.SDK, namespace: namespace, relays: [Echo.Relay], transport: :uds}
    ]

    {:ok, supervisor} = Supervisor.start_link(children, strategy: :one_for_one)

    {:ok, supervisor}
  end
end
```

[`lib/echo/application.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/echo/lib/echo/application.ex)

<!-- tabs-close -->


> Start the echo runner: open a new terminal on the controller's machine, go to
> your language's echo example folder in your clone, and run the command in its
> tab.
> {: .step}

<!-- tabs-open -->

### Java

```sh
$ mvn -q package && java -jar target/echo.jar
```

### Rust

```sh
$ cargo run --release
```

### Node

```sh
$ npm install && node runner.mjs
```

### Python

```sh
$ pip install -r requirements.txt && python3 runner.py
```

### Ruby

```sh
$ bundle install && bundle exec ruby runner.rb
```

### PHP

```sh
$ composer install && php runner.php
```

### Elixir

```sh
$ mix deps.get && mix run --no-halt
```

<!-- tabs-close -->

If the controller runs in Docker, set
`FLEZHA_SOCKET_FILES_PATH=/tmp/flz_sock` in front of the command, the folder
the controller's socket is mounted on.

The runner keeps that terminal. Leave it running. From another terminal, in the
`flezha` directory, check that it connected:

```sh
$ ./bin/flz runner list
```

```output
Fetching runners for namespace 'default'...
Runners in namespace 'default':
  - default_23fc85126379
```

If your runner is missing,
[Runner cannot connect](../troubleshooting/runner_cannot_connect.md) names
every reason with its fix.

## Run the echo

Here you start a relay from the `echo` factory your runner
registered, send it a message and watch what comes out. A factory is the
template, the code you wrote; a relay is one running instance of it, with
a name and state of its own.
[Running and testing](../topics/core/running_and_testing.md) covers starting,
watching and stopping relays in full, from `flz` and from your own code.

> From a third terminal, in the `flezha` directory, start a relay from the
> `echo` factory and name it `my_echo`. The controller and your runner have to be running.
> {: .step}

```sh
$ ./bin/flz relay start echo --id my_echo
```

```output
Started my_echo
```

`echo` is the factory id your code declares; `my_echo` is the relay id you
chose, and every later command finds the relay by it. List the relays to
see which runner it is bound to:

```sh
$ ./bin/flz relay list
```

```output
Fetching relays for namespace 'default'...
Relays in namespace 'default':

  RELAY                          HEALTH      PARTS   NODE                  RUNNER
  my_echo                           healthy     -       flezha@127.0.0.1      default_23fc85126379
```

A runner name under `RUNNER` means the controller bound the relay to your
runner. `(none)` means no runner registered `echo`, which usually means the
factory id is spelled differently in the code and on the command line.

> Send `hello world` to `my_echo`, and print everything it sends out during
> the next two seconds.
> {: .step}

```sh
$ ./bin/flz relay send my_echo --text 'hello world' --await 2000
```

```output
Delivered to 'my_echo'.
out	echo: hello world
```

The first line says the controller accepted the message. The second is the
output: the channel, `out`, a tab, and what the tasklet emitted.

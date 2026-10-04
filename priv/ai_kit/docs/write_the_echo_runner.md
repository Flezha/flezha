# Write the echo runner

On this page you get the echo runner in your language and start it. A runner is
a program of your own: it connects to the controller, registers the operator
factories it can serve, and runs what the controller places on it. The echo has
the three pieces every runner has: a tasklet, an operator and the runner itself.
[Building blocks](../../topics/basics/building_blocks.md) explains each one, and
[Connecting a runner](../../topics/deploying/connecting_a_runner.md) covers how a
runner finds its controller.

The three files, in your language. The tasklet emits `echo: ` plus what it
got; the operator routes the channel `in` into it and names its output
channel `out`; the runner registers the operator's factory, `echo`, and
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
        ctx.emit("echo: " + input);          // where it goes is the operator's business
    }
}
```

[`Echo.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/echo/src/main/java/com/flezha/examples/echo/Echo.java)

```java
// EchoOperator.java: the operator
public final class EchoOperator implements Operator {
    @Override
    @Nonnull
    public String factoryId() {
        return "echo";                       // the name `flz operator start` takes
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(String.class, "in")          // messages arrive on the entry channel "in"
                .into(Echo.class)            // each one runs through the echo tasklet
                .sink("out");                // the result leaves on "out", for a plugin to pick up
    }
}
```

[`EchoOperator.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/echo/src/main/java/com/flezha/examples/echo/EchoOperator.java)

```java
// Main.java: the runner
public final class Main {
    public static void main(String[] args) throws Exception {
        FlezhaRunner runner = Flezha.createRunner(args.length > 0 ? args[0] : "default")
                .registerFactory(EchoOperator.class)     // the operators this runner serves
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
        ctx.emit(format!("echo: {input}")) // where it goes is the operator's business
    }
}
```

[`src/tasklet.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/echo/src/tasklet.rs)

```rust
// src/operator.rs: the operator
pub struct EchoOperator;

impl Operator for EchoOperator {
    fn factory_id() -> &'static str {
        "echo" // the name `flz operator start` takes
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in") // messages arrive on the entry channel "in"
            .into::<Echo>() // each one runs through the echo tasklet
            .sink_named("out"); // the result leaves on "out", for a plugin to pick up
    }
}
```

[`src/operator.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/echo/src/operator.rs)

```rust
// src/main.rs: the runner
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "default".to_string());

    let runner = Flezha::create_runner(namespace)?;
    // connects to the controller and serves these operators
    runner.register_factory(operator::EchoOperator)?.start()?;

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
  run: (input, ctx) => ctx.emit(`echo: ${input}`), // where it goes is the operator's business
});
```

[`echo_tasklet.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/echo/echo_tasklet.mjs)

```javascript
// echo_operator.mjs: the operator
export default defineOperator({
  factoryId: 'echo', // the name `flz operator start` takes
  channels: (c) => {
    c.input('in', { codec: 'text' }) // messages arrive on the entry channel "in"
      .into(EchoTasklet) // each one runs through the echo tasklet
      .sink('out'); // the result leaves on "out", for a plugin to pick up
  },
});
```

[`echo_operator.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/echo/echo_operator.mjs)

```javascript
// runner.mjs: the runner
const namespace = process.argv[2] ?? 'default';

const runner = createRunner(namespace);
await runner.registerFactory('./echo_operator.mjs'); // the operators this runner serves

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
        ctx.emit(f"echo: {value}")  # where it goes is the operator's business
```

[`echo_tasklet.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/echo/echo_tasklet.py)

```python
# echo_operator.py: the operator
@flezha.operator("echo")  # the name `flz operator start` takes
def echo_operator(c: flezha.Channels) -> None:
    (
        c.input("in", codec="text")  # messages arrive on the entry channel "in"
        .into(Echo)  # each one runs through the echo tasklet
        .sink("out")  # the result leaves on "out", for a plugin to pick up
    )
```

[`echo_operator.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/echo/echo_operator.py)

```python
# runner.py: the runner
def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging("info")

    # the operators this runner serves
    runner = flezha.create_runner(namespace).register_factory(echo_operator)

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

  def run(input, ctx) = ctx.emit("echo: #{input}") # where it goes is the operator's business
end
```

[`echo_tasklet.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/echo/echo_tasklet.rb)

```ruby
# echo_operator.rb: the operator
class EchoOperator < Flezha::Operator
  factory_id "echo" # the name `flz operator start` takes

  channels do |c|
    c.input("in", codec: :text) # messages arrive on the entry channel "in"
     .into(EchoTasklet)          # each one runs through the echo tasklet
     .sink("out")                # the result leaves on "out", for a plugin to pick up
  end
end
```

[`echo_operator.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/echo/echo_operator.rb)

```ruby
# runner.rb: the runner
namespace = ARGV[0] || "default"

# the operators this runner serves
runner = Flezha.create_runner(namespace).register_factory(EchoOperator)

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
        $ctx->emit('echo: ' . $input); // where it goes is the operator's business
    }
}
```

[`src/EchoTasklet.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/echo/src/EchoTasklet.php)

```php
// src/EchoOperator.php: the operator
#[Operator(factoryId: 'echo')] // the name `flz operator start` takes
final class EchoOperator extends \Flezha\Operator
{
    public static function channels(Builder $c): void
    {
        $c->input('in', codec: ChannelCodec::Text) // messages arrive on the entry channel "in"
            ->into(EchoTasklet::class)             // each one runs through the echo tasklet
            ->sink('out');                         // the result leaves on "out", for a plugin to pick up
    }
}
```

[`src/EchoOperator.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/echo/src/EchoOperator.php)

```php
// runner.php: the runner
$runner = Flezha::createRunner($argv[1] ?? 'default')
    ->registerFactory(EchoOperator::class) // the operators this runner serves
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
    # where it goes is the operator's business
    emit(ctx, "echo: " <> input)
    {:ok, state}
  end
end
```

[`lib/echo/tasklet.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/echo/lib/echo/tasklet.ex)

```elixir
# lib/echo/operator.ex: the operator
defmodule Echo.Operator do
  # the name `flz operator start` takes
  use Flezha.SDK.Operator, factory_id: "echo"

  @impl true
  def channels(c) do
    c
    # messages arrive on the entry channel "in"
    |> input("in")
    # each one runs through the echo tasklet
    |> into(Echo.Tasklet)
    # the result leaves on "out", for a plugin to pick up
    |> sink("out")
  end
end
```

[`lib/echo/operator.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/echo/lib/echo/operator.ex)

```elixir
# lib/echo/application.ex: the runner
defmodule Echo.Application do
  use Application

  @impl true
  def start(_type, _args) do
    namespace = List.first(System.argv(), "default")

    children = [
      # connects to the controller and serves these operators
      {Flezha.SDK, namespace: namespace, operators: [Echo.Operator], transport: :uds}
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
[Runner cannot connect](../../troubleshooting/runner_cannot_connect.md) names
every reason with its fix.

## Next

Nothing runs on the runner yet. [Run the echo](run_the_echo.md) starts the
first operator.

# Open the desk

On this page you start a controller with authorization on, claim its admin,
give the desk a namespace of its own with a key for its runner, and connect the
runner. [Authorization](../../topics/basics/authorization.md) and
[Namespaces](../../topics/basics/namespaces.md) explain both in full.

> Start a controller as in [Start the controller](../echo/start_the_controller.md),
> from the binaries, but without `FLEZHA_AUTHZ_ENABLED=false`, so authorization
> is on. Stop the echo's controller first if it still runs. Run the commands
> below from the directory you started it in.
> {: .step}

A controller with authorization on and no admin writes a one-time token to
`app_data/auth/bootstrap.token`. Whoever claims it becomes the first admin.

> Claim the admin. The answer is your admin token, shown once, so keep it.
> {: .step}

```sh
$ flz auth bootstrap "$(cat app_data/auth/bootstrap.token)"
```

```output
bootstrap claimed; admin@_system bound to admin-8cc3f398fb1b4ae5
admin token (shown once, store it now): flzt_fwn2ctvoleymwb4blf55ybn7baw6qns44iballspqtxethhlqjhq
```

> Put your admin token where every `flz` command in this shell finds it. A new
> terminal needs the same line again.
> {: .step}

```sh
$ export FLEZHA_TOKEN=flzt_fwn2ctvoleymwb4blf55ybn7baw6qns44iballspqtxethhlqjhq
```

> Create the namespace `trading`, issue the key its runner connects with, and
> let that runner start orders (`spawner`), since each buy starts an
> `order_executor`. Keep the key: it is shown once.
> {: .step}

```sh
$ flz namespace create trading
$ flz token issue runner@trading trading runner --ttl never
$ flz role bind runner@trading spawner trading
```

```output
namespace trading created
...
token issued for runner@trading (runner@trading)
id:    c5c749c28131
token (shown once, store it now): flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra
bound runner@trading as spawner@trading
runner@trading  spawner@trading
```

`--ttl never` is required for a runner's key; without it the token expires
after a day and is refused.

The runner names the namespace it joins, and registers the desk's two
factories:

<!-- tabs-open -->

### Java

```java
// Main.java: the runner: joins trading and registers both factories
public final class Main {
    public static void main(String[] args) throws Exception {
        FlezhaRunner runner = Flezha.createRunner(args.length > 0 ? args[0] : "trading")
                .registerFactory(TradeWatcherOperator.class)
                .registerFactory(OrderExecutorOperator.class)
                .start();

        runner.awaitTermination();
    }

    private Main() {}
}
```

[`Main.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/tradedesk/src/main/java/com/flezha/examples/tradedesk/Main.java)

### Rust

```rust
// src/main.rs: the runner: joins trading and registers both factories
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "trading".to_string());

    let runner = Flezha::create_runner(namespace)?;
    runner
        .register_factory(trade_watcher::TradeWatcherOperator)?
        .register_factory(order_executor::OrderExecutorOperator)?
        .start()?;

    runner.await_termination()?;
    Ok(())
}
```

[`src/main.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/tradedesk/src/main.rs)

### Node

```javascript
// runner.mjs: the runner: joins trading and registers both factories
const runner = createRunner(process.argv[2] ?? 'trading');
await runner.registerFactory('./trade_watcher_operator.mjs');
await runner.registerFactory('./order_executor_operator.mjs');

await runner.run();
```

[`runner.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/tradedesk/runner.mjs)

### Python

```python
# runner.py: the runner: joins trading and registers both factories
def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "trading"
    flezha.configure_logging("info")

    runner = (
        flezha.create_runner(namespace)
        .register_factory(trade_watcher)
        .register_factory(order_executor)
    )

    runner.run()

if __name__ == "__main__":
    main()
```

[`runner.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/tradedesk/runner.py)

### Ruby

```ruby
# runner.rb: the runner: joins trading and registers both factories
$stdout.sync = true

runner = Flezha.create_runner(ARGV[0] || "trading")
               .register_factory(TradeWatcherOperator)
               .register_factory(OrderExecutorOperator)
               .boot

runner.await_termination
```

[`runner.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/tradedesk/runner.rb)

### PHP

```php
// runner.php: the runner: joins trading and registers both factories
$runner = Flezha::createRunner($argv[1] ?? 'trading')
    ->registerFactory(TradeWatcherOperator::class)
    ->registerFactory(OrderExecutorOperator::class)
    ->start();

$runner->awaitTermination();
```

[`runner.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/tradedesk/runner.php)

### Elixir

```elixir
# lib/tradedesk/application.ex: the runner: joins trading and registers both factories
defmodule Tradedesk.Application do
  use Application

  @impl true
  def start(_type, _args) do
    namespace = List.first(System.argv(), "trading")

    children = [
      {Flezha.SDK,
       namespace: namespace,
       operators: [Tradedesk.TradeWatcher, Tradedesk.OrderExecutor],
       transport: :uds}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

[`lib/tradedesk/application.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/tradedesk/lib/tradedesk/application.ex)

<!-- tabs-close -->

> Start the desk's runner from your language's `tradedesk` folder, with your
> runner key in place of the one here. Leave it running.
> {: .step}

<!-- tabs-open -->

### Java

```sh
$ mvn -q package
$ FLEZHA_NAMESPACE_SECRET=flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra java -jar target/tradedesk.jar
```

### Rust

```sh
$ FLEZHA_NAMESPACE_SECRET=flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra cargo run --release
```

### Node

```sh
$ npm install
$ FLEZHA_NAMESPACE_SECRET=flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra node runner.mjs
```

### Python

```sh
$ pip install -r requirements.txt
$ FLEZHA_NAMESPACE_SECRET=flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra python3 runner.py
```

### Ruby

```sh
$ bundle install
$ FLEZHA_NAMESPACE_SECRET=flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra bundle exec ruby runner.rb
```

### PHP

```sh
$ composer install
$ FLEZHA_NAMESPACE_SECRET=flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra php runner.php
```

### Elixir

```sh
$ mix deps.get
$ FLEZHA_NAMESPACE_SECRET=flzt_3j5bftycxn43ivwoxbd2peghazjgtnru5bxqgshvfqcft7lrdsra mix run --no-halt
```

<!-- tabs-close -->

From another terminal, with `FLEZHA_TOKEN` set, check that it joined `trading`:

```sh
$ flz runner list --namespace trading
```

```output
Fetching runners for namespace 'trading'...
Runners in namespace 'trading':
  - trading_2bfeedb5e74f
```

## Next

[Watch the trades](watch_the_trades.md) starts the first operator on it.

# Local mode

Local mode runs an operator inside your own process, with no controller, no
Docker and no socket. It takes the operator class you would deploy, runs its
declared graph, and hands you the output. It is the way to try an operator, to
develop one and to test its graph.

## Nothing here is durable

Local mode holds state in memory. There is no write-ahead log, no snapshot, no
dead-letter queue, and nothing survives the process. Every run prints that on
stderr before it does any work, and the notice cannot be turned off.

Local mode is not a deployment. These need a controller:

| Needs a controller | Why |
| --- | --- |
| Durability: the write-ahead log, snapshots, resuming after a crash, point-in-time queries | The controller owns the durable store |
| Effectively-once delivery | An output rides its effect record and is released once that record is durable |
| Plugins: Kafka, Postgres, HTTP, WebSocket | Plugins run on the controller |
| Placement, migration, rescale, partitioning | A member's key-group range arrives with `AddOperator` |
| The dead-letter queue | A park is durable, and the controller acknowledges it |
| Observability export, authorization, several runners | Controller surfaces |

Everything else runs as it does deployed: the entry channel's decode and parse
ladder, edge transforms (`map`, `filter`, `flat_map`) and keying (`key_by`),
one cell per resolved key with its own mailbox and overflow policy, the retry
ladder, and the delivery between tasklets.

## How it is used

Every SDK has the same four calls: start the operator, register a callback on a
sink, feed an input channel, and run until the graph is idle.

- **`feed(channel, bytes)`** enters where a frame off the wire enters, so the
  channel's declared decoder runs. It blocks while the consumer's mailbox is
  full, which is the deployed entry path's own backpressure.
- **`on_sink(channel, callback)`** registers against the sink's channel name. A
  sink names the channel it ends, which is why the Rust example below says
  `.sink_named("totals")` and registers `"totals"`. A sink without a name is
  named after the tasklet that feeds it.
- A channel name the declaration does not carry is refused, naming the channel,
  the operator and the channels that are declared. A value released on a sink
  with no callback is reported rather than discarded. A declaration that needs
  a controller is refused at startup, naming the capability.
- **`run_until_idle(budget)`** blocks until the graph has nothing left to do. A
  budget that runs out first is an error, never a silent success.

| SDK | Entry point | Worked example |
| --- | --- | --- |
| Rust | `LocalOperator::start` | [`sdk/rust/examples/local_word_count.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/local_word_count.rs) |
| Java | `LocalOperator.start` | [`sdk/java/flezha-sdk-examples/.../local/`](https://github.com/flezha/flezha_java_sdk/tree/main/flezha-sdk-examples/src/main/java/com/flezha/examples/local) |
| Node | `LocalOperator.start` | [`examples/node/local_word_count.mjs`](https://github.com/flezha/flezha/blob/main/examples/node/local_word_count.mjs) |
| Python | `LocalOperator.start` | [`sdk/python/examples/local_word_count.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/local_word_count.py) |
| Ruby | `Flezha::LocalOperator.start` | [`sdk/ruby/examples/local_word_count.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/local_word_count.rb) |
| PHP | `LocalOperator::start` | [`sdk/php/examples/E07_LocalMode.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/E07_LocalMode.php) |
| Elixir | `Flezha.SDK.Local.start` | the snippet below |

## Example: counting words

A graph that splits lines into words, keys each word to a cell of its own and
counts it. In Rust, the declaration and the run:

```rust
impl Operator for WordCount {
    fn factory_id() -> &'static str {
        "local_word_count"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("lines")
            .into::<Split>()                       // one line -> its words
            .key_by(|word: &String| word.clone())  // one Cell per word
            .into::<Tally>()                       // counts what it is handed
            .keyed()
            .sink_named("totals");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let run = LocalOperator::start::<WordCount>()?;
    run.on_sink("totals", |out| {
        println!("{}", String::from_utf8_lossy(&out.payload));
    })?;
    run.feed("lines", "the quick brown fox")?;
    run.feed("lines", "the lazy dog")?;
    run.run_until_idle(Duration::from_secs(5))?;
    Ok(())
}
```

`Split` and `Tally` are ordinary tasklets with nothing local about them. Flezha
is not on crates.io yet, so the crate is a path dependency on a checkout
(`flezha_runner = { path = "../flezha/sdk/rust" }`). From a checkout:

```sh
$ cargo run -p flezha_runner --example local_word_count
```

It prints the notice, then one line per word occurrence, counted per word:
`the 1`, `quick 1`, `brown 1`, `the 2`, and so on.

The same run in Java:

```java
try (LocalOperator run = LocalOperator.start(WordCount.class)) {
    run.onSink("totals", out -> System.out.println(new String(out.payload(), UTF_8)));
    run.feed("lines", "the quick brown fox");
    run.runUntilIdle(Duration.ofSeconds(5));
}
```

Build the host native binary once, install the reactor, then run it:

```sh
$ scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native
$ mvn -f sdk/java/pom.xml -DskipTests install
$ mvn -f sdk/java/flezha-sdk-examples/pom.xml exec:java -Dexec.mainClass=com.flezha.examples.local.Main
```

And in Elixir:

```elixir
{:ok, run} = Flezha.SDK.Local.start(WordCount)
:ok = Flezha.SDK.Local.on_sink(run, "counts", &IO.puts(&1.payload))
:ok = Flezha.SDK.Local.feed(run, "lines", "the quick brown fox")
:ok = Flezha.SDK.Local.run_until_idle(run, 5_000)
:ok = Flezha.SDK.Local.stop(run)
```

The Elixir SDK runs local mode in its own core rather than the shared Rust
kernel. Both are held to one recorded expectation,
`test/fixtures/local_mode_word_count.txt`, which every SDK's local-mode suite
reads.

## What you run is what you deploy

The class carries no local-mode member. Registered with a runner, the same type
deploys unchanged:

```rust
Flezha::create_runner("default")?.register_factory(WordCount)?.run()?;
```

## See also

- [Building blocks](building_blocks.md): tasklets, operators and runners
- [Streams](streams.md): edges, transforms and keys
- [Try it](../../getting_started/try_it.md): the same `echo` with a controller, in Docker

# Quickstart — run an operator in five minutes

Flezha runs stateful compute graphs across a cluster. This page runs one on
your laptop instead: **local mode** takes the operator class you would deploy,
runs its declared graph in your own process, and hands you the output. No
controller, no Docker, no socket.

## Read this first: nothing here is durable

Local mode holds state in memory. There is no write-ahead log, no snapshot, no
dead-letter queue, and nothing survives the process. Every run prints that on
stderr before it does any work, and it is not suppressible.

Local mode is for trying Flezha out, for developing an operator, and for
running its graph in a test. **It is not a deployment.** These need a
controller:

| Needs a controller | Why |
| --- | --- |
| Durability — WAL, snapshots, crash-resume, point-in-time query | The controller owns the durable store |
| Effectively-once delivery | An output rides its effect record and is released once that record is durable |
| Plugins — Kafka, Postgres, HTTP, WebSocket | The plugin subsystem is installed controller-side |
| Placement, migration, rescale, partitioning | A member's key-group range arrives with `AddOperator` |
| The dead-letter queue | A park is durable, and the controller acknowledges it |
| Observability export, authz, multi-runner scale | Controller surfaces |

Everything else is the deployed machinery, unchanged: the entry channel's
decode and parse ladder, edge transforms (`map` / `filter` / `flat_map`) and
keying (`key_by`), one Cell per resolved key with its own mailbox and
overflow policy, the retry ladder, and inter-tasklet delivery performed by
the runtime.

## Available in every SDK

Local mode ships in all seven SDKs. Three are walked through below; the other
four take the same shape over the same kernel, and each has a worked example
and a section in its own SDK page:

| SDK | Entry point | Worked example |
| --- | --- | --- |
| Node | `LocalOperator.start` | [`examples/node/local_word_count.mjs`](../examples/node/local_word_count.mjs) |
| Python | `LocalOperator.start` | [`sdk/python/examples/local_word_count.py`](../sdk/python/examples/local_word_count.py) |
| Ruby | `Flezha::LocalOperator.start` | [`sdk/ruby/examples/local_word_count.rb`](../sdk/ruby/examples/local_word_count.rb) |
| PHP | `LocalOperator::start` | [`sdk/php/examples/E07_LocalMode.php`](../sdk/php/examples/E07_LocalMode.php) |

## Rust

Flezha is not published to crates.io yet, so depend on a checkout:

```toml
# Cargo.toml
[dependencies]
flezha_runner = { path = "../flezha/sdk/rust" }
serde = { version = "1.0", features = ["derive"] }
```

The two local-mode-specific parts of a word-counting graph — the declaration,
and the run:

```rust
impl UnboundedOperator for WordCount {
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

`Split` and `Tally` are ordinary tasklets with nothing local-mode about
them. The complete, compiling file is
[`sdk/rust/examples/local_word_count.rs`](../sdk/rust/examples/local_word_count.rs).
Run it straight from a checkout:

```bash
cargo run -p flezha_runner --example local_word_count
```

It prints the non-durability notice, then `the 1`, `quick 1`, `brown 1`,
`the 2`, and so on — one line per word occurrence, counted per word.

## Java

```java
try (LocalOperator run = LocalOperator.start(WordCount.class)) {
    run.onSink("totals", out -> System.out.println(new String(out.payload(), UTF_8)));
    run.feed("lines", "the quick brown fox");
    run.runUntilIdle(Duration.ofSeconds(5));
}
```

The complete file is
[`sdk/java/flezha-sdk-examples/src/main/java/com/flezha/examples/local/`](../sdk/java/flezha-sdk-examples/src/main/java/com/flezha/examples/local).
Build the host native binary once, install the reactor, then run it:

```bash
scripts/build-native.sh --output sdk/java/flezha-sdk-native/src/main/resources/native
mvn -f sdk/java/pom.xml -DskipTests install
mvn -f sdk/java/flezha-sdk-examples/pom.xml exec:java \
    -Dexec.mainClass=com.flezha.examples.local.Main
```

It prints the same notice and the same seven lines the Rust example does.

## Elixir

The same graph, declared with the Elixir SDK's builder:

```elixir
defmodule Normalise do
  use Flezha.SDK.Tasklet, name: "normalise"

  @impl true
  def run(word, state, ctx) do
    emit(ctx, String.downcase(word))
    {:ok, state}
  end
end

defmodule Tally do
  use Flezha.SDK.Tasklet, name: "tally"

  state do
    field :count, :integer, default: 0
  end

  @impl true
  def run(word, state, ctx) do
    state = set_count(state, ctx, state.count + 1)
    emit(ctx, "#{word} #{state.count}")
    {:ok, state}
  end
end

defmodule WordCount do
  use Flezha.SDK.UnboundedOperator, factory_id: "word_count"

  @impl true
  def channels(c) do
    c
    |> input("lines")
    |> filter(&(not String.starts_with?(&1, "#")))
    |> flat_map(&String.split/1)
    |> into(Normalise)
    |> key_by(& &1)
    |> into(Tally)
    |> keyed()
    |> sink("counts")
  end
end
```

```elixir
{:ok, run} = Flezha.SDK.Local.start(WordCount)
:ok = Flezha.SDK.Local.on_sink(run, "counts", &IO.puts(&1.payload))
:ok = Flezha.SDK.Local.feed(run, "lines", "the quick brown fox")
:ok = Flezha.SDK.Local.run_until_idle(run, 5_000)
:ok = Flezha.SDK.Local.stop(run)
```

[`docs/sdk/elixir_sdk.md`](sdk/elixir_sdk.md) covers the SDK around it. The
Elixir SDK expresses local mode in its own core, which inherits nothing from
the shared Rust kernel — the two cores are held to one recorded expectation by
`test/fixtures/local_mode_word_count.txt`, which every SDK's local-mode suite
reads.

## What you registered is what you deploy

The class you just ran carries no local-mode member. Hand the same type to a
runner and it deploys unchanged:

```rust
Flezha::create_runner("default")?.register_factory(WordCount)?.run()?;
```

From there: [`docs/architecture/`](architecture/README.md) for the model,
[`docs/sdk/authoring_contract.md`](sdk/authoring_contract.md) for the authoring
surface, and [`docs/configuration.md`](configuration.md) for every knob a
deployed runner reads.

## Feeding, sinks and refusals

- **`feed(channel, bytes)`** enters where a frame off the wire enters, so the
  channel's declared decoder runs. It blocks while the consumer's mailbox is
  full — the deployed entry path's own backpressure.
- **`on_sink(channel, callback)`** registers against the sink's **channel**
  name, and a sink names the channel it terminates — which is why the example
  says `.sink_named("totals")` and registers `"totals"`. The no-name form
  leaves the channel named after the producing tasklet.
- A channel name the declaration does not carry is refused, naming the channel,
  the operator and the channels that are declared. A value released on a sink
  with no callback is reported rather than discarded. A declaration that needs
  a controller is refused at startup, naming the capability.
- **`run_until_idle(budget)`** blocks until the graph has nothing left to do. A
  budget that elapses first is an error, never a silent success.

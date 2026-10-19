# Place orders

On this page you look at the desk's workflow, `order_executor`: you start an
order by hand, read the journal of its steps, and make an order fail so that
its finished steps are undone.

Every order is an instance of its own that runs in steps: reserve the money,
place the order, then confirm the fill and book the order side by side. Each
finished step is written down before the next one starts, and a step that
fails for good undoes the ones before it, last one first.
[Workflows](../workflows.html) explains steps, retries and undo,
and [Relay settings](../relay_settings.html)
the `run` setting that turns steps on.

<!-- tabs-open -->

### Java

```java
// OrderExecutorRelay.java: the workflow: one instance is one order
public final class OrderExecutorRelay implements Relay {
    public record OrderTicket(String symbol, int quantity) {}

    @Override
    @Nonnull
    public String factoryId() {
        return "order_executor";
    }

    @Override
    public Run run() {
        return new Run.Steps(
                Each.ONCE,                               // one instance per order; each step runs once
                OnFailure.UNDO_ALL);                     // a failed step undoes the finished ones
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(OrderTicket.class, "order")                                // the order to carry out
                .into(ReserveFunds.class)                                  // step one
                .into(PlaceOrder.class)                                    // step two, once step one is done
                .branch(
                        fill -> fill.into(ConfirmFill.class),              // two arms, side by side
                        book -> book.into(RecordOrder.class));
    }
}
```

[`OrderExecutorRelay.java`](https://github.com/flezha/flezha_java_sdk/blob/main/examples/tradedesk/src/main/java/com/flezha/examples/tradedesk/OrderExecutorRelay.java)

### Rust

```rust
// src/order_executor.rs: the workflow: one instance is one order
#[derive(Clone, Serialize, Deserialize)]
pub struct OrderTicket {
    pub symbol: String,
    pub quantity: u32,
}

pub struct OrderExecutorRelay;

impl Relay for OrderExecutorRelay {
    fn factory_id() -> &'static str {
        "order_executor"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<OrderTicket>("order") // the order to carry out
            .into::<ReserveFunds>() // step one
            .into::<PlaceOrder>() // step two, once step one is done
            .branch([
                &|fill| {
                    fill.into::<ConfirmFill>(); // two arms, side by side
                },
                &|book| {
                    book.into::<RecordOrder>();
                },
            ]);
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            // one instance per order; each step runs once
            // a failed step undoes the finished ones
            run: Run::Steps { each: Each::Once, on_failure: OnFailure::UndoAll },
            ..RelaySettings::default()
        }
    }
}
```

[`src/order_executor.rs`](https://github.com/flezha/flezha_rust_sdk/blob/main/examples/tradedesk/src/order_executor.rs)

### Node

```javascript
// order_executor_relay.mjs: the workflow: one instance is one order
export default defineRelay({
  factoryId: 'order_executor',
  // one instance per order; each step runs once
  // a failed step undoes the finished ones
  steps: { each: 'once', onFailure: 'undo_all' },
  channels: (c) => {
    c.input('order') // the order to carry out
      .into(ReserveFunds) // step one
      .into(PlaceOrder) // step two, once step one is done
      .branch([
        (fill) => fill.into(ConfirmFill), // two arms, side by side
        (book) => book.into(RecordOrder),
      ]);
  },
});
```

[`order_executor_relay.mjs`](https://github.com/flezha/flezha_node_sdk/blob/main/examples/tradedesk/order_executor_relay.mjs)

### Python

```python
# order_executor.py: the workflow: one instance is one order
# one instance per order; each step runs once
# a failed step undoes the finished ones
@flezha.relay("order_executor", run={"steps": {"each": "once", "on_failure": "undo_all"}})
def order_executor(c: flezha.Channels) -> None:
    (
        c.input("order")  # the order to carry out
        .into(ReserveFunds)  # step one
        .into(PlaceOrder)  # step two, once step one is done
        .branch(
            lambda fill: fill.into(ConfirmFill),  # two arms, side by side
            lambda book: book.into(RecordOrder),
        )
    )
```

[`order_executor.py`](https://github.com/flezha/flezha_python_sdk/blob/main/examples/tradedesk/order_executor.py)

### Ruby

```ruby
# order_executor_relay.rb: the workflow: one instance is one order
class OrderExecutorRelay < Flezha::Relay
  factory_id "order_executor"
  # one instance per order; each step runs once
  # a failed step undoes the finished ones
  run steps: { each: :once, on_failure: :undo_all }

  channels do |c|
    c.input("order")                                         # the order to carry out
     .into(ReserveFunds)                                     # step one
     .into(PlaceOrder)                                       # step two, once step one is done
     .branch(
       ->(fill) { fill.into(ConfirmFill) },                  # two arms, side by side
       ->(book) { book.into(RecordOrder) }
     )
  end
end
```

[`order_executor_relay.rb`](https://github.com/flezha/flezha_ruby_sdk/blob/main/examples/tradedesk/order_executor_relay.rb)

### PHP

```php
// src/OrderExecutorRelay.php: the workflow: one instance is one order
// one instance per order; it stays until confirm_fill completes it
// a failed step undoes the finished ones
#[Relay(factoryId: 'order_executor', run: ['steps' => ['each' => 'per_input', 'on_failure' => 'undo_all']])]
final class OrderExecutorRelay extends \Flezha\Relay
{
    public static function channels(Builder $c): void
    {
        $c->input('order')                                            // the order to carry out
            ->into(ReserveFunds::class)                               // step one
            ->into(PlaceOrder::class)                                 // step two, once step one is done
            ->branch(
                fn (Edge $fill) => $fill->into(ConfirmFill::class),   // two arms, side by side
                fn (Edge $book) => $book->into(RecordOrder::class),
            );
    }
}
```

[`src/OrderExecutorRelay.php`](https://github.com/flezha/flezha_php_sdk/blob/main/examples/tradedesk/src/OrderExecutorRelay.php)

### Elixir

```elixir
# lib/tradedesk/order_executor.ex: the workflow: one instance is one order
defmodule Tradedesk.OrderExecutor do
  use Flezha.SDK.Relay,
    factory_id: "order_executor",
    # one instance per order; each step runs once
    # a failed step undoes the finished ones
    run: {:steps, each: :once, on_failure: :undo_all}

  @impl true
  def channels(c) do
    c
    # the order to carry out
    |> input_with("order", &OrderTicket.decode/1)
    # step one
    |> into(Tradedesk.ReserveFunds)
    # step two, once step one is done
    |> into(Tradedesk.PlaceOrder)
    # two arms, side by side
    |> branch([&into(&1, Tradedesk.ConfirmFill), &into(&1, Tradedesk.RecordOrder)])
  end
end
```

[`lib/tradedesk/order_executor.ex`](https://github.com/flezha/flezha_elixir_sdk/blob/main/examples/tradedesk/lib/tradedesk/order_executor.ex)

<!-- tabs-close -->

> Start an order by hand, with the order as its first message on the channel
> `order`.
> {: .step}

```sh
$ ./bin/flz relay start order_executor --id order_1 --namespace trading --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
```

```output
Started order_1
```

`confirm_fill` waits ten seconds for the exchange. Read the journal after that:

```sh
$ ./bin/flz state journal order_1 --namespace trading
```

```output
task-journal for instance order_1:

  seq=0 task_committed task_idx=0 iter=0 epoch=0
    ran: order_1/reserve_funds/ record=0
    ...
  seq=1 task_committed task_idx=1 iter=0 epoch=0
    ran: order_1/place_order/ record=0
    ...
  seq=2 task_committed task_idx=3 iter=0 epoch=0
    ran: order_1/record_order/
    ...
  seq=3 task_committed task_idx=2 iter=0 epoch=0
    ran: order_1/confirm_fill/
    handoff: 0 item(s), 0 byte(s)
    completes the instance: result 57 byte(s)


  leaves: 2,3  complete: true
```

Each line is a finished step. `confirm_fill` ended the order with its result,
and the instance stopped by itself.

The exchange fills at most 100 at a time, so a bigger order fails at
`confirm_fill`. The demo exchange's `/jump` moves the next trade's price far
enough for the desk to ask for more than 100.

> Type `/jump EUR/USD 12` in the demo exchange's terminal, and watch the
> runner's terminal.
> {: .step}

About half a minute later, after three tries of ten seconds each, the order is
cancelled and the money released:

<!-- tabs-open -->

### Java

```output
[book] EUR/USD 136 as ord-be4f7a593756acada0924d10da8d31c6
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 63 ms): the exchange fills at most 100 a time, and this is 136
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 66 ms): the exchange fills at most 100 a time, and this is 136
[undo] cancelled ord-be4f7a593756acada0924d10da8d31c6
[undo] released res-935e21608cc9ea9844b537d8c0c97517
```

### Rust

Other orders' bookings can land between the tries, because the wait holds
nothing else up:

```output
[book] EUR/USD 117 as ord-9a51049763f9cd2b4f9a067bd231d938
[flezha] warning: tasklet confirm_fill attempt 1/3 failed (retrying in 25 ms): the exchange fills at most 100 a time, and this is 117
[book] EUR/USD 22 as ord-fc28f9f16e7db3667f4ece6adecd28f6
[flezha] warning: tasklet confirm_fill attempt 2/3 failed (retrying in 105 ms): the exchange fills at most 100 a time, and this is 117
[book] GBP/USD 22 as ord-6d201575a91b880ce5fc020f28947f61
[undo] cancelled ord-9a51049763f9cd2b4f9a067bd231d938
[undo] released res-6f8e70c84ccf712e4bdf5306208e40c1
```

### Node

Another order's booking can land between the tries, because the wait holds
nothing else up:

```output
[book] EUR/USD 114 as ord-1583d35b849ecc1e5be0c67eb19126b1
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 41 ms): Error: the exchange fills at most 100 a time, and this is 114
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 94 ms): Error: the exchange fills at most 100 a time, and this is 114
[book] EUR/USD 26 as ord-e2b95dca4b993989b1bf0ae12b069bfd
[undo] cancelled ord-1583d35b849ecc1e5be0c67eb19126b1
[undo] released res-765f48effc6403ca7079785ffc7b8820
```

### Python

```output
[book] EUR/USD 130 as ord-147cb3cb7525c45f615cf36ae92ed0be
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 31 ms): RuntimeError: the exchange fills at most 100 a time, and this is 130
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 120 ms): RuntimeError: the exchange fills at most 100 a time, and this is 130
[undo] cancelled ord-147cb3cb7525c45f615cf36ae92ed0be
[undo] released res-494fc55d64e4cc925b295bd07fa3b41f
```

### Ruby

```output
[book] EUR/USD 124 as ord-a40ba575b8a83599f33dad116255f5c3
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 40 ms): RuntimeError: the exchange fills at most 100 a time, and this is 124
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 62 ms): RuntimeError: the exchange fills at most 100 a time, and this is 124
[undo] cancelled ord-a40ba575b8a83599f33dad116255f5c3
[undo] released res-b159dff9228e6788657db153453c4bda
```

### PHP

`confirm_fill` waits the ten seconds once, on its timer, and retries straight
away, so the undo comes about ten seconds after the booking:

```output
[book] EUR/USD 120 as ord-601b85a308803b3d10634c4833c8b7de
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 29 ms): RuntimeException: the exchange fills at most 100 a time, and this is 120
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 108 ms): RuntimeException: the exchange fills at most 100 a time, and this is 120
[undo] cancelled ord-601b85a308803b3d10634c4833c8b7de
[undo] released res-0a63b28bebbc54f887a0ac0c6891cc3f
```

### Elixir

Other orders' bookings can land between the tries, because the wait holds
nothing else up:

```output
[book] EUR/USD 107 as ord-58e3ac18a7fe8475163037e27b670fd4

18:12:11.936 [warning] tasklet confirm_fill attempt 1/3 failed (retrying in 48 ms): the exchange fills at most 100 a time, and this is 107
[book] USD/JPY 24 as ord-203543a68ad9aeb5541b9ab4518e73e5

18:12:21.983 [warning] tasklet confirm_fill attempt 2/3 failed (retrying in 139 ms): the exchange fills at most 100 a time, and this is 107
[book] USD/JPY 21 as ord-22577d9325a68a0a856ba57977031568
[book] USD/JPY 27 as ord-3b39fc582055f91a642d663827ecb601
[book] USD/JPY 30 as ord-53ae185a39600b8605e1c01546240960
[book] USD/JPY 21 as ord-afab73de2f3f9eeb5631a75052684e43
[book] USD/JPY 26 as ord-ede5ec420ad25a28e43b3ef7c91614dd
[undo] cancelled ord-58e3ac18a7fe8475163037e27b670fd4
[undo] released res-ba40dbe26a826d9b4abbabf1dda4c20d
```

<!-- tabs-close -->

## Next

[Crash the desk](crash_the_desk.md) shows what survives a crash.

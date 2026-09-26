# Building a workflow

On this page we build the desk's workflow. A workflow is a piece of work made
of steps that run in a fixed order, from the first to the last. Here the work
is one order: put the money aside, place the order with the exchange, then
confirm the fill and book the order.

We build it with a bounded operator, because an order has a start and an end.
Once it is filled or refused, there is nothing left to do. A bounded operator is
exactly that: you start it with the work it should do, it runs its steps, and
it stops by itself after the last one. Every order gets one of its own, so a
hundred orders are a hundred small runs that never get in each other's way.

Each step is written down the moment it finishes. If the runner crashes in the
middle of an order, the order carries on from the step it had reached, and the
steps before it do not run again, so the money is not put aside twice. If a
step fails for good, the steps that already ran are undone, last one first: the
order is cancelled, and the money is released. Half an order is worse than
none, and a bounded operator makes sure the desk never keeps one.

## The desk's bounded operator

The desk's bounded operator is called `paper_order`, and one instance of it is
one order. [`building_an_etl_stream.md`](building_an_etl_stream.md) starts one
for every buy, and you can start one by hand.

You write it in six pieces, one section each: the two steps that can be undone,
which reserve the money and place the order; the step that confirms the fill,
which is where an order can fail; the step that books the order; the operator,
which connects the four into the path every order takes; and the runner, which
now registers both factories. Running it comes later, in [`durability.md`](durability.md).

## A step that can be undone

`reserve_funds` takes the order, an `OrderTicket`: the symbol, and how many to
buy. It puts the money for the order aside, under a reservation id, and hands
the order on with that id. If the order fails further on, the money
has to be released again, and that is its undo: a method of its own,
`compensate`, which Flezha calls when the order ends in failure.

The undo needs the reservation's id, so the step keeps it in its state.
`compensate` is also handed what the step emitted, but that is not enough:
after a crash, a step gets its state back and not what it emitted, so the list
can be empty. The state always comes back.

> Write the step that reserves the money, and releases it on an undo. It goes
> into the same runner project as the ETL stream; the finished project is
> linked at the top of your language's tab.
> {: .step}

<!-- tabs-open -->

### Java

The whole example:
[`sdk/java/examples/tradedesk`](https://github.com/flezha/flezha_java_sdk/tree/main/examples/tradedesk).

```java
// ReserveFunds.java: step one. Puts the money aside, and releases it on an undo.
@Tasklet(id = "reserve_funds", emits = ReserveFunds.Reserved.class)
public final class ReserveFunds {

    public record Reserved(OrderTicket order, String reservationId) {}  // what it hands on

    @State
    public static class Reservation {                        // what this step remembers
        public String id = "";
    }

    // The step: reserve the money under an id, remember the id, and hand the order on.
    public void run(OrderTicket order, ReservationHandle reservation, TaskletCtx ctx) throws TaskletException {
        String id = "res-" + ctx.stableId();                 // the same id when the step runs again
        reservation.setId(id);
        ctx.emit(new Reserved(order, id));
    }

    // The undo: release the money, by the id the state kept.
    public void compensate(List<Reserved> completedOutput, ReservationHandle reservation) {
        System.out.println("[undo] released " + reservation.get().id);
    }
}
```

**`compensate` sits beside `run`, and writing it is what gives the step an
undo.** It takes the list of what the step emitted, then the same state
parameter `run` takes. A step without one is skipped when an order is undone.
`@State` and `@Tasklet` generate the rest, as in the ETL stream:
`ReservationHandle`, whose `setId` records the id and `get()` reads it back, and
`ReserveFundsTasklet`, the class the operator names.
[What the Java annotations generate](building_an_etl_stream.md#keep-each-symbol-s-prices)
lists it all.

`run` takes the context, because `ctx.stableId()` gives the same id when the
step runs again, so a retry does not put a second sum aside. A `run` that takes
the context emits through it and returns nothing, so `emits` names what it
hands on.

`OrderTicket` is declared with the operator, in
[Several tasklets, step by step](#several-tasklets-step-by-step), because it is
what the whole operator takes. `compensate` may itself be called twice, when a
crash comes in the middle of an undo, so it has to be safe to repeat.

### Rust

TODO: not written yet.

### Node

TODO: not written yet.

### Python

TODO: not written yet.

### Ruby

TODO: not written yet.

### PHP

TODO: not written yet.

### Elixir

TODO: not written yet.

<!-- tabs-close -->

## Place the order

`place_order` has the same shape. It places the order with the paper exchange,
keeps the order's id in its state, and its undo cancels the order by that id.

> Write the step that places the order, and cancels it on an undo.
> {: .step}

<!-- tabs-open -->

### Java

```java
// PlaceOrder.java: step two. Places the order, and cancels it on an undo.
@Tasklet(id = "place_order", emits = PlaceOrder.PlacedOrder.class)
public final class PlaceOrder {

    public record PlacedOrder(OrderTicket order, String orderId) {}  // what it hands on

    @State
    public static class Placement {                          // what this step remembers
        public String orderId = "";
    }

    // The step: place the order under an id, remember the id, and hand the order on.
    public void run(Reserved reserved, PlacementHandle placement, TaskletCtx ctx) throws TaskletException {
        String orderId = "ord-" + ctx.stableId();
        placement.setOrderId(orderId);
        ctx.emit(new PlacedOrder(reserved.order(), orderId));
    }

    // The undo: cancel the order, by the id the state kept.
    public void compensate(List<PlacedOrder> completedOutput, PlacementHandle placement) {
        System.out.println("[undo] cancelled " + placement.get().orderId);
    }
}
```

### Rust

TODO: not written yet.

### Node

TODO: not written yet.

### Python

TODO: not written yet.

### Ruby

TODO: not written yet.

### PHP

TODO: not written yet.

### Elixir

TODO: not written yet.

<!-- tabs-close -->

## A step that can fail

`confirm_fill` asks the paper exchange whether the order filled. The exchange
fills at most 100 at a time, so a bigger order is refused, and this is the step
where an order can fail.

A step that fails is tried again, three times in all by default, with a short
wait between tries that grows each time. When the tries run out, the order ends
in failure, and every step that had finished is undone, last one first: the
order is cancelled, then the money is released.

When the fill is confirmed, the step ends the order with a result instead of
emitting one: `ctx.complete` gives the order its answer, the symbol, the
quantity and the exchange's id for the order.

> Write the step that confirms the fill.
> {: .step}

<!-- tabs-open -->

### Java

```java
// ConfirmFill.java: asks the exchange whether the order filled, and ends the order with the answer.
@Tasklet(id = "confirm_fill")
public final class ConfirmFill {

    private static final int EXCHANGE_MAX = 100;

    public void run(PlacedOrder placed, TaskletCtx ctx) throws TaskletException {
        int quantity = placed.order().quantity();
        if (quantity > EXCHANGE_MAX) {
            throw new TaskletException(                      // tried again, then the order is undone
                    "the exchange fills at most " + EXCHANGE_MAX + " a time, and this is " + quantity);
        }
        ctx.complete("filled " + placed.order().symbol() + " " + quantity + " as " + placed.orderId());
    }
}
```

A `TaskletException` is retried. The number of tries and the wait are set
where the operator connects the step, with `retryMax` and `retryBackoffMs`;
the desk keeps the defaults.

### Rust

TODO: not written yet.

### Node

TODO: not written yet.

### Python

TODO: not written yet.

### Ruby

TODO: not written yet.

### PHP

TODO: not written yet.

### Elixir

TODO: not written yet.

<!-- tabs-close -->

## A step with nothing to undo

`record_order` writes the placed order into the desk's book. It keeps nothing
and hands nothing on. It has no undo either, on purpose: the book records what
the desk tried, and an order that was cancelled again was still tried. When an
order is undone, a step with no `compensate` is skipped.

> Write the step that books the order.
> {: .step}

<!-- tabs-open -->

### Java

```java
// RecordOrder.java: writes the placed order into the desk's book.
@Tasklet(id = "record_order")
public final class RecordOrder {

    public void run(PlacedOrder placed) {
        System.out.println(
                "[book] " + placed.order().symbol() + " " + placed.order().quantity() + " as " + placed.orderId());
    }
}
```

A `void` method with no context declares no output, which is the plainest
shape a tasklet has.

### Rust

TODO: not written yet.

### Node

TODO: not written yet.

### Python

TODO: not written yet.

### Ruby

TODO: not written yet.

### PHP

TODO: not written yet.

### Elixir

TODO: not written yet.

<!-- tabs-close -->

## Several tasklets, step by step

With the four steps written, the operator connects them. It declares the
steps an order takes, in one expression that reads in the order they run:

```mermaid
flowchart LR
    I(["order"]) --> R["reserve_funds"] --> P["place_order"]
    P --> C["confirm_fill"]
    P --> B["record_order"]
```

The order enters on the channel `order`, as the `OrderTicket` that
`reserve_funds` takes:

```json
{"symbol": "EUR/USD", "quantity": 10}
```

`into` adds a step, as it adds a tasklet to an ETL stream, but a step does not
see a value the moment the step before it emits one. It sees it once that step
has finished and the controller has written that down. So when an order stops
half way, it is known exactly which steps are done.

`branch` splits the path in two. Both arms get the same placed order, and they
run side by side: confirming the fill does not wait for the book, nor the book
for the fill.

There is no `sink`. `confirm_fill` ends the order with a result of its own,
which is the order's answer. None of the channels between the steps has a
name either, so each is named after the step that feeds it, and that is the
name you see in the journal and in an error.

An instance runs its steps once, on the first message it gets, and then it is
over; a second message to it is never read. That is the default, and all an
order needs.

> Write the operator.
> {: .step}

<!-- tabs-open -->

### Java

```java
// PaperOrderOperator.java: the bounded operator. One instance is one order.
public final class PaperOrderOperator implements BoundedOperator {

    public record OrderTicket(String symbol, int quantity) {} // the order an instance is started with

    @Override
    public String factoryId() {
        return "paper_order";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(OrderTicket.class, "order")
                .into(ReserveFundsTasklet.class)                            // step one
                .into(PlaceOrderTasklet.class)                              // step two, once step one is done
                .branch(
                        fill -> fill.into(ConfirmFillTasklet.class),        // two arms, side by side
                        book -> book.into(RecordOrderTasklet.class));
    }
}
```

`OrderTicket` is the order an instance is started with: `reserve_funds` takes
it, and `BuyAboveAverage` in the ETL stream sends it when it starts an order.
The four classes it names in `into` are the ones `@Tasklet` generated from
the four steps: `ReserveFundsTasklet`, `PlaceOrderTasklet`,
`ConfirmFillTasklet` and `RecordOrderTasklet`.

### Rust

TODO: not written yet.

### Node

TODO: not written yet.

### Python

TODO: not written yet.

### Ruby

TODO: not written yet.

### PHP

TODO: not written yet.

### Elixir

TODO: not written yet.

<!-- tabs-close -->

## The runner

One runner registers both of the desk's factories. Registering `paper_order`
starts no order: it tells the controller this runner can run orders, and each
one is started from the factory later, one per buy. A bounded operator's factory
is registered the same way as an unbounded one's.

> Register the `paper_order` factory on the runner too, before `start()`.
> {: .step}

<!-- tabs-open -->

### Java

```java
// Main.java: the runner. Joins trading and registers both factories.
public static void main(String[] args) throws Exception {
    FlezhaRunner runner = Flezha.createRunner(args.length > 0 ? args[0] : "trading")
            .registerFactory(PricesOperator.class)
            .registerFactory(PaperOrderOperator.class)
            .start();

    runner.awaitTermination();
}
```

### Rust

TODO: not written yet.

### Node

TODO: not written yet.

### Python

TODO: not written yet.

### Ruby

TODO: not written yet.

### PHP

TODO: not written yet.

### Elixir

TODO: not written yet.

<!-- tabs-close -->

## Next

The desk has both of its operators, and neither has run yet.
[`durability.md`](durability.md) runs them, crashes the runner in the middle
of an order, and watches the order carry on from its step.

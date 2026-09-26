# Building a workflow

A workflow is built as a bounded operator: every run of it has a start and an
end. It is started with the input it works on, runs its tasklets as steps, and
ends on its own once the last step is done. Each step is written down the
moment it finishes, and the step after it starts only then. When a step fails
for good, what the steps before it did is undone, last one first. It is the
kind to use for work that has to finish, and where half done is worse than not
done at all: placing an order, taking a payment, a nightly import, a booking.

## The desk's bounded operator

The desk's bounded operator is called `paper_order`, and one instance of it is
one order. [`building_an_etl_stream.md`](building_an_etl_stream.md) starts one
for every buy, and you can start one by hand. An order puts the money aside,
places the order with the paper exchange, and then does two things at once:
it confirms the fill, and it books the order.

You write it in six pieces, one section each: the operator, which declares the
steps in their order; the two steps that can be undone, which reserve the money
and place the order; the step that confirms the fill, which is where an order
can fail; the step that books the order; and the runner, which now registers both
factories. Running it comes later, in [`durability.md`](durability.md).

## Several tasklets, step by step

The operator declares the steps an order takes, in one expression that reads
in the order they run:

```mermaid
flowchart LR
    I(["order"]) --> R["reserve_funds"] --> P["place_order"]
    P --> C["confirm_fill"]
    P --> B["record_order"]
```

The order enters on the channel `order`, as an `OrderTicket`: the symbol, and
how many to buy.

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

> Write the operator, in the same runner project as the ETL stream; the
> finished project is linked at the top of your language's tab.
> {: .step}

<!-- tabs-open -->

### Java

The whole example:
[`sdk/java/examples/tradedesk`](https://github.com/flezha/flezha_java_sdk/tree/main/examples/tradedesk).

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
                .into(ReserveFunds.class)                                   // step one
                .into(PlaceOrder.class)                                     // step two, once step one is done
                .branch(
                        fill -> fill.into(ConfirmFillTasklet.class),        // two arms, side by side
                        book -> book.into(RecordOrderTasklet.class));
    }
}
```

`OrderTicket` lives inside the operator, and `BuyAboveAverage` in the ETL
stream encodes the same record when it starts an order. `ReserveFunds` and
`PlaceOrder` are wired by their own names, because they implement `Tasklet`
themselves, as the next section shows. `ConfirmFillTasklet` and
`RecordOrderTasklet` are the classes `@Tasklet` generates from the two steps
further down.

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

## A step that can be undone

`reserve_funds` puts the money for the order aside, under a reservation id,
and hands the order on with that id. If the order fails further on, the money
has to be released again, and that is its undo: a method of its own,
`compensate`, which Flezha calls when the order ends in failure.

The undo needs the reservation's id, so the step keeps it in its state.
`compensate` is also handed what the step emitted, but that is not enough:
after a crash, a step gets its state back and not what it emitted, so the list
can be empty. The state always comes back.

> Write the step that reserves the money, and releases it on an undo.
> {: .step}

<!-- tabs-open -->

### Java

```java
// ReserveFunds.java: step one. Puts the money aside, and releases it on an undo.
public final class ReserveFunds
        implements Tasklet<OrderTicket, ReserveFunds.Reservation, ReserveFunds.Reserved, ReserveFunds.Taken, WallClock, Void> {

    public static final class Reservation {                  // what this step remembers
        public String id = "";
    }

    public record Taken(String id) implements Change {}      // the one change it records

    public record Reserved(OrderTicket order, String reservationId) {}

    @Override
    public String name() {
        return "reserve_funds";
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(Reservation.class, Taken.class);
    }

    @Override
    public Reservation initState(Optional<String> cellKey, Optional<RestoreInitPayload> initPayload) {
        return new Reservation();
    }

    @Override
    public void applyChange(Reservation state, Taken change) {
        state.id = change.id();
    }

    @Override
    public void run(OrderTicket order, StateHandle<Reservation, Taken> state, TaskletCtx ctx)
            throws TaskletException {
        String reservationId = "res-" + ctx.stableId();      // the same id when the step runs again
        state.apply(new Taken(reservationId));
        ctx.emit(new Reserved(order, reservationId));
    }

    @Override
    public void compensate(List<Reserved> completedOutput, StateHandle<Reservation, Taken> state, TaskletCtx ctx)
            throws TaskletException {
        System.out.println("[undo] released " + state.get().id);
    }
}
```

**A step with an undo implements `Tasklet` itself.** `@Tasklet` cannot declare
`compensate`, so this class writes out what `@Tasklet` generates for the other
steps. The six type arguments are the input, the state, the output, the change
it records, the clock and the configuration. The state is a plain class, and
`Taken` is the one change that records it: `state.apply` writes the change
down, and `applyChange` is how it lands in the state, the first time and again
after a crash. `kryoTypes` lists the two classes, so the journal can write them.

`ctx.stableId()` gives the same id when the step runs again, so a retry does
not put a second sum aside. `compensate` may itself be called twice, when a
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
public final class PlaceOrder
        implements Tasklet<Reserved, PlaceOrder.Placement, PlaceOrder.PlacedOrder, PlaceOrder.Placed, WallClock, Void> {

    public static final class Placement {                    // what this step remembers
        public String orderId = "";
    }

    public record Placed(String orderId) implements Change {} // the one change it records

    public record PlacedOrder(OrderTicket order, String orderId) {}

    @Override
    public String name() {
        return "place_order";
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(Placement.class, Placed.class);
    }

    @Override
    public Placement initState(Optional<String> cellKey, Optional<RestoreInitPayload> initPayload) {
        return new Placement();
    }

    @Override
    public void applyChange(Placement state, Placed change) {
        state.orderId = change.orderId();
    }

    @Override
    public void run(Reserved reserved, StateHandle<Placement, Placed> state, TaskletCtx ctx)
            throws TaskletException {
        String orderId = "ord-" + ctx.stableId();
        state.apply(new Placed(orderId));
        ctx.emit(new PlacedOrder(reserved.order(), orderId));
    }

    @Override
    public void compensate(List<PlacedOrder> completedOutput, StateHandle<Placement, Placed> state, TaskletCtx ctx)
            throws TaskletException {
        System.out.println("[undo] cancelled " + state.get().orderId);
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

A `TaskletException` is retried. The number of tries and the wait are set on
the edge into the step, with `retryMax` and `retryBackoffMs`; the desk keeps
the defaults.

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

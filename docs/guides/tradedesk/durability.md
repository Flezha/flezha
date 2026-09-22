# Durability

Durability is what keeps the desk's work when something stops: a runner
crashes, a machine is switched off, the controller is restarted. Flezha writes
down every change a tasklet makes to what it remembers, and every step a
workflow finishes, on the controller's disk, as it happens. When a runner comes
back, or another one takes over, the work carries on from where it was instead
of starting again. Nothing in your code saves or loads anything: you say what a
tasklet remembers, and Flezha keeps it.

## What the desk will remember

A trade worth 10 000 is big for one symbol and small for another, so one number
for every symbol is a poor rule. On this page `spot_big_trade` compares each
trade with the same symbol's last ten trades instead: a trade is big when it is
worth at least twice their average. That needs a memory, one per symbol.

Then the page crashes the runner, twice: once to see the stream's memory come
back, and once in the middle of an order, to see the workflow continue from
its step. Last, an order that fails is undone, which is where a workflow step
needs a memory of its own.

## State: what a cell remembers

What a tasklet remembers is its state, and state belongs to a cell.
[`stream_basics.md`](stream_basics.md#several-tasklets-in-one-operative) gave
every symbol a cell of its own with `keyBy`, so every symbol now keeps its own
last ten values: AAPL's never mix with MSFT's.

Every change to that state is recorded the moment it is made, and the
controller keeps the record, in the namespace's own folder under
`app_data/state/trading/`. That record is what brings the state back after a
crash.

> Give `spot_big_trade` a memory: each symbol's last ten trade values, and the
> new rule that compares a trade with their average. A symbol's first trade has
> nothing to be compared with, so it is never big.
> {: .step}

<!-- tabs-open -->

### Java

```java
// SpotBigTrade.java: passes on a trade worth at least twice its symbol's recent average.
@Tasklet(id = "spot_big_trade")
public final class SpotBigTrade {

    private static final int WINDOW = 10;

    @State
    public static class Recent {                             // what each symbol's cell remembers
        public Deque<Double> values = new ArrayDeque<>();
    }

    public Optional<BigTrade> run(Trade trade, RecentHandle recent) {
        Deque<Double> values = recent.get().values;
        double value = trade.price() * trade.quantity();
        boolean big = !values.isEmpty() && value >= 2 * average(values);

        recent.pushBackValues(value);
        if (values.size() > WINDOW) {
            recent.popFrontValues(1);
        }
        return big ? Optional.of(new BigTrade(trade.symbol(), trade.price(), value)) : Optional.empty();
    }

    private static double average(Deque<Double> values) {
        return values.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
    }
}
```

**`@State` turns a class into the cell's state.** It sits inside the tasklet
that uses it. From it the annotation processor generates `RecentHandle`, the
parameter `run` takes, with one method per way a field can change:
`pushBackValues` and `popFrontValues` each change the list and record just the
value they add or drop, not the whole list. `recent.get()` reads it.

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

> Run the new version: stop the stream, stop the runner with Ctrl-C, start the
> runner again as in
> [`stream_basics.md`](stream_basics.md#the-runner), and start the stream
> again.
> {: .step}

```sh
$ flz operative stop prices --namespace trading
```

```sh
$ flz operative start prices --id prices --namespace trading
```

## Watch it remember

> Send AAPL four trades by hand. The first three are worth about 1 000 each,
> and the fourth, at 3 090, is more than twice their average.
> {: .step}

```sh
$ flz operative send prices --namespace trading --channel manual --text 'AAPL 100 10'
$ flz operative send prices --namespace trading --channel manual --text 'AAPL 101 12'
$ flz operative send prices --namespace trading --channel manual --text 'AAPL 102 11'
$ flz operative send prices --namespace trading --channel manual --text 'AAPL 103 30'
```

The listener from [`stream_basics.md`](stream_basics.md#send-it-trades) shows
one big trade, the fourth, and its decision to buy:

```output
TODO: output from an end-to-end run.
```

`flz state get` reads a live cell's state, by
`<operative>/<tasklet>/<key>`. AAPL's cell holds the four values it has seen:

```sh
$ flz state get prices/spot_big_trade/AAPL
```

```output
TODO: output from an end-to-end run.
```

## Crash the runner

A runner can stop in two ways, and they differ in what reaches the controller.

- **Ctrl-C, or a SIGTERM from whatever runs it.** The runner hands its last
  state to the controller before it exits, so nothing is lost.
- **A crash**, such as `kill -9`, a power cut or the machine dying. There is no
  goodbye. The controller has every change the runner had already sent it,
  which can miss the last few.

> Crash the runner with `kill -9`. The controller sees it gone within a few
> seconds, and `prices` waits for a runner.
> {: .step}

```sh
$ pkill -9 -f tradedesk.jar
```

> Start the runner again, with the same command as before, and read AAPL's
> cell once more: its last values are back.
> {: .step}

```sh
$ flz state get prices/spot_big_trade/AAPL
```

```output
TODO: output from an end-to-end run.
```

The next trade is compared with what the cell remembers, not with an empty
list:

```sh
$ flz operative send prices --namespace trading --channel manual --text 'AAPL 104 40'
```

```output
TODO: output from an end-to-end run.
```

## An order in the middle of a crash

A workflow keeps a journal: every step that finishes is written down, with what
it handed to the next step. When the runner crashes in the middle of an order,
the order continues at the first step that had not finished. The steps before
it do not run again, so the money is not reserved twice and the order is not
placed twice. That needs nothing in your code.

To crash the runner in the middle of an order, the order has to take a while.
The paper exchange now takes ten seconds to confirm a fill.

> Make `confirm_fill` wait ten seconds for the exchange before it answers.
> {: .step}

<!-- tabs-open -->

### Java

```java
// ConfirmFill.java: asks the exchange whether the order filled.
@Tasklet(id = "confirm_fill")
public final class ConfirmFill {

    private static final int EXCHANGE_MAX = 100;

    public void run(PlacedOrder placed, TaskletCtx ctx) throws TaskletException {
        try {
            Thread.sleep(10_000);                            // the paper exchange takes its time
        } catch (InterruptedException stopped) {
            throw new TaskletException("stopped while waiting for the exchange");
        }
        int quantity = placed.order().quantity();
        if (quantity > EXCHANGE_MAX) {
            throw new TaskletException(
                    "the exchange fills at most " + EXCHANGE_MAX + " a time, and this is " + quantity);
        }
        ctx.complete("filled " + placed.order().symbol() + " " + quantity + " as " + placed.orderId());
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

> Restart the runner with the new version, start an order by hand, and crash
> the runner within ten seconds. Then start the runner again.
> {: .step}

```sh
$ flz operative start paper_order --id order_1 --namespace trading --text '{"symbol": "AAPL", "quantity": 10}' --channel order
```

```sh
$ pkill -9 -f tradedesk.jar
```

Once the runner is back, the order finishes. `flz state journal` shows what the
journal holds for it: the two steps that had finished before the crash, and the
one that ran after it.

```sh
$ flz state journal order_1
```

```output
TODO: output from an end-to-end run.
```

## Undo an order

When a step fails, it is tried again, and when its tries run out, the order
ends in failure. Flezha then undoes the steps that had finished, in reverse:
the order is cancelled, then the money is released. Each step says how it is
undone in a method of its own, `compensate`. A step with nothing to undo has
none and is skipped.

The undo needs to know what to undo: the reservation's id, the order's id. In
an ordinary failure, `compensate` is handed what the step emitted, and the id
is in there. After a crash that list is empty: the step gets its state back,
not what it emitted. So a step that can be undone keeps its id in its state.

> Give `reserve_funds` and `place_order` an undo: each keeps the id it created
> in its state, and `compensate` reads it from there.
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
        String reservationId = "res-" + ctx.stableId();
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

`PlaceOrder` has the same shape, keeping the order's id and cancelling it in
its `compensate`.

**A step with an undo implements `Tasklet` itself.** `@Tasklet` cannot declare
`compensate`, so this class writes out what `@Tasklet` generates for the other
steps. The six type arguments are the input, the state, the output, the change
it records, the clock and the configuration. `ctx.stableId()` gives the same id
when a step runs again, so a retry does not put a second sum aside.

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

> Restart the runner with the new version, and start an order the exchange
> refuses: it fills at most 100.
> {: .step}

```sh
$ flz operative start paper_order --id order_2 --namespace trading --text '{"symbol": "AAPL", "quantity": 500}' --channel order
```

After ten seconds `confirm_fill` refuses it, is tried again, and once its tries
run out the runner's terminal shows the undo, in reverse:

```output
TODO: output from an end-to-end run.
```

## What the controller keeps

The runner is not the only thing that can stop. The controller keeps its own
records on disk: the namespaces, the tokens and the keys under
`app_data/auth/`, and each namespace's state under `app_data/state/<namespace>/`,
apart from every other's. A controller restart brings back `trading` and its
limits before any runner reconnects, then its operatives, which wait for a
runner to take them. The runner's key still works.

What this page does not cover is delivery. After a crash, a result can reach a
sink twice. Making every result arrive exactly once is Part 3's
[`effectively_once.md`](../advanced/effectively_once.md).

## Next

The desk remembers, survives a crash and undoes a failed order.
[`plugins.md`](plugins.md) connects it to the exchange, so the trades arrive on
their own.

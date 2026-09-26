# Durability

Durability is what keeps the desk's work when something stops: a runner
crashes, a machine is switched off, the controller is restarted. Flezha writes
down every change a tasklet makes to what it remembers, and every step a
bounded operator finishes, on the controller's disk, as it happens. When a runner comes
back, or another one takes over, the work carries on from where it was instead
of starting again. Nothing in your code saves or loads anything: you say what a
tasklet remembers, and Flezha keeps it.

## Where it is kept

The controller keeps everything it saves on its own disk, in the folder it was
started in: each namespace's state under `app_data/state/<namespace>/`, apart
from every other's, and the namespaces, tokens and keys under `app_data/auth/`.
That includes the write-ahead log, the running record of every change, which is
always on the local disk, whatever else is configured.

The local disk is enough as long as it survives: a crashed runner or a restarted
controller gets everything back from it. A controller that loses its disk loses
what was on it. For that case the controller can also send a copy of everything
it saves to storage that lives somewhere else, chosen with
`FLEZHA_DURABILITY_BACKEND`:

| Backend | Where the copy goes |
|---|---|
| `none` | nowhere: the local disk is the only copy. This is the default |
| `postgres` | a table in a PostgreSQL database |
| `s3` | a bucket on Amazon S3 or any S3-compatible store |
| `local_path` | a folder that outlives the controller's own disk, such as a network mount |

The two copies are kept on different schedules:

- **The controller's disk** gets every change within a few milliseconds, always,
  whichever backend is set.
- **The extra copy** follows in the background: a workflow's after every
  finished step, a stream's at least every 30 seconds
  (`FLEZHA_DURABILITY_SHIP_INTERVAL_MS`). The 30 seconds applies to this copy
  only, and only matters if the controller loses its disk.

A controller that starts on an empty disk fetches the copy and carries on from
it. Part 2 keeps the default, the local disk, and
[`configuration.md`](../../configuration.md#state-wal-and-durability) lists the
settings for each backend.

> #### Fast in memory, saved in the background {: .info}
>
> A cell's state lives in the memory of the runner that runs it, so a tasklet
> reads and changes it at memory speed, with no disk in the way. Every change is
> also sent to the controller, which adds it to the write-ahead log, and neither
> step holds the tasklet up: the runner sends its changes in small batches about
> every 5 milliseconds, and the controller writes its log to disk (an `fsync`)
> in batches, also about every 5 milliseconds, rather than once per change. The
> copy to Postgres or S3, when one is set, is sent in the background as well.
>
> The cost is a small window. A runner that crashes, rather than stopping
> cleanly, can lose its last few milliseconds of changes. A clean stop, with
> Ctrl-C or a SIGTERM, hands everything to the controller first and loses
> nothing.
>
> A workflow is the exception, on purpose: a bounded operator waits until each
> finished step is on the controller's disk before the next step starts, so a
> finished step is kept even when the runner crashes.

## What the desk will remember

A trade worth 10 000 is big for one symbol and small for another, so one number
for every symbol is a poor rule. On this page `spot_big_trade` compares each
trade with the same symbol's last ten trades instead: a trade is big when it is
worth at least twice their average. That needs a memory, one per symbol.

Then the page crashes the runner, twice: once to see the operator's memory come
back, and once in the middle of an order, to see the bounded operator continue from
its step. Last, an order that fails is undone, which is where a bounded operator step
needs a memory of its own.

## State: what a cell remembers

What a tasklet remembers is its state, and state belongs to a cell.
[`building_an_etl_stream.md`](building_an_etl_stream.md#several-tasklets-in-one-operator) gave
every symbol a cell of its own with `keyBy`, so every symbol now keeps its own
last ten values: EUR/USD's never mix with USD/JPY's.

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

> Run the new version: stop the operator, stop the runner with Ctrl-C, start the
> runner again as in
> [`building_an_etl_stream.md`](building_an_etl_stream.md#the-runner), and start the operator
> again.
> {: .step}

```sh
$ flz operator stop prices --namespace trading
```

```sh
$ flz operator start prices --id prices --namespace trading
```

## Watch it remember

> Send EUR/USD four trades by hand. The first three are worth about 1 000 each,
> and the fourth, at 3 090, is more than twice their average.
> {: .step}

```sh
$ flz operator send prices --namespace trading --channel manual --text 'EUR/USD 100 10'
$ flz operator send prices --namespace trading --channel manual --text 'EUR/USD 101 12'
$ flz operator send prices --namespace trading --channel manual --text 'EUR/USD 102 11'
$ flz operator send prices --namespace trading --channel manual --text 'EUR/USD 103 30'
```

A listener, as in Part 1's
[Or watch it from another terminal](../running_operators.md#or-watch-it-from-another-terminal), shows
one big trade, the fourth, and its decision to buy:

```output
TODO: output from an end-to-end run.
```

`flz state get` reads a live cell's state, by
`<operator>/<tasklet>/<key>`. EUR/USD's cell holds the four values it has seen:

```sh
$ flz state get prices/spot_big_trade/EUR/USD
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

> Start the runner again, with the same command as before, and read EUR/USD's
> cell once more: its last values are back.
> {: .step}

```sh
$ flz state get prices/spot_big_trade/EUR/USD
```

```output
TODO: output from an end-to-end run.
```

The next trade is compared with what the cell remembers, not with an empty
list:

```sh
$ flz operator send prices --namespace trading --channel manual --text 'EUR/USD 104 40'
```

```output
TODO: output from an end-to-end run.
```

## An order in the middle of a crash

A bounded operator keeps a journal: every step that finishes is written down, with what
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
$ flz operator start paper_order --id order_1 --namespace trading --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
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
@Tasklet(id = "reserve_funds", emits = ReserveFunds.Reserved.class)
public final class ReserveFunds {

    public record Reserved(OrderTicket order, String reservationId) {}

    @State
    public static class Reservation {                        // what this step remembers
        public String id = "";
    }

    public void run(OrderTicket order, ReservationHandle reservation, TaskletCtx ctx) throws TaskletException {
        String id = "res-" + ctx.stableId();
        reservation.setId(id);
        ctx.emit(new Reserved(order, id));
    }

    public void compensate(List<Reserved> completedOutput, ReservationHandle reservation) {
        System.out.println("[undo] released " + reservation.get().id);
    }
}
```

`PlaceOrder` has the same shape, keeping the order's id and cancelling it in
its `compensate`.

**`compensate` sits beside `run`,** and takes what the step emitted and the
same state parameter `run` takes. `ctx.stableId()` gives the same id when a
step runs again, so a retry does not put a second sum aside.

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
$ flz operator start paper_order --id order_2 --namespace trading --text '{"symbol": "EUR/USD", "quantity": 500}' --channel order
```

After ten seconds `confirm_fill` refuses it, is tried again, and once its tries
run out the runner's terminal shows the undo, in reverse:

```output
TODO: output from an end-to-end run.
```

## The dead-letter queue

TODO: not written yet. A message the desk keeps failing on is tried again, then
set aside in the dead-letter queue instead of blocking the ones behind it. This
section shows one being parked, for example a GBP/USD trade sent to a partition
that does not own it, then finds it with `flz dlq list <partition>`, reads it
with `flz dlq show`, and replays or drops it with `flz dlq replay` and
`flz dlq drop`.

## What the controller keeps

The runner is not the only thing that can stop. The controller reads back
everything it keeps, as [Where it is kept](#where-it-is-kept) describes, when it
restarts: it brings back `trading` and its limits before any runner reconnects, then its operators, which wait for a
runner to take them. The runner's key still works.

What this page does not cover is delivery. After a crash, a result can reach a
sink twice. Making every result arrive exactly once is Part 3's
[`effectively_once.md`](../advanced/effectively_once.md).

## Next

That is the end of Part 2: the desk remembers what it needs, and survives a
crash of its runner or its controller. [Part 3](../guides.md#part-3-advanced-topics)
goes deeper.

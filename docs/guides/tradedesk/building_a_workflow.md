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

The desk's bounded operator is called `order_executor`, and one instance of it is
one order. [`building_an_etl_stream.md`](building_an_etl_stream.md) starts one
for every buy, and you can start one by hand.

You write it in six pieces, one section each: the two steps that can be undone,
which reserve the money and place the order; the step that confirms the fill,
which is where an order can fail; the step that books the order; the operator,
which connects the four into the path every order takes; and the runner, which
now registers both factories. Running it comes later, in [`partitioning.md`](partitioning.md).

## A step that can be undone

`reserve_funds` is the first step. It puts the money for the order aside and
gives that reservation an id. Then it hands the order on to the next step,
together with the id.

If the order fails later, the money has to be released again. That is the
step's undo. To release the money, the undo has to know which reservation to
release, so the step writes the id down in its state, its memory, where the
undo can find it.

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
@Tasklet.Spec(name = "reserve_funds")
public final class ReserveFunds implements Tasklet<OrderTicket, ReserveFunds.Reservation, ReserveFunds.Reserved> {

    public record Reserved(OrderTicket order, String reservationId) {}  // what it hands on

    public static class Reservation {                        // what this step remembers
        public String id = "";
    }

    public record Reserving(String id) implements Change<Reservation> {  // how the id is written down
        @Override
        public void applyTo(Reservation reservation) {
            reservation.id = id;
        }
    }

    // The step: reserve the money under an id, remember the id, and hand the order on.
    @Override
    public void run(OrderTicket order, Ctx<Reservation, Reserved> ctx) throws TaskletException {
        String id = "res-" + ctx.stableId();                 // the same id when the step runs again
        ctx.change(new Reserving(id));
        ctx.emit(new Reserved(order, id));
    }

    // The undo: release the money, by the id the state kept.
    @Override
    public void compensate(List<Reserved> completedOutput, Ctx<Reservation, Reserved> ctx) {
        System.out.println("[undo] released " + ctx.state().id);
    }
}
```

**Flezha calls `run` with the order**, and passes it two things: the input and
the context, as it did for the ETL stream's tasklets.

- `order` is the input, what arrives on the step's channel. For the first step,
  that is the order the instance was started with, an `OrderTicket`.
- `ctx` is the context, the step's way of talking to Flezha. `ctx.change(...)`
  writes the id down in the step's state, the `Reservation` class above, and
  `ctx.state()` reads it back. `ctx.emit(...)` hands a value to the next step,
  and `ctx.stableId()` gives an id that stays the same when the step runs
  again, so a retry does not put the money aside twice.

`Tasklet<OrderTicket, Reservation, Reserved>` names what comes in, what the step
remembers, and what it hands on: a `Reserved`, the order and the id.
`Reserving` is the change that writes the id down, applied by its `applyTo`.

**Flezha calls `compensate` only when the order fails**, and passes it two
things:

- `completedOutput` is what `run` handed on, a list of `Reserved`.
- `ctx` is the context again, and `ctx.state()` is the same state `run` wrote
  the id into.

The id is read from the state, not from `completedOutput`, because after a
crash Flezha brings back the step's state but not what it handed on, so the
list can be empty. Writing a `compensate` is what gives a step an undo, and a
step without one is skipped. It can run twice, when a crash comes in the middle
of an undo, so it has to be safe to repeat.

The operator names the class itself, `ReserveFunds`.
[What `@Tasklet.Spec` declares](building_an_etl_stream.md#keep-each-symbol-s-prices)
lists what the annotation carries. `OrderTicket` is declared with the operator, in
[Several tasklets, step by step](#several-tasklets-step-by-step), because it is
what the whole operator takes.

### Rust

The whole example:
[`sdk/rust/examples/tradedesk`](https://github.com/flezha/flezha_rust_sdk/tree/main/examples/tradedesk).

```rust
// src/reserve_funds.rs: step one. Puts the money aside, and releases it on an undo.
#[derive(Clone, Serialize, Deserialize)]
pub struct Reserved {
    pub order: OrderTicket,
    pub reservation_id: String,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Reservation {
    id: String,
}

#[derive(Serialize, Deserialize)]
pub struct Reserving(String);

impl Change<Reservation> for Reserving {
    fn apply_to(&self, reservation: &mut Reservation) {
        reservation.id = self.0.clone();
    }
}

#[derive(Default)]
pub struct ReserveFunds;

#[tasklet(name = "reserve_funds")]
impl Tasklet for ReserveFunds {
    type Input = OrderTicket;
    type Output = Reserved;
    type State = Reservation;
    type Change = Reserving;

    // The step: reserve the money under an id, remember the id, and hand the order on.
    fn run(&mut self, order: OrderTicket, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let id = format!("res-{}", ctx.stable_id()); // the same id when the step runs again
        ctx.change(Reserving(id.clone()));
        ctx.emit(Reserved {
            order,
            reservation_id: id,
        })
    }

    // The undo: release the money, by the id the state kept.
    fn compensate(
        &mut self,
        _: Vec<Arc<Reserved>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        println!("[undo] released {}", ctx.state().id);
        Ok(())
    }
}
```

**Flezha calls `run` with the order**, and passes it two things: the input and
the context, as it did for the ETL stream's tasklets.

- `order` is the input, what arrives on the step's channel. For the first step,
  that is the order the instance was started with, an `OrderTicket`.
- `ctx` is the context, the step's way of talking to Flezha. `ctx.change(...)`
  writes the id down in the step's state, the `Reservation` struct above, and
  `ctx.state()` reads it back. `ctx.emit(...)` hands a value to the next step,
  and `ctx.stable_id()` gives an id that stays the same when the step runs
  again, so a retry does not put the money aside twice.

The step takes an `OrderTicket`, remembers a `Reservation`, and hands on a
`Reserved`, the order and the id. `Reserving` is the change that writes the id
down, applied by its `apply_to`.

**Flezha calls `compensate` only when the order fails**, and passes it two
things:

- the first is what `run` handed on, a `Vec` of `Reserved`.
- `ctx` is the context again, and `ctx.state()` is the same state `run` wrote
  the id into.

The id is read from the state, not from what was handed on, because after a
crash Flezha brings back the step's state but not what it handed on, so the
list can be empty. Writing a `compensate` is what gives a step an undo, and a
step without one is skipped. It can run twice, when a crash comes in the middle
of an undo, so it has to be safe to repeat.

The operator names the type itself, `ReserveFunds`.
[What `#[tasklet]` writes](building_an_etl_stream.md#keep-each-symbol-s-prices)
lists what the attribute carries. `OrderTicket` is declared with the operator, in
[Several tasklets, step by step](#several-tasklets-step-by-step), because it is
what the whole operator takes.

### Node

The whole example:
[`sdk/node/examples/tradedesk`](https://github.com/flezha/flezha_node_sdk/tree/main/examples/tradedesk).

```javascript
// reserve_funds.mjs: step one. Puts the money aside, and releases it on an undo.
import { defineTasklet } from '@flezha/sdk';

export const ReserveFunds = defineTasklet({
  name: 'reserve_funds',

  // What this step remembers, and how the id is written down.
  initState: () => ({ id: '' }),
  applyChange: (state, change) => {
    state.id = change.id;
  },

  // The step: reserve the money under an id, remember the id, and hand the order on.
  run: (order, ctx) => {
    const id = `res-${ctx.stableId()}`; // the same id when the step runs again
    ctx.change({ id });
    ctx.emit({ order, reservationId: id });
  },

  // The undo: release the money, by the id the state kept.
  compensate: (_outputs, ctx) => {
    console.log(`[undo] released ${ctx.state.id}`);
  },
});
```

**Flezha calls `run` with the order**, and passes it two things: the input and
the context, as it did for the ETL stream's tasklets.

- `order` is the input, what arrives on the step's channel. For the first
  step, that is the order the instance was started with, the object
  `{ symbol, quantity }` its JSON decodes to.
- `ctx` is the context, the step's way of talking to Flezha.
  `ctx.change(...)` writes the id down in the step's state, and `ctx.state`
  reads it back. `ctx.emit(...)` hands a value to the next step, and
  `ctx.stableId()` gives an id that stays the same when the step runs again, so
  a retry does not put the money aside twice.

The step starts its state as `{ id: '' }`, records the change `{ id }`, folded
by `applyChange`, and hands on the order and the id.

**Flezha calls `compensate` only when the order fails**, and passes it two
things:

- `_outputs` is what `run` handed on, a list.
- `ctx` is the context again, and `ctx.state` is the same state `run` wrote the
  id into.

The id is read from the state, not from what was handed on, because after a
crash Flezha brings back the step's state but not what it handed on, so the
list can be empty. Writing a `compensate` is what gives a step an undo, and a
step without one is skipped. It can run twice, when a crash comes in the middle
of an undo, so it has to be safe to repeat.

The operator names what `defineTasklet` returned, `ReserveFunds`.
[What a `defineTasklet` definition carries](building_an_etl_stream.md#keep-each-symbol-s-prices)
lists its fields.

### Python

The whole example:
[`sdk/python/examples/tradedesk`](https://github.com/flezha/flezha_python_sdk/tree/main/examples/tradedesk).

```python
# reserve_funds.py: step one. Puts the money aside, and releases it on an undo.
class OrderTicket(TypedDict):  # the order an instance is started with
    symbol: str
    quantity: int


class Reserved(TypedDict):  # what it hands on
    order: OrderTicket
    reservation_id: str


@flezha.state
@dataclass
class Reservation:  # what this step remembers
    id: str = ""


class Reserving(TypedDict):  # how the id is written down
    id: str


@flezha.tasklet(name="reserve_funds")
class ReserveFunds:
    # The step: reserve the money under an id, remember the id, and hand the order on.
    def run(self, order: OrderTicket, ctx: flezha.Ctx[Reservation, Reserved]) -> None:
        reservation_id = f"res-{ctx.stable_id()}"  # the same id when the step runs again
        ctx.change(Reserving(id=reservation_id))
        ctx.emit(Reserved(order=order, reservation_id=reservation_id))

    def apply_change(self, reservation: Reservation, change: Reserving) -> None:
        reservation.id = change["id"]

    # The undo: release the money, by the id the state kept.
    def compensate(self, outputs: list[Reserved], ctx: flezha.Ctx[Reservation, Reserved]) -> None:
        print(f"[undo] released {ctx.state.id}", flush=True)
```

**Flezha calls `run` with the order**, and passes it two things: the input and
the context, as it did for the ETL stream's tasklets.

- `order` is the input, what arrives on the step's channel. For the first
  step, that is the order the instance was started with, an `OrderTicket`: the
  dict `{"symbol": ..., "quantity": ...}` its JSON decodes to.
- `ctx` is the context, the step's way of talking to Flezha.
  `ctx.change(...)` writes the id down in the step's state, and `ctx.state`
  reads it back. `ctx.emit(...)` hands a value to the next step, and
  `ctx.stable_id()` gives an id that stays the same when the step runs again,
  so a retry does not put the money aside twice.

`Ctx[Reservation, Reserved]` names both: the step starts its state as
`Reservation()`, records the change `Reserving`, folded by `apply_change`, and
hands on a `Reserved`, the order and the id.

**Flezha calls `compensate` only when the order fails**, and passes it two
things:

- `outputs` is what `run` handed on, a list.
- `ctx` is the context again, and `ctx.state` is the same state `run` wrote the
  id into.

The id is read from the state, not from what was handed on, because after a
crash Flezha brings back the step's state but not what it handed on, so the
list can be empty. Writing a `compensate` is what gives a step an undo, and a
step without one is skipped. It can run twice, when a crash comes in the middle
of an undo, so it has to be safe to repeat.

The operator names the class itself, `ReserveFunds`.
[What `@flezha.tasklet` declares](building_an_etl_stream.md#keep-each-symbol-s-prices)
lists what the decorator takes.

### Ruby

The whole example:
[`sdk/ruby/examples/tradedesk`](https://github.com/flezha/flezha_ruby_sdk/tree/main/examples/tradedesk).

```ruby
# reserve_funds.rb: step one. Puts the money aside, and releases it on an undo.
class ReserveFunds < Flezha::Tasklet
  tasklet_name "reserve_funds"

  # What this step remembers, and how the id is written down.
  def init_state(_cell_key) = { id: "" }
  def apply_change(state, change) = state[:id] = change[:id]

  # The step: reserve the money under an id, remember the id, and hand the order on.
  def run(order, ctx)
    id = "res-#{ctx.stable_id}" # the same id when the step runs again
    ctx.change({ id: id })
    ctx.emit({ order: order, reservation_id: id })
  end

  # The undo: release the money, by the id the state kept.
  def compensate(_outputs, ctx)
    puts "[undo] released #{ctx.state[:id]}"
  end
end
```

**Flezha calls `run` with the order**, and passes it two things: the input and
the context, as it did for the ETL stream's tasklets.

- `order` is the input, what arrives on the step's channel. For the first
  step, that is the order the instance was started with, the Hash
  `{ symbol: ..., quantity: ... }` its JSON decodes to.
- `ctx` is the context, the step's way of talking to Flezha.
  `ctx.change(...)` writes the id down in the step's state, and `ctx.state`
  reads it back. `ctx.emit(...)` hands a value to the next step, and
  `ctx.stable_id` gives an id that stays the same when the step runs again, so
  a retry does not put the money aside twice.

The step starts its state as `{ id: "" }`, records the change `{ id: ... }`,
folded by `apply_change`, and hands on the order and the id.

**Flezha calls `compensate` only when the order fails**, and passes it two
things:

- `_outputs` is what `run` handed on, a list.
- `ctx` is the context again, and `ctx.state` is the same state `run` wrote the
  id into.

The id is read from the state, not from what was handed on, because after a
crash Flezha brings back the step's state but not what it handed on, so the
list can be empty. Writing a `compensate` is what gives a step an undo, and a
step without one is skipped. It can run twice, when a crash comes in the middle
of an undo, so it has to be safe to repeat.

The operator names the class itself, `ReserveFunds`.
[What a `Flezha::Tasklet` class body declares](building_an_etl_stream.md#keep-each-symbol-s-prices)
lists what the class body carries.

### PHP

The whole example:
[`sdk/php/examples/tradedesk`](https://github.com/flezha/flezha_php_sdk/tree/main/examples/tradedesk).

```php
// src/ReserveFunds.php: step one. Puts the money aside, and releases it on an undo.
#[Task(name: 'reserve_funds')]
final class ReserveFunds extends Tasklet
{
    public function initState(?string $cellKey): mixed
    {
        return ['id' => ''];                                 // what this step remembers
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['id'] = $change['id'];                        // how the id is written down
    }

    // The step: reserve the money under an id, remember the id, and hand the order on.
    public function run(mixed $input, Ctx $ctx): void
    {
        $id = 'res-' . $ctx->stableId();                     // the same id when the step runs again
        $ctx->change(['id' => $id]);
        $ctx->emit(['order' => $input, 'reservation_id' => $id]);
    }

    // The undo: release the money, by the id the state kept.
    public function compensate(array $outputs, Ctx $ctx): void
    {
        echo "[undo] released {$ctx->state['id']}\n";
    }
}
```

**Flezha calls `run` with the order**, and passes it two things: the input and
the context, as it did for the ETL stream's tasklets.

- `$input` is the input, what arrives on the step's channel. For the first
  step, that is the order the instance was started with, the array
  `['symbol' => ..., 'quantity' => ...]`.
- `$ctx` is the context, the step's way of talking to Flezha.
  `$ctx->change(...)` writes the id down in the step's state, and
  `$ctx->state` reads it back. `$ctx->emit(...)` hands a value to the next
  step, and `$ctx->stableId()` gives an id that stays the same when the step
  runs again, so a retry does not put the money aside twice.

The step starts its state as `['id' => '']`, records the change `['id' => ...]`,
folded by `applyChange`, and hands on the order and the id.

**Flezha calls `compensate` only when the order fails**, and passes it two
things:

- `$outputs` is what `run` handed on, a list.
- `$ctx` is the context again, and `$ctx->state` is the same state `run` wrote
  the id into.

The id is read from the state, not from what was handed on, because after a
crash Flezha brings back the step's state but not what it handed on, so the
list can be empty. Writing a `compensate` is what gives a step an undo, and a
step without one is skipped. It can run twice, when a crash comes in the middle
of an undo, so it has to be safe to repeat.

The operator names the class itself, `ReserveFunds`.
[What `#[Task]` declares](building_an_etl_stream.md#keep-each-symbol-s-prices)
lists what the attribute carries.

### Elixir

The whole example:
[`sdk/elixir/examples/tradedesk`](https://github.com/flezha/flezha_elixir_sdk/tree/main/examples/tradedesk).

```elixir
# lib/tradedesk/reserve_funds.ex: step one. Puts the money aside, and releases it on an undo.
defmodule Tradedesk.ReserveFunds do
  use Flezha.SDK.Tasklet, name: "reserve_funds"

  # What this step remembers.
  state do
    field(:id, :string, default: "")
  end

  # How the id is written down.
  defchange reserving(reservation, id) do
    %{reservation | id: id}
  end

  # The step: reserve the money under an id, remember the id, and hand the
  # order on.
  @impl true
  def run(order, reservation, ctx) do
    # The same id when the step runs again.
    id = "res-" <> stable_id(ctx)
    emit(ctx, %{order: order, reservation_id: id})
    {:ok, reserving(reservation, ctx, id)}
  end

  # The undo: release the money, by the id the state kept.
  @impl true
  def compensate(_outputs, reservation, _ctx) do
    IO.puts("[undo] released #{reservation.id}")
    {:ok, reservation}
  end
end
```

**Flezha calls `run/3` with the order**, and passes it three things: the
input, the state and the context, as it did for the ETL stream's tasklets.

- `order` is the input, what arrives on the step's channel. For the first
  step, that is the order the instance was started with, the `OrderTicket`
  struct the operator decodes its JSON into.
- `reservation` is the step's state, the map its `state` block starts as
  `%{id: ""}`.
- `ctx` is the context, the step's way of talking to Flezha. `emit(ctx, ...)`
  hands a value to the next step, and `stable_id(ctx)` gives an id that stays
  the same when the step runs again, so a retry does not put the money aside
  twice.

`reserving(reservation, ctx, id)` is the recorder `defchange` generated: it
writes the change `{:reserving, id}` down and answers the state with the id
in it, which `run/3` returns.

**Flezha calls `compensate/3` only when the order fails**, and passes it three
things:

- `_outputs` is what `run/3` handed on, a list.
- `reservation` is the same state `run/3` wrote the id into.
- `_ctx` is the context again.

The id is read from the state, not from what was handed on, because after a
crash Flezha brings back the step's state but not what it handed on, so the
list can be empty. Writing `compensate/3` is what gives a step an undo, and a
step without one is skipped. It can run twice, when a crash comes in the middle
of an undo, so it has to be safe to repeat.

The operator names the module, `Tradedesk.ReserveFunds`.
[What `use Flezha.SDK.Tasklet` declares](building_an_etl_stream.md#keep-each-symbol-s-prices)
lists its options.

<!-- tabs-close -->

## Place the order

`place_order` has the same shape. Its `run` gets what `reserve_funds` handed on,
a `Reserved`, places the order with the paper exchange under an id, writes the
id into its state, and hands the order on. Its `compensate` cancels the order
by that id.

> Write the step that places the order, and cancels it on an undo.
> {: .step}

<!-- tabs-open -->

### Java

```java
// PlaceOrder.java: step two. Places the order, and cancels it on an undo.
@Tasklet.Spec(name = "place_order")
public final class PlaceOrder implements Tasklet<Reserved, PlaceOrder.Placement, PlaceOrder.PlacedOrder> {

    public record PlacedOrder(OrderTicket order, String orderId) {}  // what it hands on

    public static class Placement {                          // what this step remembers
        public String orderId = "";
    }

    public record Placing(String orderId) implements Change<Placement> {  // how the id is written down
        @Override
        public void applyTo(Placement placement) {
            placement.orderId = orderId;
        }
    }

    // The step: place the order under an id, remember the id, and hand the order on.
    @Override
    public void run(Reserved reserved, Ctx<Placement, PlacedOrder> ctx) throws TaskletException {
        String orderId = "ord-" + ctx.stableId();
        ctx.change(new Placing(orderId));
        ctx.emit(new PlacedOrder(reserved.order(), orderId));
    }

    // The undo: cancel the order, by the id the state kept.
    @Override
    public void compensate(List<PlacedOrder> completedOutput, Ctx<Placement, PlacedOrder> ctx) {
        System.out.println("[undo] cancelled " + ctx.state().orderId);
    }
}
```

### Rust

```rust
// src/place_order.rs: step two. Places the order, and cancels it on an undo.
#[derive(Clone, Serialize, Deserialize)]
pub struct PlacedOrder {
    pub order: OrderTicket,
    pub order_id: String,
}

#[derive(Default, Serialize, Deserialize)]
pub struct Placement {
    order_id: String,
}

#[derive(Serialize, Deserialize)]
pub struct Placing(String);

impl Change<Placement> for Placing {
    fn apply_to(&self, placement: &mut Placement) {
        placement.order_id = self.0.clone();
    }
}

#[derive(Default)]
pub struct PlaceOrder;

#[tasklet(name = "place_order")]
impl Tasklet for PlaceOrder {
    type Input = Reserved;
    type Output = PlacedOrder;
    type State = Placement;
    type Change = Placing;

    // The step: place the order under an id, remember the id, and hand the order on.
    fn run(&mut self, reserved: Reserved, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let order_id = format!("ord-{}", ctx.stable_id());
        ctx.change(Placing(order_id.clone()));
        ctx.emit(PlacedOrder {
            order: reserved.order,
            order_id,
        })
    }

    // The undo: cancel the order, by the id the state kept.
    fn compensate(
        &mut self,
        _: Vec<Arc<PlacedOrder>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        println!("[undo] cancelled {}", ctx.state().order_id);
        Ok(())
    }
}
```

### Node

```javascript
// place_order.mjs: step two. Places the order, and cancels it on an undo.
import { defineTasklet } from '@flezha/sdk';

export const PlaceOrder = defineTasklet({
  name: 'place_order',

  // What this step remembers, and how the id is written down.
  initState: () => ({ orderId: '' }),
  applyChange: (state, change) => {
    state.orderId = change.orderId;
  },

  // The step: place the order under an id, remember the id, and hand the order on.
  run: (reserved, ctx) => {
    const orderId = `ord-${ctx.stableId()}`;
    ctx.change({ orderId });
    ctx.emit({ order: reserved.order, orderId });
  },

  // The undo: cancel the order, by the id the state kept.
  compensate: (_outputs, ctx) => {
    console.log(`[undo] cancelled ${ctx.state.orderId}`);
  },
});
```

### Python

```python
# place_order.py: step two. Places the order, and cancels it on an undo.
class PlacedOrder(TypedDict):  # what it hands on
    order: OrderTicket
    order_id: str


@flezha.state
@dataclass
class Placement:  # what this step remembers
    order_id: str = ""


class Placing(TypedDict):  # how the id is written down
    order_id: str


@flezha.tasklet(name="place_order")
class PlaceOrder:
    # The step: place the order under an id, remember the id, and hand the order on.
    def run(self, reserved: Reserved, ctx: flezha.Ctx[Placement, PlacedOrder]) -> None:
        order_id = f"ord-{ctx.stable_id()}"
        ctx.change(Placing(order_id=order_id))
        ctx.emit(PlacedOrder(order=reserved["order"], order_id=order_id))

    def apply_change(self, placement: Placement, change: Placing) -> None:
        placement.order_id = change["order_id"]

    # The undo: cancel the order, by the id the state kept.
    def compensate(
        self, outputs: list[PlacedOrder], ctx: flezha.Ctx[Placement, PlacedOrder]
    ) -> None:
        print(f"[undo] cancelled {ctx.state.order_id}", flush=True)
```

### Ruby

```ruby
# place_order.rb: step two. Places the order, and cancels it on an undo.
class PlaceOrder < Flezha::Tasklet
  tasklet_name "place_order"

  # What this step remembers, and how the id is written down.
  def init_state(_cell_key) = { order_id: "" }
  def apply_change(state, change) = state[:order_id] = change[:order_id]

  # The step: place the order under an id, remember the id, and hand the order on.
  def run(reserved, ctx)
    order_id = "ord-#{ctx.stable_id}"
    ctx.change({ order_id: order_id })
    ctx.emit({ order: reserved[:order], order_id: order_id })
  end

  # The undo: cancel the order, by the id the state kept.
  def compensate(_outputs, ctx)
    puts "[undo] cancelled #{ctx.state[:order_id]}"
  end
end
```

### PHP

```php
// src/PlaceOrder.php: step two. Places the order, and cancels it on an undo.
#[Task(name: 'place_order')]
final class PlaceOrder extends Tasklet
{
    public function initState(?string $cellKey): mixed
    {
        return ['order_id' => ''];                           // what this step remembers
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['order_id'] = $change['order_id'];            // how the id is written down
    }

    // The step: place the order under an id, remember the id, and hand the order on.
    public function run(mixed $input, Ctx $ctx): void
    {
        $orderId = 'ord-' . $ctx->stableId();
        $ctx->change(['order_id' => $orderId]);
        $ctx->emit(['order' => $input['order'], 'order_id' => $orderId]);
    }

    // The undo: cancel the order, by the id the state kept.
    public function compensate(array $outputs, Ctx $ctx): void
    {
        echo "[undo] cancelled {$ctx->state['order_id']}\n";
    }
}
```

### Elixir

```elixir
# lib/tradedesk/place_order.ex: step two. Places the order, and cancels it on an undo.
defmodule Tradedesk.PlaceOrder do
  use Flezha.SDK.Tasklet, name: "place_order"

  # What this step remembers.
  state do
    field(:order_id, :string, default: "")
  end

  # How the id is written down.
  defchange placing(placement, order_id) do
    %{placement | order_id: order_id}
  end

  # The step: place the order under an id, remember the id, and hand the order
  # on.
  @impl true
  def run(%{order: order}, placement, ctx) do
    order_id = "ord-" <> stable_id(ctx)
    emit(ctx, %{order: order, order_id: order_id})
    {:ok, placing(placement, ctx, order_id)}
  end

  # The undo: cancel the order, by the id the state kept.
  @impl true
  def compensate(_outputs, placement, _ctx) do
    IO.puts("[undo] cancelled #{placement.order_id}")
    {:ok, placement}
  end
end
```

<!-- tabs-close -->

## A step that can fail

`confirm_fill` asks the paper exchange whether the order filled. The exchange
takes ten seconds to answer, and it fills at most 100 at a time, so a bigger
order is refused: this is the step where an order can fail. The ten seconds are what let
[`durability.md`](durability.md#an-order-in-the-middle-of-a-crash) crash the
runner in the middle of an order.

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
@Tasklet.Spec(name = "confirm_fill")
public final class ConfirmFill implements Tasklet<PlacedOrder, Void, Void> {

    private static final int EXCHANGE_MAX = 100;

    @Override
    public void run(PlacedOrder placed, Ctx<Void, Void> ctx) throws TaskletException {
        try {
            Thread.sleep(10_000);                            // the paper exchange takes ten seconds to answer
        } catch (InterruptedException stopped) {
            Thread.currentThread().interrupt();
            throw new TaskletException("stopped while waiting for the exchange");
        }
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

```rust
// src/confirm_fill.rs: asks the exchange whether the order filled, and ends the order with the answer.
const EXCHANGE_MAX: u32 = 100;

#[derive(Default)]
pub struct ConfirmFill;

#[tasklet(name = "confirm_fill")]
impl Tasklet for ConfirmFill {
    type Input = PlacedOrder;

    fn run(&mut self, placed: PlacedOrder, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        std::thread::sleep(Duration::from_secs(10)); // the paper exchange takes ten seconds to answer
        let quantity = placed.order.quantity;
        if quantity > EXCHANGE_MAX {
            return Err(TaskletError::Custom(format!(
                "the exchange fills at most {EXCHANGE_MAX} a time, and this is {quantity}"
            ))); // tried again, then the order is undone
        }
        ctx.complete(format!(
            "filled {} {quantity} as {}",
            placed.order.symbol, placed.order_id
        ))
    }
}
```

An `Err` from `run` is retried. The number of tries and the wait are set where
the operator connects the step, with `retry_max` and `retry_backoff_ms`; the
desk keeps the defaults.

### Node

```javascript
// confirm_fill.mjs: asks the exchange whether the order filled, and ends the order with the answer.
import { setTimeout as sleep } from 'node:timers/promises';

import { defineTasklet } from '@flezha/sdk';

const EXCHANGE_MAX = 100;

export const ConfirmFill = defineTasklet({
  name: 'confirm_fill',

  run: async (placed, ctx) => {
    await sleep(10_000); // the paper exchange takes ten seconds to answer
    const { symbol, quantity } = placed.order;
    if (quantity > EXCHANGE_MAX) {
      // tried again, then the order is undone
      throw new Error(`the exchange fills at most ${EXCHANGE_MAX} a time, and this is ${quantity}`);
    }
    ctx.complete(`filled ${symbol} ${quantity} as ${placed.orderId}`);
  },
});
```

The wait is awaited, so it holds nothing: `run` returns a promise, the event
loop serves every other cell while it is pending, and the price stream keeps
flowing while an order waits. A blocking wait, such as a busy loop or
`Atomics.wait`, would hold the one loop and stall every cell on the runner for
the ten seconds.

An error thrown from `run`, or a promise it returns that rejects, is retried.
The number of tries and the wait are set where the operator connects the step,
with `retryMax` and `retryBackoffMs`; the desk keeps the defaults. An error
carrying `nonRetryable: true` is not retried.

### Python

```python
# confirm_fill.py: asks the exchange whether the order filled, and ends the order with the answer.
EXCHANGE_MAX = 100


@flezha.tasklet(name="confirm_fill")
class ConfirmFill:
    def run(self, placed: PlacedOrder, ctx: flezha.Ctx[None, None]) -> None:
        time.sleep(10)  # the paper exchange takes ten seconds to answer
        quantity = placed["order"]["quantity"]
        if quantity > EXCHANGE_MAX:  # tried again, then the order is undone
            raise RuntimeError(
                f"the exchange fills at most {EXCHANGE_MAX} a time, and this is {quantity}"
            )
        ctx.complete(f"filled {placed['order']['symbol']} {quantity} as {placed['order_id']}")
```

The wait holds one worker of the default thread executor; under the asyncio
executor it would hold up the one loop. When every worker has been busy for
100 ms the runner adds one, up to `FLEZHA_WORKER_POOL_MAX` (ten times the
processor count by default), so the price stream keeps flowing while an order
waits; more orders waiting at once than that would stall it.

An exception raised from `run` is retried. The number of tries and the wait
are set where the operator connects the step, with `retry_max` and
`retry_backoff_ms`; the desk keeps the defaults. `NonRetryableError` is the one
that is not retried.

### Ruby

```ruby
# confirm_fill.rb: asks the exchange whether the order filled, and ends the order with the answer.
class ConfirmFill < Flezha::Tasklet
  tasklet_name "confirm_fill"

  EXCHANGE_MAX = 100

  def run(placed, ctx)
    sleep 10                                                 # the paper exchange takes ten seconds to answer
    quantity = placed[:order][:quantity]
    if quantity > EXCHANGE_MAX                               # tried again, then the order is undone
      raise "the exchange fills at most #{EXCHANGE_MAX} a time, and this is #{quantity}"
    end

    ctx.complete("filled #{placed[:order][:symbol]} #{quantity} as #{placed[:order_id]}")
  end
end
```

Under the default thread executor the wait holds one worker, and `sleep` lets
the other workers run meanwhile. When every worker has been busy for 100 ms the
runner adds one, up to `FLEZHA_WORKER_POOL_MAX` (ten times the processor count
by default), so the price stream keeps flowing while an order waits; more
orders waiting at once than that would stall it. Under the fiber executor
(`FLEZHA_RUBY_EXECUTOR=fibers`, with the `async` gem) `sleep` yields its fiber
instead, so the wait stalls nothing and that cap does not apply.

An exception raised from `run` is retried. The number of tries and the wait
are set where the operator connects the step, with `retry_max` and
`retry_backoff_ms`; the desk keeps the defaults. `Flezha::NonRetryableError` is
the one that is not retried.

### PHP

```php
// src/ConfirmFill.php: asks the exchange whether the order filled, and ends the order with the answer.
#[Task(name: 'confirm_fill')]
final class ConfirmFill extends Tasklet
{
    private const EXCHANGE_MAX = 100;

    public function initState(?string $cellKey): mixed
    {
        return ['placed' => null];                           // the order the exchange is asked about
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['placed'] = $change;
    }

    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->change($input);
        $ctx->timerAfter(10_000);                            // the paper exchange answers ten seconds later
    }

    public function onTimer(Timer $timer, Ctx $ctx): void
    {
        $placed = $ctx->state['placed'];
        $quantity = $placed['order']['quantity'];
        if ($quantity > self::EXCHANGE_MAX) {
            throw new \RuntimeException(                     // tried again, then the order is undone
                'the exchange fills at most ' . self::EXCHANGE_MAX . " a time, and this is $quantity",
            );
        }
        $ctx->complete("filled {$placed['order']['symbol']} $quantity as {$placed['order_id']}");
    }
}
```

A PHP runner serves every operator on one thread, so a ten-second `sleep()` here
would hold up the price stream as well. The step asks for a timer instead:
`run` keeps the placed order in the cell and calls `$ctx->timerAfter(10_000)`,
and ten seconds later Flezha calls `onTimer`, which reads the exchange's answer.
The timer is saved with the cell, so a runner that crashes while it waits fires
it once it is back. A timer outlives `run` only while the instance does, which
is why `order_executor` below keeps its instance running until this step calls
`$ctx->complete`.

An exception thrown from `onTimer` is retried as one from `run` is, straight
away rather than ten seconds apart. The number of tries and the wait are set
where the operator connects the step, with `retryMax` and `retryBackoffMs`; the
desk keeps the defaults.

### Elixir

```elixir
# lib/tradedesk/confirm_fill.ex: asks the exchange whether the order filled, and ends the order with the answer.
defmodule Tradedesk.ConfirmFill do
  use Flezha.SDK.Tasklet, name: "confirm_fill"

  @exchange_max 100

  @impl true
  def run(%{order: order, order_id: order_id}, state, ctx) do
    # The paper exchange takes ten seconds to answer. Each cell is a process of
    # its own, so only this order waits; every other cell keeps running.
    Process.sleep(10_000)

    if order.quantity > @exchange_max do
      # Tried again, then the order is undone.
      {:error,
       "the exchange fills at most #{@exchange_max} a time, and this is #{order.quantity}"}
    else
      complete(ctx, "filled #{order.symbol} #{order.quantity} as #{order_id}")
      {:ok, state}
    end
  end
end
```

A cell is a BEAM process of its own, so `Process.sleep/1` here holds only
this order: the price stream and every other order keep running while it
waits. What it does hold is the rest of this order. Until `run/3` returns, the
order's driver waits for the cell, so `record_order` runs only after the fill
is confirmed, and after the last try of a refused one.

An `{:error, reason}` answer, or anything `run/3` raises, is retried. The
number of tries and the wait are set where the operator connects the step,
with `retry_max/2` and `retry_backoff_ms/2`; the desk keeps the defaults. An
`{:error, {:non_retryable, reason}}` answer is not retried.

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
@Tasklet.Spec(name = "record_order")
public final class RecordOrder implements Tasklet<PlacedOrder, Void, Void> {

    @Override
    public void run(PlacedOrder placed, Ctx<Void, Void> ctx) {
        System.out.println(
                "[book] " + placed.order().symbol() + " " + placed.order().quantity() + " as " + placed.orderId());
    }
}
```

A `Void` state and a `Void` output keep nothing and hand nothing on, which is
the plainest shape a tasklet has.

### Rust

```rust
// src/record_order.rs: writes the placed order into the desk's book.
#[derive(Default)]
pub struct RecordOrder;

#[tasklet(name = "record_order")]
impl Tasklet for RecordOrder {
    type Input = PlacedOrder;

    fn run(&mut self, placed: PlacedOrder, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        println!(
            "[book] {} {} as {}",
            placed.order.symbol, placed.order.quantity, placed.order_id
        );
        Ok(())
    }
}
```

With no `type State` and no `type Output` written, it keeps nothing and hands
nothing on, which is the plainest shape a tasklet has.

### Node

```javascript
// record_order.mjs: writes the placed order into the desk's book.
import { defineTasklet } from '@flezha/sdk';

export const RecordOrder = defineTasklet({
  name: 'record_order',

  run: (placed) => {
    const { symbol, quantity } = placed.order;
    console.log(`[book] ${symbol} ${quantity} as ${placed.orderId}`);
  },
});
```

With no `initState` written and no `ctx.emit`, it keeps nothing and hands
nothing on, which is the plainest shape a tasklet has.

### Python

```python
# record_order.py: writes the placed order into the desk's book.
@flezha.tasklet(name="record_order")
class RecordOrder:
    def run(self, placed: PlacedOrder, ctx: flezha.Ctx[None, None]) -> None:
        order = placed["order"]
        print(f"[book] {order['symbol']} {order['quantity']} as {placed['order_id']}", flush=True)
```

`Ctx[None, None]` names no state and no output, so it keeps nothing and hands
nothing on, which is the plainest shape a tasklet has.

### Ruby

```ruby
# record_order.rb: writes the placed order into the desk's book.
class RecordOrder < Flezha::Tasklet
  tasklet_name "record_order"

  def run(placed, _ctx)
    order = placed[:order]
    puts "[book] #{order[:symbol]} #{order[:quantity]} as #{placed[:order_id]}"
  end
end
```

With no `init_state` written and no `ctx.emit`, it keeps nothing and hands
nothing on, which is the plainest shape a tasklet has.

### PHP

```php
// src/RecordOrder.php: writes the placed order into the desk's book.
#[Task(name: 'record_order')]
final class RecordOrder extends Tasklet
{
    public function run(mixed $input, Ctx $ctx): void
    {
        echo "[book] {$input['order']['symbol']} {$input['order']['quantity']} as {$input['order_id']}\n";
    }
}
```

With no `initState` written and no `$ctx->emit`, it keeps nothing and hands
nothing on, which is the plainest shape a tasklet has.

### Elixir

```elixir
# lib/tradedesk/record_order.ex: writes the placed order into the desk's book.
defmodule Tradedesk.RecordOrder do
  use Flezha.SDK.Tasklet, name: "record_order"

  @impl true
  def run(%{order: order, order_id: order_id}, state, _ctx) do
    IO.puts("[book] #{order.symbol} #{order.quantity} as #{order_id}")
    {:ok, state}
  end
end
```

With no `state` block and no `emit/2`, it keeps nothing and hands nothing on,
which is the plainest shape a tasklet has. `IO.puts/1` writes to the runner's
terminal.

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
// OrderExecutorOperator.java: the bounded operator. One instance is one order.
public final class OrderExecutorOperator implements BoundedOperator {

    public record OrderTicket(String symbol, int quantity) {}

    @Override
    @Nonnull
    public String factoryId() {
        return "order_executor";
    }

    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(OrderTicket.class, "order")
                .into(ReserveFunds.class)                                  // step one
                .into(PlaceOrder.class)                                    // step two, once step one is done
                .branch(
                        fill -> fill.into(ConfirmFill.class),              // two arms, side by side
                        book -> book.into(RecordOrder.class));
    }
}
```

`OrderTicket` is the order an instance is started with: `reserve_funds` takes
it, and `BuyAboveAverage` in the ETL stream sends it when it starts an order.
The four classes it names in `into` are the four steps themselves:
`ReserveFunds`, `PlaceOrder`, `ConfirmFill` and `RecordOrder`.

### Rust

```rust
// src/order_executor.rs: the bounded operator. One instance is one order.
#[derive(Clone, Serialize, Deserialize)]
pub struct OrderTicket {
    pub symbol: String,
    pub quantity: u32,
}

pub struct OrderExecutorOperator;

impl BoundedOperator for OrderExecutorOperator {
    fn factory_id() -> &'static str {
        "order_executor"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<OrderTicket>("order")
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
}
```

`OrderTicket` is the order an instance is started with: `reserve_funds` takes
it, and `BuyAboveAverage` in the ETL stream sends it when it starts an order.
The four types it names in `into::<...>()` are the four steps themselves:
`ReserveFunds`, `PlaceOrder`, `ConfirmFill` and `RecordOrder`. Each arm of
`branch` is a closure that gets the same handle.

### Node

```javascript
// order_executor_operator.mjs: the bounded operator. One instance is one order.
import { defineBounded } from '@flezha/sdk';

import { ConfirmFill } from './confirm_fill.mjs';
import { PlaceOrder } from './place_order.mjs';
import { RecordOrder } from './record_order.mjs';
import { ReserveFunds } from './reserve_funds.mjs';

export default defineBounded({
  factoryId: 'order_executor',
  channels: (c) => {
    c.input('order')
      .into(ReserveFunds) // step one
      .into(PlaceOrder) // step two, once step one is done
      .branch([
        (fill) => fill.into(ConfirmFill), // two arms, side by side
        (book) => book.into(RecordOrder),
      ]);
  },
});
```

The order is the entry's first message, and the entry names no codec, so
`reserve_funds` gets the object its JSON decodes to: the one `BuyAboveAverage`
in the ETL stream sends when it starts an order. The four tasklets it names in
`into` are the four steps themselves. `branch` takes a list of arms, and each
is a function that gets the same edge.

### Python

```python
# order_executor.py: the bounded operator. One instance is one order.
@flezha.bounded_operator("order_executor")
def order_executor(c: flezha.Channels) -> None:
    (
        c.input("order")
        .into(ReserveFunds)  # step one
        .into(PlaceOrder)  # step two, once step one is done
        .branch(
            lambda fill: fill.into(ConfirmFill),  # two arms, side by side
            lambda book: book.into(RecordOrder),
        )
    )
```

The order is the entry's first message, and the entry names no codec, so
`reserve_funds` gets the dict its JSON decodes to, the `OrderTicket` declared
beside it and sent by `BuyAboveAverage` in the ETL stream when it starts an
order. The four classes it names in `into` are the four steps themselves:
`ReserveFunds`, `PlaceOrder`, `ConfirmFill` and `RecordOrder`. Each arm of
`branch` is a function that gets the same edge.

### Ruby

```ruby
# order_executor_operator.rb: the bounded operator. One instance is one order.
class OrderExecutorOperator < Flezha::BoundedOperator
  factory_id "order_executor"

  channels do |c|
    c.input("order")
     .into(ReserveFunds)                                     # step one
     .into(PlaceOrder)                                       # step two, once step one is done
     .branch(
       ->(fill) { fill.into(ConfirmFill) },                  # two arms, side by side
       ->(book) { book.into(RecordOrder) }
     )
  end
end
```

The order is the entry's first message, and the entry names no codec, so
`reserve_funds` gets the Hash its JSON decodes to: the one `BuyAboveAverage` in
the ETL stream sends when it starts an order. The four classes it names in
`into` are the four steps themselves: `ReserveFunds`, `PlaceOrder`,
`ConfirmFill` and `RecordOrder`. Each arm of `branch` is a lambda that gets the
same edge.

### PHP

```php
// src/OrderExecutorOperator.php: the bounded operator. One instance is one order.
#[Operator(factoryId: 'order_executor')]
#[Bounded(loopMode: LoopMode::PerInput)]
final class OrderExecutorOperator extends BoundedOperator
{
    public static function channels(Builder $c): void
    {
        $c->input('order')
            ->into(ReserveFunds::class)                               // step one
            ->into(PlaceOrder::class)                                 // step two, once step one is done
            ->branch(
                fn (Edge $fill) => $fill->into(ConfirmFill::class),   // two arms, side by side
                fn (Edge $book) => $book->into(RecordOrder::class),
            );
    }
}
```

The order is the entry's first message, and the entry names no codec, so
`reserve_funds` gets the array its JSON decodes to: the one `BuyAboveAverage`
in the ETL stream sends when it starts an order. The four classes it names in
`into` are the four steps themselves: `ReserveFunds`, `PlaceOrder`,
`ConfirmFill` and `RecordOrder`. Each arm of `branch` is a closure that gets the
same edge.

`#[Bounded(loopMode: LoopMode::PerInput)]` is where the PHP desk departs from
the default above: the instance stays after its steps have run, until
`confirm_fill`'s timer calls `$ctx->complete`. A default instance would end
with its last step, and its pending timer with it. A second message to an
instance would run the steps again, and nothing sends one.

### Elixir

```elixir
# lib/tradedesk/order_executor.ex: the bounded operator. One instance is one order.
defmodule Tradedesk.OrderExecutor do
  use Flezha.SDK.BoundedOperator, factory_id: "order_executor"

  defmodule OrderTicket do
    defstruct [:symbol, :quantity]

    def decode(json) do
      case Jason.decode(json) do
        {:ok, %{"symbol" => symbol, "quantity" => quantity}}
        when is_binary(symbol) and is_integer(quantity) ->
          {:ok, %__MODULE__{symbol: symbol, quantity: quantity}}

        _other ->
          {:error, "an order is {\"symbol\": <text>, \"quantity\": <whole number>}"}
      end
    end
  end

  @impl true
  def channels(c) do
    placed =
      c
      |> input_with("order", &OrderTicket.decode/1)
      |> into(Tradedesk.ReserveFunds)
      |> into(Tradedesk.PlaceOrder)

    # Two arms on the one placed channel.
    placed |> into(Tradedesk.ConfirmFill)
    placed |> into(Tradedesk.RecordOrder)
  end
end
```

The order is the entry's first message, and it arrives as bytes, so
`input_with/3` names the parser that turns its JSON into an `OrderTicket`, the
struct `reserve_funds` takes; a message that is not an order is refused there.
The four modules it names in `into` are the four steps themselves:
`ReserveFunds`, `PlaceOrder`, `ConfirmFill` and `RecordOrder`. Elixir has no
`branch`: the placed channel is a value, `placed`, and naming it in two `into`
calls is the fan-out, each arm getting the same placed order.

An Elixir order runs the two arms one after the other, the fill first, because
the driver waits for each step's cell in turn: the book is written once the
fill is confirmed, rather than beside it.

<!-- tabs-close -->

## The runner

One runner registers both of the desk's factories. Registering `order_executor`
starts no order: it tells the controller this runner can run orders, and each
one is started from the factory later, one per buy. A bounded operator's factory
is registered the same way as an unbounded one's.

> Register the `order_executor` factory on the runner too, before `start()`.
> {: .step}

<!-- tabs-open -->

### Java

```java
// Main.java: the runner. Joins trading and registers both factories.
public static void main(String[] args) throws Exception {
    FlezhaRunner runner = Flezha.createRunner(args.length > 0 ? args[0] : "trading")
            .registerFactory(TradeWatcherOperator.class)
            .registerFactory(OrderExecutorOperator.class)
            .start();

    runner.awaitTermination();
}
```

### Rust

```rust
// src/main.rs: the runner. Joins trading and registers both factories.
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

### Node

```javascript
// runner.mjs: the runner. Joins trading and registers both factories.
import { createRunner } from '@flezha/sdk';

const runner = createRunner(process.argv[2] ?? 'trading');
await runner.registerFactory('./trade_watcher_operator.mjs');
await runner.registerFactory('./order_executor_operator.mjs');

// Resolves once the runner is connected and serving. The process then keeps
// running until a SIGTERM, Ctrl-C, or its standard input closing.
await runner.run();
```

### Python

```python
# runner.py: the runner. Joins trading and registers both factories.
def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "trading"
    flezha.configure_logging("info")

    runner = (
        flezha.create_runner(namespace)
        .register_factory(trade_watcher)
        .register_factory(order_executor)
    )

    # Blocks until shutdown: a SIGTERM, Ctrl-C, or standard input closing.
    runner.run()
```

### Ruby

```ruby
# runner.rb: the runner. Joins trading and registers both factories.
# The desk prints its bookings and undos; written at once, they reach a log in
# the order they happened.
$stdout.sync = true

runner = Flezha.create_runner(ARGV[0] || "trading")
               .register_factory(TradeWatcherOperator)
               .register_factory(OrderExecutorOperator)
               .boot

# Blocks until shutdown: a SIGTERM, Ctrl-C, or standard input closing.
runner.await_termination
```

### PHP

```php
// runner.php: the runner. Joins trading and registers both factories.
$runner = Flezha::createRunner($argv[1] ?? 'trading')
    ->registerFactory(TradeWatcherOperator::class)
    ->registerFactory(OrderExecutorOperator::class)
    ->start();

$runner->awaitTermination();
```

### Elixir

```elixir
# lib/tradedesk/application.ex: the runner. Joins trading and registers both factories.
defmodule Tradedesk.Application do
  use Application

  @impl true
  def start(_type, _args) do
    namespace = List.first(System.argv(), "trading")

    children = [
      # `:uds` because this node is not the controller's.
      {Flezha.SDK,
       namespace: namespace,
       operators: [Tradedesk.TradeWatcher, Tradedesk.OrderExecutor],
       transport: :uds}
    ]

    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

`operators:` takes both kinds: the kind is read from each module's `use`,
never named by the list.

<!-- tabs-close -->

## Next

The desk has both of its operators, and neither has run yet.
[`partitioning.md`](partitioning.md) starts them, and spreads the desk's
symbols over partitions.

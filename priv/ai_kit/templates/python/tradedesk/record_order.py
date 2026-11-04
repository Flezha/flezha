"""Writes the placed order into the desk's book, on its own arm of the fan-out.

Confirming the fill and booking the order are two independent consequences of
the same event, and neither waits for the other, which is what a branch is for.

The plainest shape a tasklet has: a value in, nothing kept, nothing out. Its
context, `Ctx[None, None]`, names neither a state nor an output.

It writes no `compensate` on purpose. A rollback undoes what the desk did to the
world, not what it wrote down about it: the attempt happened, and the book says
so even when the order was cancelled again.
"""

from place_order import PlacedOrder

import flezha


@flezha.tasklet(name="record_order")
class RecordOrder:
    def run(self, placed: PlacedOrder, ctx: flezha.Ctx[None, None]) -> None:
        order = placed["order"]
        print(f"[book] {order['symbol']} {order['quantity']} as {placed['order_id']}", flush=True)

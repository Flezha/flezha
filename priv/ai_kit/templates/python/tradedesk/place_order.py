"""Step two: place the order with the paper exchange, keeping the id it answers
with, and cancel the order again if it is undone. It has the same shape as step
one.
"""

from dataclasses import dataclass
from typing import TypedDict

from reserve_funds import OrderTicket, Reserved

import flezha


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

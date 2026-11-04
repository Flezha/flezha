"""Step one: put the money for the order aside, and release it again if the
order is undone.

The reservation id comes from `ctx.stable_id()`, and the step both remembers it
and hands it on, beside the order.
"""

from dataclasses import dataclass
from typing import TypedDict

import flezha


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

"""A graph wiring an `Invoice` producer into a `Shipment` consumer.

A standard type checker must report the mismatch here, naming both handlers —
which is what this SDK has in place of the Rust SDK's `tests/ui` compile-fail
pins. The registration-time refusal is the backstop, not the first line.
"""

from __future__ import annotations

from dataclasses import dataclass

import flezha


@dataclass
class Order:
    id: str


@dataclass
class Invoice:
    order_id: str


@dataclass
class Shipment:
    tracking: str


@flezha.procedure
def bill(order: Order) -> Invoice:
    return Invoice(order_id=order.id)


@flezha.procedure
def dispatch(shipment: Shipment) -> str:
    return shipment.tracking


@flezha.stream_operative("mismatched")
def mismatched(c: flezha.Channels) -> None:
    # `bill` produces Invoice; `dispatch` accepts Shipment.
    c.input("orders").into(bill).named("invoices").into(dispatch).sink("shipped")

"""A graph whose edge types line up. A type checker must report nothing here."""

from __future__ import annotations

from dataclasses import dataclass

import flezha


@dataclass
class Order:
    id: str


@dataclass
class Invoice:
    order_id: str
    total: int


@flezha.procedure
def bill(order: Order) -> Invoice:
    return Invoice(order_id=order.id, total=1)


@flezha.procedure
def render(invoice: Invoice) -> str:
    return f"{invoice.order_id}:{invoice.total}"


@flezha.stream_operative("billing")
def billing(c: flezha.Channels) -> None:
    c.input("orders").into(bill).named("invoices").into(render).sink()

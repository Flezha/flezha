"""Keeps a symbol's last ten prices, and compares each new trade with their average.

One cell per symbol, because the relay keys the channel by symbol, and the
window is that cell's state. Every change to it is recorded as it happens, so a
runner that is killed and started again finds the window it had rather than an
empty one.
"""

from dataclasses import dataclass, field
from typing import TypedDict

import flezha

WINDOW = 10


class Trade(TypedDict):  # the exchange sends more than the desk reads
    symbol: str
    price: float
    ord_type: str


class Move(TypedDict):
    symbol: str
    price: float
    average: float


@flezha.state
@dataclass
class PriceWindow:  # what each symbol's cell remembers
    prices: list[float] = field(default_factory=list)


class PriceSeen(TypedDict):  # how a price joins the window
    price: float


@flezha.tasklet(name="track_prices")
class TrackPrices:
    def run(self, trade: Trade, ctx: flezha.Ctx[PriceWindow, Move]) -> None:
        price = trade["price"]
        if not isinstance(price, int | float):  # JSON checks no types
            raise TypeError(f"the price is not a number: {price!r}")
        prices = ctx.state.prices
        if prices:  # a symbol's first trade has nothing to compare with
            average = sum(prices) / len(prices)
            ctx.emit(Move(symbol=trade["symbol"], price=price, average=average))
        ctx.change(PriceSeen(price=price))

    def apply_change(self, window: PriceWindow, seen: PriceSeen) -> None:
        window.prices.append(seen["price"])
        if len(window.prices) > WINDOW:
            del window.prices[0]

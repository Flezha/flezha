"""Buys when a price is far enough above its symbol's average.

It remembers nothing: the move it is handed already carries the average, so the
decision is a function of one value, and a restart costs this stage nothing
because there is nothing to bring back. That is why its context names no state,
`Ctx[None, str]`, beside `TrackPrices`, which keeps the window.

It does two things per input, both through the context: it starts a steps
relay and it emits.
"""

from reserve_funds import OrderTicket
from track_prices import Move

import flezha

BUY_ABOVE_PCT = 2.0
UNITS_PER_PERCENT = 10


@flezha.tasklet(name="buy_above_average")
class BuyAboveAverage:
    def run(self, move: Move, ctx: flezha.Ctx[None, str]) -> None:
        percent = (move["price"] - move["average"]) / move["average"] * 100.0
        if percent < BUY_ABOVE_PCT:
            return
        quantity = round(percent * UNITS_PER_PERCENT)  # a stronger move buys more

        order = OrderTicket(symbol=move["symbol"], quantity=quantity)
        ctx.spawn_steps("order_executor", order)  # one steps relay per order

        ctx.emit(
            f"{move['symbol']} {move['price']:.4f} is {percent:.2f}% above its average "
            f"of {move['average']:.4f}, buying {quantity}"
        )

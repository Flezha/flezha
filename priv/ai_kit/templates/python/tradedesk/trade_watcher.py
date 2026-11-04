"""The desk's stream: trades in, each price compared with its symbol's recent
average, and an order started when a price jumps far enough above it.

A flow relay runs for as long as the desk does. The exchange sends
other messages beside its trades and delivers several trades in one message, so
the wiring keeps the trade messages, splits each into its trades before
anything else sees them, drops what the desk does not follow, and routes the
rest to a cell per symbol.
"""

from buy_above_average import BuyAboveAverage
from track_prices import TrackPrices

import flezha


@flezha.relay("trade_watcher")
def trade_watcher(c: flezha.ChannelBuilder) -> None:
    """Four transforms on the wiring, then two tasklets, and one place output
    leaves the desk.

    The entry names no codec, so each message arrives as the dict its JSON
    decodes to, with every field the exchange sent. The first `filter` keeps
    the exchange's trade messages: the same connection also carries the
    confirmation of every subscription, which has no trades to split.
    `flat_map` turns one message into its trades, each routed on its own. The
    second `filter` keeps the market orders. `key_by` names the cell each trade
    belongs to, which is what gives every symbol its own state and its own
    order of arrival, and it is the last link before `into`, because it keys
    whatever the transforms above it produced. The four run at the routing seam
    rather than as tasklets of their own, which is why they must stay cheap and
    free of side effects.

    Only the first stage is keyed: a plain `into` leaves its consumer
    single-cell, which is right for the second stage, since it keeps nothing
    per symbol.
    """
    (
        c.input("trades")  # JSON from the exchange
        .filter(lambda message: message.get("channel") == "trade")  # trade messages only
        .flat_map(lambda message: message["data"])  # a message into its trades
        .filter(lambda trade: trade["ord_type"] == "market")  # market orders only
        .key_by(lambda trade: trade["symbol"])  # one cell per symbol
        .into(TrackPrices)
        .into(BuyAboveAverage)  # fed by the tasklet above
        .output("signals")  # out to the file plugin
    )

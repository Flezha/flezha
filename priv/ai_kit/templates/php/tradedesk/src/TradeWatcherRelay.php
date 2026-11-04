<?php

declare(strict_types=1);

namespace Flezha\Examples\Tradedesk;

use Flezha\Attribute\Relay;
use Flezha\ChannelBuilder;

/**
 * The desk's stream: trades in, each price compared with its symbol's recent average, and an order
 * started when a price jumps far enough above it.
 *
 * A flow relay runs for as long as the desk does. The exchange sends other messages
 * beside its trades and delivers several trades in one message, so the wiring keeps the trade
 * messages, splits each into its trades before anything else sees them, drops what the desk does
 * not follow, and routes the rest to a cell per symbol.
 */
#[Relay(factoryId: 'trade_watcher')]
final class TradeWatcherRelay extends \Flezha\Relay
{
    /**
     * Four transforms on the wiring, then two tasklets, and one place output leaves the desk.
     *
     * The entry names no codec, so each message arrives as the array its JSON decodes to, with
     * every field the exchange sent. The first `filter` keeps the exchange's trade messages: the
     * same connection also carries the confirmation of every subscription, which has no trades to
     * split. `flatMap` turns one message into its trades, each routed on its own. The second
     * `filter` keeps the market orders. `keyBy` names the cell each trade belongs to, which is
     * what gives every symbol its own state and its own order of arrival, and it is the last link
     * before `into`, because it keys whatever the transforms above it produced. The four run at
     * the routing seam rather than as tasklets of their own, which is why they must stay cheap and
     * free of side effects.
     *
     * Only the first stage is keyed: a plain `into` leaves its consumer single-cell, which is
     * right for the second stage, since it keeps nothing per symbol.
     */
    public static function channels(ChannelBuilder $c): void
    {
        $c->input('trades')                                                            // JSON from the exchange
            ->filter(fn (array $message) => ($message['channel'] ?? null) === 'trade') // trade messages only
            ->flatMap(fn (array $message) => $message['data'])                         // a message into its trades
            ->filter(fn (array $trade) => $trade['ord_type'] === 'market')             // market orders only
            ->keyBy(fn (array $trade) => $trade['symbol'])                             // one cell per symbol
            ->into(TrackPrices::class)
            ->into(BuyAboveAverage::class)                                             // fed by the tasklet above
            ->output('signals');                                                       // out to the file plugin
    }
}

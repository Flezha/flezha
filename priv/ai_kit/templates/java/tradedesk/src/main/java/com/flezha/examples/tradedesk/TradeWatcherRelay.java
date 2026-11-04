package com.flezha.examples.tradedesk;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flezha.examples.tradedesk.TrackPrices.Trade;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.relay.Relay;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * The desk's stream: trades in, each price compared with its symbol's recent average, and an order
 * started when a price jumps far enough above it.
 *
 * <p>A flow relay runs for as long as the desk does. The exchange sends other messages
 * beside its trades and delivers several trades in one message, so the wiring keeps the trade
 * messages, splits each into its trades before anything else sees them, drops what the desk does
 * not follow, and routes the rest to a cell per symbol.
 */
public final class TradeWatcherRelay implements Relay {

    /**
     * One message from the exchange: {@code channel} says what it is, and a trade message carries
     * one or more trades in {@code data}. The exchange sends more fields than the desk reads, and
     * the default JSON decoding refuses a field the record does not name unless the record says
     * to ignore it.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TradeMessage(String channel, List<Trade> data) {}

    @Override
    @Nonnull
    public String factoryId() {
        return "trade_watcher";
    }

    /**
     * Four transforms on the wiring, then two tasklets, and one place output leaves the desk.
     *
     * <p>The first {@code filter} keeps the exchange's trade messages: the same connection also
     * carries the confirmation of every subscription, which has no trades to split. {@code flatMap}
     * turns one message into its trades, each routed on its own. The second {@code filter} keeps
     * the market orders, the trades that went through at whatever the price was.
     * {@code keyBy} names the cell each trade belongs to, which is what gives every
     * symbol its own state and its own order of arrival; it has to be the last link before
     * {@code into}, because it is evaluated on whatever the transforms above it produced. The four
     * run at the routing seam rather than as tasklets of their own: no cell, no mailbox, no
     * dispatch hop, which is why they must stay cheap and free of side effects.
     *
     * <p>A tasklet's output is an ordinary channel, so each stage is wired onto the one before it
     * with the same {@code into} the entry channel used, and the expression reads in flow order.
     *
     * <p>Only the first stage is keyed, because keying is declared per edge rather than inherited:
     * a plain {@code into} leaves its consumer single-cell. That is the right shape here, since
     * the second stage keeps nothing per symbol. A stage that did would need its own
     * {@code keyBy}, or one cell would hold every symbol at once.
     */
    @Override
    public void channels(@Nonnull ChannelBuilder c) {
        c.input(TradeMessage.class, "trades")                        // JSON from the exchange
                .filter(message -> "trade".equals(message.channel())) // trade messages only
                .flatMap(Trade.class, TradeMessage::data)            // a message into its trades
                .filter(trade -> "market".equals(trade.ord_type()))  // market orders only
                .keyBy(Trade::symbol)                                // one cell per symbol
                .into(TrackPrices.class)
                .into(BuyAboveAverage.class)                         // fed by the tasklet above
                .output("signals");                                  // out to the file plugin
    }
}

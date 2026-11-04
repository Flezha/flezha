package com.flezha.examples.tradedesk;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Keeps a symbol's last ten prices, and compares each new trade with their average.
 *
 * <p>One cell per symbol, because the relay keys the channel by symbol, and the window is that
 * cell's state. Every change to it is recorded as it happens, so a runner that is killed and
 * started again finds the window it had rather than an empty one.
 */
@Tasklet.Spec(name = "track_prices")
public final class TrackPrices implements Tasklet<TrackPrices.Trade, TrackPrices.PriceWindow, TrackPrices.Move> {

    @JsonIgnoreProperties(ignoreUnknown = true)              // the exchange sends more than the desk reads
    public record Trade(String symbol, double price, String ord_type) {}
    public record Move(String symbol, double price, double average) {}

    private static final int WINDOW = 10;

    public static class PriceWindow {                        // what each symbol's cell remembers
        public Deque<Double> prices = new ArrayDeque<>();
    }

    public record PriceSeen(double price) implements Change<PriceWindow> {  // how a price joins the window
        @Override
        public void applyTo(PriceWindow window) {
            window.prices.addLast(price);
            if (window.prices.size() > WINDOW) {
                window.prices.removeFirst();
            }
        }
    }

    @Override
    public void run(Trade trade, Ctx<PriceWindow, Move> ctx) throws TaskletException {
        Deque<Double> prices = ctx.state().prices;
        if (!prices.isEmpty()) {                             // a symbol's first trade has nothing to compare with
            ctx.emit(new Move(trade.symbol(), trade.price(), average(prices)));
        }
        ctx.change(new PriceSeen(trade.price()));
    }

    private static double average(Deque<Double> prices) {
        return prices.stream().mapToDouble(Double::doubleValue).average().orElseThrow();
    }
}

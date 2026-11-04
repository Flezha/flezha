package com.flezha.examples.tradedesk;

import com.flezha.examples.tradedesk.OrderExecutorRelay.OrderTicket;
import com.flezha.examples.tradedesk.TrackPrices.Move;
import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;

/**
 * Buys when a price is far enough above its symbol's average.
 *
 * <p>It remembers nothing: the move it is handed already carries the average, so the decision is a
 * function of one value, and a restart costs this stage nothing because there is nothing to bring
 * back. That is why its state is {@code Void}, beside {@link TrackPrices}, which keeps the window.
 *
 * <p>It does two things per input, both through the context: it starts a steps relay and it
 * emits.
 */
@Tasklet.Spec(name = "buy_above_average")
public final class BuyAboveAverage implements Tasklet<Move, Void, String> {

    private static final double BUY_ABOVE_PCT = 2.0;
    private static final int UNITS_PER_PERCENT = 10;

    @Override
    public void run(Move move, Ctx<Void, String> ctx) throws TaskletException {
        double percent = (move.price() - move.average()) / move.average() * 100.0;
        if (percent < BUY_ABOVE_PCT) {
            return;
        }
        int quantity = (int) Math.round(percent * UNITS_PER_PERCENT);   // a stronger move buys more

        ctx.spawnSteps("order_executor", new OrderTicket(move.symbol(), quantity));   // one steps relay per order

        ctx.emit(String.format(
                "%s %.4f is %.2f%% above its average of %.4f, buying %d",
                move.symbol(), move.price(), percent, move.average(), quantity));
    }
}

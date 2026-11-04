package com.flezha.examples.tradedesk;

import com.flezha.examples.tradedesk.PlaceOrder.PlacedOrder;
import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;

/**
 * Asks the exchange whether the order filled, on one arm of the fan-out. The paper exchange
 * rejects anything above its own size, which is what makes this the step that fails, and takes
 * ten seconds to answer, which leaves time to stop the runner in the middle of an order.
 *
 * <p>It remembers nothing and has nothing to undo, so it writes only {@code run}. It
 * ends the instance with a result rather than emitting one: the order's outcome is the instance's
 * own answer, which {@code flz relay info} reads back, so the desk needs no output here.
 *
 * <p>A failure here is retried first, and once the retries are spent the instance ends abnormally.
 * Everything that finished is then undone in reverse, across both arms: the order is cancelled and
 * the money released, while the desk's book is skipped because it declares no {@code compensate}.
 */
@Tasklet.Spec(name = "confirm_fill")
public final class ConfirmFill implements Tasklet<PlacedOrder, Void, Void> {

    private static final int EXCHANGE_MAX = 100;

    @Override
    public void run(PlacedOrder placed, Ctx<Void, Void> ctx) throws TaskletException {
        try {
            Thread.sleep(10_000);                            // the paper exchange takes ten seconds to answer
        } catch (InterruptedException stopped) {
            Thread.currentThread().interrupt();
            throw new TaskletException("stopped while waiting for the exchange");
        }
        int quantity = placed.order().quantity();
        if (quantity > EXCHANGE_MAX) {
            throw new TaskletException(                      // tried again, then the order is undone
                    "the exchange fills at most " + EXCHANGE_MAX + " a time, and this is " + quantity);
        }
        ctx.complete("filled " + placed.order().symbol() + " " + quantity + " as " + placed.orderId());
    }
}

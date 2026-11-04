package com.flezha.examples.tradedesk;

import com.flezha.examples.tradedesk.OrderExecutorRelay.OrderTicket;
import com.flezha.examples.tradedesk.ReserveFunds.Reserved;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;
import java.util.List;

/**
 * Step two: place the order with the paper exchange, keeping the id it answers with, and cancel
 * the order again if it is undone. It has the same shape as step one.
 */
@Tasklet.Spec(name = "place_order")
public final class PlaceOrder implements Tasklet<Reserved, PlaceOrder.Placement, PlaceOrder.PlacedOrder> {

    public record PlacedOrder(OrderTicket order, String orderId) {}  // what it hands on

    public static class Placement {                          // what this step remembers
        public String orderId = "";
    }

    public record Placing(String orderId) implements Change<Placement> {  // how the id is written down
        @Override
        public void applyTo(Placement placement) {
            placement.orderId = orderId;
        }
    }

    // The step: place the order under an id, remember the id, and hand the order on.
    @Override
    public void run(Reserved reserved, Ctx<Placement, PlacedOrder> ctx) throws TaskletException {
        String orderId = "ord-" + ctx.stableId();
        ctx.change(new Placing(orderId));
        ctx.emit(new PlacedOrder(reserved.order(), orderId));
    }

    // The undo: cancel the order, by the id the state kept.
    @Override
    public void compensate(List<PlacedOrder> completedOutput, Ctx<Placement, PlacedOrder> ctx) {
        System.out.println("[undo] cancelled " + ctx.state().orderId);
    }
}

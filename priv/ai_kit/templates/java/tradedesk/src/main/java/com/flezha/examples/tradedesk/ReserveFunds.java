package com.flezha.examples.tradedesk;

import com.flezha.examples.tradedesk.OrderExecutorRelay.OrderTicket;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.tasklet.Ctx;
import com.flezha.sdk.tasklet.Tasklet;
import com.flezha.sdk.tasklet.TaskletException;
import java.util.List;

/**
 * Step one: put the money for the order aside, and release it again if the order is undone.
 *
 * <p>The reservation id comes from {@code ctx.stableId()}, and the step both remembers it and hands
 * it on.
 */
@Tasklet.Spec(name = "reserve_funds")
public final class ReserveFunds implements Tasklet<OrderTicket, ReserveFunds.Reservation, ReserveFunds.Reserved> {

    public record Reserved(OrderTicket order, String reservationId) {}  // what it hands on

    public static class Reservation {                        // what this step remembers
        public String id = "";
    }

    public record Reserving(String id) implements Change<Reservation> {  // how the id is written down
        @Override
        public void applyTo(Reservation reservation) {
            reservation.id = id;
        }
    }

    // The step: reserve the money under an id, remember the id, and hand the order on.
    @Override
    public void run(OrderTicket order, Ctx<Reservation, Reserved> ctx) throws TaskletException {
        String id = "res-" + ctx.stableId();                 // the same id when the step runs again
        ctx.change(new Reserving(id));
        ctx.emit(new Reserved(order, id));
    }

    // The undo: release the money, by the id the state kept.
    @Override
    public void compensate(List<Reserved> completedOutput, Ctx<Reservation, Reserved> ctx) {
        System.out.println("[undo] released " + ctx.state().id);
    }
}

package com.flezha.examples;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import com.flezha.sdk.workflow.RollbackMode;
import com.flezha.sdk.workflow.WorkflowOperative;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * <b>05 — a workflow operative: a durable, compensating transaction.</b>
 *
 * <p>An <i>stream</i> operative runs forever over a stream. A <i>workflow</i>
 * one is started with arguments, runs a chain of procedures to completion, and is
 * torn down — a checkout, a payment, a provisioning job.
 *
 * <p>Two guarantees make it worth using over a plain method call:
 *
 * <p><b>It resumes, it does not restart.</b> Each procedure commits at its
 * barrier: the controller journals what ran and the handoff to the next step, and
 * only then does the driver proceed. A crash after {@code reserve} committed
 * therefore <b>skips</b> {@code reserve} on resume and continues at {@code charge}
 * — it does not re-reserve. That is why every inter-proc channel must declare a
 * {@code .codec()}: the handoff is what the resumed run re-seeds from, and with no
 * codec there is nothing to journal.
 *
 * <p><b>It unwinds.</b> If a later step fails, the completed ones are compensated
 * in reverse order — the saga pattern. Compensation is best-effort and idempotent
 * by contract: a crash mid-rollback re-invokes the hook, so it must tolerate
 * running twice.
 *
 * <p>A note on the undo handle: a completed procedure's emitted output is not
 * durable, so on a crash-resumed rollback your {@code compensate} receives an
 * <b>empty</b> output list. Anything you need to undo with — a reservation id, a
 * charge id — must live in the Entity's State, which <i>is</i> restored.
 */
public final class E05_WorkflowSaga {

    public static final class CheckoutOperative implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "checkout";
        }

        /** Undo everything on failure. KEEP_PROGRESS only matters for loops. */
        @Override
        public RollbackMode rollbackMode() {
            return RollbackMode.ALL_OR_NOTHING;
        }

        /** A failing step is retried before the saga gives up and unwinds. */
        @Override
        public int retryAttempts() {
            return 3;
        }

        @Override
        public Duration retryBackoff() {
            return Duration.ofMillis(500);
        }

        @Override
        public void procedures(ChannelBuilder c) {
            var sReserve = c.input(String.class, "order").into(Reserve.class);
            var sCharge = sReserve.named("reserved").codec().into(Charge.class);
            var sShip = sCharge.named("charged").codec().into(Ship.class);
            sShip.named("shipped").output("orders_sink");
        }
    }

    /** Shared shape: String in, String out, with an undo handle kept in State. */
    abstract static class Step implements Procedure<String, Step.Handle, String, NoChange, WallClock, Void> {

        /** The undo handle. State is restored on resume; emitted output is NOT. */
        public static final class Handle {
            public String reservationId;
        }

        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<NoChange> changeType() {
            return NoChange.class;
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public Handle initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Handle();
        }
    }

    public static final class Reserve extends Step {
        @Override
        public String name() {
            return "reserve";
        }

        @Override
        public void run(String order, StateHandle<Handle, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            // ctx.stableId() — not UUID.randomUUID(). A re-run derives the SAME id,
            // so the downstream system sees a retry, not a second reservation.
            String reservationId = ctx.stableId().toString();
            state.get().reservationId = reservationId;
            ctx.emit(order + "|reservation=" + reservationId);
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Handle, NoChange> state, ProcedureCtx ctx) {
            // completedOutput may be EMPTY on a crash-resumed rollback. The handle
            // comes from State, which is restored.
            System.out.println("releasing reservation " + state.get().reservationId);
        }
    }

    public static final class Charge extends Step {
        @Override
        public String name() {
            return "charge";
        }

        @Override
        public void run(String reserved, StateHandle<Handle, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit(reserved + "|charged");
        }

        @Override
        public boolean compensateOverridden() {
            return true;
        }

        @Override
        public void compensate(List<String> completedOutput, StateHandle<Handle, NoChange> state, ProcedureCtx ctx) {
            System.out.println("refunding charge");
        }
    }

    /** No compensate hook: shipping is the last step, and there is nothing after it to fail. */
    public static final class Ship extends Step {
        @Override
        public String name() {
            return "ship";
        }

        @Override
        public void run(String charged, StateHandle<Handle, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            ctx.emit(charged + "|shipped");
        }
    }

    private E05_WorkflowSaga() {}
}

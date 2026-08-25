package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Shared base for the three bounded-mailbox probes: a capacity-4 per-Entity
 * mailbox counting what it processed and echoing the input bytes. One concrete
 * procedure per overflow policy, because the graph-shape rule allows exactly one
 * entry channel per operative. Mirrors the Rust test app's
 * {@code DropNothingTestProcedure} / {@code DropNewestTestProcedure} /
 * {@code DropOldestTestProcedure}.
 */
public abstract class BoundedMailboxProcedure
        implements Procedure<
                String, BoundedMailboxProcedure.State, byte[], BoundedMailboxProcedure.Inc, WallClock, Void> {

    public static final class State {
        public long processed;
    }

    public record Inc() implements Change {}

    @Override
    public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new State();
    }

    @Override
    public void applyChange(State state, Inc change) {
        state.processed += 1;
    }

    @Override
    public List<Class<?>> kryoTypes() {
        return List.of(State.class, Inc.class);
    }

    @Override
    public int mailboxCapacity() {
        return 4;
    }

    @Override
    public boolean mailboxCapacityOverridden() {
        return true;
    }

    @Override
    public void run(String input, StateHandle<State, Inc> state, ProcedureCtx ctx) throws ProcedureException {
        state.apply(new Inc());
        ctx.emit(input.getBytes(StandardCharsets.UTF_8));
    }
}

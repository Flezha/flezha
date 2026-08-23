package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.Stateless;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

/**
 * Java equivalent of the Rust SDK's {@code TickEntryProcedure}: parses the entry
 * channel's {@code <interval>:<ts>:<delta>} text into the {@link TickInput} the
 * {@code ticks} channel carries.
 */
public final class TickEntryProcedure
        implements Procedure<String, Stateless, TickInput, NoChange, WallClock, Void> {

    @Override
    public String name() {
        return "tick_entry_procedure";
    }

    @Override
    public Stateless initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new Stateless();
    }

    @Override
    public void run(String input, StateHandle<Stateless, NoChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        String[] parts = input.split(":", 3);
        ctx.emit(new TickInput(
                (int) parseAt(parts, 0), parseAt(parts, 1), parseAt(parts, 2)));
    }

    private static long parseAt(String[] parts, int index) {
        if (index >= parts.length) {
            return 0L;
        }
        try {
            return Long.parseLong(parts[index]);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}

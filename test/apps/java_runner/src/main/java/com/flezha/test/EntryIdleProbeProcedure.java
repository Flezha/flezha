package com.flezha.test;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

/**
 * Mirror of the Rust test app's {@code EntryIdleProbeProcedure} (entry-idle
 * e2e): reports what the tick thread saw, as
 * {@code <absentAtBoot>:<episodes>:<resumes>}. An {@code IdleObserver} has no
 * ctx and cannot emit, so this is how an episode becomes observable. Both entry
 * channels fan into this one node, so either of them asks for a report.
 */
public final class EntryIdleProbeProcedure
        implements Procedure<String, Object, String, NoChange, WallClock, Void> {

    @Override
    public String name() {
        return "entry_idle_probe_procedure";
    }

    @Override
    public Object initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
        return new Object();
    }

    @Override
    public void run(String input, StateHandle<Object, NoChange> state, ProcedureCtx ctx)
            throws ProcedureException {
        ctx.emit(EntryIdleProbeStreamOperative.ABSENT_AT_BOOT.get()
                + ":" + EntryIdleProbeStreamOperative.EPISODES.get()
                + ":" + EntryIdleProbeStreamOperative.RESUMES.get());
    }
}

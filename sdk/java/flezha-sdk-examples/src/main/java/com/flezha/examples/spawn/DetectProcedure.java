package com.flezha.examples.spawn;

import com.flezha.sdk.procedure.IfRunning;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.annotation.Nonnull;

/** Watches the stream and asks for a triage worker per subject. */
public final class DetectProcedure implements Procedure<String, Void, String, NoChange, WallClock, Void> {

    @Override
    @Nonnull
    public String name() {
        return "detect";
    }

    @Override
    public Void initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return null;
    }

    @Override
    public void run(@Nonnull String input, @Nonnull StateHandle<Void, NoChange> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        if (!input.startsWith("alert:")) {
            return;
        }
        String subject = input.substring("alert:".length());

        // One worker per subject. The handle is ignored on purpose: a refused request logs
        // itself, and this procedure carries on detecting.
        ctx.spawnWorkflow("triage@" + subject, "triage", input.getBytes(StandardCharsets.UTF_8), IfRunning.DELIVER);

        ctx.emit("detected:" + subject);
    }
}

package com.flezha.examples;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.IfRunning;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import com.flezha.sdk.workflow.WorkflowOperative;
import com.flezha.sdk.workflow.WorkflowOperativeCtx;
import com.flezha.sdk.workflow.WorkflowOperativeException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * <b>08 — a procedure that asks for a worker.</b>
 *
 * <p>The pattern this exists for: a stream operative notices something worth
 * doing real work about, and wants a worker for it — without an external
 * dispatcher, and without becoming one itself.
 *
 * <p>{@code ctx.spawnWorkflow} is spawn-<i>or</i>-deliver on one id. The first
 * alert for {@code ACME} starts {@code triage@ACME} with that alert as its first
 * input; every later alert for {@code ACME} is fed to the instance already
 * working on it. Concurrent alerts — from other Entities, other runners —
 * resolve controller-side to exactly one instance, so there is no
 * check-then-create race to write around.
 *
 * <p>The call is fire-and-forget. It returns before the controller has decided
 * anything, and a refusal (quota, an unknown factory, an unreachable
 * controller) never becomes this procedure's failure: it resolves the returned
 * handle, and logs itself if nobody reads it. That is deliberate — a detector's
 * own correctness must not depend on worker availability.
 *
 * <p>One thing to know: spawning is a side effect of <b>live</b> processing. A
 * replayed invocation (crash resume, late-input correction) issues no request
 * and answers {@code SKIPPED_ON_REPLAY}, so a replay never doubles the workers
 * a run created.
 */
public final class E08_SpawnWorkflowWorker {

    /** The detector: stream, one instance, runs forever over the stream. */
    public static final class AlertDetector implements StreamOperative {
        @Override
        public String factoryId() {
            return "alert_detector";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "events").into(Detect.class).named("alerts").sink("alerts_sink");
        }
    }

    public static final class Detect implements Procedure<String, Void, String, NoChange, WallClock, Void> {
        @Override
        public String name() {
            return "detect";
        }

        @Override
        public Void initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return null;
        }

        @Override
        public void run(String input, StateHandle<Void, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            if (!input.startsWith("alert:")) {
                return;
            }
            String subject = input.substring("alert:".length());

            // One worker per subject. The handle is ignored: a failed request
            // logs itself and this procedure carries on emitting.
            ctx.spawnWorkflow("triage@" + subject, "triage", input.getBytes(StandardCharsets.UTF_8), IfRunning.DELIVER);

            ctx.emit("detected:" + subject);
        }
    }

    /** The worker: workflow, one per subject, torn down when it returns. */
    public static final class Triage implements WorkflowOperative {
        @Override
        public String factoryId() {
            return "triage";
        }

        @Override
        public void run(WorkflowOperativeCtx ctx, String args) throws WorkflowOperativeException {
            ctx.emit(("triaged:" + args).getBytes(StandardCharsets.UTF_8));
        }
    }

    private E08_SpawnWorkflowWorker() {}
}

package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
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
 * Mirror of the Rust test app's {@code WedgeProbeOperative} (control-plane
 * silent-wedge e2e): the input {@code wedge} blocks the dispatch inside user
 * code forever, so the SDK's dispatch counter stops advancing while the
 * heartbeat thread keeps reporting the runner alive — the
 * {@code runner_pool_wedged} signature. Every other input emits
 * {@code ok:<input>}, which is how the e2e proves the operative produces again
 * once it has been rebound elsewhere.
 */
public final class WedgeProbeStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "wedge_probe";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(WedgeProbeProcedure.class).named("out").sink("wedge_sink");
    }

    public static final class WedgeProbeProcedure
            implements Procedure<String, Stateless, String, NoChange, WallClock, Void> {

        @Override
        public String name() {
            return "wedge_probe_procedure";
        }

        @Override
        public Stateless initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Stateless();
        }

        @Override
        public void run(String input, StateHandle<Stateless, NoChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            if ("wedge".equals(input)) {
                // Never returns. The runner stays connected and heartbeating;
                // only its dispatch progress dies. Teardown is the stdin-EOF watcher.
                while (true) {
                    try {
                        Thread.sleep(3_600_000L);
                    } catch (InterruptedException ignored) {
                        // An interrupt must not release the wedge.
                    }
                }
            }
            ctx.emit("ok:" + input);
        }
    }
}

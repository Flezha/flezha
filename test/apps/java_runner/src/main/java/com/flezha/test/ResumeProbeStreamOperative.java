package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Mirror of the Rust test app's {@code ResumeProbeOperative}: a stateful counter
 * that EMITS its running total on every input, so a Kafka plugin on the output
 * makes the persisted value externally observable. {@code chaos/tests/stateful-resume.sh}
 * uses it to prove WAL-backed state survives a hard controller kill / runner
 * restart. Input is a decimal delta string; output is the new running total as
 * ASCII bytes.
 */
public final class ResumeProbeStreamOperative implements StreamOperative {

    @Override
    public String factoryId() {
        return "resume_probe";
    }

    @Override
    public void channels(ChannelBuilder c) {
        c.input(String.class, "input").into(ResumeProbeProcedure.class).named("total").sink("resume_probe_out");
    }

    public static final class ResumeProbeProcedure
            implements Procedure<String, ResumeProbeProcedure.State, byte[], CounterChange, WallClock, Void> {

        public static final class State {
            public long total;
        }

        @Override
        public String name() {
            return "resume_probe_procedure";
        }

        @Override
        public State initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            // Default + change replay rebuilds the total on restore (same
            // pattern as stateful_counter); no snapshot fast-path needed.
            return new State();
        }

        @Override
        public void applyChange(State state, CounterChange change) {
            state.total += change.delta();
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(State.class, CounterChange.class);
        }

        @Override
        public void run(String input, StateHandle<State, CounterChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            long delta;
            try {
                delta = Long.parseLong(input);
            } catch (NumberFormatException e) {
                delta = 0L;
            }
            long newTotal = state.get().total + delta;
            state.apply(new CounterChange(delta));
            ctx.emit(Long.toString(newTotal).getBytes(StandardCharsets.UTF_8));
        }
    }
}

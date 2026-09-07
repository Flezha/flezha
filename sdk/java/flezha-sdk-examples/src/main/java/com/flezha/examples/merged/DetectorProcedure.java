package com.flezha.examples.merged;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import javax.annotation.Nonnull;

/** Alerts when a reading goes over the threshold it was last told about. */
public final class DetectorProcedure
        implements Procedure<
                DetectorInput, DetectorProcedure.State, String, DetectorProcedure.Recorded, WallClock, Void> {

    public static final class State {
        /** Alert on nothing until the config channel says otherwise. */
        public double maxCelsius = Double.POSITIVE_INFINITY;

        public long alerts;
    }

    public record Recorded(Optional<Double> thresholdSet, boolean alerted) implements Change {}

    @Override
    @Nonnull
    public String name() {
        return "detector";
    }

    @Override
    @Nonnull
    public State initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return new State();
    }

    @Override
    public void applyChange(@Nonnull State state, @Nonnull Recorded change) {
        change.thresholdSet().ifPresent(v -> state.maxCelsius = v);
        if (change.alerted()) {
            state.alerts++;
        }
    }

    @Override
    public void run(
            @Nonnull DetectorInput input, @Nonnull StateHandle<State, Recorded> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        if (input.threshold().isPresent()) {
            state.apply(new Recorded(Optional.of(input.threshold().get().maxCelsius()), false));
            return;
        }

        Reading r = input.reading().orElseThrow();
        if (r.celsius() > state.get().maxCelsius) {
            state.apply(new Recorded(Optional.empty(), true));
            ctx.emit(r.sensor() + " over threshold at " + r.celsius() + "C");
        }
    }
}

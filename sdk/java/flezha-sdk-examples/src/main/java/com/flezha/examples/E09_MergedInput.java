package com.flezha.examples;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

/**
 * <b>09 — one procedure, two input channels.</b>
 *
 * <p>A procedure whose behaviour must change while it runs has, without a
 * merge, only bad options: restart the operative with new environment (losing
 * the state it was accumulating), or staple the configuration onto every data
 * payload (coupling the producer to the consumer's knobs).
 *
 * <p>{@code c.merge(I.class)} declares the procedure's input as one sum type
 * and gives each source channel a mapping into it. Everything downstream —
 * {@code run}, {@code entitiesFor}, state, emit — sees exactly one type, so
 * nothing else about the procedure changes. Sources may be entry channels (as
 * here) or other procedures' output streams, freely mixed.
 *
 * <p>Two things worth knowing before reaching for it:
 *
 * <ul>
 *   <li><b>Per-source FIFO only.</b> Readings arrive in order and thresholds
 *       arrive in order, but nothing orders one against the other. Correlate in
 *       state, as {@code DetectorProcedure} does by remembering the last
 *       threshold it saw.</li>
 *   <li><b>The mappings must be pure.</b> They run again on WAL replay, and a
 *       replay that mapped differently would rebuild a state the live run never
 *       had — the same contract entry parsers and output serializers carry.</li>
 * </ul>
 *
 * <p>A config frame is an ordinary input: it lands in the same durable state as
 * the readings, so a restart resumes with the threshold that was in force.
 */
public final class E09_MergedInput {

    public record Reading(String sensor, double celsius) {}

    public record Threshold(double maxCelsius) {}

    /**
     * The merged input: one sum type over everything the detector consumes.
     * Java has no sum types, so a record with one populated slot per source and
     * a static factory per variant plays the role the Rust enum plays — and the
     * factories are the mappings, referenced as {@code DetectorInput::ofReading}.
     */
    public record DetectorInput(Optional<Reading> reading, Optional<Threshold> threshold) {
        public static DetectorInput ofReading(Reading r) {
            return new DetectorInput(Optional.of(r), Optional.empty());
        }

        public static DetectorInput ofThreshold(Threshold t) {
            return new DetectorInput(Optional.empty(), Optional.of(t));
        }
    }

    /** Mutable per-Entity state, folded from the changes below. */
    public static final class DetectorState {
        /** The safe default until the config channel says otherwise: an
         * operative that starts before its config must not alert on everything. */
        public double maxCelsius = Double.POSITIVE_INFINITY;

        public long alerts;
    }

    public record DetectorChange(Optional<Double> thresholdSet, boolean alerted) implements Change {
        @Override
        public Optional<CoalesceKey> coalesceKey() {
            return Optional.empty();
        }
    }

    public static final class DetectorProcedure
            implements Procedure<DetectorInput, DetectorState, String, DetectorChange, WallClock, Void> {

        @Override
        public String name() {
            return "detector";
        }

        @Override
        public DetectorState initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new DetectorState();
        }

        @Override
        public void applyChange(DetectorState state, DetectorChange change) {
            change.thresholdSet().ifPresent(v -> state.maxCelsius = v);
            if (change.alerted()) {
                state.alerts += 1;
            }
        }

        @Override
        public void run(DetectorInput input, StateHandle<DetectorState, DetectorChange> state, ProcedureCtx ctx)
                throws ProcedureException {
            if (input.threshold().isPresent()) {
                state.apply(
                        new DetectorChange(Optional.of(input.threshold().get().maxCelsius()), false));
                return;
            }
            Reading r = input.reading().orElseThrow();
            if (r.celsius() > state.get().maxCelsius) {
                state.apply(new DetectorChange(Optional.empty(), true));
                ctx.emit(r.sensor() + " over threshold at " + r.celsius() + "C");
            }
        }
    }

    public static final class Thermostat implements StreamOperative {
        @Override
        public String factoryId() {
            return "thermostat";
        }

        @Override
        public void channels(ChannelBuilder c) {
            // Two entry channels, one consumer. Either source may equally be
            // another procedure's output stream — `from` takes both.
            c.merge(DetectorInput.class)
                    .from(c.input(Reading.class, "readings"), DetectorInput::ofReading)
                    .from(c.input(Threshold.class, "config"), DetectorInput::ofThreshold)
                    .into(DetectorProcedure.class)
                    .named("alerts")
                    .sink("alerts_sink");
        }
    }

    private E09_MergedInput() {}
}

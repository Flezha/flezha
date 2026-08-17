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
import java.util.List;
import java.util.Optional;

/**
 * <b>02 — durable state, and why you never assign to a field.</b>
 *
 * <p>State is persisted as a log of <b>Changes</b>, not as snapshots of fields.
 * You never write {@code state.count++}; you {@code state.apply(new Increment())},
 * and the runtime folds it through {@link Procedure#applyChange} and journals it.
 * A crash then rebuilds the state by replaying that log.
 *
 * <p>That indirection is the entire durability contract. A direct field write is
 * invisible to the journal, so it survives in memory and vanishes on restart —
 * silently, and only under a crash you were not testing for.
 *
 * <p>Two more rules follow from it:
 *
 * <ul>
 *   <li><b>Kryo needs to know your types.</b> {@link Procedure#kryoTypes()} pins
 *       their stable ids. Append to that list, never reorder it — the ids are
 *       positional, and reordering silently re-interprets old journals.
 *   <li><b>{@code run} must be deterministic.</b> Replay re-runs it, so reading
 *       {@code System.currentTimeMillis()} or {@code UUID.randomUUID()} makes the
 *       replayed state differ from the original. Use {@code ctx.now()} and
 *       {@code ctx.stableId()} — see {@link E03_EffectivelyOnce}.
 * </ul>
 */
public final class E02_StatefulCounter {

    /** The mutable per-Entity state. Plain fields; only {@code applyChange} writes them. */
    public static final class Counter {
        public long count;
        public long lastSeenMs;
    }

    /** One journaled mutation. A record is the natural shape. */
    public record Increment(long atMs) implements Change {
        @Override
        public Optional<CoalesceKey> coalesceKey() {
            // Empty = every Change is kept. Returning a key instead collapses
            // same-key Changes at flush time, keeping only the latest — the right
            // move for "current value of X" updates, wrong for "+1" deltas like
            // this one, which would lose counts.
            return Optional.empty();
        }
    }

    public static final class CountProcedure implements Procedure<String, Counter, String, Increment, WallClock, Void> {

        @Override
        public String name() {
            return "count";
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
        public Class<Increment> changeType() {
            return Increment.class;
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        /** Stable Kryo ids, positional. Append only. */
        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(Increment.class, Counter.class);
        }

        /** The ONE place state is written. Replay calls exactly this. */
        @Override
        public void applyChange(Counter state, Increment change) {
            state.count++;
            state.lastSeenMs = change.atMs();
        }

        @Override
        public Counter initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            // Cold start. On a resume the runtime restores the snapshot and folds
            // the change log on top of whatever this returns.
            return new Counter();
        }

        @Override
        public void run(String input, StateHandle<Counter, Increment> state, ProcedureCtx ctx)
                throws ProcedureException {
            // ctx.now(), not System.currentTimeMillis(): the runtime evaluates the
            // declared Clock once per invocation, and a replay serves the same value.
            state.apply(new Increment(ctx.now()));

            ctx.emit("count=" + state.get().count);
        }
    }

    public static final class CounterOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "counter";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sCountProcedure = c.input(String.class, "in").into(CountProcedure.class);
            sCountProcedure.named("out").sink("my_sink");
        }
    }

    private E02_StatefulCounter() {}
}

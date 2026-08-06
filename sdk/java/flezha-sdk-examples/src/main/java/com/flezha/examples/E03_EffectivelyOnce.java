package com.flezha.examples;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.UnboundedOperative;
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
 * <b>03 — effectively-once output.</b> Each invocation's state changes, its
 * plugin output, and the offset of the input it consumed all become durable in
 * <b>one</b> record, under one fsync.
 *
 * <p>That atomicity is the whole point: a crash can never advance the consumed
 * offset while losing the output it was advanced for. Without the tier those are
 * separate writes, and a crash between them silently drops output that the
 * controller believes was produced.
 *
 * <p>What the tier asks of you in return:
 *
 * <ul>
 *   <li><b>Every plugin channel must be {@code .keyed()}.</b> A replay re-emits
 *       outputs, and only a keyed sink can absorb that as an idempotent upsert
 *       rather than a duplicate row. Declaring the tier without keys is refused at
 *       registration.
 *   <li><b>Keys must be replay-stable.</b> Derive them from the input, the state,
 *       or {@code ctx.stableId()} — never from {@code UUID.randomUUID()}, which
 *       would mint a different key on replay and defeat the upsert.
 * </ul>
 *
 * <p>{@code checkpointAck()} is the optional second half: it withholds the
 * consumed offset until you call {@code ctx.checkpoint()}, so state that is only
 * coherent at a boundary — a closed window, a completed batch — is never
 * persisted half-built. The interval you leave open is the work a crash replays.
 */
public final class E03_EffectivelyOnce {

    public static final class Totals {
        public long sum;
        public long ticks;
    }

    public record AddTick(long value) implements Change {
        @Override
        public Optional<CoalesceKey> coalesceKey() {
            return Optional.empty();
        }
    }

    public static final class RollupProcedure implements Procedure<String, Totals, String, AddTick, WallClock, Void> {

        @Override
        public String name() {
            return "rollup";
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
        public Class<AddTick> changeType() {
            return AddTick.class;
        }

        @Override
        public Class<WallClock> clockType() {
            return WallClock.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(AddTick.class, Totals.class);
        }

        @Override
        public void applyChange(Totals state, AddTick change) {
            state.sum += change.value();
            state.ticks++;
        }

        /** One Entity per symbol: the state is per-key, so the durability is too. */
        @Override
        public List<String> entityKeys() {
            return List.of("BTC", "ETH");
        }

        @Override
        public List<String> entitiesFor(String input) {
            return List.of(symbolOf(input));
        }

        @Override
        public boolean entitiesForOverridden() {
            return true;
        }

        @Override
        public Totals initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Totals();
        }

        @Override
        public void run(String input, StateHandle<Totals, AddTick> state, ProcedureCtx ctx) throws ProcedureException {
            state.apply(new AddTick(valueOf(input)));

            // The key is derived from the input, so a replay of the same input
            // re-emits under the SAME key and the sink upserts instead of
            // duplicating. That is what makes the re-emission harmless.
            ctx.emitKeyed(symbolOf(input), "sum=" + state.get().sum);

            // Close the interval every 100 ticks. Until this call, the consumed
            // offset is withheld and the changes stay staged — so a crash replays
            // the whole open interval rather than persisting a half-built window.
            if (state.get().ticks % 100 == 0) {
                ctx.checkpoint();
            }
        }

        private static String symbolOf(String input) {
            return input.split(":", 2)[0];
        }

        private static long valueOf(String input) {
            return Long.parseLong(input.split(":", 2)[1]);
        }
    }

    public static final class RollupOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "rollup";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.checkpointAck();
            var sRollupProcedure = c.input(String.class, "ticks").into(RollupProcedure.class);
            sRollupProcedure.named("rollups").keyed().output("postgres_sink");
        }
    }

    private E03_EffectivelyOnce() {}
}

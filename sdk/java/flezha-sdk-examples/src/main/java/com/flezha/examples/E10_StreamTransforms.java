package com.flezha.examples;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.List;
import java.util.Optional;

/**
 * <b>10 — reshaping and keying on the wiring.</b>
 *
 * <p>One trade feed, four candle timeframes, one candle Entity per
 * symbol-and-timeframe. Written with Procedures alone that is a filter class, a
 * fan-out class, and a keying hook buried inside the consumer — three places to
 * look and none of them the topology. Written on the edge it is four lines you
 * can read top to bottom:
 *
 * <pre>{@code
 * c.input(Trade.class, "trades")
 *         .filter(t -> t.qty() > 0)                       // no Procedure
 *         .flatMap(Slot.class, E10::explode)              // one trade → four slots
 *         .keyBy(slot -> slot.symbol() + "@" + slot.tf()) // one Entity per slot
 *         .into(CandleProcedure.class)
 *         .sink("candles");
 * }</pre>
 *
 * <p>The whole topology contains exactly ONE Procedure node. The transforms add
 * no Entity, no mailbox and no dispatch hop: everything declared before
 * {@code into} composes into a single closure stored on that one consumer edge.
 * Fusion is structural — {@code into} IS the boundary — so there is no
 * optimizer to second-guess and nothing to disable.
 *
 * <p>Two obligations come with running on the edge, and they are the whole
 * contract:
 *
 * <ul>
 *   <li><b>Pure.</b> Edge closures are evaluated at the routing seam, which
 *       live delivery and WAL replay both travel. A closure that read the clock
 *       would rebuild a state the live run never had.</li>
 *   <li><b>Cheap.</b> They run on the routing thread. Real work belongs in the
 *       Procedure behind {@code keyBy}, which is where per-Entity parallelism
 *       comes from. Reshaping a value is cheap; parsing a megabyte is not.</li>
 * </ul>
 *
 * <p>{@code keyBy} is <b>total</b>: it names an Entity, it never declines.
 * Discarding an input is {@code filter}'s job — which is why the qty check
 * comes first. And {@code map} / {@code flatMap} take the result type as a
 * {@code Class} token because erasure cannot recover it from a lambda;
 * {@code filter} and {@code keyBy} need none.
 */
public final class E10_StreamTransforms {

    private E10_StreamTransforms() {}

    /** The timeframes every trade is folded into. */
    private static final List<String> TIMEFRAMES = List.of("1m", "5m", "1h", "1d");

    public record Trade(String symbol, double price, double qty) {}

    /** One trade, addressed to one timeframe's candle. */
    public record Slot(String symbol, String tf, double price) {}

    /** One trade → one Slot per timeframe. Pure, and cheap. */
    static List<Slot> explode(Trade t) {
        return TIMEFRAMES.stream()
                .map(tf -> new Slot(t.symbol(), tf, t.price()))
                .toList();
    }

    public static final class CandleOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "example_candles";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(Trade.class, "trades")
                    .filter(t -> t.qty() > 0)
                    .flatMap(Slot.class, E10_StreamTransforms::explode)
                    .keyBy(slot -> slot.symbol() + "@" + slot.tf())
                    .into(CandleProcedure.class)
                    .keyed()
                    .sink("candles");
        }
    }

    /**
     * The one Procedure in the graph. Note what it does NOT declare: no
     * {@code entityKeys()}, no keying member of any kind. It is dynamic-mode
     * because a keyed edge feeds it — and its Entities are created on first
     * sight of each key, exactly as the deleted {@code entityKey} hook produced.
     */
    public static final class CandleProcedure
            implements Procedure<Slot, CandleProcedure.Candle, String, CandleProcedure.Tick, WallClock, Void> {

        /**
         * The running candle for ONE symbol-and-timeframe. Mutable because
         * {@code applyChange} folds a Change into it in place — the change log
         * is the source of truth, never a field assignment from {@code run}.
         */
        public static final class Candle {
            public double high = Double.MIN_VALUE;
            public double low = Double.MAX_VALUE;
            public long trades;
        }

        public record Tick(double price) implements Change {}

        @Override
        public String name() {
            return "example_candle_procedure";
        }

        @Override
        public Candle initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Candle();
        }

        @Override
        public void applyChange(Candle state, Tick change) {
            state.high = Math.max(state.high, change.price());
            state.low = Math.min(state.low, change.price());
            state.trades++;
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(Candle.class, Tick.class);
        }

        @Override
        public void run(Slot input, StateHandle<Candle, Tick> state, ProcedureCtx ctx) throws ProcedureException {
            state.apply(new Tick(input.price()));
            Candle candle = state.get();
            // The Entity IS the candle, so the plain emit already carries the
            // Entity's own key — `keyBy`'s value — and cannot drift from it.
            // `emitKeyed` is for the output whose key is a different one.
            ctx.emit(input.symbol() + " " + input.tf() + " h=" + candle.high + " l=" + candle.low);
        }
    }
}

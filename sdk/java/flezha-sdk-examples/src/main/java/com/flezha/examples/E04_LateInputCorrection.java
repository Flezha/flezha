package com.flezha.examples;

import com.flezha.sdk.channel.BeyondWindowPolicy;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import java.util.List;
import java.util.Optional;

/**
 * <b>04 — repairing the past when an input arrives late.</b>
 *
 * <p>A trade timestamped 09:00 that reaches you at 09:05 belongs in the 09:00
 * candle, not the 09:05 one. Ordinary stream processing folds it in at arrival,
 * which quietly corrupts the earlier window.
 *
 * <p>Declare a {@code correctionWindow} and the controller instead <b>rewinds</b>
 * the Segment to a baseline before that event time and replays the affected slice
 * in event-time order. Your {@code run} is invoked again for those inputs, in the
 * order they should have arrived, and the corrected output overwrites the stale
 * output — which is why every channel must be keyed.
 *
 * <p>The window is a promise the controller has to be able to keep: it sizes the
 * input log's retention and the snapshot age floor, because the past being
 * repaired must still exist. Declare the smallest window that covers your real
 * lateness, not the largest one you can imagine.
 *
 * <p>An input older than the window is <b>beyond</b> it — the state it would fix
 * is gone. The default is {@link BeyondWindowPolicy#PROCESS_AS_ARRIVAL} (fold it
 * in at arrival, today's behaviour) precisely because declaring a window must
 * never silently start dropping data. Choose {@code DROP} only when a stale
 * result is worse than a missing one.
 */
public final class E04_LateInputCorrection {

    public static final class Candle {
        public long open;
        public long high;
        public long low;
        public long close;
    }

    public record Tick(long price) implements Change {
        @Override
        public Optional<CoalesceKey> coalesceKey() {
            return Optional.empty();
        }
    }

    /** Input format: {@code <epochMillis>:<price>}. */
    public static final class CandleOperation implements Operation<String, Candle, String, Tick, EventTimeFrom, Void> {

        @Override
        public String name() {
            return "candle";
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
        public Class<Tick> changeType() {
            return Tick.class;
        }

        /** Time comes from the INPUT, not the wall clock — that is what "late" means. */
        @Override
        public Class<EventTimeFrom> clockType() {
            return EventTimeFrom.class;
        }

        @Override
        public Class<Void> configType() {
            return Void.class;
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(Tick.class, Candle.class);
        }

        /**
         * The extractor. Rust binds this at compile time via a type bound; Java's
         * erasure cannot, so declaring {@code EventTimeFrom} WITHOUT overriding
         * this fails at registration instead.
         */
        @Override
        public long eventTime(String input) {
            return Long.parseLong(input.split(":", 2)[0]);
        }

        @Override
        public boolean eventTimeOverridden() {
            return true;
        }

        @Override
        public void applyChange(Candle state, Tick tick) {
            if (state.open == 0) {
                state.open = tick.price();
                state.low = tick.price();
            }
            state.high = Math.max(state.high, tick.price());
            state.low = Math.min(state.low, tick.price());
            state.close = tick.price();
        }

        @Override
        public Candle initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new Candle();
        }

        @Override
        public void run(String input, StateHandle<Candle, Tick> state, OperationCtx ctx) throws OperationException {
            state.apply(new Tick(Long.parseLong(input.split(":", 2)[1])));

            // ctx.now() is the EVENT time here, not the wall clock. On a correction
            // replay it is the late input's own timestamp — so bucketing by it puts
            // the corrected candle in the bucket it always belonged to.
            long bucket = ctx.now() / 60_000;

            // The key is the bucket: re-emitting a corrected candle overwrites the
            // stale one instead of appending a second row.
            ctx.emitKeyed(String.valueOf(bucket), "close=" + state.get().close);
        }
    }

    public static final class CandleOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "candles";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.effectivelyOnce();
            c.correctionWindow(5 * 60_000L);
            c.beyondWindowPolicy(BeyondWindowPolicy.PROCESS_AS_ARRIVAL);
            var sCandleOperation = c.input(String.class, "trades").into(CandleOperation.class);
            sCandleOperation.named("candles").keyed().output("postgres_sink");
        }
    }

    private E04_LateInputCorrection() {}
}

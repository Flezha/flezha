package com.flezha.examples.corrections;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.EventTimeFrom;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import java.util.Optional;
import javax.annotation.Nonnull;

/** A one-minute candle. Input is {@code <epochMillis>:<price>}. */
public final class CandleProcedure
        implements Procedure<String, CandleProcedure.Candle, String, CandleProcedure.Tick, EventTimeFrom, Void> {

    private static final long ONE_MINUTE_MS = 60_000L;

    public static final class Candle {
        public long open;
        public long high;
        public long low;
        public long close;
    }

    public record Tick(long price) implements Change {}

    @Override
    @Nonnull
    public String name() {
        return "candle";
    }

    /**
     * Where the event time comes from. Rust binds this through a type bound; Java's erasure
     * cannot, so declaring {@code EventTimeFrom} without overriding this fails at registration.
     */
    @Override
    public long eventTime(@Nonnull String input) {
        return Long.parseLong(input.split(":", 2)[0]);
    }

    @Override
    public boolean eventTimeOverridden() {
        return true;
    }

    @Override
    public void applyChange(@Nonnull Candle state, @Nonnull Tick tick) {
        if (state.open == 0) {
            state.open = tick.price();
            state.low = tick.price();
        }
        state.high = Math.max(state.high, tick.price());
        state.low = Math.min(state.low, tick.price());
        state.close = tick.price();
    }

    @Override
    @Nonnull
    public Candle initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return new Candle();
    }

    @Override
    public void run(@Nonnull String input, @Nonnull StateHandle<Candle, Tick> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(new Tick(Long.parseLong(input.split(":", 2)[1])));

        // ctx.now() is the event time, so on a correction replay it is the late message's own
        // timestamp. Bucketing by it puts the corrected candle where it always belonged.
        long bucket = ctx.now() / ONE_MINUTE_MS;

        ctx.emitKeyed(String.valueOf(bucket), "close=" + state.get().close);
    }
}

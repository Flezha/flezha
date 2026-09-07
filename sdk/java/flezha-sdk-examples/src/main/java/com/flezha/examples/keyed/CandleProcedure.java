package com.flezha.examples.keyed;

import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * The running candle for one symbol and timeframe.
 *
 * <p>Notice there is no keying here at all. A keyed edge feeds this procedure, so entities
 * appear on first sight of each key without the procedure knowing about it.
 */
public final class CandleProcedure
        implements Procedure<Slot, CandleProcedure.Candle, String, CandleProcedure.Tick, WallClock, Void> {

    /** Mutable, because {@link #applyChange} folds into it in place. */
    public static final class Candle {
        public double high = Double.MIN_VALUE;
        public double low = Double.MAX_VALUE;
        public long trades;
    }

    public record Tick(double price) implements Change {}

    @Override
    @Nonnull
    public String name() {
        return "candle";
    }

    @Override
    @Nonnull
    public Candle initState(@Nonnull Optional<String> entityKey, @Nonnull Optional<RestoreInitPayload> initPayload) {
        return new Candle();
    }

    @Override
    public void applyChange(@Nonnull Candle state, @Nonnull Tick change) {
        state.high = Math.max(state.high, change.price());
        state.low = Math.min(state.low, change.price());
        state.trades++;
    }

    @Override
    public void run(@Nonnull Slot input, @Nonnull StateHandle<Candle, Tick> state, @Nonnull ProcedureCtx ctx)
            throws ProcedureException {
        state.apply(new Tick(input.price()));
        Candle candle = state.get();
        ctx.emit(input.symbol() + " " + input.tf() + " h=" + candle.high + " l=" + candle.low);
    }
}

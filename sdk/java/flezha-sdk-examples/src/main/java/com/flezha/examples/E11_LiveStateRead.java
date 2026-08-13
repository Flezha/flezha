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
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * <b>11 — reading an Entity's live state.</b>
 *
 * <p>Something outside the pipeline needs the CURRENT value of one key — a
 * dashboard, an ops question, a health check. Before {@code flz state get} the
 * only read was {@code flz state at}, which serves the controller's WAL at a
 * point in the PAST, so "what is BTCUSD doing right now" meant waiting for the
 * value to reach a sink and reading the copy.
 *
 * <p><b>This example declares nothing special</b>, and that is the point: every
 * Entity of a running Operative is readable, because the read reuses the
 * {@code EntityState} command the SDK has always answered for migration.
 * Declaring a {@code Change} type is what makes the value durable; nothing extra
 * makes it readable.
 *
 * <p>Feed it two trades:
 *
 * <pre>{@code
 * flz operative send market --channel trades --text '{"symbol":"BTCUSD","price":64100.0,"qty":0.5}'
 * flz operative send market --channel trades --text '{"symbol":"ETHUSD","price":3120.0,"qty":4.0}'
 * }</pre>
 *
 * <p>then read ONE Entity by name:
 *
 * <pre>{@code
 * $ flz state get market/candle/BTCUSD
 * state get market/candle/BTCUSD:
 *   change_seq: 1
 *   state:      {"candle":{"BTCUSD":{"high":64100.0,"trades":1,"volume":0.5}}}
 * }</pre>
 *
 * <p>The {@code change_seq} is the version the state was serialized at, and it
 * may be <b>ahead of what is durable</b>: the read takes the Entity's state lock
 * and does not drain pending Changes, so a Change {@code run} applied a
 * millisecond ago is in the answer whether or not the flusher has shipped it.
 * That is the contract — freshest value, version reported — and it is why a read
 * triggers no compaction, no snapshot and no WAL truncation.
 *
 * <p>Narrowing costs what it asks for. Drop the key to read the whole Procedure
 * (no single version, so no seq):
 *
 * <pre>{@code
 * $ flz state get market/candle --output json
 * {"change_seq":null,"path":"market/candle","state":{"candle":{"BTCUSD":{...},"ETHUSD":{...}}}}
 * }</pre>
 *
 * <p>An Entity nothing holds is <b>refused</b>, naming which case and pointing at
 * the durable path — it is never quietly served from disk:
 *
 * <pre>{@code
 * $ flz state get market/candle/DOGEUSD
 * state get: entity_not_found: procedure `candle` holds no Entity keyed `DOGEUSD`
 * — read its durable state with `flz state at`
 *
 * $ flz state get market/candle/ETHUSD          # after the 10-minute idle TTL
 * state get: entity_evicted: Entity `ETHUSD` of procedure `candle` was
 * idle-evicted; eviction is destructive, so there is no durable value to serve
 * — read its durable state with `flz state at`
 * }</pre>
 *
 * <p>Both exit non-zero. The two are different diagnoses on purpose: eviction
 * unlinks the Entity's files, so after the fact only the SDK's workflow eviction
 * trace can tell you the key ever existed.
 *
 * <p>One caveat worth knowing before you wire a reader to it: each read is a
 * control round-trip plus one state-lock acquisition, so it is a <b>point read
 * for operators and low-rate readers</b>, not a serving API. A high-rate path is
 * still a sink — {@code .output(...)} costs the Entity nothing per reader.
 */
public final class E11_LiveStateRead {

    private E11_LiveStateRead() {}

    public record Trade(String symbol, double price, double qty) {}

    public static final class MarketOperative implements StreamOperative {
        @Override
        public String factoryId() {
            // The FIRST path segment of `flz state get <operative>/...`.
            return "market";
        }

        @Override
        public void channels(ChannelBuilder c) {
            // `keyBy` is what makes the Entity keys addressable by name: one
            // Entity per symbol, and `BTCUSD` is exactly what you type.
            c.input(Trade.class, "trades").keyBy(Trade::symbol).into(CandleProcedure.class);
        }
    }

    public static final class CandleProcedure
            implements Procedure<Trade, CandleProcedure.Candle, String, CandleProcedure.Traded, WallClock, Void> {

        /**
         * What a live read returns. Every field shows up in the JSON, so this
         * type IS the read model — keep it something an operator can act on.
         */
        public static final class Candle {
            public double high;
            public double volume;
            public long trades;
        }

        public record Traded(double price, double qty) implements Change {}

        @Override
        public String name() {
            // The MIDDLE path segment of `flz state get <operative>/<procedure>/<key>`.
            return "candle";
        }

        @Override
        public Class<Trade> inputType() {
            return Trade.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<Traded> changeType() {
            return Traded.class;
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
            return List.of(Candle.class, Traded.class);
        }

        @Override
        public Candle initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Candle();
        }

        @Override
        public void applyChange(Candle state, Traded change) {
            state.high = Math.max(state.high, change.price());
            state.volume += change.qty();
            state.trades++;
        }

        /**
         * A quiet symbol is evicted after ten idle minutes. Eviction is
         * destructive, so a later read of that key is refused with
         * {@code entity_evicted} rather than served a stale value.
         */
        @Override
        public Optional<Duration> entityIdleTtl() {
            return Optional.of(Duration.ofMinutes(10));
        }

        @Override
        public void run(Trade trade, StateHandle<Candle, Traded> state, ProcedureCtx ctx) throws ProcedureException {
            // Applied here, visible to a read immediately — before the flusher
            // has shipped it, and before any sink has seen it.
            state.apply(new Traded(trade.price(), trade.qty()));
        }
    }
}

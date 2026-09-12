package com.flezha.test;

import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.runtime.state.KryoCodec;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

/**
 * Twin of the Rust test app's schema-evolution fixtures: the same class either
 * side of a deploy that edits its own state type.
 *
 * <p>Both halves declare the SAME procedure name, so an operative started under
 * {@code schema_evolution_v1} and restarted under {@code schema_evolution_v2}
 * restores from the very WAL v1 wrote — a deploy, with the two binaries
 * collapsed into one runner for the fixture. Output is {@code total:currency}
 * so the migrated field is observable on the wire.
 */
public final class SchemaEvolutionStreamOperatives {

    private SchemaEvolutionStreamOperatives() {}

    /** v1: {@code {total}} at the default schema version. */
    public static final class V1 implements StreamOperative {

        @Override
        public String factoryId() {
            return "schema_evolution_v1";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(Proc.class).sink("total");
        }

        public static final class CartV1 {
            public long total;
        }

        public static final class AddV1 implements Change {
            public long delta;

            public AddV1() {}

            AddV1(long delta) {
                this.delta = delta;
            }
        }

        public static final class Proc
                implements Procedure<String, CartV1, byte[], AddV1, WallClock, Void> {

            @Override
            public String name() {
                return "evolving_cart";
            }

            @Override
            public List<Class<?>> kryoTypes() {
                return List.of(CartV1.class, AddV1.class);
            }

            @Override
            public CartV1 initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
                return new CartV1();
            }

            @Override
            public void applyChange(CartV1 state, AddV1 change) {
                state.total += change.delta;
            }

            @Override
            public void run(String input, StateHandle<CartV1, AddV1> state, ProcedureCtx ctx)
                    throws ProcedureException {
                long delta = parse(input);
                long total = state.get().total + delta;
                state.apply(new AddV1(delta));
                ctx.emit((total + ":").getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    /** v2: {@code {total, currency}} at schema version 2, reading down to 1. */
    public static final class V2 implements StreamOperative {

        @Override
        public String factoryId() {
            return "schema_evolution_v2";
        }

        @Override
        public void channels(ChannelBuilder c) {
            c.input(String.class, "input").into(Proc.class).sink("total");
        }

        public static final class CartV2 {
            public long total;
            public String currency = "";
        }

        public static final class AddV2 implements Change {
            public long delta;
            public String note = "";

            public AddV2() {}

            AddV2(long delta, String note) {
                this.delta = delta;
                this.note = note;
            }
        }

        public static final class Proc
                implements Procedure<String, CartV2, byte[], AddV2, WallClock, Void> {

            /**
             * The v1 type table, in v1's registration order — Kryo ids are
             * positional, so a migration decodes with the ids the old version
             * wrote, not with this version's.
             */
            private static final KryoCodec V1_CODEC =
                    new KryoCodec(List.of(V1.CartV1.class, V1.AddV1.class));

            @Override
            public String name() {
                return "evolving_cart";
            }

            @Override
            public int schemaVersion() {
                return 2;
            }

            @Override
            public int minReadableSchemaVersion() {
                return 1;
            }

            @Override
            public List<Class<?>> kryoTypes() {
                return List.of(CartV2.class, AddV2.class);
            }

            @Override
            public CartV2 initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
                return new CartV2();
            }

            @Override
            public CartV2 migrateState(int fromVersion, byte[] bytes) {
                if (fromVersion != 1) {
                    throw new IllegalStateException("no migration from " + fromVersion);
                }
                V1.CartV1 old = V1_CODEC.deserialize(bytes, V1.CartV1.class);
                CartV2 migrated = new CartV2();
                migrated.total = old.total;
                migrated.currency = "EUR";
                return migrated;
            }

            @Override
            public AddV2 migrateChange(int fromVersion, byte[] bytes) {
                if (fromVersion != 1) {
                    throw new IllegalStateException("no migration from " + fromVersion);
                }
                V1.AddV1 old = V1_CODEC.deserialize(bytes, V1.AddV1.class);
                return new AddV2(old.delta, "migrated");
            }

            @Override
            public void applyChange(CartV2 state, AddV2 change) {
                state.total += change.delta;
            }

            @Override
            public void run(String input, StateHandle<CartV2, AddV2> state, ProcedureCtx ctx)
                    throws ProcedureException {
                long delta = parse(input);
                CartV2 current = state.get();
                long total = current.total + delta;
                String currency = current.currency.isEmpty() ? "EUR" : current.currency;
                state.apply(new AddV2(delta, "live"));
                ctx.emit((total + ":" + currency).getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private static long parse(String input) {
        try {
            return Long.parseLong(input);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}

package com.flezha.examples;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

/**
 * <b>01 — the smallest thing that runs.</b> A stateless Operation that reads a
 * String off an entry channel and forwards it to a plugin.
 *
 * <p>Three pieces, always:
 *
 * <ol>
 *   <li>an <b>Operation</b> — the unit of work, run per Segment;
 *   <li>an <b>Operative</b> — the topology that wires Operations to Channels;
 *   <li><b>registration</b>, before {@code Flezha.newInstance(...)}.
 * </ol>
 *
 * <p>{@code Change = NoChange} means "no state to persist", which opts the whole
 * persistence machinery out — nothing is journaled, and a crash restarts cold.
 * {@link E02_StatefulCounter} is what you want as soon as that is not true.
 */
public final class E01_Echo {

    /** Operation<Input, State, Output, Change, Clock, Config>. */
    public static final class EchoOperation
            implements Operation<String, EchoOperation.Empty, String, NoChange, WallClock, Void> {

        /** No state: this Operation is a pure function of its input. */
        public record Empty() {}

        @Override
        public String name() {
            return "echo";
        }

        // Java erases generics, so the runtime needs these Class handles to wire
        // and validate the topology. Every Operation declares all five.
        @Override
        public Class<String> inputType() {
            return String.class;
        }

        @Override
        public Class<String> outputType() {
            return String.class;
        }

        @Override
        public Class<NoChange> changeType() {
            return NoChange.class;
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
        public Empty initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
            return new Empty();
        }

        @Override
        public void run(String input, StateHandle<Empty, NoChange> state, OperationCtx ctx) throws OperationException {
            ctx.emit("echo: " + input);
        }
    }

    public static final class EchoOperative implements UnboundedOperative {
        @Override
        public String factoryId() {
            return "echo";
        }

        @Override
        public void channels(ChannelBuilder c) {
            // Entry channels accept byte[] or String — they carry what arrives
            // from outside the operative.
            c.input(String.class, "in").into(EchoOperation.class);
            c.channel(String.class, "out").from(EchoOperation.class).toPlugin("my_sink");
        }
    }

    public static void main(String[] args) throws Exception {
        // Registration must happen BEFORE newInstance: the registry freezes there,
        // and the handshake tells the controller which factories this executor serves.
        Flezha.registerUnbounded(EchoOperative.class);

        try (Flezha sdk = Flezha.newInstance(args.length > 0 ? args[0] : "my_app")) {
            System.out.println("executor id: " + sdk.getId());
            Thread.currentThread().join();
        }
    }

    private E01_Echo() {}
}

package com.flezha.examples;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;

/**
 * <b>01 — the smallest thing that runs.</b> A stateless Procedure that reads a
 * String off an entry channel and forwards it to a plugin.
 *
 * <p>Three pieces, always:
 *
 * <ol>
 *   <li>an <b>Procedure</b> — the unit of work, run per Entity;
 *   <li>an <b>Operative</b> — the topology that wires Procedures to Channels;
 *   <li><b>declaration</b>, via {@code Flezha.createRunner(...).defineStream(...)}.
 * </ol>
 *
 * <p>{@code Change = NoChange} means "no state to persist", which opts the whole
 * persistence machinery out — nothing is journaled, and a crash restarts cold.
 * {@link E02_StatefulCounter} is what you want as soon as that is not true.
 */
public final class E01_Echo {

    /** Procedure<Input, State, Output, Change, Clock, Config>. */
    public static final class EchoProcedure
            implements Procedure<String, EchoProcedure.Empty, String, NoChange, WallClock, Void> {

        /** No state: this Procedure is a pure function of its input. */
        public record Empty() {}

        @Override
        public String name() {
            return "echo";
        }

        // Java erases generics, so the runtime needs these Class handles to wire
        // and validate the topology. Every Procedure declares all five.
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
        public Empty initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Empty();
        }

        @Override
        public void run(String input, StateHandle<Empty, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            ctx.emit("echo: " + input);
        }
    }

    public static final class EchoOperative implements StreamOperative {
        @Override
        public String factoryId() {
            return "echo";
        }

        @Override
        public void channels(ChannelBuilder c) {
            var sEchoProcedure = c.input(String.class, "in").into(EchoProcedure.class);
            sEchoProcedure.named("out").sink("my_sink");
        }
    }

    public static void main(String[] args) throws Exception {
        // Declare every operative type on the builder, then run(): connect,
        // then block until shutdown (SIGTERM or close() from another thread).
        // Use .start() instead of .run() to keep the calling thread.
        Flezha.createRunner(args.length > 0 ? args[0] : "my_app")
                .defineStream(EchoOperative.class)
                .run();
    }

    private E01_Echo() {}
}

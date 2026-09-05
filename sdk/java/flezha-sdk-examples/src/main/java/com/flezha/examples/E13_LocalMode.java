package com.flezha.examples;

import com.flezha.sdk.LocalOperative;
import com.flezha.sdk.channel.ChannelBuilder;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.procedure.Procedure;
import com.flezha.sdk.procedure.ProcedureCtx;
import com.flezha.sdk.procedure.ProcedureException;
import com.flezha.sdk.state.Change;
import com.flezha.sdk.state.CoalesceKey;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * <b>13 — a two-procedure graph run in this process, with no controller.</b>
 *
 * <p>Everything up to here needed a controller to see anything happen. This one
 * does not: {@link LocalOperative#start} takes the same operative class you
 * deploy, runs the {@code channels()} graph it already declares, and hands sink
 * output back to a callback. There is no local-mode member on the class — pass
 * {@code WordCount.class} to {@code Flezha.createRunner(ns).defineStream(..)}
 * and it deploys unchanged.
 *
 * <p>What runs is the deployed machinery: the entry channel's decode,
 * {@code keyBy} on the edge, one Entity per word with its own mailbox and
 * retry ladder, and the hop from {@code Split} to {@code Tally} performed by
 * the runtime rather than by this file.
 *
 * <p><b>State is held in memory and nothing is durable.</b> No write-ahead log,
 * no snapshots, no dead-letter queue — the run says so on stderr before it does
 * any work. Durability, plugins, placement and migration, the dead-letter
 * queue, observability export, authz and multi-runner scale all need a
 * controller.
 *
 * <p>Run it with:
 *
 * <pre>{@code
 * mvn -f sdk/java/pom.xml -q -pl flezha-sdk-examples -am \
 *     exec:java -Dexec.mainClass=com.flezha.examples.E13_LocalMode
 * }</pre>
 */
public final class E13_LocalMode {

    /** One line into its words: the hop to {@code Tally} is the runtime's job. */
    public static final class Split implements Procedure<String, Void, String, NoChange, WallClock, Void> {

        @Override
        public String name() {
            return "split";
        }

        @Override
        public Void initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return null;
        }

        @Override
        public void applyChange(Void state, NoChange change) {}

        @Override
        public void run(String line, StateHandle<Void, NoChange> state, ProcedureCtx ctx) throws ProcedureException {
            for (String word : line.split("\\s+")) {
                if (!word.isEmpty()) {
                    ctx.emit(word.toLowerCase());
                }
            }
        }
    }

    /** One occurrence of the word this Entity is. */
    public record Seen() implements Change {
        @Override
        public Optional<CoalesceKey> coalesceKey() {
            return Optional.empty();
        }
    }

    /** The running count for ONE word. */
    public static final class Count {
        public long seen;
    }

    public static final class Tally implements Procedure<String, Count, String, Seen, WallClock, Void> {

        @Override
        public String name() {
            return "tally";
        }

        @Override
        public List<Class<?>> kryoTypes() {
            return List.of(Seen.class, Count.class);
        }

        @Override
        public Count initState(Optional<String> entityKey, Optional<RestoreInitPayload> initPayload) {
            return new Count();
        }

        @Override
        public void applyChange(Count state, Seen change) {
            state.seen++;
        }

        @Override
        public void run(String word, StateHandle<Count, Seen> state, ProcedureCtx ctx) throws ProcedureException {
            state.apply(new Seen());
            ctx.emit(word + " " + state.get().seen);
        }
    }

    public static final class WordCount implements StreamOperative {
        @Override
        public String factoryId() {
            return "local_word_count_java";
        }

        @Override
        public void channels(ChannelBuilder c) {
            // `named("totals")` is what the sink callback registers against: a
            // sink channel takes the producing procedure's name unless the
            // declaration names it.
            c.input(String.class, "lines")
                    .into(Split.class)
                    .keyBy(word -> word)
                    .into(Tally.class)
                    .named("totals")
                    .keyed()
                    .sink("word_totals");
        }
    }

    public static void main(String[] args) {
        try (LocalOperative run = LocalOperative.start(WordCount.class)) {
            run.onSink(
                    "totals",
                    out -> System.out.println("totals <- " + new String(out.payload(), StandardCharsets.UTF_8)));
            run.feed("lines", "the quick brown fox");
            run.feed("lines", "the lazy dog");
            run.runUntilIdle(Duration.ofSeconds(10));
        }
    }

    private E13_LocalMode() {}
}

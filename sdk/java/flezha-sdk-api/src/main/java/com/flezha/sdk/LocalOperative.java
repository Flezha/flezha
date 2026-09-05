package com.flezha.sdk;

import com.flezha.sdk.internal.LocalRunHandle;
import com.flezha.sdk.internal.RuntimeBridge;
import com.flezha.sdk.operative.StreamOperative;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

/**
 * Run a declared operative graph in this process, with no controller.
 *
 * <p>{@link #start} takes the operative class you deploy and runs the
 * {@code channels()} graph it already declares — no local-mode member on the
 * class, no second declaration. What runs is the deployed machinery: entry
 * decode, edge transforms and keying, one Entity per resolved key with its
 * own mailbox, the retry ladder, and inter-procedure delivery performed by
 * the runtime.
 *
 * <pre>{@code
 * try (LocalOperative run = LocalOperative.start(WordCount.class)) {
 *     run.onSink("totals", out -> System.out.println(new String(out.payload())));
 *     run.feed("lines", "the quick brown fox");
 *     run.runUntilIdle(Duration.ofSeconds(5));
 * }
 * }</pre>
 *
 * <p><b>State is held in memory and nothing is durable.</b> Durability,
 * plugins, placement and migration, the dead-letter queue, observability
 * export, authz and multi-runner scale all need a controller; the run says so
 * on stderr every time it starts. {@code docs/quickstart.md} walks through it.
 */
public final class LocalOperative implements AutoCloseable {

    private static final String IMPL = "com.flezha.sdk.runtime.LocalImpl";

    private final LocalRunHandle handle;

    private LocalOperative(LocalRunHandle handle) {
        this.handle = handle;
    }

    /**
     * Register {@code type}'s declared graph and start it locally.
     *
     * @throws RuntimeException naming the capability when the declaration
     *     needs a controller
     */
    @Nonnull
    public static LocalOperative start(@Nonnull Class<? extends StreamOperative> type) {
        return new LocalOperative((LocalRunHandle)
                RuntimeBridge.invoke(IMPL, "start", new Class<?>[] {Class.class}, "Failed to start a local run", type));
    }

    /**
     * Deliver every value released on the sink {@code channel} to
     * {@code callback}, in release order. A sink channel takes the producing
     * procedure's name unless the declaration {@code named} it.
     */
    @Nonnull
    public LocalOperative onSink(@Nonnull String channel, @Nonnull Consumer<SinkOutput> callback) {
        handle.onSink(channel, callback);
        return this;
    }

    /** Feed one value's wire bytes to a declared entry channel. */
    @Nonnull
    public LocalOperative feed(@Nonnull String channel, @Nonnull byte[] payload) {
        handle.feed(channel, payload);
        return this;
    }

    /** {@link #feed(String, byte[])} with the text's UTF-8 bytes. */
    @Nonnull
    public LocalOperative feed(@Nonnull String channel, @Nonnull String payload) {
        return feed(channel, payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Block until the graph has nothing left to do.
     *
     * @throws RuntimeException when {@code budget} elapses first — a timeout
     *     is reported, never passed off as done
     */
    public void runUntilIdle(@Nonnull Duration budget) {
        handle.runUntilIdle(budget);
    }

    @Override
    public void close() {
        handle.close();
    }
}

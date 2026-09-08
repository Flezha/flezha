package com.flezha.sdk.runtime;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.Logger;
import com.flezha.sdk.SinkOutput;
import com.flezha.sdk.internal.LocalRunHandle;
import com.flezha.sdk.native_.FlzCore;
import com.flezha.sdk.operative.StreamOperative;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.core.ActivationCodec;
import com.flezha.sdk.runtime.core.ActivationDispatcher;
import com.flezha.sdk.runtime.core.HostClassDeclaration;
import com.flezha.sdk.runtime.core.Output;
import com.flezha.sdk.runtime.core.OutputTarget;
import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

/**
 * Runtime implementation of {@link LocalRunHandle}, resolved by reflection
 * from the public {@code LocalOperative}. The local twin of
 * {@link FlezhaImpl}: it declares the class exactly as a deployed runner
 * does and brings the same {@link PollHost} threads, and differs in one call
 * — {@code localStart} instead of {@code start}, so nothing dials a
 * controller. Sink output is drained from the core on a thread of its own
 * and handed to the callback registered for the channel.
 */
public final class LocalImpl implements LocalRunHandle {

    private static final Logger LOG = Flezha.log();

    /** Bounded so a stopped run's drain thread leaves promptly. */
    private static final long OUTPUT_POLL_MS = 100;

    private final long core;
    private final PollHost pollHost;
    private final Map<String, Consumer<SinkOutput>> sinks = new ConcurrentHashMap<>();
    private final Thread outputs;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private LocalImpl(long core, ActivationDispatcher dispatcher, FlezhaEnv.ResolvedConfig config) {
        this.core = core;
        this.pollHost = new PollHost(
                PollHost.of(core), dispatcher::dispatch, config.fetcherCount(), config.fetcherMax(), () -> {});
        this.outputs = new Thread(this::drainOutputs, "flz-local-sink");
        this.outputs.setDaemon(true);
    }

    /** Entry point invoked by reflection from {@code LocalOperative.start}. */
    @Nonnull
    public static LocalRunHandle start(@Nonnull Class<? extends StreamOperative> type) {
        String factoryId = StreamOperativeClassRegistry.registerClass(type);
        long core = FlzCore.newContext();
        try {
            FlzCore.declareClass(core, HostClassDeclaration.forFactory(factoryId));
            FlzCore.localStart(core, factoryId, factoryId + "-local");
        } catch (RuntimeException | Error e) {
            FlzCore.contextFree(core);
            throw e;
        }
        LocalImpl handle = new LocalImpl(
                core,
                new ActivationDispatcher(instanceId -> {
                    throw new IllegalStateException("local mode runs stream operatives, not workflows");
                }),
                FlezhaEnv.resolveFromEnv());
        handle.pollHost.start();
        handle.outputs.start();
        return handle;
    }

    @Override
    public void onSink(@Nonnull String channel, @Nonnull Consumer<SinkOutput> callback) {
        // Registered with the core first: a channel the declaration does not
        // carry is refused there, naming it, rather than silently accepted.
        FlzCore.localOnSink(core, channel);
        sinks.put(channel, callback);
    }

    @Override
    public void feed(@Nonnull String channel, @Nonnull byte[] payload) {
        FlzCore.localFeed(core, channel, payload);
    }

    @Override
    public void runUntilIdle(@Nonnull Duration budget) {
        FlzCore.localRunUntilIdle(core, budget.toMillis());
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        FlzCore.localStop(core);
        joinOutputs();
        // Not `FlzCore.shutdown`: that stops the process-wide poll core, and
        // a process may start another local run after this one.
        pollHost.stopFetching();
        FlzCore.contextFree(core);
    }

    /** One released value per pass, handed to whoever registered its channel. */
    private void drainOutputs() {
        while (!closed.get()) {
            byte[] encoded;
            try {
                encoded = FlzCore.localNextOutput(core, OUTPUT_POLL_MS);
            } catch (RuntimeException e) {
                // The run ended under us; the close path owns the teardown.
                return;
            }
            if (encoded == null) {
                continue;
            }
            deliver(ActivationCodec.decodeOutput(encoded));
        }
    }

    private void deliver(Output output) {
        String channel =
                switch (output.target()) {
                    case OutputTarget.Plugin plugin -> plugin.channel();
                        // The core queues what a sink released and nothing else; an edge
                        // output is an in-process mailbox push that never leaves.
                    case OutputTarget.Edge edge -> throw new IllegalStateException(
                            "local mode released an edge output on channel " + edge.channel());
                };
        Consumer<SinkOutput> callback = sinks.get(channel);
        if (callback == null) {
            LOG.warn("local mode: a value was released on sink `{}`, which has no callback", channel);
            return;
        }
        Map<String, String> metadata = new LinkedHashMap<>();
        output.metadata().forEach(entry -> metadata.put(entry.getKey(), entry.getValue()));
        callback.accept(new SinkOutput(channel, output.payload(), Map.copyOf(metadata)));
    }

    private void joinOutputs() {
        try {
            outputs.join(OUTPUT_POLL_MS * 20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

package com.flezha.sdk.runtime;

import com.flezha.sdk.Flezha;
import com.flezha.sdk.native_.FlezhaNativeException;
import com.flezha.sdk.native_.FlezhaStatus;
import com.flezha.sdk.native_.NativeEventDispatcher;
import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.relay.RelayRegistry;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.CommandRouter;
import com.flezha.sdk.runtime.control.ControlChannel;
import com.flezha.sdk.runtime.control.GracefulShutdown;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.reconnect.ReconnectCoordinator;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime implementation of the public {@link Flezha} interface. Resolved
 * via reflection from {@code Flezha.newInstance(namespace)} so the public
 * API module stays free of runtime / native dependencies.
 */
public final class FlezhaImpl implements Flezha {

    private static final Logger LOG = LoggerFactory.getLogger(FlezhaImpl.class);

    private final ComputeEngineContext ctx;
    private final ReconnectCoordinator reconnectCoordinator;
    private final AtomicReference<GracefulShutdown> shutdownRef;

    private FlezhaImpl(
            ComputeEngineContext ctx,
            ReconnectCoordinator reconnectCoordinator,
            AtomicReference<GracefulShutdown> shutdownRef) {
        this.ctx = ctx;
        this.reconnectCoordinator = reconnectCoordinator;
        this.shutdownRef = shutdownRef;
    }

    /**
     * Entry point invoked via reflection by {@link Flezha#newInstance(String)}.
     *
     * <p>Steps:
     * <ol>
     *   <li>Freeze the {@link RelayRegistry}.</li>
     *   <li>Resolve config from env vars; log it at INFO.</li>
     *   <li>Create the native context.</li>
     *   <li>Initialize the worker pool (consumed by every {@code RelayInstance}'s {@code ShardDispatcher}).</li>
     *   <li>Open the control stream and send the handshake init JSON.</li>
     *   <li>Block until the agent replies with {@code AssignComputeEngineId}.</li>
     * </ol>
     */
    @Nonnull
    public static Flezha start(@Nonnull String namespace) {
        if (RelayRegistry.isEmpty()) {
            throw new IllegalStateException(
                    "No relay classes registered; call Flezha.registerRelay(MyRelay.class) first.");
        }
        RelayRegistry.freeze();

        FlezhaEnv.ResolvedConfig resolved = FlezhaEnv.resolveFromEnv();
        FlezhaEnv.logConfig(resolved);

        long nativeHandle = NativeTransport.contextNew(resolved.config());
        if (nativeHandle == 0) {
            throw new IllegalStateException("NativeTransport.contextNew returned 0");
        }
        NativeTransport.installDefaultLogCallback(nativeHandle);

        ComputeEngineContext ctx = new ComputeEngineContext(namespace, resolved, nativeHandle);

        ExecutorService workerPool = Executors.newFixedThreadPool(
                Math.max(1, resolved.workerPoolSize()), new WorkerThreadFactory("flz-worker"));
        ctx.setWorkerPool(workerPool);

        CountDownLatch assignLatch = new CountDownLatch(1);
        ReconnectCoordinator[] coordinatorRef = new ReconnectCoordinator[1];

        NativeEventDispatcher eventDispatcher = new NativeEventDispatcher() {
            private final FrameParser controlParser = new FrameParser();
            private CommandRouter controlRouter;

            @Override
            public void onData(long streamId, byte[] data) {
                // Primary stream? Route to its per-stream reader.
                PrimaryStreamReader reader = ctx.primaryReaders().get(streamId);
                if (reader != null) {
                    reader.onData(data);
                    return;
                }
                // Otherwise it's the control stream.
                routeControlData(streamId, data);
            }

            @Override
            public void onStreamClosed(long streamId) {
                ctx.primaryReaders().remove(streamId);
            }

            @Override
            public void onConnectionLost(long streamId) {
                LOG.warn("connection lost on stream {}", streamId);
                if (coordinatorRef[0] != null) {
                    coordinatorRef[0].onConnectionLost();
                }
            }

            private void routeControlData(long streamId, byte[] data) {
                if (controlRouter == null) {
                    controlRouter = new CommandRouter(ctx, streamId);
                }
                Iterable<Frame> frames;
                try {
                    frames = controlParser.feed(data);
                } catch (Throwable t) {
                    LOG.error("control stream frame parse failed: {}", t.toString());
                    return;
                }
                for (Frame frame : frames) {
                    if (ControlChannel.HANDSHAKE_PAYLOAD_ID.equals(frame.payloadId())) {
                        continue;
                    }
                    try {
                        controlRouter.route(frame);
                    } catch (FlezhaNativeException e) {
                        // Agent closed the control socket (shutdown / disconnect)
                        // before the response write completed. Benign at teardown.
                        if (e.getStatus() == FlezhaStatus.IO_ERROR) {
                            LOG.debug("control stream closed before response: {}", e.getMessage());
                        } else {
                            LOG.error("control stream route failed: {}", e.toString());
                        }
                        return;
                    } catch (Throwable t) {
                        LOG.error("control stream route failed: {}", t.toString());
                        return;
                    }
                    if (ctx.computeEngineId() != null
                            && !ctx.computeEngineId().equals("undefined")
                            && assignLatch.getCount() > 0) {
                        assignLatch.countDown();
                    }
                }
            }
        };
        NativeTransport.setDispatcher(nativeHandle, eventDispatcher);

        // Open the control stream and send the handshake.
        long controlStreamId = NativeTransport.openControlStream(nativeHandle);
        ctx.streamRegistry().setControlStreamId(controlStreamId);
        try {
            new ControlChannel(ctx, controlStreamId).sendHandshake(namespace);
        } catch (Exception e) {
            NativeTransport.contextDestroy(nativeHandle);
            throw new IllegalStateException("Failed to send handshake: " + e.getMessage(), e);
        }

        // Block until AssignComputeEngineId arrives.
        try {
            if (!assignLatch.await(30, TimeUnit.SECONDS)) {
                LOG.warn("Timed out waiting for AssignComputeEngineId from agent");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // The shutdown holder is updated on every reconnect because the
        // control stream id changes; the JVM hook resolves it lazily so
        // both the initial and the post-reconnect value are honoured.
        AtomicReference<GracefulShutdown> shutdownRef =
                new AtomicReference<>(new GracefulShutdown(ctx, controlStreamId));

        ReconnectCoordinator coordinator = new ReconnectCoordinator(ctx, () -> {
            // Reconnect hook: attempt to re-open the control stream and
            // re-send the handshake. Returning true means "succeeded".
            try {
                long newControl = NativeTransport.openControlStream(nativeHandle);
                ctx.streamRegistry().setControlStreamId(newControl);
                new ControlChannel(ctx, newControl).sendHandshake(namespace);
                shutdownRef.set(new GracefulShutdown(ctx, newControl));
                return true;
            } catch (Throwable t) {
                return false;
            }
        });
        coordinator.start();
        coordinatorRef[0] = coordinator;

        // Register a JVM shutdown hook so SIGTERM (Kubernetes pre-stop)
        // triggers the graceful-shutdown sequence even when the user
        // never calls close() explicitly. Idempotent with close() via
        // the GracefulShutdown's internal AtomicBoolean.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            GracefulShutdown active = shutdownRef.get();
            if (active != null) {
                active.run();
            }
        }, "flezha-graceful-shutdown"));

        return new FlezhaImpl(ctx, coordinator, shutdownRef);
    }

    @Override
    @Nonnull
    public String getId() {
        return ctx.computeEngineId();
    }

    @Override
    @Nonnull
    public Map<String, ?> getProcessors() {
        return ctx.relayInstanceRegistry().snapshot();
    }

    private static final class WorkerThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger counter = new AtomicInteger();

        WorkerThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }

    @Override
    public void close() {
        GracefulShutdown active = shutdownRef.get();
        if (active != null) {
            active.run();
        }
    }
}

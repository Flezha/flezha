package com.flezha.sdk.runtime;

import com.flezha.sdk.OperativeTypeInUseException;
import com.flezha.sdk.internal.RunnerHandle;
import com.flezha.sdk.native_.FlezhaNativeException;
import com.flezha.sdk.native_.FlezhaStatus;
import com.flezha.sdk.native_.NativeEventDispatcher;
import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.operative.UnboundedOperative;
import com.flezha.sdk.procedure.SpawnRequests;
import com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry;
import com.flezha.sdk.runtime.bounded.BoundedSpec;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.CommandRouter;
import com.flezha.sdk.runtime.control.ControlChannel;
import com.flezha.sdk.runtime.control.DataHeartbeatSender;
import com.flezha.sdk.runtime.control.FactorySync;
import com.flezha.sdk.runtime.control.GracefulShutdown;
import com.flezha.sdk.runtime.control.KeepaliveSender;
import com.flezha.sdk.runtime.control.SpawnRequester;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.Operative;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.reconnect.ReconnectCoordinator;
import com.flezha.sdk.runtime.state.EntityStateSink;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime implementation of the internal {@link RunnerHandle} SPI the public
 * {@code FlezhaRunner} holds. Resolved via reflection from
 * {@code FlezhaRunner.start()} so the public API module stays free of
 * runtime / native dependencies. The api-side runner owns the per-runner
 * declared set and the JVM-wide singleton guard; this class owns connect +
 * lifecycle + the race-transparent type-removal check.
 */
public final class FlezhaImpl implements RunnerHandle {

    private static final Logger LOG = LoggerFactory.getLogger(FlezhaImpl.class);

    private final RunnerContext ctx;
    private final ReconnectCoordinator reconnectCoordinator;
    private final KeepaliveSender keepaliveSender;
    private final DataHeartbeatSender dataHeartbeatSender;
    private final AtomicReference<GracefulShutdown> shutdownRef;
    private final FactorySync factorySync;
    private final Runnable onTerminated;
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private FlezhaImpl(
            RunnerContext ctx,
            ReconnectCoordinator reconnectCoordinator,
            KeepaliveSender keepaliveSender,
            DataHeartbeatSender dataHeartbeatSender,
            AtomicReference<GracefulShutdown> shutdownRef,
            Runnable onTerminated) {
        this.ctx = ctx;
        this.reconnectCoordinator = reconnectCoordinator;
        this.keepaliveSender = keepaliveSender;
        this.dataHeartbeatSender = dataHeartbeatSender;
        this.shutdownRef = shutdownRef;
        this.factorySync = new FactorySync(ctx);
        // The api layer cannot see the wire, so the control-plane requester
        // behind `ctx.spawnBounded` is installed here and dropped on close.
        SpawnRequests.install(new SpawnRequester(ctx));
        this.onTerminated = onTerminated;
    }

    /**
     * Entry point invoked via reflection by {@code FlezhaRunner.start()}.
     * {@code declaredFactoryIds} supplies the runner's CURRENT declared
     * factory set — read at send time, it feeds the initial handshake AND
     * every reconnect re-handshake, so post-start defines/removes resync
     * naturally. {@code onTerminated} is the api layer's release hook for the
     * JVM-wide runner slot; it runs exactly once, when {@link #close()}
     * completes.
     *
     * <p>Steps:
     * <ol>
     *   <li>Resolve config from env vars; log it at INFO.</li>
     *   <li>Create the native context.</li>
     *   <li>Initialize the worker pool (consumed by every {@code UnboundedOperativeInstance}'s {@code ProcedureDispatcher}).</li>
     *   <li>Open the control stream and send the handshake init JSON.</li>
     *   <li>Block until the controller replies with {@code AssignRunnerId}.</li>
     * </ol>
     */
    @Nonnull
    public static RunnerHandle start(
            @Nonnull String namespace,
            @Nonnull Supplier<List<String>> declaredFactoryIds,
            @Nonnull Runnable onTerminated) {
        FlezhaEnv.ResolvedConfig resolved = FlezhaEnv.resolveFromEnv();
        FlezhaEnv.logConfig(resolved);

        long nativeHandle = NativeTransport.contextNew(resolved.config());
        if (nativeHandle == 0) {
            throw new IllegalStateException("NativeTransport.contextNew returned 0");
        }
        NativeTransport.installDefaultLogCallback(nativeHandle);

        RunnerContext ctx = new RunnerContext(namespace, resolved, nativeHandle);

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
                    if (KeepaliveSender.CONTROL_PING_PAYLOAD_ID.equals(frame.payloadId())) {
                        // Controller's keepalive echo; the read-level idle
                        // watchdog already re-armed in flz_jni. Nothing to route.
                        continue;
                    }
                    try {
                        controlRouter.route(frame);
                    } catch (FlezhaNativeException e) {
                        // Controller closed the control socket (shutdown / disconnect)
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
                    if (ctx.runnerId() != null && !ctx.runnerId().equals("undefined") && assignLatch.getCount() > 0) {
                        assignLatch.countDown();
                    }
                }
            }
        };
        // The shutdown holder is updated on every reconnect because the
        // control stream id changes; the JVM hook resolves it lazily so
        // both the initial and the post-reconnect value are honoured. Starts
        // null and is set once the first control stream opens.
        AtomicReference<GracefulShutdown> shutdownRef = new AtomicReference<>(null);

        // Build and start the coordinator BEFORE wiring the dispatcher and
        // opening the control stream, so an early onConnectionLost (the flz_jni
        // control reader's ack/idle deadline firing) is never dropped because
        // coordinatorRef[0] was still null — the prior startup race.
        ReconnectCoordinator coordinator = new ReconnectCoordinator(ctx, () -> {
            // Reconnect hook: re-open the control stream and re-send the
            // handshake. Returning true means "succeeded".
            try {
                long newControl = NativeTransport.openControlStream(nativeHandle);
                ctx.streamRegistry().setControlStreamId(newControl);
                new ControlChannel(ctx, newControl).sendHandshake(namespace, declaredFactoryIds.get());
                shutdownRef.set(new GracefulShutdown(ctx, newControl));
                return true;
            } catch (Throwable t) {
                return false;
            }
        });
        coordinatorRef[0] = coordinator;
        coordinator.start();

        // A broken-pipe flood on the data stream (control channel still alive)
        // drives a full control reconnect via the coordinator — the same
        // recovery path as an observed connection-lost event.
        EntityStateSink.installReconnectRequester(coordinator::onConnectionLost);

        NativeTransport.setDispatcher(nativeHandle, eventDispatcher);

        // Open the control stream and send the handshake. A failure here drives
        // reconnect rather than stranding a dead start() (the runner keeps
        // retrying until it lands on a live controller).
        long controlStreamId = NativeTransport.openControlStream(nativeHandle);
        ctx.streamRegistry().setControlStreamId(controlStreamId);
        shutdownRef.set(new GracefulShutdown(ctx, controlStreamId));
        try {
            // The handshake presents the runner's self-owned stable id (the
            // same on initial connect and reconnect), so the controller resolves
            // a reconnect as a binding swap.
            new ControlChannel(ctx, controlStreamId).sendHandshake(namespace, declaredFactoryIds.get());
        } catch (Exception e) {
            LOG.warn("Initial handshake send failed: {}; driving reconnect", e.toString());
            coordinator.onConnectionLost();
        }

        // Steady-state control-channel keepalive: keeps a quiet-but-healthy
        // channel inside the flz_jni read-level idle window (so the watchdog
        // never false-fires) and follows reconnects via the stream registry.
        KeepaliveSender keepalive = new KeepaliveSender(ctx);
        keepalive.start();

        // Steady-state data-channel heartbeat: keeps the controller's per-channel
        // liveness clock fresh even when a operative produces no real output.
        DataHeartbeatSender dataHeartbeat = new DataHeartbeatSender(ctx);
        dataHeartbeat.start();

        // Bound the wait for the controller's first frame (the ack). On timeout
        // drive reconnect instead of warn-and-continue; the coordinator keeps
        // retrying in the background, so start() returns a live-eventually SDK.
        try {
            long ackMs = FlezhaEnv.controlAckTimeoutMs();
            if (!assignLatch.await(ackMs, TimeUnit.MILLISECONDS)) {
                LOG.warn("No AssignRunnerId within {} ms; driving reconnect", ackMs);
                coordinator.onConnectionLost();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Register a JVM shutdown hook so SIGTERM (Kubernetes pre-stop)
        // triggers the graceful-shutdown sequence even when the user
        // never calls close() explicitly. Idempotent with close() via
        // the GracefulShutdown's internal AtomicBoolean.
        Runtime.getRuntime()
                .addShutdownHook(new Thread(
                        () -> {
                            GracefulShutdown active = shutdownRef.get();
                            if (active != null) {
                                active.run();
                            }
                        },
                        "flezha-graceful-shutdown"));

        return new FlezhaImpl(ctx, coordinator, keepalive, dataHeartbeat, shutdownRef, onTerminated);
    }

    @Override
    @Nonnull
    public String runnerId() {
        return ctx.runnerId();
    }

    @Override
    @Nonnull
    public Map<String, ?> operatives() {
        return ctx.operativeInstanceRegistry().snapshot();
    }

    /**
     * Remove {@code factoryId}'s class from the runtime registries:
     * unregister-first, then scan live instances — a live hit re-registers
     * the class and refuses. Java has no single coordinator thread, so the
     * transient unregistered window is the race posture: an AddOperative
     * landing inside it fails as an unknown factory and convergence
     * re-places, the same failure mode as a runner dying mid-placement.
     */
    @Override
    public void removeType(@Nonnull String factoryId) {
        Class<? extends UnboundedOperative> removedClass = UnboundedOperativeClassRegistry.unregisterClass(factoryId);
        BoundedSpec removedSpec =
                removedClass == null ? BoundedOperativeClassRegistry.unregisterClass(factoryId) : null;
        if (removedClass == null && removedSpec == null) {
            // Nothing registered runtime-side (the api layer already
            // validated the declared set) — nothing to remove.
            return;
        }

        int live = countLiveInstances(factoryId);
        if (live > 0) {
            if (removedClass != null) {
                UnboundedOperativeClassRegistry.registerClass(removedClass);
            } else {
                BoundedOperativeClassRegistry.restore(factoryId, removedSpec);
            }
            throw new OperativeTypeInUseException("factory '" + factoryId + "' has " + live
                    + " live operative instance(s); stop them before removing the type");
        }
    }

    @Override
    public void syncFactories(@Nonnull List<String> factoryIds) {
        factorySync.ship(factoryIds);
    }

    private int countLiveInstances(String factoryId) {
        int live = 0;
        for (Operative proc : ctx.operativeInstanceRegistry().snapshot().values()) {
            String fid = proc instanceof UnboundedOperativeInstance ri
                    ? ri.topology().factoryId()
                    : ((BoundedOperativeInstance) proc).factoryId();
            if (factoryId.equals(fid)) {
                live++;
            }
        }
        return live;
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
    public void awaitTermination() throws InterruptedException {
        terminationLatch.await();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        SpawnRequests.uninstall();
        keepaliveSender.stop();
        dataHeartbeatSender.stop();
        reconnectCoordinator.shutdown();
        GracefulShutdown active = shutdownRef.get();
        if (active != null) {
            active.run();
        }
        // Release the JVM-wide runner slot before waking waiters, so a thread
        // unblocked from awaitTermination() can immediately start a new runner.
        onTerminated.run();
        terminationLatch.countDown();
    }
}

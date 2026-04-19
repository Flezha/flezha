package com.flezha.sdk.runtime;

import com.flezha.sdk.FactoryRegistry;
import com.flezha.sdk.Flezha;
import com.flezha.sdk.native_.NativeEventDispatcher;
import com.flezha.sdk.native_.NativeTransport;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.control.CommandRouter;
import com.flezha.sdk.runtime.control.ControlChannel;
import com.flezha.sdk.runtime.framing.Frame;
import com.flezha.sdk.runtime.framing.FrameParser;
import com.flezha.sdk.runtime.reconnect.ReconnectCoordinator;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import com.flezha.sdk.runtime.worker.DefaultProcessorDispatcher;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

    private FlezhaImpl(ComputeEngineContext ctx, ReconnectCoordinator reconnectCoordinator) {
        this.ctx = ctx;
        this.reconnectCoordinator = reconnectCoordinator;
    }

    /**
     * Entry point invoked via reflection by {@link Flezha#newInstance(String)}.
     *
     * <p>Steps:
     * <ol>
     *   <li>Freeze the {@link FactoryRegistry}.</li>
     *   <li>Resolve config from env vars; log it at INFO.</li>
     *   <li>Create the native context.</li>
     *   <li>Register the dispatcher (primary stream readers + connection-lost).</li>
     *   <li>Open the control stream and send the handshake init JSON.</li>
     *   <li>Block until the agent replies with {@code AssignComputeEngineId}.</li>
     * </ol>
     */
    @Nonnull
    public static Flezha start(@Nonnull String namespace) {
        if (FactoryRegistry.isEmpty()) {
            throw new IllegalStateException(
                    "No factories registered; call FactoryRegistry.registerFactory(...) first.");
        }
        FactoryRegistry.freeze();

        FlezhaEnv.ResolvedConfig resolved = FlezhaEnv.resolveFromEnv();
        FlezhaEnv.logConfig(resolved);

        long nativeHandle = NativeTransport.contextNew(resolved.config());
        if (nativeHandle == 0) {
            throw new IllegalStateException("NativeTransport.contextNew returned 0");
        }
        NativeTransport.installDefaultLogCallback(nativeHandle);

        ComputeEngineContext ctx = new ComputeEngineContext(namespace, resolved, nativeHandle);

        DefaultProcessorDispatcher dispatcher =
                new DefaultProcessorDispatcher(ctx, resolved.workerPoolSize());
        ctx.setProcessorDispatcher(dispatcher);

        // Dispatcher will route events by streamId: every opened stream
        // registers its own PrimaryStreamReader in streamReaders below.
        Map<Long, PrimaryStreamReader> streamReaders = new ConcurrentHashMap<>();

        CountDownLatch assignLatch = new CountDownLatch(1);
        ReconnectCoordinator[] coordinatorRef = new ReconnectCoordinator[1];

        NativeEventDispatcher eventDispatcher = new NativeEventDispatcher() {
            @Override
            public void onData(long streamId, byte[] data) {
                PrimaryStreamReader reader = streamReaders.get(streamId);
                if (reader != null) {
                    reader.onData(data);
                    return;
                }
                // Control-stream frames aren't registered in streamReaders;
                // they are handled by the latch-driven control loop below.
                routeControlData(streamId, data);
            }

            @Override
            public void onStreamClosed(long streamId) {
                streamReaders.remove(streamId);
            }

            @Override
            public void onConnectionLost(long streamId) {
                LOG.warn("connection lost on stream {}", streamId);
                if (coordinatorRef[0] != null) {
                    coordinatorRef[0].onConnectionLost();
                }
            }

            private final FrameParser controlParser = new FrameParser();
            private CommandRouter controlRouter;

            private void routeControlData(long streamId, byte[] data) {
                if (controlRouter == null) {
                    controlRouter = new CommandRouter(ctx, streamId);
                }
                for (Frame frame : controlParser.feed(data)) {
                    if (ControlChannel.HANDSHAKE_PAYLOAD_ID.equals(frame.payloadId())) {
                        continue;
                    }
                    controlRouter.route(frame);
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

        // Register PrimaryStreamReaders for any already-opened primary streams.
        // In practice the agent sends ConnectSocket later, at which point the
        // handler opens streams and we need to register readers. The
        // ConnectSocketHandler does this directly via streamRegistry + this
        // map — but because the reader must know the ctx, we expose a hook.
        ctx.streamRegistry().clear(); // fresh start; handler will re-populate

        ReconnectCoordinator coordinator = new ReconnectCoordinator(ctx, () -> {
            // Reconnect hook: attempt to re-open the control stream and
            // re-send the handshake. Returning true means "succeeded".
            try {
                long newControl = NativeTransport.openControlStream(nativeHandle);
                ctx.streamRegistry().setControlStreamId(newControl);
                new ControlChannel(ctx, newControl).sendHandshake(namespace);
                return true;
            } catch (Throwable t) {
                return false;
            }
        });
        coordinator.start();
        coordinatorRef[0] = coordinator;

        return new FlezhaImpl(ctx, coordinator);
    }

    @Override
    @Nonnull
    public String getId() {
        return ctx.computeEngineId();
    }

    @Override
    @Nonnull
    public Map<String, ?> getProcessors() {
        return ctx.processorRegistry().snapshot();
    }
}

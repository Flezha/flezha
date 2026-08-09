package com.flezha.sdk.runtime;

import com.flezha.sdk.OperativeTypeInUseException;
import com.flezha.sdk.internal.RunnerHandle;
import com.flezha.sdk.native_.FlzCore;
import com.flezha.sdk.procedure.SpawnRequests;
import com.flezha.sdk.runtime.bounded.BoundedOperativeClassRegistry;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.core.Activation;
import com.flezha.sdk.runtime.core.ActivationCodec;
import com.flezha.sdk.runtime.core.ActivationDispatcher;
import com.flezha.sdk.runtime.core.ActivationResult;
import com.flezha.sdk.runtime.core.BoundedSession;
import com.flezha.sdk.runtime.core.CoreSpawnRequester;
import com.flezha.sdk.runtime.core.HostClassDeclaration;
import com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime implementation of the internal {@link RunnerHandle} SPI the public
 * {@code FlezhaRunner} holds, resolved via reflection from
 * {@code FlezhaRunner.start()}. Hosts the poll core: declares every
 * registered class as {@code HostClassDecl} JSON, starts the core's runner
 * loop, and drives N poller threads through
 * {@code poll → ActivationDispatcher.dispatch → complete}. The core owns the
 * wire — transport, framing, reconnect, retry ladders, durability records —
 * so this class owns only declaration and lifecycle.
 */
public final class FlezhaImpl implements RunnerHandle {

    private static final Logger LOG = LoggerFactory.getLogger(FlezhaImpl.class);

    /** Poll timeout per loop turn; bounds how fast close() can join a poller. */
    private static final long POLL_TIMEOUT_MS = 250;

    private final String namespace;
    private final long core;
    private final ActivationDispatcher dispatcher;
    private final List<Thread> pollers;
    private final Set<String> declared;
    private final Runnable onTerminated;
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger livePollers;

    private FlezhaImpl(
            String namespace,
            long core,
            ActivationDispatcher dispatcher,
            List<Thread> pollers,
            Set<String> declared,
            Runnable onTerminated) {
        this.namespace = namespace;
        this.core = core;
        this.dispatcher = dispatcher;
        this.pollers = pollers;
        this.declared = declared;
        this.onTerminated = onTerminated;
        this.livePollers = new AtomicInteger(0);
    }

    /**
     * Entry point invoked via reflection by {@code FlezhaRunner.start()}.
     * {@code declaredFactoryIds} supplies the runner's declared factory set;
     * {@code onTerminated} is the api layer's release hook for the JVM-wide
     * runner slot, run exactly once when {@link #close()} completes.
     */
    @Nonnull
    public static RunnerHandle start(
            @Nonnull String namespace,
            @Nonnull Supplier<List<String>> declaredFactoryIds,
            @Nonnull Runnable onTerminated) {
        int poolSize = FlezhaEnv.resolveFromEnv().workerPoolSize();

        long core = FlzCore.newContext();
        Set<String> declared = ConcurrentHashMap.newKeySet();
        try {
            for (String factoryId : declaredFactoryIds.get()) {
                FlzCore.declareClass(core, HostClassDeclaration.forFactory(factoryId));
                declared.add(factoryId);
            }
            FlzCore.start(core, namespace);
        } catch (RuntimeException | Error e) {
            FlzCore.contextFree(core);
            throw e;
        }

        ActivationDispatcher dispatcher =
                new ActivationDispatcher(instanceId -> new FlzCoreBoundedSession(core, instanceId));
        SpawnRequests.install(new CoreSpawnRequester(core));

        List<Thread> pollers = new ArrayList<>();
        FlezhaImpl handle = new FlezhaImpl(namespace, core, dispatcher, pollers, declared, onTerminated);
        for (int i = 0; i < Math.max(1, poolSize); i++) {
            Thread t = new Thread(handle::pollLoop, "flz-poller-" + (i + 1));
            t.setDaemon(true);
            pollers.add(t);
        }
        handle.livePollers.set(pollers.size());
        pollers.forEach(Thread::start);

        // SIGTERM (Kubernetes pre-stop) triggers the graceful teardown even
        // when the user never calls close(). Idempotent with close().
        Runtime.getRuntime().addShutdownHook(new Thread(handle::close, "flezha-graceful-shutdown"));

        LOG.info("Flezha runner started on namespace `{}` with {} poller(s)", namespace, pollers.size());
        return handle;
    }

    /**
     * One poller loop: poll ≤{@link #POLL_TIMEOUT_MS}, dispatch, complete.
     * Per-activation failures are logged, never rethrown — a poller must not
     * die on one bad activation. A zero-length poll result means the core is
     * shutting down and ends the loop.
     */
    private void pollLoop() {
        while (true) {
            byte[] bytes;
            try {
                bytes = FlzCore.poll(core, POLL_TIMEOUT_MS);
            } catch (Throwable t) {
                if (closed.get()) {
                    break;
                }
                LOG.error("core poll failed: {}", t.toString());
                continue;
            }
            if (bytes == null) {
                continue;
            }
            if (bytes.length == 0) {
                break;
            }
            try {
                Activation activation = ActivationCodec.decodeActivation(bytes);
                ActivationResult result = dispatcher.dispatch(activation);
                FlzCore.complete(core, ActivationCodec.encodeResult(result));
            } catch (Throwable t) {
                LOG.error("activation processing failed: {}", t.toString());
            }
        }
        if (livePollers.decrementAndGet() == 0) {
            terminationLatch.countDown();
        }
    }

    /**
     * The runner's namespace. The core owns the controller-assigned wire
     * runner id (it rotates across the reconnects the core handles
     * internally), so the namespace is the stable host-side identity.
     */
    @Override
    @Nonnull
    public String runnerId() {
        return namespace;
    }

    @Override
    @Nonnull
    public Map<String, ?> operatives() {
        return dispatcher.instances();
    }

    @Override
    public void removeType(@Nonnull String factoryId) {
        if (dispatcher.hasInstanceOf(factoryId)) {
            throw new OperativeTypeInUseException(
                    "factory '" + factoryId + "' has live operative instance(s); stop them before removing the type");
        }
        if (UnboundedOperativeClassRegistry.unregisterClass(factoryId) == null) {
            BoundedOperativeClassRegistry.unregisterClass(factoryId);
        }
        declared.remove(factoryId);
    }

    /**
     * Post-start define/remove resync. New ids are declared to the core;
     * removals have no ABI surface (the core keeps the declaration until the
     * context ends) — a known limitation, logged and otherwise a no-op.
     */
    @Override
    public void syncFactories(@Nonnull List<String> factoryIds) {
        for (String factoryId : factoryIds) {
            if (declared.contains(factoryId)) {
                continue;
            }
            if (UnboundedOperativeClassRegistry.getTopology(factoryId) == null
                    && BoundedOperativeClassRegistry.getSpec(factoryId) == null) {
                LOG.warn("syncFactories: `{}` is not in the class registries; skipping", factoryId);
                continue;
            }
            try {
                FlzCore.declareClass(core, HostClassDeclaration.forFactory(factoryId));
                declared.add(factoryId);
            } catch (RuntimeException e) {
                LOG.error("declaring `{}` post-start failed: {}", factoryId, e.getMessage());
            }
        }
        for (String known : declared) {
            if (!factoryIds.contains(known)) {
                LOG.debug("syncFactories: removal of `{}` has no core surface; declaration stays until close", known);
            }
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
        FlzCore.shutdown(core);
        for (Thread poller : pollers) {
            try {
                poller.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        FlzCore.contextFree(core);
        // Release the JVM-wide runner slot before waking waiters, so a thread
        // unblocked from awaitTermination() can immediately start a new runner.
        onTerminated.run();
        terminationLatch.countDown();
    }

    /** Production {@link BoundedSession}: the core's per-instance bounded ABI. */
    private static final class FlzCoreBoundedSession implements BoundedSession {
        private final long core;
        private final String instanceId;

        FlzCoreBoundedSession(long core, String instanceId) {
            this.core = core;
            this.instanceId = instanceId;
        }

        @Override
        public void emit(@Nonnull byte[] payload) {
            FlzCore.boundedEmit(core, instanceId, payload);
        }

        @Override
        @Nullable
        public byte[] recv(long timeoutMs) {
            return FlzCore.boundedRecv(core, instanceId, timeoutMs);
        }

        @Override
        public boolean cancelled() {
            return FlzCore.boundedCancelled(core, instanceId);
        }
    }
}

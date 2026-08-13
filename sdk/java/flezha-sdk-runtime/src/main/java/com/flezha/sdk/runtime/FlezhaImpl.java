package com.flezha.sdk.runtime;

import com.flezha.sdk.OperativeTypeInUseException;
import com.flezha.sdk.internal.RunnerHandle;
import com.flezha.sdk.native_.FlzCore;
import com.flezha.sdk.procedure.SpawnRequests;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.core.ActivationDispatcher;
import com.flezha.sdk.runtime.core.CoreSpawnRequester;
import com.flezha.sdk.runtime.core.HostClassDeclaration;
import com.flezha.sdk.runtime.core.WorkflowSession;
import com.flezha.sdk.runtime.operative.StreamOperativeClassRegistry;
import com.flezha.sdk.runtime.workflow.WorkflowOperativeClassRegistry;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * loop, and brings the threads the core's activations run on (see
 * {@link PollHost}). The core owns the wire — transport, framing, reconnect,
 * retry ladders, durability records — so this class owns only declaration and
 * lifecycle.
 */
public final class FlezhaImpl implements RunnerHandle {

    private static final Logger LOG = LoggerFactory.getLogger(FlezhaImpl.class);

    private final String namespace;
    private final long core;
    private final ActivationDispatcher dispatcher;
    private final PollHost pollHost;
    private final Set<String> declared;
    private final Runnable onTerminated;
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private FlezhaImpl(
            String namespace,
            long core,
            ActivationDispatcher dispatcher,
            int fetchers,
            Set<String> declared,
            Runnable onTerminated) {
        this.namespace = namespace;
        this.core = core;
        this.dispatcher = dispatcher;
        this.declared = declared;
        this.onTerminated = onTerminated;
        this.pollHost = new PollHost(
                new PollHost.CoreCalls() {
                    @Override
                    @Nullable
                    public byte[] poll(long timeoutMs) {
                        return FlzCore.poll(core, timeoutMs);
                    }

                    @Override
                    public void complete(@Nonnull byte[] result) {
                        FlzCore.complete(core, result);
                    }
                },
                dispatcher::dispatch,
                fetchers,
                terminationLatch::countDown);
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
        int fetchers = FlezhaEnv.resolveFromEnv().fetcherCount();

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
                new ActivationDispatcher(instanceId -> new FlzCoreWorkflowSession(core, instanceId));
        SpawnRequests.install(new CoreSpawnRequester(core));

        FlezhaImpl handle = new FlezhaImpl(namespace, core, dispatcher, fetchers, declared, onTerminated);
        handle.pollHost.start();

        // SIGTERM (Kubernetes pre-stop) triggers the graceful teardown even
        // when the user never calls close(). Idempotent with close().
        Runtime.getRuntime().addShutdownHook(new Thread(handle::close, "flezha-graceful-shutdown"));

        LOG.info(
                "Flezha runner started on namespace `{}` with {} fetcher(s); activations run on virtual threads",
                namespace,
                handle.pollHost.fetcherCount());
        return handle;
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
        if (StreamOperativeClassRegistry.unregisterClass(factoryId) == null) {
            WorkflowOperativeClassRegistry.unregisterClass(factoryId);
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
            if (StreamOperativeClassRegistry.getTopology(factoryId) == null
                    && WorkflowOperativeClassRegistry.getSpec(factoryId) == null) {
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
        // Drains the in-flight activations too — each still owes the core a
        // `complete` on this context, which the free below invalidates.
        pollHost.close();
        FlzCore.contextFree(core);
        // Release the JVM-wide runner slot before waking waiters, so a thread
        // unblocked from awaitTermination() can immediately start a new runner.
        onTerminated.run();
        terminationLatch.countDown();
    }

    /** Production {@link WorkflowSession}: the core's per-instance workflow ABI. */
    private static final class FlzCoreWorkflowSession implements WorkflowSession {
        private final long core;
        private final String instanceId;

        FlzCoreWorkflowSession(long core, String instanceId) {
            this.core = core;
            this.instanceId = instanceId;
        }

        @Override
        public void emit(@Nonnull byte[] payload) {
            FlzCore.workflowEmit(core, instanceId, payload);
        }

        @Override
        @Nullable
        public byte[] recv(long timeoutMs) {
            return FlzCore.workflowRecv(core, instanceId, timeoutMs);
        }

        @Override
        public boolean cancelled() {
            return FlzCore.workflowCancelled(core, instanceId);
        }
    }
}

package com.flezha.sdk.runtime;

import com.flezha.sdk.OperativeTypeInUseException;
import com.flezha.sdk.internal.RunnerHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

/**
 * Test double for the runtime entry point, resolved reflectively by
 * {@code FlezhaRunner.start()} (the real one lives in
 * {@code flezha-sdk-runtime} and needs native transport + a controller).
 * Implements the {@code RunnerHandle} SPI with the real lifecycle contract:
 * {@code close()} is idempotent, runs the api layer's {@code onTerminated}
 * release hook, then wakes every thread blocked in
 * {@code awaitTermination()}. Live instances are fakeable per factory id
 * ({@link #LIVE_INSTANCES}) so removal-refusal tests need no controller;
 * {@code syncFactories} records every shipped set ({@link #SYNCED_SETS}).
 */
public final class FlezhaImpl implements RunnerHandle {

    /** Successful start() count — guard tests assert a rejected start never reaches the runtime. */
    public static final AtomicInteger STARTS = new AtomicInteger();

    /** Fakeable live-instance counts per factory id, consulted by {@link #removeType(String)}. */
    public static final Map<String, Integer> LIVE_INSTANCES = new ConcurrentHashMap<>();

    /** Every factory set {@link #syncFactories(List)} shipped, in call order. */
    public static final List<List<String>> SYNCED_SETS = new ArrayList<>();

    /** The declared-set supplier of the LAST started runner — handshake/reconnect resync assertions. */
    public static volatile Supplier<List<String>> LAST_DECLARED_SUPPLIER;

    private final String namespace;
    private final Runnable onTerminated;
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private FlezhaImpl(String namespace, Runnable onTerminated) {
        this.namespace = namespace;
        this.onTerminated = onTerminated;
    }

    @Nonnull
    public static RunnerHandle start(
            @Nonnull String namespace,
            @Nonnull Supplier<List<String>> declaredFactoryIds,
            @Nonnull Runnable onTerminated) {
        STARTS.incrementAndGet();
        LAST_DECLARED_SUPPLIER = declaredFactoryIds;
        return new FlezhaImpl(namespace, onTerminated);
    }

    /** Test hook: clear the fake state between tests. */
    public static void resetForTests() {
        LIVE_INSTANCES.clear();
        synchronized (SYNCED_SETS) {
            SYNCED_SETS.clear();
        }
        LAST_DECLARED_SUPPLIER = null;
    }

    @Override
    @Nonnull
    public String runnerId() {
        return namespace;
    }

    @Override
    @Nonnull
    public Map<String, ?> operatives() {
        return Map.of();
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        terminationLatch.await();
    }

    @Override
    public void removeType(@Nonnull String factoryId) {
        int live = LIVE_INSTANCES.getOrDefault(factoryId, 0);
        if (live > 0) {
            throw new OperativeTypeInUseException("factory '" + factoryId + "' has " + live
                    + " live operative instance(s); stop them before removing the type");
        }
        com.flezha.sdk.runtime.operative.UnboundedOperativeClassRegistry.unregisterClass(factoryId);
    }

    @Override
    public void syncFactories(@Nonnull List<String> factoryIds) {
        synchronized (SYNCED_SETS) {
            SYNCED_SETS.add(List.copyOf(factoryIds));
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        onTerminated.run();
        terminationLatch.countDown();
    }
}

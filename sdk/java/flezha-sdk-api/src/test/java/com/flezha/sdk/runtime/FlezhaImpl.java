package com.flezha.sdk.runtime;

import com.flezha.sdk.FlezhaRunner;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

/**
 * Test double for the runtime entry point, resolved reflectively by
 * {@code RunnerBuilder.start()} (the real one lives in
 * {@code flezha-sdk-runtime} and needs native transport + a controller).
 * Mirrors the real lifecycle contract exactly: {@code close()} is idempotent,
 * runs the builder's {@code onTerminated} release hook, then wakes every
 * thread blocked in {@code awaitTermination()}.
 */
public final class FlezhaImpl implements FlezhaRunner {

    /** Successful start() count — guard tests assert a rejected start never reaches the runtime. */
    public static final AtomicInteger STARTS = new AtomicInteger();

    private final String namespace;
    private final Runnable onTerminated;
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private FlezhaImpl(String namespace, Runnable onTerminated) {
        this.namespace = namespace;
        this.onTerminated = onTerminated;
    }

    @Nonnull
    public static FlezhaRunner start(@Nonnull String namespace, @Nonnull Runnable onTerminated) {
        STARTS.incrementAndGet();
        return new FlezhaImpl(namespace, onTerminated);
    }

    @Override
    @Nonnull
    public String getId() {
        return namespace;
    }

    @Override
    @Nonnull
    public Map<String, ?> getOperatives() {
        return Map.of();
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
        onTerminated.run();
        terminationLatch.countDown();
    }
}

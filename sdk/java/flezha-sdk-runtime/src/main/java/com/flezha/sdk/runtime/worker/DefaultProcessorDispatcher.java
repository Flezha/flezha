package com.flezha.sdk.runtime.worker;

import com.flezha.sdk.ProcessorInput;
import com.flezha.sdk.ProcessorRunner;
import com.flezha.sdk.runtime.ComputeEngineContext;
import com.flezha.sdk.runtime.processor.ProcessorExecutor;
import com.flezha.sdk.runtime.processor.ProcessorInstance;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Routes work to either the shared {@link ThreadPoolRunner} or a new
 * {@link ThreadSpawnRunner} invocation depending on the processor's
 * {@link ProcessorRunner}.
 */
public final class DefaultProcessorDispatcher implements ProcessorDispatcher {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultProcessorDispatcher.class);

    private final ComputeEngineContext ctx;
    private final ProcessorExecutor executor;
    private final ThreadPoolRunner threadPoolRunner;
    private final ThreadSpawnRunner threadSpawnRunner;

    public DefaultProcessorDispatcher(@Nonnull ComputeEngineContext ctx, int workerPoolSize) {
        this.ctx = ctx;
        this.executor = new ProcessorExecutor(ctx);
        this.threadPoolRunner = new ThreadPoolRunner(executor, workerPoolSize);
        this.threadSpawnRunner = new ThreadSpawnRunner(executor);
    }

    @Override
    public void enqueue(@Nonnull String processorId, @Nonnull ProcessorInput input, long primaryStreamId) {
        ProcessorInstance<?, ?, ?> instance = ctx.processorRegistry().get(processorId);
        if (instance == null) {
            LOG.warn("enqueue: processor '{}' not found", processorId);
            return;
        }
        switch (instance.runner()) {
            case THREAD_POOL -> threadPoolRunner.enqueue(processorId, input, primaryStreamId);
            case THREAD_SPAWN -> threadSpawnRunner.run(processorId, input, primaryStreamId);
        }
    }

    @Override
    public void shutdown() {
        threadPoolRunner.shutdown();
        threadSpawnRunner.shutdown();
    }

    @Override
    public void shutdownAndWait(long millis) throws InterruptedException {
        shutdown();
        threadPoolRunner.awaitTermination(millis, TimeUnit.MILLISECONDS);
    }

    // Keep visible for tests
    ThreadPoolRunner threadPoolRunner() {
        return threadPoolRunner;
    }

    // ------------------------------------------------------------------
    // Thread pool runner — shared bounded executor + per-processor
    // sticky ordering via a keyed task-queue layer.
    // ------------------------------------------------------------------

    public static final class ThreadPoolRunner {
        private final ProcessorExecutor executor;
        private final ExecutorService pool;
        private final Object lock = new Object();
        private final Map<String, Deque<WorkItem>> perProcessorQueue = new HashMap<>();
        private final Map<String, Boolean> inFlight = new HashMap<>();

        ThreadPoolRunner(ProcessorExecutor executor, int size) {
            this.executor = executor;
            this.pool = Executors.newFixedThreadPool(Math.max(1, size), new WorkerThreadFactory("flz-worker"));
        }

        void enqueue(String processorId, ProcessorInput input, long primaryStreamId) {
            WorkItem item = new WorkItem(input, primaryStreamId);
            synchronized (lock) {
                if (Boolean.TRUE.equals(inFlight.get(processorId))) {
                    perProcessorQueue.computeIfAbsent(processorId, k -> new ArrayDeque<>()).addLast(item);
                    return;
                }
                inFlight.put(processorId, Boolean.TRUE);
            }
            pool.execute(() -> drain(processorId, item));
        }

        private void drain(String processorId, WorkItem first) {
            WorkItem current = first;
            while (current != null) {
                try {
                    executor.execute(processorId, current.input(), current.primaryStreamId());
                } catch (Throwable t) {
                    LOG.error("ThreadPoolRunner: processor '{}' threw: {}", processorId, t.toString());
                }
                synchronized (lock) {
                    Deque<WorkItem> queue = perProcessorQueue.get(processorId);
                    if (queue == null || queue.isEmpty()) {
                        perProcessorQueue.remove(processorId);
                        inFlight.put(processorId, Boolean.FALSE);
                        current = null;
                    } else {
                        current = queue.pollFirst();
                    }
                }
            }
        }

        void shutdown() {
            pool.shutdown();
        }

        void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            pool.awaitTermination(timeout, unit);
        }

        // Test visibility
        int queueDepth(String processorId) {
            synchronized (lock) {
                Deque<WorkItem> q = perProcessorQueue.get(processorId);
                return q == null ? 0 : q.size();
            }
        }
    }

    // ------------------------------------------------------------------
    // Thread spawn runner — one virtual thread per invocation.
    // ------------------------------------------------------------------

    public static final class ThreadSpawnRunner {
        private final ProcessorExecutor executor;
        private final ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor();

        ThreadSpawnRunner(ProcessorExecutor executor) {
            this.executor = executor;
        }

        void run(String processorId, ProcessorInput input, long primaryStreamId) {
            pool.execute(() -> {
                try {
                    executor.execute(processorId, input, primaryStreamId);
                } catch (Throwable t) {
                    LOG.error("ThreadSpawnRunner: processor '{}' threw: {}", processorId, t.toString());
                }
            });
        }

        void shutdown() {
            pool.shutdown();
        }
    }

    private record WorkItem(ProcessorInput input, long primaryStreamId) {
    }

    private static final class WorkerThreadFactory implements java.util.concurrent.ThreadFactory {
        private final String prefix;
        private final java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger();

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
}

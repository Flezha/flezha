package com.flezha.sdk.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flezha.sdk.runtime.config.FlezhaEnv;
import com.flezha.sdk.runtime.relay.RelayInstanceRegistry;
import com.flezha.sdk.runtime.stream.PrimaryStreamReader;
import com.flezha.sdk.runtime.stream.StreamRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;

/**
 * Shared runtime state. Threads own references to the context and read/write
 * sub-components under their own synchronization (each sub-component is
 * independently thread-safe).
 */
public final class ComputeEngineContext {

    private final String namespace;
    private final FlezhaEnv.ResolvedConfig config;
    private final long nativeHandle;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RelayInstanceRegistry relayInstanceRegistry = new RelayInstanceRegistry();
    private final StreamRegistry streamRegistry = new StreamRegistry();
    private final Map<Long, PrimaryStreamReader> primaryReaders = new ConcurrentHashMap<>();
    private final AtomicReference<String> computeEngineId = new AtomicReference<>("undefined");
    private final AtomicReference<ComputeEngineStatus> status =
            new AtomicReference<>(ComputeEngineStatus.STARTING);
    private ExecutorService workerPool;

    public ComputeEngineContext(
            @Nonnull String namespace,
            @Nonnull FlezhaEnv.ResolvedConfig config,
            long nativeHandle) {
        this.namespace = namespace;
        this.config = config;
        this.nativeHandle = nativeHandle;
    }

    @Nonnull
    public String namespace() {
        return namespace;
    }

    @Nonnull
    public FlezhaEnv.ResolvedConfig config() {
        return config;
    }

    public long nativeHandle() {
        return nativeHandle;
    }

    @Nonnull
    public ObjectMapper objectMapper() {
        return objectMapper;
    }

    @Nonnull
    public RelayInstanceRegistry relayInstanceRegistry() {
        return relayInstanceRegistry;
    }

    public void setWorkerPool(@Nonnull ExecutorService pool) {
        this.workerPool = pool;
    }

    public ExecutorService workerPool() {
        return workerPool;
    }

    @Nonnull
    public StreamRegistry streamRegistry() {
        return streamRegistry;
    }

    /** Per-primary-stream frame parsers + dispatch. Populated by ConnectSocketHandler. */
    @Nonnull
    public Map<Long, PrimaryStreamReader> primaryReaders() {
        return primaryReaders;
    }

    @Nonnull
    public String computeEngineId() {
        return computeEngineId.get();
    }

    public void setComputeEngineId(@Nonnull String id) {
        computeEngineId.set(id);
    }

    @Nonnull
    public ComputeEngineStatus status() {
        return status.get();
    }

    public void setStatus(@Nonnull ComputeEngineStatus newStatus) {
        status.set(newStatus);
    }
}

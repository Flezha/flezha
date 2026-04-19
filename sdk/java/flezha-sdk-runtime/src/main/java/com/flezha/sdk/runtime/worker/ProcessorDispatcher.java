package com.flezha.sdk.runtime.worker;

import com.flezha.sdk.ProcessorInput;
import javax.annotation.Nonnull;

/**
 * Schedules processor invocations according to the processor's configured
 * {@link com.flezha.sdk.ProcessorRunner}. Implementations own one or more
 * runner pools and drain them on {@link #shutdown()}.
 */
public interface ProcessorDispatcher {

    void enqueue(@Nonnull String processorId, @Nonnull ProcessorInput input, long primaryStreamId);

    /** Stop accepting new work and drain in-flight work. */
    void shutdown();

    /** Blocking variant of {@link #shutdown()} that waits up to the given millis. */
    void shutdownAndWait(long millis) throws InterruptedException;
}

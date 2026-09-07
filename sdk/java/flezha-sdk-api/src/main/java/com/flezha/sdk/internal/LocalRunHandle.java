package com.flezha.sdk.internal;

import com.flezha.sdk.SinkOutput;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * SPI the runtime's {@code LocalImpl} implements and the public
 * {@code LocalOperative} holds — the local-mode twin of {@link RunnerHandle},
 * resolved through {@link RuntimeBridge} so the api module still compiles
 * standalone.
 *
 * <p><b>Internal.</b> Not part of the SDK's public API; user code never calls
 * this.
 */
public interface LocalRunHandle {

    /** Deliver every value released on {@code channel} to {@code callback}, in release order. */
    void onSink(String channel, Consumer<SinkOutput> callback);

    /** Feed one value's wire bytes to a declared entry channel. */
    void feed(String channel, byte[] payload);

    /** Block until the graph is idle, or fail when {@code budget} elapses. */
    void runUntilIdle(Duration budget);

    /** End the run and release its Entities; idempotent. */
    void close();
}

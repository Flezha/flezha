package com.flezha.test;

import com.flezha.sdk.operation.Operation;
import com.flezha.sdk.operation.OperationCtx;
import com.flezha.sdk.operation.OperationException;
import com.flezha.sdk.state.NoChange;
import com.flezha.sdk.state.RestoreInitPayload;
import com.flezha.sdk.state.StateHandle;
import com.flezha.sdk.state.WallClock;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mirror of the Rust test app's {@code PoisonProbeOperation}
 * (platform-dead-letter-queue e2e): fails on the input {@code "poison"}
 * until an {@code "antidote"} input flips a process-global flag, so a
 * parked entry's replay succeeds after the fix.
 */
public final class PoisonProbeOperation implements Operation<String, Object, String, NoChange, WallClock, Void> {

    static final AtomicBoolean ANTIDOTE = new AtomicBoolean(false);

    @Override
    public String name() {
        return "java_poison_probe_operation";
    }

    @Override
    public Class<String> inputType() {
        return String.class;
    }

    @Override
    public Class<String> outputType() {
        return String.class;
    }

    @Override
    public Class<NoChange> changeType() {
        return NoChange.class;
    }

    @Override
    public Class<WallClock> clockType() {
        return WallClock.class;
    }

    @Override
    public Class<Void> configType() {
        return Void.class;
    }

    @Override
    public Object initState(Optional<String> operationKey, Optional<RestoreInitPayload> initPayload) {
        return new Object();
    }

    @Override
    public void run(String input, StateHandle<Object, NoChange> state, OperationCtx ctx) throws OperationException {
        if ("antidote".equals(input)) {
            ANTIDOTE.set(true);
        } else if ("poison".equals(input) && !ANTIDOTE.get()) {
            throw new OperationException("poison payload");
        }
        ctx.emit("ok:" + input);
    }
}

package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.BoundedOperativeInstance;
import com.flezha.sdk.runtime.operative.Operative;
import java.util.List;

/**
 * Handler for {@code BoundedStop} (Controller → SDK). Cooperative external stop
 * for a bounded operative ({@code flz operative stop}): trips the running
 * driver's cancellation token so it unwinds in-flight work to the single
 * teardown and emits {@code BoundedCompleted(status="cancelled")}. A no-op if
 * the instance is not running / not bounded (already finished / never started),
 * distinct from the hard {@code RemOperative} removal.
 */
public final class BoundedStopHandler implements CommandHandler {

    @Override
    public byte[] handle(RunnerContext ctx, CommandRequest request, List<String> errors) {
        String operativeId = request.getBoundedStop().getOperativeId();
        Operative operative = ctx.operativeInstanceRegistry().get(operativeId);
        if (operative instanceof BoundedOperativeInstance bounded) {
            bounded.cancel();
        }
        return new byte[0];
    }
}

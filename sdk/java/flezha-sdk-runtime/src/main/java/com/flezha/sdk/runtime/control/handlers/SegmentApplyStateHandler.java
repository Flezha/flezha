package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.proto.SegmentApplyState;
import com.flezha.sdk.runtime.RunnerContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.UnboundedOperativeInstance;
import com.flezha.sdk.runtime.operative.UnboundedOperativeStateSerializer;
import java.util.List;

/**
 * Applies an opaque state blob captured by a previous snapshot to an
 * already-registered {@link UnboundedOperativeInstance} before any input is delivered.
 * Used by the controller's cluster-snapshot restore path.
 */
public final class SegmentApplyStateHandler implements CommandHandler {

    @Override
    public byte[] handle(RunnerContext ctx, CommandRequest request, List<String> errors) {
        SegmentApplyState cmd = request.getSegmentApplyState();
        String operativeId = cmd.getOperativeId();
        byte[] stateBytes = cmd.getStateBinary().toByteArray();

        UnboundedOperativeInstance instance = ctx.operativeInstanceRegistry().getUnboundedOperative(operativeId);
        if (instance == null) {
            errors.add("UnboundedOperative with id " + operativeId + " not found");
            return new byte[0];
        }

        if (stateBytes.length == 0) {
            return new byte[0];
        }

        try {
            UnboundedOperativeStateSerializer.deserialize(instance, stateBytes);
        } catch (Exception e) {
            errors.add("apply_state_not_supported");
        }

        return new byte[0];
    }
}

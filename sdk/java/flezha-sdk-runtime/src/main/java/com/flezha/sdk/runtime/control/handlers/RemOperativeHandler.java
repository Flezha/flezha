package com.flezha.sdk.runtime.control.handlers;

import com.flezha.sdk.proto.CommandRequest;
import com.flezha.sdk.runtime.ExecutorContext;
import com.flezha.sdk.runtime.control.CommandHandler;
import com.flezha.sdk.runtime.operative.Operative;
import java.util.List;

/**
 * Handler for {@code RemOperative}. Removes the {@link Operative} (operative or bounded)
 * from the registry; for a operative, pending mailbox items on its segments
 * are dropped without being drained (matches Rust SDK v1 — bounded
 * mailboxes / drain-on-remove is tracked in a future change). This is also
 * the teardown the controller drives on a {@code BoundedCompleted}.
 */
public final class RemOperativeHandler implements CommandHandler {

    @Override
    public byte[] handle(ExecutorContext ctx, CommandRequest request, List<String> errors) {
        String operativeId = request.getRemOperative().getOperativeId();
        Operative removed = ctx.operativeInstanceRegistry().remove(operativeId);
        if (removed == null) {
            errors.add("ContinuousOperative not found");
        }
        return new byte[0];
    }
}
